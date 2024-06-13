/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/hyperledger/fabric/bccsp/factory"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
)

// CreateChaincodeProposal 根据给定的输入创建一个提案。
// 输入参数：
//   - typ：提案的头部类型。
//   - channelID：通道ID。
//   - cis：链码调用规范。
//   - creator：序列化的身份。
//
// 返回值：
//   - *peer.Proposal：创建的提案。
//   - string：与提案关联的交易ID。
//   - error：如果创建提案过程中出现错误，则返回错误；否则返回nil。
func CreateChaincodeProposal(typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, creator []byte) (*peer.Proposal, string, error) {
	// 根据给定的输入创建一个提案，并包含交易ID、随机数和临时数据。
	return CreateChaincodeProposalWithTransient(typ, channelID, cis, creator, nil)
}

// CreateChaincodeProposalWithTransient 根据给定的输入创建一个提案，并包含交易ID、随机数和临时数据。
// 输入参数：
//   - typ：提案的头部类型。
//   - channelID：通道ID。
//   - cis：链码调用规范。
//   - creator：序列化的身份。
//   - transientMap：临时数据映射。
//
// 返回值：
//   - *peer.Proposal：创建的提案。
//   - string：与提案关联的交易ID。
//   - error：如果创建提案过程中出现错误，则返回错误；否则返回nil。
func CreateChaincodeProposalWithTransient(typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, creator []byte, transientMap map[string][]byte) (*peer.Proposal, string, error) {
	// 生成一个随机的随机数
	nonce, err := getRandomNonce()
	if err != nil {
		return nil, "", err
	}

	// 将交易ID计算为连接nonce和creator后的哈希值
	txid := ComputeTxID(nonce, creator)

	// 调用 CreateChaincodeProposalWithTxIDNonceAndTransient 函数创建提案
	return CreateChaincodeProposalWithTxIDNonceAndTransient(txid, typ, channelID, cis, nonce, creator, transientMap)
}

// CreateChaincodeProposalWithTxIDAndTransient 方法根据给定的输入创建一个提案。
// 它返回创建的提案和与提案相关联的交易ID。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - typ：common.HeaderType 类型，表示头部类型。
//   - channelID：string 类型，表示通道ID。
//   - cis：*peer.ChaincodeInvocationSpec 类型，表示链码调用规范。
//   - creator：[]byte 类型，表示创建者。
//   - txid：string 类型，表示交易ID。
//   - transientMap：map[string][]byte 类型，表示临时数据映射。
//
// 返回值：
//   - *peer.Proposal：表示创建的提案。
//   - string：表示与提案相关联的交易ID。
//   - error：如果创建提案时出错，则返回错误。
func CreateChaincodeProposalWithTxIDAndTransient(typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, creator []byte, txid string, transientMap map[string][]byte) (*peer.Proposal, string, error) {
	// 生成一个随机的nonce
	nonce, err := getRandomNonce()
	if err != nil {
		return nil, "", err
	}

	// 如果未提供交易ID，则计算交易ID
	if txid == "" {
		txid = ComputeTxID(nonce, creator)
	}

	// 调用CreateChaincodeProposalWithTxIDNonceAndTransient函数创建提案
	return CreateChaincodeProposalWithTxIDNonceAndTransient(txid, typ, channelID, cis, nonce, creator, transientMap)
}

// CreateChaincodeProposalWithTxIDNonceAndTransient 创建链码提案
func CreateChaincodeProposalWithTxIDNonceAndTransient(txid string, typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, nonce, creator []byte, transientMap map[string][]byte) (*peer.Proposal, string, error) {
	// 链码头部
	ccHdrExt := &peer.ChaincodeHeaderExtension{ChaincodeId: cis.ChaincodeSpec.ChaincodeId}
	ccHdrExtBytes, err := proto.Marshal(ccHdrExt)
	if err != nil {
		return nil, "", errors.Wrap(err, "序列化链码头部 ChaincodeHeaderExtension (链码id)异常")
	}

	// 链码规则
	cisBytes, err := proto.Marshal(cis)
	if err != nil {
		return nil, "", errors.Wrap(err, "序列化链码规则 ChaincodeInvocationSpec (链码id+链码包)异常")
	}

	// 链码提案有效负载(链码规则、加密材料)
	ccPropPayload := &peer.ChaincodeProposalPayload{Input: cisBytes, TransientMap: transientMap}
	ccPropPayloadBytes, err := proto.Marshal(ccPropPayload)
	if err != nil {
		return nil, "", errors.Wrap(err, "序列化链码提案有效负载 ChaincodeProposalPayload 异常")
	}

	// TODO: 现在将epoch设置为零。一旦我们得到一个更合适的机制来处理它，这必须改变。
	var epoch uint64

	// 获取当前时间并将其转换为Timestamp类型
	timestamp, err := ptypes.TimestampProto(time.Now().UTC())
	if err != nil {
		return nil, "", errors.Wrap(err, "验证时间戳时出错")
	}

	hdr := &common.Header{
		ChannelHeader: MarshalOrPanic(
			&common.ChannelHeader{
				Type:      int32(typ),    // 链码类型(java/go)
				TxId:      txid,          // 交易id
				Timestamp: timestamp,     // 时间戳
				ChannelId: channelID,     // 通道名称
				Extension: ccHdrExtBytes, // 链码头部(链码名称)
				Epoch:     epoch,         // 在区块链网络中确保提案响应的时代一致性和唯一性, 该消息只能被看到一次（即没有被重放）
			},
		),
		SignatureHeader: MarshalOrPanic(
			&common.SignatureHeader{
				Nonce:   nonce,   // 随机数
				Creator: creator, // 签名者
			},
		),
	}

	// 将头部序列化
	hdrBytes, err := proto.Marshal(hdr)
	if err != nil {
		return nil, "", err
	}

	// 将提案发送给背书人背书
	prop := &peer.Proposal{
		Header:  hdrBytes,           // 头部
		Payload: ccPropPayloadBytes, // 有效数据
	}
	return prop, txid, nil
}

// GetBytesProposalResponsePayload gets proposal response payload
func GetBytesProposalResponsePayload(hash []byte, response *peer.Response, result []byte, event []byte, ccid *peer.ChaincodeID) ([]byte, error) {
	cAct := &peer.ChaincodeAction{
		Events: event, Results: result,
		Response:    response,
		ChaincodeId: ccid,
	}
	cActBytes, err := proto.Marshal(cAct)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling ChaincodeAction")
	}

	prp := &peer.ProposalResponsePayload{
		Extension:    cActBytes,
		ProposalHash: hash,
	}
	prpBytes, err := proto.Marshal(prp)
	return prpBytes, errors.Wrap(err, "error marshaling ProposalResponsePayload")
}

// GetBytesChaincodeProposalPayload 方法用于获取链码提案负载的字节表示。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - cpp：*peer.ChaincodeProposalPayload 类型，表示链码提案负载。
//
// 返回值：
//   - []byte：表示链码提案负载的字节表示。
//   - error：如果序列化 ChaincodeProposalPayload 时出错，则返回错误。
func GetBytesChaincodeProposalPayload(cpp *peer.ChaincodeProposalPayload) ([]byte, error) {
	// 使用 proto.Marshal 函数将 ChaincodeProposalPayload 序列化为字节数组
	cppBytes, err := proto.Marshal(cpp)
	// 使用 errors.Wrap 函数将错误信息包装为更详细的错误信息
	return cppBytes, errors.Wrap(err, "序列化 ChaincodeProposalPayload 链码提案负载时出错")
}

// GetBytesResponse gets the bytes of Response
func GetBytesResponse(res *peer.Response) ([]byte, error) {
	resBytes, err := proto.Marshal(res)
	return resBytes, errors.Wrap(err, "error marshaling Response")
}

// GetBytesChaincodeEvent gets the bytes of ChaincodeEvent
func GetBytesChaincodeEvent(event *peer.ChaincodeEvent) ([]byte, error) {
	eventBytes, err := proto.Marshal(event)
	return eventBytes, errors.Wrap(err, "error marshaling ChaincodeEvent")
}

// GetBytesChaincodeActionPayload 从消息中获取 ChaincodeActionPayload 链码操作有效负载的字节
func GetBytesChaincodeActionPayload(cap *peer.ChaincodeActionPayload) ([]byte, error) {
	capBytes, err := proto.Marshal(cap)
	return capBytes, errors.Wrap(err, "序列化 ChaincodeActionPayload 链码操作有效负载时出错")
}

// GetBytesProposalResponse gets proposal bytes response
func GetBytesProposalResponse(pr *peer.ProposalResponse) ([]byte, error) {
	respBytes, err := proto.Marshal(pr)
	return respBytes, errors.Wrap(err, "error marshaling ProposalResponse")
}

// GetBytesHeader get the bytes of Header from the message
func GetBytesHeader(hdr *common.Header) ([]byte, error) {
	bytes, err := proto.Marshal(hdr)
	return bytes, errors.Wrap(err, "error marshaling Header")
}

// GetBytesSignatureHeader get the bytes of SignatureHeader from the message
func GetBytesSignatureHeader(hdr *common.SignatureHeader) ([]byte, error) {
	bytes, err := proto.Marshal(hdr)
	return bytes, errors.Wrap(err, "error marshaling SignatureHeader")
}

// GetBytesTransaction 从消息中获取事务的字节
func GetBytesTransaction(tx *peer.Transaction) ([]byte, error) {
	bytes, err := proto.Marshal(tx)
	return bytes, errors.Wrap(err, "序列化 peer.Transaction 事务时出错")
}

// GetBytesPayload 从消息中获取有效负载的字节
func GetBytesPayload(payl *common.Payload) ([]byte, error) {
	bytes, err := proto.Marshal(payl)
	return bytes, errors.Wrap(err, "序列化 common.Payload 有效负载出错")
}

// GetBytesEnvelope get the bytes of Envelope from the message
func GetBytesEnvelope(env *common.Envelope) ([]byte, error) {
	bytes, err := proto.Marshal(env)
	return bytes, errors.Wrap(err, "error marshaling Envelope")
}

// GetActionFromEnvelope extracts a ChaincodeAction message from a
// serialized Envelope
// TODO: fix function name as per FAB-11831
func GetActionFromEnvelope(envBytes []byte) (*peer.ChaincodeAction, error) {
	env, err := GetEnvelopeFromBlock(envBytes)
	if err != nil {
		return nil, err
	}
	return GetActionFromEnvelopeMsg(env)
}

func GetActionFromEnvelopeMsg(env *common.Envelope) (*peer.ChaincodeAction, error) {
	payl, err := UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	tx, err := UnmarshalTransaction(payl.Data)
	if err != nil {
		return nil, err
	}

	if len(tx.Actions) == 0 {
		return nil, errors.New("at least one TransactionAction required")
	}

	_, respPayload, err := GetPayloads(tx.Actions[0])
	return respPayload, err
}

// CreateProposalFromCISAndTxid returns a proposal given a serialized identity
// and a ChaincodeInvocationSpec
func CreateProposalFromCISAndTxid(txid string, typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, creator []byte) (*peer.Proposal, string, error) {
	nonce, err := getRandomNonce()
	if err != nil {
		return nil, "", err
	}
	return CreateChaincodeProposalWithTxIDNonceAndTransient(txid, typ, channelID, cis, nonce, creator, nil)
}

// CreateProposalFromCIS 根据序列化的身份和 ChaincodeInvocationSpec 创建一个提案。
// 输入参数：
//   - typ：提案的头部类型。
//   - channelID：通道ID。
//   - cis：链码调用规范。
//   - creator：序列化的身份。
//
// 返回值：
//   - *peer.Proposal：创建的提案。
//   - string：提案的哈希值。
//   - error：如果创建提案过程中出现错误，则返回错误；否则返回nil。
func CreateProposalFromCIS(typ common.HeaderType, channelID string, cis *peer.ChaincodeInvocationSpec, creator []byte) (*peer.Proposal, string, error) {
	// 根据给定的输入创建一个提案。
	return CreateChaincodeProposal(typ, channelID, cis, creator)
}

// CreateGetChaincodesProposal returns a GETCHAINCODES proposal given a
// serialized identity
func CreateGetChaincodesProposal(channelID string, creator []byte) (*peer.Proposal, string, error) {
	ccinp := &peer.ChaincodeInput{Args: [][]byte{[]byte("getchaincodes")}}
	lsccSpec := &peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{
			Type:        peer.ChaincodeSpec_GOLANG,
			ChaincodeId: &peer.ChaincodeID{Name: "lscc"},
			Input:       ccinp,
		},
	}
	return CreateProposalFromCIS(common.HeaderType_ENDORSER_TRANSACTION, channelID, lsccSpec, creator)
}

// CreateGetInstalledChaincodesProposal returns a GETINSTALLEDCHAINCODES
// proposal given a serialized identity
func CreateGetInstalledChaincodesProposal(creator []byte) (*peer.Proposal, string, error) {
	ccinp := &peer.ChaincodeInput{Args: [][]byte{[]byte("getinstalledchaincodes")}}
	lsccSpec := &peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{
			Type:        peer.ChaincodeSpec_GOLANG,
			ChaincodeId: &peer.ChaincodeID{Name: "lscc"},
			Input:       ccinp,
		},
	}
	return CreateProposalFromCIS(common.HeaderType_ENDORSER_TRANSACTION, "", lsccSpec, creator)
}

// CreateInstallProposalFromCDS returns a install proposal given a serialized
// identity and a ChaincodeDeploymentSpec
func CreateInstallProposalFromCDS(ccpack proto.Message, creator []byte) (*peer.Proposal, string, error) {
	return createProposalFromCDS("", ccpack, creator, "install")
}

// CreateDeployProposalFromCDS returns a deploy proposal given a serialized
// identity and a ChaincodeDeploymentSpec
func CreateDeployProposalFromCDS(
	channelID string,
	cds *peer.ChaincodeDeploymentSpec,
	creator []byte,
	policy []byte,
	escc []byte,
	vscc []byte,
	collectionConfig []byte) (*peer.Proposal, string, error) {
	if collectionConfig == nil {
		return createProposalFromCDS(channelID, cds, creator, "deploy", policy, escc, vscc)
	}
	return createProposalFromCDS(channelID, cds, creator, "deploy", policy, escc, vscc, collectionConfig)
}

// CreateUpgradeProposalFromCDS returns a upgrade proposal given a serialized
// identity and a ChaincodeDeploymentSpec
func CreateUpgradeProposalFromCDS(
	channelID string,
	cds *peer.ChaincodeDeploymentSpec,
	creator []byte,
	policy []byte,
	escc []byte,
	vscc []byte,
	collectionConfig []byte) (*peer.Proposal, string, error) {
	if collectionConfig == nil {
		return createProposalFromCDS(channelID, cds, creator, "upgrade", policy, escc, vscc)
	}
	return createProposalFromCDS(channelID, cds, creator, "upgrade", policy, escc, vscc, collectionConfig)
}

// createProposalFromCDS returns a deploy or upgrade proposal given a
// serialized identity and a ChaincodeDeploymentSpec
func createProposalFromCDS(channelID string, msg proto.Message, creator []byte, propType string, args ...[]byte) (*peer.Proposal, string, error) {
	// in the new mode, cds will be nil, "deploy" and "upgrade" are instantiates.
	var ccinp *peer.ChaincodeInput
	var b []byte
	var err error
	if msg != nil {
		b, err = proto.Marshal(msg)
		if err != nil {
			return nil, "", err
		}
	}
	switch propType {
	case "deploy":
		fallthrough
	case "upgrade":
		cds, ok := msg.(*peer.ChaincodeDeploymentSpec)
		if !ok || cds == nil {
			return nil, "", errors.New("invalid message for creating lifecycle chaincode proposal")
		}
		Args := [][]byte{[]byte(propType), []byte(channelID), b}
		Args = append(Args, args...)

		ccinp = &peer.ChaincodeInput{Args: Args}
	case "install":
		ccinp = &peer.ChaincodeInput{Args: [][]byte{[]byte(propType), b}}
	}

	// wrap the deployment in an invocation spec to lscc...
	lsccSpec := &peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{
			Type:        peer.ChaincodeSpec_GOLANG,
			ChaincodeId: &peer.ChaincodeID{Name: "lscc"},
			Input:       ccinp,
		},
	}

	// ...and get the proposal for it
	return CreateProposalFromCIS(common.HeaderType_ENDORSER_TRANSACTION, channelID, lsccSpec, creator)
}

// ComputeTxID 将交易ID计算为连接nonce和creator后的哈希值。
// 输入参数：
//   - nonce：随机数。
//   - creator：序列化的身份。
//
// 返回值：
//   - string：计算得到的交易ID。
func ComputeTxID(nonce, creator []byte) string {
	// todo luode 进行国密sm3的改造
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		hasher := sm3.New()
		// 将随机数和身份连接后进行哈希计算
		hasher.Write(nonce)
		hasher.Write(creator)
		// 返回计算得到的哈希值的十六进制表示
		return hex.EncodeToString(hasher.Sum(nil))
	default:
		hasher := sha256.New()
		// 将随机数和身份连接后进行哈希计算
		hasher.Write(nonce)
		hasher.Write(creator)
		// 返回计算得到的哈希值的十六进制表示
		return hex.EncodeToString(hasher.Sum(nil))
	}
}

// CheckTxID checks that txid is equal to the Hash computed
// over the concatenation of nonce and creator.
func CheckTxID(txid string, nonce, creator []byte) error {
	computedTxID := ComputeTxID(nonce, creator)

	if txid != computedTxID {
		return errors.Errorf("invalid txid. got [%s], expected [%s]", txid, computedTxID)
	}

	return nil
}

// InvokedChaincodeName 从SignedProposal的提案字节中解析出链码名称。
// 输入参数：
//   - proposalBytes：SignedProposal的提案字节。
//
// 返回值：
//   - string：解析出的链码名称。
//   - error：如果解析过程中出现错误，则返回错误；否则返回nil。
func InvokedChaincodeName(proposalBytes []byte) (string, error) {
	proposal := &peer.Proposal{}
	err := proto.Unmarshal(proposalBytes, proposal)
	if err != nil {
		return "", errors.WithMessage(err, "反序列化提案 Proposal 出错")
	}

	proposalPayload := &peer.ChaincodeProposalPayload{}
	err = proto.Unmarshal(proposal.Payload, proposalPayload)
	if err != nil {
		return "", errors.WithMessage(err, "反序列化链码负载 ChaincodeProposalPayload 出错")
	}

	cis := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(proposalPayload.Input, cis)
	if err != nil {
		return "", errors.WithMessage(err, "反序列化链码规则 ChaincodeSpec 出错")
	}

	if cis.ChaincodeSpec == nil {
		return "", errors.Errorf("链码规则 ChaincodeSpec 不能为nil")
	}

	if cis.ChaincodeSpec.ChaincodeId == nil {
		return "", errors.Errorf("链码id不能为nil")
	}

	return cis.ChaincodeSpec.ChaincodeId.Name, nil
}
