/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/sha256"
	b64 "encoding/base64"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/pkg/errors"
)

// GetPayloads gets the underlying payload objects in a TransactionAction
func GetPayloads(txActions *peer.TransactionAction) (*peer.ChaincodeActionPayload, *peer.ChaincodeAction, error) {
	// TODO: pass in the tx type (in what follows we're assuming the
	// type is ENDORSER_TRANSACTION)
	ccPayload, err := UnmarshalChaincodeActionPayload(txActions.Payload)
	if err != nil {
		return nil, nil, err
	}

	if ccPayload.Action == nil || ccPayload.Action.ProposalResponsePayload == nil {
		return nil, nil, errors.New("no payload in ChaincodeActionPayload")
	}
	pRespPayload, err := UnmarshalProposalResponsePayload(ccPayload.Action.ProposalResponsePayload)
	if err != nil {
		return nil, nil, err
	}

	if pRespPayload.Extension == nil {
		return nil, nil, errors.New("response payload is missing extension")
	}

	respPayload, err := UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		return ccPayload, nil, err
	}
	return ccPayload, respPayload, nil
}

// GetEnvelopeFromBlock 从块的Data字段中获取一个envelope 带有签名的有效负载,以便可以对消息进行身份验证。
// 输入参数：
//   - data：块的Data字段的字节。
//
// 返回值：
//   - *common.Envelope：从块中提取的envelope。
//   - error：如果提取过程中出现错误，则返回错误；否则返回nil。
func GetEnvelopeFromBlock(data []byte) (*common.Envelope, error) {
	// 块的Data字段始终以一个envelope开始
	var err error
	env := &common.Envelope{}
	if err = proto.Unmarshal(data, env); err != nil {
		return nil, errors.Wrap(err, "反序列化带有签名的有效负载出错")
	}

	return env, nil
}

// CreateSignedEnvelope 用于创建一个带有TLS绑定的已签名信封。
func CreateSignedEnvelope(
	txType common.HeaderType,
	channelID string,
	signer Signer,
	dataMsg proto.Message,
	msgVersion int32,
	epoch uint64,
) (*common.Envelope, error) {
	// 用于创建一个带有TLS绑定的已签名信封。
	return CreateSignedEnvelopeWithTLSBinding(txType, channelID, signer, dataMsg, msgVersion, epoch, nil)
}

// CreateSignedEnvelopeWithTLSBinding 函数用于创建一个带有TLS绑定的已签名信封。
// 输入参数：
//   - txType：信封的类型。
//   - channelID：通道的ID。
//   - signer：签名者。
//   - dataMsg：要封装的数据消息。
//   - msgVersion：消息的版本。
//   - epoch：通道的时期。
//   - tlsCertHash：TLS证书的哈希值。
//
// 返回值：
//   - *common.Envelope：创建的已签名信封。
//   - error：如果创建过程中出现错误，则返回相应的错误信息。
func CreateSignedEnvelopeWithTLSBinding(
	txType common.HeaderType,
	channelID string,
	signer Signer,
	dataMsg proto.Message,
	msgVersion int32,
	epoch uint64,
	tlsCertHash []byte,
) (*common.Envelope, error) {
	// 创建一个 ChannelHeader 通道头部。包含了给定的头部类型、版本号、时间戳、通道ID和时期信息。
	payloadChannelHeader := MakeChannelHeader(txType, msgVersion, channelID, epoch)
	payloadChannelHeader.TlsCertHash = tlsCertHash
	var err error
	payloadSignatureHeader := &common.SignatureHeader{}

	if signer != nil {
		// 函数用于返回一个带有有效随机数的 SignatureHeader。
		payloadSignatureHeader, err = NewSignatureHeader(signer)
		if err != nil {
			return nil, err
		}
	}

	data, err := proto.Marshal(dataMsg)
	if err != nil {
		return nil, errors.Wrap(err, "解析处理时出错")
	}

	paylBytes := MarshalOrPanic(
		&common.Payload{
			Header: MakePayloadHeader(payloadChannelHeader, payloadSignatureHeader),
			Data:   data,
		},
	)

	// 签名
	var sig []byte
	if signer != nil {
		sig, err = signer.Sign(paylBytes)
		if err != nil {
			return nil, err
		}
	}

	// 提交消息
	env := &common.Envelope{
		Payload:   paylBytes,
		Signature: sig,
	}

	return env, nil
}

// Signer is the interface needed to sign a transaction
type Signer interface {
	Sign(msg []byte) ([]byte, error)
	Serialize() ([]byte, error)
}

// CreateSignedTx 方法根据提案、背书和签名者组装一个Envelope消息。
// 当客户端收集到足够的背书来创建一个交易并将其提交给对等节点进行排序时，应调用此函数。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - proposal：*peer.Proposal 类型，表示提案。
//   - signer：Signer 接口类型，表示签名者。
//   - resps：...*peer.ProposalResponse 类型，表示背书响应。
//
// 返回值：
//   - *common.Envelope：表示创建的Envelope消息。
//   - error：如果创建Envelope消息时出错，则返回错误。
func CreateSignedTx(
	proposal *peer.Proposal,
	signer Signer,
	resps ...*peer.ProposalResponse,
) (*common.Envelope, error) {
	if len(resps) == 0 {
		return nil, errors.New("至少需要一个成功的提案响应")
	}

	if signer == nil {
		return nil, errors.New("签名者未nil,创建签名事务时需要签名者")
	}

	// 提案原始标头(通道头部[链码类型、交易id、时间戳、通道名称、链码名称]、签名者头部[随机数、签名者])
	hdr, err := UnmarshalHeader(proposal.Header)
	if err != nil {
		return nil, err
	}

	// 原始有效载荷(链码规则、加密材料)
	pPayl, err := UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return nil, err
	}

	// 检查签名者是否与标头中引用的签名者相同
	signerBytes, err := signer.Serialize()
	if err != nil {
		return nil, err
	}

	// 字节反序列化签名者头部[随机数、签名者]
	shdr, err := UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(signerBytes, shdr.Creator) {
		return nil, errors.New("签名者必须与提案头中设置的签名者相同")
	}

	// 确保所有操作都按位相等并且成功
	var a1 []byte
	for n, r := range resps {
		if r.Response.Status < 200 || r.Response.Status >= 400 {
			return nil, errors.Errorf("提案响应未成功, 错误code: %d, 消息: %s", r.Response.Status, r.Response.Message)
		}

		if n == 0 {
			// 响应的有效负载
			a1 = r.Payload
			continue
		}

		if !bytes.Equal(a1, r.Payload) {
			return nil, errors.Errorf("提案响应负载不匹配 (base64): '%s' vs '%s'",
				b64.StdEncoding.EncodeToString(r.Payload), b64.StdEncoding.EncodeToString(a1))
		}
	}

	// 根据其独特性填写背书
	endorsersUsed := make(map[string]struct{})
	// 背书是背书人在提案响应上的签名。
	// 通过产生背书消息，背书人隐含地 “批准” 该提案响应以及其中包含的动作。
	// 当已经收集到足够的背书时，可以从一组提议响应中生成交易。
	// 请注意，此消息仅包含标识和签名，但不包含签名的有效载荷。
	// 这是有意的，因为背书应该在交易中收集，并且它们都应该背书单个提案响应/动作 (在单个提案响应上的许多背书)
	var endorsements []*peer.Endorsement
	for _, r := range resps {
		// 提案的背书，基本上是背书人在有效载荷上的签名
		if r.Endorsement == nil {
			continue
		}

		// 背书人的身份 (例如其证书)
		key := string(r.Endorsement.Endorser)
		if _, used := endorsersUsed[key]; used {
			continue
		}

		// 收集响应背书
		endorsements = append(endorsements, r.Endorsement)
		endorsersUsed[key] = struct{}{}
	}

	if len(endorsements) == 0 {
		return nil, errors.Errorf("没有收到任务有效的背书响应")
	}

	// 创建 ChaincodeEndorsedAction 携带有关特定提案认可的信息
	cea := &peer.ChaincodeEndorsedAction{ProposalResponsePayload: resps[0].Payload, Endorsements: endorsements}

	// 获取将转到事务的提案有效负载的字节, pPayl = 原始有效载荷(链码规则、加密材料)
	// 接收一个 ChaincodeProposalPayload 链码提案负载数据，并根据其 visibility 字段将其序列化
	propPayloadBytes, err := GetBytesProposalPayloadForTx(pPayl)
	if err != nil {
		return nil, err
	}

	cap := &peer.ChaincodeActionPayload{ChaincodeProposalPayload: propPayloadBytes, Action: cea}
	// 序列化 chaincode 链码操作有效负载
	capBytes, err := GetBytesChaincodeActionPayload(cap)
	if err != nil {
		return nil, err
	}

	// 创建事务, 将提案绑定到其操作
	taa := &peer.TransactionAction{Header: hdr.SignatureHeader, Payload: capBytes}
	taas := make([]*peer.TransactionAction, 1)
	taas[0] = taa
	// 要发送到 orderer 服务的事务。
	// 事务包含一个或多个TransactionAction。每个TransactionAction都将一个建议绑定到可能的多个操作。
	// 事务是原子的，这意味着事务中的所有操作都将被提交，或者没有任何操作。
	// 请注意，虽然一个事务可能包含多个标头，但每个标头中的Header.creator 签名者字段必须相同。
	// 单个客户端可以自由发出许多独立的提案 ，每个提案都有其标头 (header) 和请求有效负载
	tx := &peer.Transaction{Actions: taas}

	// 序列化tx
	txBytes, err := GetBytesTransaction(tx)
	if err != nil {
		return nil, err
	}

	// 创建有效负载
	payl := &common.Payload{Header: hdr, Data: txBytes}
	paylBytes, err := GetBytesPayload(payl)
	if err != nil {
		return nil, err
	}

	// 签名有效载荷
	sig, err := signer.Sign(paylBytes)
	if err != nil {
		return nil, err
	}

	// 装带有签名的有效负载 ，以便可以对消息进行身份验证
	return &common.Envelope{Payload: paylBytes, Signature: sig}, nil
}

// CreateProposalResponse creates a proposal response.
func CreateProposalResponse(
	hdrbytes []byte,
	payl []byte,
	response *peer.Response,
	results []byte,
	events []byte,
	ccid *peer.ChaincodeID,
	signingEndorser Signer,
) (*peer.ProposalResponse, error) {
	hdr, err := UnmarshalHeader(hdrbytes)
	if err != nil {
		return nil, err
	}

	// obtain the proposal hash given proposal header, payload and the
	// requested visibility
	pHashBytes, err := GetProposalHash1(hdr, payl)
	if err != nil {
		return nil, errors.WithMessage(err, "error computing proposal hash")
	}

	// get the bytes of the proposal response payload - we need to sign them
	prpBytes, err := GetBytesProposalResponsePayload(pHashBytes, response, results, events, ccid)
	if err != nil {
		return nil, err
	}

	// serialize the signing identity
	endorser, err := signingEndorser.Serialize()
	if err != nil {
		return nil, errors.WithMessage(err, "error serializing signing identity")
	}

	// sign the concatenation of the proposal response and the serialized
	// endorser identity with this endorser's key
	signature, err := signingEndorser.Sign(append(prpBytes, endorser...))
	if err != nil {
		return nil, errors.WithMessage(err, "could not sign the proposal response payload")
	}

	resp := &peer.ProposalResponse{
		// Timestamp: TODO!
		Version: 1, // TODO: pick right version number
		Endorsement: &peer.Endorsement{
			Signature: signature,
			Endorser:  endorser,
		},
		Payload: prpBytes,
		Response: &peer.Response{
			Status:  200,
			Message: "OK",
		},
	}

	return resp, nil
}

// CreateProposalResponseFailure creates a proposal response for cases where
// endorsement proposal fails either due to a endorsement failure or a
// chaincode failure (chaincode response status >= shim.ERRORTHRESHOLD)
func CreateProposalResponseFailure(
	hdrbytes []byte,
	payl []byte,
	response *peer.Response,
	results []byte,
	events []byte,
	chaincodeName string,
) (*peer.ProposalResponse, error) {
	hdr, err := UnmarshalHeader(hdrbytes)
	if err != nil {
		return nil, err
	}

	// obtain the proposal hash given proposal header, payload and the requested visibility
	pHashBytes, err := GetProposalHash1(hdr, payl)
	if err != nil {
		return nil, errors.WithMessage(err, "error computing proposal hash")
	}

	// get the bytes of the proposal response payload
	prpBytes, err := GetBytesProposalResponsePayload(pHashBytes, response, results, events, &peer.ChaincodeID{Name: chaincodeName})
	if err != nil {
		return nil, err
	}

	resp := &peer.ProposalResponse{
		// Timestamp: TODO!
		Payload:  prpBytes,
		Response: response,
	}

	return resp, nil
}

// GetSignedProposal 根据提案消息和签名身份返回一个已签名的提案。
// 输入参数：
//   - prop：提案消息。
//   - signer：签名身份。
//
// 返回值：
//   - *peer.SignedProposal：已签名的提案。
//   - error：如果获取已签名提案过程中出现错误，则返回错误；否则返回nil。
func GetSignedProposal(prop *peer.Proposal, signer Signer) (*peer.SignedProposal, error) {
	// 检查参数是否为nil
	if prop == nil || signer == nil {
		return nil, errors.New("提案消息和签名者不能为nil")
	}

	// 序列化提案消息
	propBytes, err := proto.Marshal(prop)
	if err != nil {
		return nil, err
	}

	// 对提案消息进行签名
	signature, err := signer.Sign(propBytes)
	if err != nil {
		return nil, err
	}

	// 返回已签名的提案
	return &peer.SignedProposal{ProposalBytes: propBytes, Signature: signature}, nil
}

// MockSignedEndorserProposalOrPanic creates a SignedProposal with the
// passed arguments
func MockSignedEndorserProposalOrPanic(
	channelID string,
	cs *peer.ChaincodeSpec,
	creator,
	signature []byte,
) (*peer.SignedProposal, *peer.Proposal) {
	prop, _, err := CreateChaincodeProposal(
		common.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: cs},
		creator)
	if err != nil {
		panic(err)
	}

	propBytes, err := proto.Marshal(prop)
	if err != nil {
		panic(err)
	}

	return &peer.SignedProposal{ProposalBytes: propBytes, Signature: signature}, prop
}

func MockSignedEndorserProposal2OrPanic(
	channelID string,
	cs *peer.ChaincodeSpec,
	signer Signer,
) (*peer.SignedProposal, *peer.Proposal) {
	serializedSigner, err := signer.Serialize()
	if err != nil {
		panic(err)
	}

	prop, _, err := CreateChaincodeProposal(
		common.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{}},
		serializedSigner)
	if err != nil {
		panic(err)
	}

	sProp, err := GetSignedProposal(prop, signer)
	if err != nil {
		panic(err)
	}

	return sProp, prop
}

// GetBytesProposalPayloadForTx 方法接收一个 ChaincodeProposalPayload 链码提案负载数据，并根据其 visibility 字段返回其序列化版本。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - payload：*peer.ChaincodeProposalPayload 类型，表示链码提案负载。
//
// 返回值：
//   - []byte：表示序列化后的 ChaincodeProposalPayload。
//   - error：如果序列化 ChaincodeProposalPayload 时出错，则返回错误。
func GetBytesProposalPayloadForTx(
	payload *peer.ChaincodeProposalPayload,
) ([]byte, error) {
	// 检查参数是否为 nil
	if payload == nil {
		return nil, errors.New("链码提案负载为nil")
	}

	// 去除负载中的临时数据字节
	cppNoTransient := &peer.ChaincodeProposalPayload{Input: payload.Input, TransientMap: nil}
	// 序列化链码提案负载
	cppBytes, err := GetBytesChaincodeProposalPayload(cppNoTransient)
	if err != nil {
		return nil, err
	}

	return cppBytes, nil
}

// GetProposalHash2 gets the proposal hash - this version
// is called by the committer where the visibility policy
// has already been enforced and so we already get what
// we have to get in ccPropPayl
func GetProposalHash2(header *common.Header, ccPropPayl []byte) ([]byte, error) {
	// check for nil argument
	if header == nil ||
		header.ChannelHeader == nil ||
		header.SignatureHeader == nil ||
		ccPropPayl == nil {
		return nil, errors.New("nil arguments")
	}

	// todo luode 进行国密sm3的改造
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		hash := sm3.New()
		// hash the serialized Channel Header object
		hash.Write(header.ChannelHeader)
		// hash the serialized Signature Header object
		hash.Write(header.SignatureHeader)
		// hash the bytes of the chaincode proposal payload that we are given
		hash.Write(ccPropPayl)
		return hash.Sum(nil), nil
	default:
		hash := sha256.New()
		// hash the serialized Channel Header object
		hash.Write(header.ChannelHeader)
		// hash the serialized Signature Header object
		hash.Write(header.SignatureHeader)
		// hash the bytes of the chaincode proposal payload that we are given
		hash.Write(ccPropPayl)
		return hash.Sum(nil), nil
	}

}

// GetProposalHash1 gets the proposal hash bytes after sanitizing the
// chaincode proposal payload according to the rules of visibility
func GetProposalHash1(header *common.Header, ccPropPayl []byte) ([]byte, error) {
	// check for nil argument
	if header == nil ||
		header.ChannelHeader == nil ||
		header.SignatureHeader == nil ||
		ccPropPayl == nil {
		return nil, errors.New("nil arguments")
	}

	// unmarshal the chaincode proposal payload
	cpp, err := UnmarshalChaincodeProposalPayload(ccPropPayl)
	if err != nil {
		return nil, err
	}

	ppBytes, err := GetBytesProposalPayloadForTx(cpp)
	if err != nil {
		return nil, err
	}

	// todo luode 进行国密sm3的改造
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		hash := sm3.New()
		// hash the serialized Channel Header object
		hash.Write(header.ChannelHeader)
		// hash the serialized Signature Header object
		hash.Write(header.SignatureHeader)
		// hash the bytes of the chaincode proposal payload that we are given
		hash.Write(ppBytes)
		return hash.Sum(nil), nil
	default:
		hash := sha256.New()
		// hash the serialized Channel Header object
		hash.Write(header.ChannelHeader)
		// hash the serialized Signature Header object
		hash.Write(header.SignatureHeader)
		// hash the bytes of the chaincode proposal payload that we are given
		hash.Write(ppBytes)
		return hash.Sum(nil), nil
	}
}

// GetOrComputeTxIDFromEnvelope gets the txID present in a given transaction
// envelope. If the txID is empty, it constructs the txID from nonce and
// creator fields in the envelope.
func GetOrComputeTxIDFromEnvelope(txEnvelopBytes []byte) (string, error) {
	txEnvelope, err := UnmarshalEnvelope(txEnvelopBytes)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from envelope")
	}

	txPayload, err := UnmarshalPayload(txEnvelope.Payload)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from payload")
	}

	if txPayload.Header == nil {
		return "", errors.New("error getting txID from header: payload header is nil")
	}

	chdr, err := UnmarshalChannelHeader(txPayload.Header.ChannelHeader)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from channel header")
	}

	if chdr.TxId != "" {
		return chdr.TxId, nil
	}

	sighdr, err := UnmarshalSignatureHeader(txPayload.Header.SignatureHeader)
	if err != nil {
		return "", errors.WithMessage(err, "error getting nonce and creator for computing txID")
	}

	txid := ComputeTxID(sighdr.Nonce, sighdr.Creator)
	return txid, nil
}
