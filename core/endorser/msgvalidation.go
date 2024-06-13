/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"crypto/sha256"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// UnpackedProposal 包含了提案中的有趣的组件。
type UnpackedProposal struct {
	ChaincodeName   string                  // 链码名称
	ChannelHeader   *common.ChannelHeader   // 通道头部
	Input           *peer.ChaincodeInput    // 链码输入
	Proposal        *peer.Proposal          // 提案
	SignatureHeader *common.SignatureHeader // 签名头部
	SignedProposal  *peer.SignedProposal    // 签名的提案
	ProposalHash    []byte                  // 提案哈希
}

func (up *UnpackedProposal) ChannelID() string {
	return up.ChannelHeader.ChannelId
}

func (up *UnpackedProposal) TxID() string {
	return up.ChannelHeader.TxId
}

// UnpackProposal 解析提案, 并包装一个解析完成的提案接哦古
func UnpackProposal(signedProp *peer.SignedProposal) (*UnpackedProposal, error) {
	// 解析提案消息
	prop, err := protoutil.UnmarshalProposal(signedProp.ProposalBytes)
	if err != nil {
		return nil, err
	}

	// 解析提案头部(通道头部[链码类型、交易id、时间戳、通道名称、链码名称]、签名者头部[随机数、签名者])
	hdr, err := protoutil.UnmarshalHeader(prop.Header)
	if err != nil {
		return nil, err
	}

	// 解析通道头部[链码类型、交易id、时间戳、通道名称、链码名称]
	chdr, err := protoutil.UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return nil, err
	}

	// 解析签名者头部[随机数、签名者]
	shdr, err := protoutil.UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	// 解析链码消息[链码名称]
	chaincodeHdrExt, err := protoutil.UnmarshalChaincodeHeaderExtension(chdr.Extension)
	if err != nil {
		return nil, err
	}

	if chaincodeHdrExt.ChaincodeId == nil {
		return nil, errors.Errorf("链码id为nil")
	}

	if chaincodeHdrExt.ChaincodeId.Name == "" {
		return nil, errors.Errorf("链码名称为空")
	}

	// 解析负载有效数据(链码规则、加密材料)
	cpp, err := protoutil.UnmarshalChaincodeProposalPayload(prop.Payload)
	if err != nil {
		return nil, err
	}

	// 解析链码规则(链码类型、链码名称、有效的数据如: 区块文件)
	cis, err := protoutil.UnmarshalChaincodeInvocationSpec(cpp.Input)
	if err != nil {
		return nil, err
	}

	if cis.ChaincodeSpec == nil {
		return nil, errors.Errorf("链码调用规范不能为nil")
	}

	if cis.ChaincodeSpec.Input == nil {
		return nil, errors.Errorf("链码输入区块内容不包含任何输入")
	}

	// 序列化链码提案有效负载(链码规则、加密材料)
	cppNoTransient := &peer.ChaincodeProposalPayload{Input: cpp.Input, TransientMap: nil}
	ppBytes, err := proto.Marshal(cppNoTransient)
	if err != nil {
		return nil, errors.WithMessage(err, "编码链码提案有效负载 ChaincodeProposalPayload 异常")
	}

	// TODO, 这是从proputils的东西保存下来的，但这应该是BCCSP吗？
	// 建议哈希是:
	// 1) 序列化的通道标头对象
	// 2) 序列化的签名标头对象
	// 3) 将转到tx的chaincode建议有效负载的部分的哈希
	// (即，没有瞬态数据的部分)
	var bytes []byte
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		propHash := sm3.New()
		propHash.Write(hdr.ChannelHeader)
		propHash.Write(hdr.SignatureHeader)
		propHash.Write(ppBytes)
		bytes = propHash.Sum(nil)[:]
	default:
		propHash := sha256.New()
		propHash.Write(hdr.ChannelHeader)
		propHash.Write(hdr.SignatureHeader)
		propHash.Write(ppBytes)
		bytes = propHash.Sum(nil)[:]
	}

	// 包含了提案中的有趣的组件
	return &UnpackedProposal{
		SignedProposal:  signedProp,                       // 签名的提案
		Proposal:        prop,                             // 提案
		ChannelHeader:   chdr,                             // 通道头部
		SignatureHeader: shdr,                             // 签名头部
		ChaincodeName:   chaincodeHdrExt.ChaincodeId.Name, // 链码名称
		Input:           cis.ChaincodeSpec.Input,          // 链码输入
		ProposalHash:    bytes,                            // 提案哈希
	}, nil
}

// Validate 验证提案信息和签名
func (up *UnpackedProposal) Validate(idDeserializer msp.IdentityDeserializer) error {
	logger := decorateLogger(endorserLogger, &ccprovider.TransactionParams{
		ChannelID: up.ChannelHeader.ChannelId,
		TxID:      up.TxID(),
	})

	// 验证标头类型
	switch common.HeaderType(up.ChannelHeader.Type) {
	case common.HeaderType_ENDORSER_TRANSACTION:
	case common.HeaderType_CONFIG:
		// CONFIG事务类型有 _no_business来到propose API。
		// 实际上，根据定义，进入提议API的任何内容都是背书人事务，
		// 所以任何其他标题类型似乎应该是一个错误...哦，好吧。

	default:
		return errors.Errorf("无效的标头类型 %s", common.HeaderType(up.ChannelHeader.Type))
	}

	// 在区块链网络中确保提案响应的时代一致性和唯一性, 该消息只能被看到一次（即没有被重放）
	if up.ChannelHeader.Epoch != 0 {
		return errors.Errorf("区块链网络中确保提案响应的epoch必须为0")
	}

	// 确保有一个nonce随机数
	if len(up.SignatureHeader.Nonce) == 0 {
		return errors.Errorf("nonce签名随机数不能为空")
	}

	// 确保有一个签名者
	if len(up.SignatureHeader.Creator) == 0 {
		return errors.New("creator签名者为空")
	}

	// 将交易ID计算为连接nonce和creator后的哈希值
	expectedTxID := protoutil.ComputeTxID(up.SignatureHeader.Nonce, up.SignatureHeader.Creator)
	if up.TxID() != expectedTxID {
		return errors.Errorf("验证提案的交易id不正确 '%s' -- 预期 '%s'", up.TxID(), expectedTxID)
	}

	if up.SignedProposal.ProposalBytes == nil {
		return errors.Errorf("提案内容不能为nil")
	}

	if up.SignedProposal.Signature == nil {
		return errors.Errorf("提案签名不能为nil")
	}

	// 获取签名者的身份(mspid + 证书内容)
	creator, err := idDeserializer.DeserializeIdentity(up.SignatureHeader.Creator)
	if err != nil {
		logger.Warnw("拒绝访问", "error", err, "identity", protoutil.LogMessageForSerializedIdentity(up.SignatureHeader.Creator))
		return errors.Errorf("拒绝访问: 通道 [%s] 签名者身份未知，签名人格式错误", up.ChannelID())
	}

	genericAuthError := errors.Errorf("拒绝访问: 频道 [%s] 签名者身份未知: [%s]", up.ChannelID(), creator.GetMSPIdentifier())
	// 确保签名者是有效的证书
	err = creator.Validate()
	if err != nil {
		logger.Warnw("拒绝访问: 签名者身份标识无效", "error", err, "identity", protoutil.LogMessageForSerializedIdentity(up.SignatureHeader.Creator))
		return genericAuthError
	}

	logger = logger.With("mspID", creator.GetMSPIdentifier())

	logger.Debug("签名者有效")

	// 验证签名
	err = creator.Verify(up.SignedProposal.ProposalBytes, up.SignedProposal.Signature)
	if err != nil {
		logger.Warnw("拒绝访问: 提案上的签名者签名无效", "error", err, "identity", protoutil.LogMessageForSerializedIdentity(up.SignatureHeader.Creator))
		return genericAuthError
	}

	logger.Debug("签名有效")

	return nil
}
