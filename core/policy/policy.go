/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy

import (
	"errors"
	"fmt"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
)

var logger = flogging.MustGetLogger("policy")

// PolicyChecker offers methods to check a signed proposal against a specific policy
// defined in a channel or not.
type PolicyChecker interface {
	// CheckPolicy checks that the passed signed proposal is valid with the respect to
	// passed policy on the passed channel.
	// If no channel is passed, CheckPolicyNoChannel is invoked directly.
	CheckPolicy(channelID, policyName string, signedProp *pb.SignedProposal) error

	// CheckPolicyBySignedData checks that the passed signed data is valid with the respect to
	// passed policy on the passed channel.
	// If no channel is passed, the method will fail.
	CheckPolicyBySignedData(channelID, policyName string, sd []*protoutil.SignedData) error

	// CheckPolicyNoChannel checks that the passed signed proposal is valid with the respect to
	// passed policy on the local MSP.
	CheckPolicyNoChannel(policyName string, signedProp *pb.SignedProposal) error

	// CheckPolicyNoChannelBySignedData checks that the passed signed data is valid with the respect to
	// passed policy on the local MSP.
	CheckPolicyNoChannelBySignedData(policyName string, signedData []*protoutil.SignedData) error
}

type policyChecker struct {
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter
	localMSP                   msp.IdentityDeserializer
	principalGetter            MSPPrincipalGetter
}

// NewPolicyChecker 创建PolicyChecker的新实例
func NewPolicyChecker(channelPolicyManagerGetter policies.ChannelPolicyManagerGetter, localMSP msp.MSP) PolicyChecker {
	return &policyChecker{
		channelPolicyManagerGetter: channelPolicyManagerGetter,
		localMSP:                   localMSP,
		principalGetter:            &localMSPPrincipalGetter{localMSP: localMSP}, // 包装msp实例
	}
}

// CheckPolicy 检查所传递的已签名提案在传递通道上的传递策略方面是否有效。
func (p *policyChecker) CheckPolicy(channelID, policyName string, signedProp *pb.SignedProposal) error {
	if channelID == "" {
		return p.CheckPolicyNoChannel(policyName, signedProp)
	}

	if policyName == "" {
		return fmt.Errorf("在通道 [%s] 上检查策略期间策略名称无效. 通道名称必须不等于nil.", channelID)
	}

	if signedProp == nil {
		return fmt.Errorf("在使用策略 [%s] 的通道 [%s] 上检查策略期间, 签名的提案内容为空", channelID, policyName)
	}

	// 获取策略
	policyManager := p.channelPolicyManagerGetter.Manager(channelID)
	if policyManager == nil {
		return fmt.Errorf("无法获取通道 [%s] 的策略管理器", channelID)
	}

	// 准备签名数据
	proposal, err := protoutil.UnmarshalProposal(signedProp.ProposalBytes)
	if err != nil {
		return fmt.Errorf("Failing extracting proposal during check policy on channel [%s] with policy [%s]: [%s]", channelID, policyName, err)
	}

	header, err := protoutil.UnmarshalHeader(proposal.Header)
	if err != nil {
		return fmt.Errorf("Failing extracting header during check policy on channel [%s] with policy [%s]: [%s]", channelID, policyName, err)
	}

	shdr, err := protoutil.UnmarshalSignatureHeader(header.SignatureHeader)
	if err != nil {
		return fmt.Errorf("Invalid Proposal's SignatureHeader during check policy on channel [%s] with policy [%s]: [%s]", channelID, policyName, err)
	}

	sd := []*protoutil.SignedData{{
		Data:      signedProp.ProposalBytes,
		Identity:  shdr.Creator,
		Signature: signedProp.Signature,
	}}

	return p.CheckPolicyBySignedData(channelID, policyName, sd)
}

// CheckPolicyNoChannel 检查通过的已签名提案是否与本地MSP上通过的策略有关。
func (p *policyChecker) CheckPolicyNoChannel(policyName string, signedProp *pb.SignedProposal) error {
	if policyName == "" {
		return errors.New("无通道检查策略期间的策略名称 policyName 无效, 名称必须不等于nil.")
	}

	if signedProp == nil {
		return fmt.Errorf("策略 [%s] 的无通道检查策略期间的签名提案 SignedProposal 无效", policyName)
	}
	// 解析提案的签名原始数据
	proposal, err := protoutil.UnmarshalProposal(signedProp.ProposalBytes)
	if err != nil {
		return fmt.Errorf("在策略为 [%s] 的无通道检查策略期间反序列化提案失败: [%s]", policyName, err)
	}
	// 反序列化提案头部(通道头部[链码类型、交易id、时间戳、通道名称、链码名称]、签名者头部[随机数、签名者])
	header, err := protoutil.UnmarshalHeader(proposal.Header)
	if err != nil {
		return fmt.Errorf("在策略为 [%s] 的无通道检查策略期间提取提案 Header 头部失败: [%s]", policyName, err)
	}
	// 反序列化签名者头部[随机数、签名者]
	shdr, err := protoutil.UnmarshalSignatureHeader(header.SignatureHeader)
	if err != nil {
		return fmt.Errorf("在使用策略 [%s] 的无通道检查策略期间，提案的签名头部 SignatureHeader 无效: [%s]", policyName, err)
	}

	// 使用本地MSP对提案的签名者进行反序列化
	id, err := p.localMSP.DeserializeIdentity(shdr.Creator)
	if err != nil {
		logger.Warnw("在无通道检查策略期间，对提案签名者进行反序列化失败", "error", err, "policyName", policyName, "identity", protoutil.LogMessageForSerializedIdentity(shdr.Creator))
		return fmt.Errorf("在使用策略 [%s] 的无通道检查策略期间，反序列化提案签名者失败: [%s]", policyName, err)
	}

	// 加载策略的MSPPrincipal
	principal, err := p.principalGetter.Get(policyName)
	if err != nil {
		return fmt.Errorf("在策略为 [%s] 的无通道检查策略期间获取本地 MSP principal 负责人失败: [%s]", policyName, err)
	}

	// 验证提案的签名者是否满足委托人的权限要求(加入通道需要admin权限的证书签名提案)
	err = id.SatisfiesPrincipal(principal)
	if err != nil {
		logger.Warnw("在无通道检查策略期间验证提案的签名者是否满足本地MSP负责人失败", "error", err, "policyName", policyName, "requiredPrincipal", principal, "signingIdentity", protoutil.LogMessageForSerializedIdentity(shdr.Creator))
		return fmt.Errorf("在策略为 [%s] 的无通道检查策略期间, 无法验证提案的签名者是否满足本地 MSP 主体负责人权限: [%s]", policyName, err)
	}

	// 验证签名
	return id.Verify(signedProp.ProposalBytes, signedProp.Signature)
}

// CheckPolicyBySignedData checks that the passed signed data is valid with the respect to
// passed policy on the passed channel.
func (p *policyChecker) CheckPolicyBySignedData(channelID, policyName string, sd []*protoutil.SignedData) error {
	if channelID == "" {
		return errors.New("Invalid channel ID name during check policy on signed data. Name must be different from nil.")
	}

	if policyName == "" {
		return fmt.Errorf("Invalid policy name during check policy on signed data on channel [%s]. Name must be different from nil.", channelID)
	}

	if sd == nil {
		return fmt.Errorf("Invalid signed data during check policy on channel [%s] with policy [%s]", channelID, policyName)
	}

	// Get Policy
	policyManager := p.channelPolicyManagerGetter.Manager(channelID)
	if policyManager == nil {
		return fmt.Errorf("Failed to get policy manager for channel [%s]", channelID)
	}

	// Recall that get policy always returns a policy object
	policy, _ := policyManager.GetPolicy(policyName)

	// Evaluate the policy
	err := policy.EvaluateSignedData(sd)
	if err != nil {
		logger.Warnw("Failed evaluating policy on signed data", "error", err, "policyName", policyName, "identities", protoutil.LogMessageForSerializedIdentities(sd))
		return fmt.Errorf("Failed evaluating policy on signed data during check policy on channel [%s] with policy [%s]: [%s]", channelID, policyName, err)
	}

	return nil
}

// CheckPolicyNoChannelBySignedData checks that the passed signed data are valid with the respect to
// passed policy on the local MSP.
func (p *policyChecker) CheckPolicyNoChannelBySignedData(policyName string, signedData []*protoutil.SignedData) error {
	if policyName == "" {
		return errors.New("invalid policy name during channelless check policy. Name must be different from nil.")
	}

	if len(signedData) == 0 {
		return fmt.Errorf("no signed data during channelless check policy with policy [%s]", policyName)
	}

	for _, data := range signedData {
		// Deserialize identity with the local MSP
		id, err := p.localMSP.DeserializeIdentity(data.Identity)
		if err != nil {
			logger.Warnw("Failed deserializing signed data identity during channelless check policy", "error", err, "policyName", policyName, "identity", protoutil.LogMessageForSerializedIdentity(data.Identity))
			return fmt.Errorf("failed deserializing signed data identity during channelless check policy with policy [%s]: [%s]", policyName, err)
		}

		// Load MSPPrincipal for policy
		principal, err := p.principalGetter.Get(policyName)
		if err != nil {
			return fmt.Errorf("failed getting local MSP principal during channelless check policy with policy [%s]: [%s]", policyName, err)
		}

		// Verify that proposal's creator satisfies the principal
		err = id.SatisfiesPrincipal(principal)
		if err != nil {
			logger.Warnw("failed verifying that the signed data identity satisfies local MSP principal during channelless check policy", "error", err, "policyName", policyName, "requiredPrincipal", principal, "identity", protoutil.LogMessageForSerializedIdentity(data.Identity))
			return fmt.Errorf("failed verifying that the signed data identity satisfies local MSP principal during channelless check policy with policy [%s]: [%s]", policyName, err)
		}

		// Verify the signature
		if err = id.Verify(data.Data, data.Signature); err != nil {
			return err
		}
	}

	return nil
}
