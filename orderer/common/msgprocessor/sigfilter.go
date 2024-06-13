/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	"fmt"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// SigFilterSupport provides the resources required for the signature filter
type SigFilterSupport interface {
	// PolicyManager returns a reference to the current policy manager
	PolicyManager() policies.Manager
	// OrdererConfig returns the config.Orderer for the channel and whether the Orderer config exists
	OrdererConfig() (channelconfig.Orderer, bool)
}

// SigFilter stores the name of the policy to apply to deliver requests to
// determine whether a client is authorized
type SigFilter struct {
	normalPolicyName      string
	maintenancePolicyName string
	support               SigFilterSupport
}

// NewSigFilter creates a new signature filter, at every evaluation, the policy manager is called
// to retrieve the latest version of the policy.
//
// normalPolicyName is applied when Orderer/ConsensusType.State = NORMAL
// maintenancePolicyName is applied when Orderer/ConsensusType.State = MAINTENANCE
func NewSigFilter(normalPolicyName, maintenancePolicyName string, support SigFilterSupport) *SigFilter {
	return &SigFilter{
		normalPolicyName:      normalPolicyName,
		maintenancePolicyName: maintenancePolicyName,
		support:               support,
	}
}

// Apply 应用给定的策略，导致拒绝或转发，从不接受
func (sf *SigFilter) Apply(message *cb.Envelope) error {
	ordererConf, ok := sf.support.OrdererConfig()
	if !ok {
		logger.Panic("编程错误: 未找到 orderer 配置")
	}

	signedData, err := protoutil.EnvelopeAsSignedData(message)
	if err != nil {
		return fmt.Errorf("无法将消息转换为签名数据: %s", err)
	}

	// 在维护模式下，我们通常需要 /Channel/Orderer/Writers 的签名。
	// 这将过滤掉与共识类型迁移无关的配置更改
	// (例如在/Channel/Application上)，并将阻止来自对等体 (通常是/Channel/reader) 的传递请求。
	policyName := sf.normalPolicyName
	if ordererConf.ConsensusState() == orderer.ConsensusType_STATE_MAINTENANCE {
		policyName = sf.maintenancePolicyName
	}

	policy, ok := sf.support.PolicyManager().GetPolicy(policyName)
	if !ok {
		return fmt.Errorf("找不到策略 %s", policyName)
	}

	err = policy.EvaluateSignedData(signedData)
	if err != nil {
		logger.Warnw("SigFilter 评估失败", "error", err.Error(), "ConsensusState", ordererConf.ConsensusState(), "policyName", policyName, "signingIdentity", protoutil.LogMessageForSerializedIdentities(signedData))
		return errors.Wrap(errors.WithStack(ErrPermissionDenied), err.Error())
	}
	return nil
}
