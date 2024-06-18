/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// 确保通道配置与当前二进制版本兼容，并记录基本的合理性检查日志。
// 参数res是通道配置的资源集合。
func checkResources(res channelconfig.Resources) error {
	// 记录通道配置的合理性检查日志，帮助进行配置的初步诊断。
	channelconfig.LogSanityChecks(res)

	// 尝试获取通道的排序服务配置。
	oc, ok := res.OrdererConfig()
	if !ok {
		// 如果找不到排序服务配置，则返回错误。
		return errors.New("配置中不包含排序服务配置")
	}

	// 检查排序服务配置所要求的能力是否被当前系统支持。
	if err := oc.Capabilities().Supported(); err != nil {
		// 如果发现不支持的能力，则以错误消息的形式包裹原错误并返回。
		return errors.WithMessagef(err, "配置要求了不被支持的排序服务能力: %s", err)
	}

	// 检查通道配置整体所要求的能力是否被当前系统支持。
	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		// 如果发现不支持的能力，则以错误消息的形式包裹原错误并返回。
		return errors.WithMessagef(err, "配置要求了不被支持的通道能力: %s", err)
	}

	// 如果所有检查均通过，则返回nil，表示没有错误。
	return nil
}

// checkResourcesOrPanic invokes checkResources and panics if an error is returned
func checkResourcesOrPanic(res channelconfig.Resources) {
	if err := checkResources(res); err != nil {
		logger.Panicf("[channel %s] %s", res.ConfigtxValidator().ChannelID(), err)
	}
}

type mutableResources interface {
	channelconfig.Resources
	Update(*channelconfig.Bundle)
}

type configResources struct {
	mutableResources
	bccsp bccsp.BCCSP
}

// CreateBundle 根据给定的通道ID和配置结构创建一个通道配置Bundle实例。
// 这个Bundle封装了通道的各种配置信息，如排序服务、锚节点、MSP等，
// 并提供了访问这些配置信息的便捷方法。bccsp参数用于加密相关的操作。
func (cr *configResources) CreateBundle(channelID string, config *common.Config) (*channelconfig.Bundle, error) {
	return channelconfig.NewBundle(channelID, config, cr.bccsp)
}

func (cr *configResources) Update(bndl *channelconfig.Bundle) {
	checkResourcesOrPanic(bndl)
	cr.mutableResources.Update(bndl)
}

func (cr *configResources) SharedConfig() channelconfig.Orderer {
	oc, ok := cr.OrdererConfig()
	if !ok {
		logger.Panicf("[channel %s] has no orderer configuration", cr.ConfigtxValidator().ChannelID())
	}
	return oc
}

type ledgerResources struct {
	*configResources
	blockledger.ReadWriter
}

// ChannelID passes through to the underlying configtx.Validator
func (lr *ledgerResources) ChannelID() string {
	return lr.ConfigtxValidator().ChannelID()
}

// VerifyBlockSignature verifies a signature of a block.
// It has an optional argument of a configuration envelope which would make the block verification to use validation
// rules based on the given configuration in the ConfigEnvelope. If the config envelope passed is nil, then the
// validation rules used are the ones that were applied at commit of previous blocks.
func (lr *ledgerResources) VerifyBlockSignature(sd []*protoutil.SignedData, envelope *common.ConfigEnvelope) error {
	policyMgr := lr.PolicyManager()
	// If the envelope passed isn't nil, we should use a different policy manager.
	if envelope != nil {
		bundle, err := channelconfig.NewBundle(lr.ChannelID(), envelope.Config, lr.bccsp)
		if err != nil {
			return err
		}
		policyMgr = bundle.PolicyManager()
	}
	policy, exists := policyMgr.GetPolicy(policies.BlockValidation)
	if !exists {
		return errors.Errorf("policy %s wasn't found", policies.BlockValidation)
	}
	err := policy.EvaluateSignedData(sd)
	if err != nil {
		return errors.WithMessage(err, "block verification failed")
	}
	return nil
}

// Block returns a block with the following number, or nil if such a block doesn't exist.
func (lr *ledgerResources) Block(number uint64) *common.Block {
	if lr.Height() <= number {
		return nil
	}
	return blockledger.GetBlock(lr, number)
}
