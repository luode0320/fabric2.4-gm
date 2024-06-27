/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("common.channelconfig")

// RootGroupKey 是通道配置命名空间的关键，特别是对于策略评估。
const RootGroupKey = "Channel"

// Bundle 是一组资源的集合，它将始终具有一致的通道配置视图。
// 特别地，对于给定的 Bundle 引用，配置序列、策略管理器等将始终返回完全相同的值。
// Bundle 结构是不可变的，并且将始终以完全替换的方式进行替换，使用新的内存支持。
type Bundle struct {
	policyManager   policies.Manager   // 策略管理器
	channelConfig   *ChannelConfig     // 通道配置
	configtxManager configtx.Validator // 配置事务管理器
}

// PolicyManager returns the policy manager constructed for this config.
func (b *Bundle) PolicyManager() policies.Manager {
	return b.policyManager
}

// MSPManager returns the MSP manager constructed for this config.
func (b *Bundle) MSPManager() msp.MSPManager {
	return b.channelConfig.MSPManager()
}

// ChannelConfig returns the config.Channel for the chain.
func (b *Bundle) ChannelConfig() Channel {
	return b.channelConfig
}

// OrdererConfig 返回通道的config.Orderer以及Orderer配置是否存在。
func (b *Bundle) OrdererConfig() (Orderer, bool) {
	result := b.channelConfig.OrdererConfig()
	return result, result != nil
}

// ConsortiumsConfig returns the config.Consortiums for the channel
// and whether the consortiums config exists.
func (b *Bundle) ConsortiumsConfig() (Consortiums, bool) {
	result := b.channelConfig.ConsortiumsConfig()
	return result, result != nil
}

// ApplicationConfig returns the configtxapplication.SharedConfig for the channel
// and whether the Application config exists.
func (b *Bundle) ApplicationConfig() (Application, bool) {
	result := b.channelConfig.ApplicationConfig()
	return result, result != nil
}

// ConfigtxValidator returns the configtx.Validator for the channel.
func (b *Bundle) ConfigtxValidator() configtx.Validator {
	return b.configtxManager
}

// ValidateNew checks if a new bundle's contained configuration is valid to be derived from the current bundle.
// This allows checks of the nature "Make sure that the consensus type did not change".
func (b *Bundle) ValidateNew(nb Resources) error {
	if oc, ok := b.OrdererConfig(); ok {
		noc, ok := nb.OrdererConfig()
		if !ok {
			return errors.New("current config has orderer section, but new config does not")
		}

		// Prevent consensus-type migration when channel capabilities ConsensusTypeMigration is disabled
		if !b.channelConfig.Capabilities().ConsensusTypeMigration() {
			if oc.ConsensusType() != noc.ConsensusType() {
				return errors.Errorf("attempted to change consensus type from %s to %s",
					oc.ConsensusType(), noc.ConsensusType())
			}
		}

		for orgName, org := range oc.Organizations() {
			norg, ok := noc.Organizations()[orgName]
			if !ok {
				continue
			}
			mspID := org.MSPID()
			if mspID != norg.MSPID() {
				return errors.Errorf("orderer org %s attempted to change MSP ID from %s to %s", orgName, mspID, norg.MSPID())
			}
		}
	}

	if ac, ok := b.ApplicationConfig(); ok {
		nac, ok := nb.ApplicationConfig()
		if !ok {
			return errors.New("current config has application section, but new config does not")
		}

		for orgName, org := range ac.Organizations() {
			norg, ok := nac.Organizations()[orgName]
			if !ok {
				continue
			}
			mspID := org.MSPID()
			if mspID != norg.MSPID() {
				return errors.Errorf("application org %s attempted to change MSP ID from %s to %s", orgName, mspID, norg.MSPID())
			}
		}
	}

	if cc, ok := b.ConsortiumsConfig(); ok {
		ncc, ok := nb.ConsortiumsConfig()
		if !ok {
			return errors.Errorf("current config has consortiums section, but new config does not")
		}

		for consortiumName, consortium := range cc.Consortiums() {
			nconsortium, ok := ncc.Consortiums()[consortiumName]
			if !ok {
				continue
			}

			for orgName, org := range consortium.Organizations() {
				norg, ok := nconsortium.Organizations()[orgName]
				if !ok {
					continue
				}
				mspID := org.MSPID()
				if mspID != norg.MSPID() {
					return errors.Errorf("consortium %s org %s attempted to change MSP ID from %s to %s", consortiumName, orgName, mspID, norg.MSPID())
				}
			}
		}
	} else if _, okNew := nb.ConsortiumsConfig(); okNew {
		return errors.Errorf("current config has no consortiums section, but new config does")
	}

	return nil
}

// NewBundleFromEnvelope 函数从完整的配置信封中提取信息并创建一个新的 Bundle 对象。
// 参数:
// env *cb.Envelope       // 配置信封，包含了通道配置信息
// bccsp bccsp.BCCSP      // 加密服务提供者，用于处理加密和解密操作
//
// 返回值:
// *Bundle                 // 通道配置的 Bundle 对象
// error                   // 错误信息，如果出现任何问题则返回非空错误
func NewBundleFromEnvelope(env *cb.Envelope, bccsp bccsp.BCCSP) (*Bundle, error) {
	// 将配置信封的 Payload 部分反序列化
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, errors.Wrap(err, "无法从信封中解封有效负载") // 如果反序列化失败，返回错误
	}

	// 从 Payload 的 Data 部分反序列化配置信封
	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.Wrap(err, "无法从有效负载中取消编组配置信封") // 如果反序列化失败，返回错误
	}

	// 检查信封的 Header 部分是否为 nil
	if payload.Header == nil {
		return nil, errors.Errorf("有效负载头部不能为空") // 如果 Header 为 nil，返回错误
	}

	// 将 Header 的 ChannelHeader 部分反序列化
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.Wrap(err, "反序列化通道标头失败") // 如果反序列化失败，返回错误
	}

	// 使用从配置信封中提取的通道 ID 和配置信息，结合 BCCSP 创建一个新的 Bundle 对象
	return NewBundle(chdr.ChannelId, configEnvelope.Config, bccsp)
}

// NewBundle 创建一个新的不可变的通道配置。
// 输入参数：
//   - channelID：字符串，表示通道ID
//   - config：*cb.Config，表示配置
//   - bccsp：bccsp.BCCSP，表示加密算法提供者
//
// 返回值：
//   - *Bundle：表示配置束的指针
//   - error：表示创建过程中可能出现的错误
func NewBundle(channelID string, config *cb.Config, bccsp bccsp.BCCSP) (*Bundle, error) {
	// 预先验证配置, 是否包含通道组, 是否需要先启用 orderer 支持才能启用通道能力
	if err := preValidate(config); err != nil {
		return nil, err
	}

	// 创建通道配置
	channelConfig, err := NewChannelConfig(config.ChannelGroup, bccsp)
	if err != nil {
		return nil, errors.Wrap(err, "初始化通道配置失败")
	}

	// 创建策略提供者映射
	policyProviderMap := make(map[int32]policies.Provider)
	for pType := range cb.Policy_PolicyType_name {
		rtype := cb.Policy_PolicyType(pType)
		switch rtype {
		case cb.Policy_UNKNOWN:
			// 不注册处理程序
		case cb.Policy_SIGNATURE:
			policyProviderMap[pType] = cauthdsl.NewPolicyProvider(channelConfig.MSPManager())
		case cb.Policy_MSP:
			// 在此处添加 MSP 处理程序的钩子
		}
	}

	// 创建策略管理器
	policyManager, err := policies.NewManagerImpl(RootGroupKey, policyProviderMap, config.ChannelGroup)
	if err != nil {
		return nil, errors.Wrap(err, "初始化策略管理器失败")
	}

	// 创建配置事务管理器
	configtxManager, err := configtx.NewValidatorImpl(channelID, config, RootGroupKey, policyManager)
	if err != nil {
		return nil, errors.Wrap(err, "初始化configtx manager失败")
	}

	// Bundle 是一组资源的集合，它将始终具有一致的通道配置视图。
	return &Bundle{
		policyManager:   policyManager,   // 策略管理器
		channelConfig:   channelConfig,   // 通道配置
		configtxManager: configtxManager, // 配置事务管理器
	}, nil
}

// preValidate 对配置进行预验证。
// 输入参数：
//   - config：*cb.Config，表示配置
//
// 返回值：
//   - error：表示预验证过程中可能出现的错误
func preValidate(config *cb.Config) error {
	// 检查配置是否为 nil
	if config == nil {
		return errors.New("Channelconfig config 不能为nil")
	}

	// 检查配置是否包含通道组
	if config.ChannelGroup == nil {
		return errors.New("config必须包含一个通道组")
	}

	// 检查是否需要先启用 orderer 支持才能启用通道能力
	if og, ok := config.ChannelGroup.Groups[OrdererGroupKey]; ok {
		if _, ok := og.Values[CapabilitiesKey]; !ok {
			if _, ok := config.ChannelGroup.Values[CapabilitiesKey]; ok {
				return errors.New("在没有orderer支持的情况下，无法首先启用通道功能")
			}

			if ag, ok := config.ChannelGroup.Groups[ApplicationGroupKey]; ok {
				if _, ok := ag.Values[CapabilitiesKey]; ok {
					return errors.New("在没有orderer支持的情况下，无法启用应用程序Application功能")
				}
			}
		}
	}

	return nil
}
