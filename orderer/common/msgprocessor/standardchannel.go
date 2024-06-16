/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"

	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/signer_serializer.go --fake-name SignerSerializer . signerSerializer

type signerSerializer interface {
	identity.SignerSerializer
}

// StandardChannelSupport includes the resources needed for the StandardChannel processor.
type StandardChannelSupport interface {
	// Sequence should return the current configSeq
	Sequence() uint64

	// ChannelID returns the ChannelID
	ChannelID() string

	// Signer returns the signer for this orderer
	Signer() identity.SignerSerializer

	// ProposeConfigUpdate 接收CONFIG_UPDATE类型的信封并生成 ConfigEnvelope 作为CONFIG消息的信封负载数据
	ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error)

	OrdererConfig() (channelconfig.Orderer, bool)
}

// StandardChannel implements the Processor interface for standard extant channels
type StandardChannel struct {
	support           StandardChannelSupport
	filters           *RuleSet // Rules applicable to both normal and config messages
	maintenanceFilter Rule     // Rule applicable only to config messages
}

// NewStandardChannel creates a new standard message processor
func NewStandardChannel(support StandardChannelSupport, filters *RuleSet, bccsp bccsp.BCCSP) *StandardChannel {
	return &StandardChannel{
		filters:           filters,
		support:           support,
		maintenanceFilter: NewMaintenanceFilter(support, bccsp),
	}
}

// CreateStandardChannelFilters creates the set of filters for a normal (non-system) chain.
//
// In maintenance mode, require the signature of /Channel/Orderer/Writer. This will filter out configuration
// changes that are not related to consensus-type migration (e.g on /Channel/Application).
func CreateStandardChannelFilters(filterSupport channelconfig.Resources, config localconfig.TopLevel) *RuleSet {
	rules := []Rule{
		EmptyRejectRule,
		NewSizeFilter(filterSupport),
		NewSigFilter(policies.ChannelWriters, policies.ChannelOrdererWriters, filterSupport),
	}

	if !config.General.Authentication.NoExpirationChecks {
		expirationRule := NewExpirationRejectRule(filterSupport)
		// In case of DoS, expiration is inserted before SigFilter, so it is evaluated first
		rules = append(rules[:2], append([]Rule{expirationRule}, rules[2:]...)...)
	}

	return NewRuleSet(rules)
}

// ClassifyMsg inspects the message to determine which type of processing is necessary
func (s *StandardChannel) ClassifyMsg(chdr *cb.ChannelHeader) Classification {
	switch chdr.Type {
	case int32(cb.HeaderType_CONFIG_UPDATE):
		return ConfigUpdateMsg
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		// In order to maintain backwards compatibility, we must classify these messages
		return ConfigMsg
	case int32(cb.HeaderType_CONFIG):
		// In order to maintain backwards compatibility, we must classify these messages
		return ConfigMsg
	default:
		return NormalMsg
	}
}

// ProcessNormalMsg 根据当前配置检查消息的有效性。在成功时，它将返回当前配置的序列号和nil；如果消息无效，则返回错误。
func (s *StandardChannel) ProcessNormalMsg(env *cb.Envelope) (configSeq uint64, err error) {
	// 尝试从支持组件中获取排序服务的配置信息
	oc, ok := s.support.OrdererConfig()
	// 如果没有找到排序服务配置，则触发恐慌，因为这是运行的基本前提
	if !ok {
		logger.Panicf("缺少排序器配置")
	}
	// 检查是否启用了共识类型迁移的能力
	if oc.Capabilities().ConsensusTypeMigration() {
		// 如果当前共识状态不是正常状态，则拒绝处理常规交易，并返回维护模式错误
		if oc.ConsensusState() != orderer.ConsensusType_STATE_NORMAL {
			return 0, errors.WithMessage(ErrMaintenanceMode, "在维护模式下，拒绝常规交易")
		}
	}

	// 获取当前的配置序列号
	configSeq = s.support.Sequence()
	// 应用过滤器链，进一步检查消息的有效性
	err = s.filters.Apply(env)
	return
}

// ProcessConfigUpdateMsg 方法尝试将配置更新消息应用于当前配置，如果成功，则返回生成的配置消息和计算配置的 configSeq。
// 如果配置更新消息无效，则返回错误。
// 方法接收者：s *StandardChannel，表示 StandardChannel 结构体的指针。
// 输入参数：
//   - env *cb.Envelope，表示要处理的配置更新消息的信封对象。
//
// 返回值：
//   - config *cb.Envelope，表示生成的配置消息的信封对象。
//   - configSeq uint64，表示计算配置的 configSeq。
//   - error，表示处理过程中的错误，如果处理成功则返回nil。
func (s *StandardChannel) ProcessConfigUpdateMsg(env *cb.Envelope) (config *cb.Envelope, configSeq uint64, err error) {
	logger.Debugf("处理现有通道的配置更新消息 %s", s.support.ChannelID())

	// 首先调用序列。如果seq在提案和接受之间提前，这是可以的，并且会导致重新处理
	// 但是，如果最后调用序列，则成功可能会错误地归因于较新的configSeq
	seq := s.support.Sequence()
	err = s.filters.Apply(env)
	if err != nil {
		return nil, 0, errors.WithMessage(err, "现有通道的配置更新未通过初始检查")
	}

	// 接收CONFIG_UPDATE类型的信封并生成 ConfigEnvelope 作为CONFIG消息的信封负载数据
	configEnvelope, err := s.support.ProposeConfigUpdate(env)
	if err != nil {
		return nil, 0, errors.WithMessagef(err, "将配置更新应用于现有通道时出错 '%s'", s.support.ChannelID())
	}

	config, err = protoutil.CreateSignedEnvelope(cb.HeaderType_CONFIG, s.support.ChannelID(), s.support.Signer(), configEnvelope, msgVersion, epoch)
	if err != nil {
		return nil, 0, err
	}

	// 我们在这里重新应用过滤器，特别是对于大小过滤器，以确保我们的交易
	// 刚刚构造的对于我们的consenter来说不是太大。它还会重新应用签名
	// 检查，虽然不是严格必要的，是一个很好的健全性检查，以防orderer
	// 尚未配置正确的cert材料。签名的额外开销
	// 检查是可以忽略的，因为这是重新配置路径，而不是正常路径。
	err = s.filters.Apply(config)
	if err != nil {
		return nil, 0, errors.WithMessage(err, "现有通道的配置更新未通过最终检查")
	}

	err = s.maintenanceFilter.Apply(config)
	if err != nil {
		return nil, 0, errors.WithMessage(err, "现有通道的配置更新未通过维护检查")
	}

	return config, seq, nil
}

// ProcessConfigMsg takes an envelope of type `HeaderType_CONFIG`, unpacks the `ConfigEnvelope` from it
// extracts the `ConfigUpdate` from `LastUpdate` field, and calls `ProcessConfigUpdateMsg` on it.
func (s *StandardChannel) ProcessConfigMsg(env *cb.Envelope) (config *cb.Envelope, configSeq uint64, err error) {
	logger.Debugf("Processing config message for channel %s", s.support.ChannelID())

	configEnvelope := &cb.ConfigEnvelope{}
	_, err = protoutil.UnmarshalEnvelopeOfType(env, cb.HeaderType_CONFIG, configEnvelope)
	if err != nil {
		return
	}

	return s.ProcessConfigUpdateMsg(configEnvelope.LastUpdate)
}
