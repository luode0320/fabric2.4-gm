/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/orderer/common/blockcutter"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/inactive"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// ChainSupport holds the resources for a particular channel.
type ChainSupport struct {
	*ledgerResources
	msgprocessor.Processor
	*BlockWriter
	consensus.Chain
	cutter blockcutter.Receiver
	identity.SignerSerializer
	BCCSP bccsp.BCCSP

	// NOTE: It makes sense to add this to the ChainSupport since the design of Registrar does not assume
	// that there is a single consensus type at this orderer node and therefore the resolution of
	// the consensus type too happens only at the ChainSupport level.
	consensus.MetadataValidator

	// The registrar is not aware of the exact type that the Chain is, e.g. etcdraft, inactive, or follower.
	// Therefore, we let each chain report its cluster relation and status through this interface. Non cluster
	// type chains (solo, kafka) are assigned a static reporter.
	consensus.StatusReporter
}

// newChainSupport 用于创建一个链共识对象（ChainSupport），它封装了用于处理特定通道所需的各种资源和服务。
// 参数:
//   - registrar: 订单服务注册器，包含配置和报告功能。
//   - ledgerResources: 提供访问账本和配置验证器的资源。
//   - consenters: 一个映射，键为共识类型，值为对应的共识器实例。
//   - signer: 签名和序列化器，用于生成签名。
//   - blockcutterMetrics: 用于跟踪blockcutter性能指标的对象。
//   - bccsp: BCCSP（区块链加密服务提供者）实例，用于加密操作。
//
// 返回:
//   - *ChainSupport: 新创建的链支持对象。
//   - error: 如果创建过程中出现错误。
func newChainSupport(
	registrar *Registrar,
	ledgerResources *ledgerResources,
	consenters map[string]consensus.Consenter,
	signer identity.SignerSerializer,
	blockcutterMetrics *blockcutter.Metrics,
	bccsp bccsp.BCCSP,
) (*ChainSupport, error) {
	// 从账本中读取最新块和该通道的元数据
	lastBlock := blockledger.GetBlock(ledgerResources, ledgerResources.Height()-1)
	metadata, err := protoutil.GetConsenterMetadataFromBlock(lastBlock)
	// 即使元数据为空，也应该能正常处理，因为使用cb.NewBlock()创建的块
	if err != nil {
		return nil, errors.WithMessagef(err, "为通道[%s]提取排序器元数据时出错", ledgerResources.ConfigtxValidator().ChannelID())
	}

	// 构建ChainSupport的基本结构
	cs := &ChainSupport{
		ledgerResources:  ledgerResources,
		SignerSerializer: signer,
		cutter: blockcutter.NewReceiverImpl(
			ledgerResources.ConfigtxValidator().ChannelID(),
			ledgerResources,
			blockcutterMetrics,
		),
		BCCSP: bccsp,
	}

	// 设置消息处理器
	cs.Processor = msgprocessor.NewStandardChannel(cs, msgprocessor.CreateStandardChannelFilters(cs, registrar.config), bccsp)

	// 初始化区块写入器
	cs.BlockWriter = newBlockWriter(lastBlock, registrar, cs)

	// 获取并设置共识器
	consenterType := ledgerResources.SharedConfig().ConsensusType()
	consenter, ok := consenters[consenterType]
	if !ok {
		return nil, errors.Errorf("找不到类型为[%s]的共识器", consenterType)
	}

	cs.Chain, err = consenter.HandleChain(cs, metadata)
	if err != nil {
		return nil, errors.WithMessagef(err, "为通道[%s]创建共识器时出错", cs.ChannelID())
	}

	// 设置元数据验证器和状态报告器
	if cv, ok := cs.Chain.(consensus.MetadataValidator); ok {
		cs.MetadataValidator = cv
	} else {
		cs.MetadataValidator = consensus.NoOpMetadataValidator{}
	}

	if sr, ok := cs.Chain.(consensus.StatusReporter); ok {
		cs.StatusReporter = sr
	} else { // 对于非集群类型，如solo或kafka
		cs.StatusReporter = consensus.StaticStatusReporter{ConsensusRelation: types.ConsensusRelationOther, Status: types.StatusActive}
	}

	// 报告共识关系和状态指标
	clusterRelation, status := cs.StatusReporter.StatusReport()
	registrar.ReportConsensusRelationAndStatusMetrics(cs.ChannelID(), clusterRelation, status)

	logger.Debugf("[channel: %s] 成功创建链共识资源", cs.ChannelID())

	return cs, nil
}
func (cs *ChainSupport) Reader() blockledger.Reader {
	return cs
}

// Signer returns the SignerSerializer for this channel.
func (cs *ChainSupport) Signer() identity.SignerSerializer {
	return cs
}

func (cs *ChainSupport) start() {
	cs.Chain.Start()
}

// BlockCutter 返回与此通道关联的 blockcutter.Receiver 实例。
// blockcutter.Receiver 负责接收交易并将它们组织成批次，以便高效地组成区块。
func (cs *ChainSupport) BlockCutter() blockcutter.Receiver {
	return cs.cutter
}

// Validate passes through to the underlying configtx.Validator
func (cs *ChainSupport) Validate(configEnv *cb.ConfigEnvelope) error {
	return cs.ConfigtxValidator().Validate(configEnv)
}

// ProposeConfigUpdate validates a config update using the underlying configtx.Validator
// and the consensus.MetadataValidator.
func (cs *ChainSupport) ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error) {
	env, err := cs.ConfigtxValidator().ProposeConfigUpdate(configtx)
	if err != nil {
		return nil, err
	}

	bundle, err := cs.CreateBundle(cs.ChannelID(), env.Config)
	if err != nil {
		return nil, err
	}

	if err = checkResources(bundle); err != nil {
		return nil, errors.WithMessage(err, "config update is not compatible")
	}

	if err = cs.ValidateNew(bundle); err != nil {
		return nil, err
	}

	oldOrdererConfig, ok := cs.OrdererConfig()
	if !ok {
		logger.Panic("old config is missing orderer group")
	}

	// we can remove this check since this is being validated in checkResources earlier
	newOrdererConfig, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.New("new config is missing orderer group")
	}

	if err = cs.ValidateConsensusMetadata(oldOrdererConfig, newOrdererConfig, false); err != nil {
		return nil, errors.WithMessage(err, "consensus metadata update for channel config update is invalid")
	}
	return env, nil
}

// ConfigProto passes through to the underlying configtx.Validator
func (cs *ChainSupport) ConfigProto() *cb.Config {
	return cs.ConfigtxValidator().ConfigProto()
}

// Sequence passes through to the underlying configtx.Validator
func (cs *ChainSupport) Sequence() uint64 {
	return cs.ConfigtxValidator().Sequence()
}

// Append appends a new block to the ledger in its raw form,
// unlike WriteBlock that also mutates its metadata.
func (cs *ChainSupport) Append(block *cb.Block) error {
	return cs.ledgerResources.ReadWriter.Append(block)
}

func newOnBoardingChainSupport(
	ledgerResources *ledgerResources,
	config localconfig.TopLevel,
	bccsp bccsp.BCCSP,
) (*ChainSupport, error) {
	cs := &ChainSupport{ledgerResources: ledgerResources}
	cs.Processor = msgprocessor.NewStandardChannel(cs, msgprocessor.CreateStandardChannelFilters(cs, config), bccsp)
	cs.Chain = &inactive.Chain{Err: errors.New("system channel creation pending: server requires restart")}
	cs.StatusReporter = consensus.StaticStatusReporter{ConsensusRelation: types.ConsensusRelationConsenter, Status: types.StatusInactive}

	logger.Debugf("[channel: %s] Done creating onboarding channel support resources", cs.ChannelID())

	return cs, nil
}
