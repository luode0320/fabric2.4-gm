/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package multichannel tracks the channel resources for the orderer.  It initially
// loads the set of existing channels, and provides an interface for users of these
// channels to retrieve them, or create new ones.
package multichannel

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/orderer/common/blockcutter"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/filerepo"
	"github.com/hyperledger/fabric/orderer/common/follower"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/etcdraft"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	msgVersion = int32(0)
	epoch      = 0
)

var logger = flogging.MustGetLogger("orderer.commmon.multichannel")

// Registrar 作为一个访问点和控制中心，管理各个通道资源。
type Registrar struct {
	// config 存储顶层配置信息
	config localconfig.TopLevel

	// lock 用于保护内部字段的读写锁，确保并发安全
	lock sync.RWMutex
	// chains 存储由键（通道ID）索引的ChainSupport共识实例，每个实例对应一个通道的管理逻辑
	chains map[string]*ChainSupport
	// followers 类似chains，但用于管理跟随链（可能为系统链的备份或其他特殊用途）
	followers map[string]*follower.Chain
	// pendingRemoval 记录正在进行移除操作或移除失败的通道，以及它们的状态报告者
	pendingRemoval map[string]consensus.StaticStatusReporter
	// systemChannelID 存储系统通道的ID，如果有的话
	systemChannelID string
	// systemChannel 是指向系统通道的ChainSupport实例
	systemChannel *ChainSupport

	// consenters 保存不同类型共识算法的实例，键为共识算法的名称
	consenters map[string]consensus.Consenter
	// ledgerFactory 用于创建和访问区块链账本的工厂
	ledgerFactory blockledger.Factory
	// signer 用于签名和序列化的身份管理工具
	signer identity.SignerSerializer
	// blockcutterMetrics 区块裁剪模块的性能指标
	blockcutterMetrics *blockcutter.Metrics
	// templator 用于根据通道配置模板处理消息的组件
	templator msgprocessor.ChannelConfigTemplator
	// callbacks 在通道配置包更改时调用的回调函数列表
	callbacks []channelconfig.BundleActor
	// bccsp 加密服务提供者，用于密码学操作
	bccsp bccsp.BCCSP
	// clusterDialer 用于与集群中其他节点建立连接的拨号器
	clusterDialer *cluster.PredicateDialer
	// channelParticipationMetrics 记录通道参与度相关的性能指标
	channelParticipationMetrics *Metrics

	// joinBlockFileRepo 用于管理加入通道区块的文件存储仓库
	joinBlockFileRepo *filerepo.Repo
}

// ConfigBlockOrPanic retrieves the last configuration block from the given ledger.
// Panics on failure.
func ConfigBlockOrPanic(reader blockledger.Reader) *cb.Block {
	lastBlock, err := blockledger.GetBlockByNumber(reader, reader.Height()-1)
	if err != nil {
		logger.Panicw("Failed to retrieve block", "blockNum", reader.Height()-1, "error", err)
	}
	index, err := protoutil.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		logger.Panicw("Chain did not have appropriately encoded last config in its latest block", "error", err)
	}
	configBlock, err := blockledger.GetBlockByNumber(reader, index)
	if err != nil {
		logger.Panicw("Failed to retrieve config block", "blockNum", index, "error", err)
	}
	return configBlock
}

func configTx(reader blockledger.Reader) *cb.Envelope {
	return protoutil.ExtractEnvelopeOrPanic(ConfigBlockOrPanic(reader), 0)
}

// NewRegistrar produces an instance of a *Registrar.
// 新建一个Registrar 类型的实例并返回
func NewRegistrar(
	config localconfig.TopLevel,
	ledgerFactory blockledger.Factory,
	signer identity.SignerSerializer,
	metricsProvider metrics.Provider,
	bccsp bccsp.BCCSP,
	clusterDialer *cluster.PredicateDialer,
	callbacks ...channelconfig.BundleActor) *Registrar {
	r := &Registrar{
		config:                      config,
		chains:                      make(map[string]*ChainSupport),
		followers:                   make(map[string]*follower.Chain),
		pendingRemoval:              make(map[string]consensus.StaticStatusReporter),
		ledgerFactory:               ledgerFactory,
		signer:                      signer,
		blockcutterMetrics:          blockcutter.NewMetrics(metricsProvider),
		callbacks:                   callbacks,
		bccsp:                       bccsp,
		clusterDialer:               clusterDialer,
		channelParticipationMetrics: NewMetrics(metricsProvider),
	}

	if config.ChannelParticipation.Enabled {
		var err error
		r.joinBlockFileRepo, err = InitJoinBlockFileRepo(&r.config)
		if err != nil {
			logger.Panicf("Error initializing joinblock file repo: %s", err)
		}
	}

	return r
}

// InitJoinBlockFileRepo initialize the channel participation API joinblock file repo. This creates
// the fileRepoDir on the filesystem if it does not already exist.
func InitJoinBlockFileRepo(config *localconfig.TopLevel) (*filerepo.Repo, error) {
	fileRepoDir := filepath.Join(config.FileLedger.Location, "pendingops")
	logger.Infof("已启用通道参与API, 注册商使用文件存储库进行初始化 %s", fileRepoDir)

	joinBlockFileRepo, err := filerepo.New(fileRepoDir, "join")
	if err != nil {
		return nil, err
	}
	return joinBlockFileRepo, nil
}

func (r *Registrar) Initialize(consenters map[string]consensus.Consenter) {
	r.init(consenters)

	r.lock.Lock()
	defer r.lock.Unlock()
	r.startChannels()
}

// init 用于初始化注册器。
// 方法接收者：r（Registrar类型的指针）
// 输入参数：
//   - consenters：共识者映射，将共识者ID映射到共识者实例。
func (r *Registrar) init(consenters map[string]consensus.Consenter) {
	r.consenters = consenters

	// 发现和加载join-blocks。如果存在join-block，则必须存在账本；如果不存在，则创建一个。
	// channelsWithJoinBlock将通道ID映射到join-block。
	channelsWithJoinBlock := r.loadJoinBlocks()

	// 发现所有账本。这应该已经包括所有具有join-block的通道。
	// 确保没有没有对应join-block的空账本。
	existingChannels := r.discoverLedgers(channelsWithJoinBlock)

	// 扫描并初始化系统通道（如果存在）。
	// 注意，可能存在具有空账本但始终具有join-block的通道。
	r.initSystemChannel(existingChannels)

	// 初始化应用通道，通过创建consensus.Chain或follower.Chain。
	if r.systemChannelID == "" {
		r.initAppChannels(existingChannels, channelsWithJoinBlock)
	} else {
		r.initAppChannelsWhenSystemChannelExists(existingChannels)
	}
}

// startChannels 启动注册器中所有通道的内部goroutine，包括chains（主链）和followers（跟随链）的goroutine。
// 因为这些goroutine可能回调到Registrar上，所以在执行此操作前需要加锁保护，以防止并发访问导致的数据竞争。
func (r *Registrar) startChannels() {
	// 遍历所有的主链支持对象，并启动每个链的内部goroutine
	for _, chainSupport := range r.chains {
		chainSupport.start()
	}

	// 遍历所有的跟随链，并启动每个跟随链的内部goroutine
	for _, fChain := range r.followers {
		fChain.Start()
	}

	// 如果没有设置系统通道ID，打印日志信息
	if r.systemChannelID == "" {
		logger.Infof("注册器已初始化，未指定系统通道, 当前应用程序通道总数: %d, 其中包含 %d 个共识主链和 %d 个跟随链",
			len(r.chains)+len(r.followers), len(r.chains), len(r.followers))
	}
}

// discoverLedgers 用于发现所有账本。
// 方法接收者：r（Registrar类型的指针）
// 输入参数：
//   - channelsWithJoinBlock：具有join-block的通道的映射，将通道ID映射到join-block。
//
// 返回值：
//   - []string：已发现的所有账本的通道ID列表。
func (r *Registrar) discoverLedgers(channelsWithJoinBlock map[string]*cb.Block) []string {
	// 发现所有账本。这应该已经包括所有具有join-block的通道。
	existingChannels := r.ledgerFactory.ChannelIDs()

	for _, channelID := range existingChannels {
		// 获取或创建账本
		rl, err := r.ledgerFactory.GetOrCreate(channelID)
		if err != nil {
			logger.Panicf("通道账本工厂报告了需要账本 channelID %s, 但没有找到账本: %s", channelID, err)
		}
		// 删除没有join-block的空账本
		if rl.Height() == 0 {
			if _, ok := channelsWithJoinBlock[channelID]; !ok {
				logger.Warnf("Channel '%s' has an empty ledger without a join-block, removing it", channelID)
				if err := r.ledgerFactory.Remove(channelID); err != nil {
					logger.Panicf("Ledger factory failed to remove empty ledger '%s', error: %s", channelID, err)
				}
			}
		}
	}

	return r.ledgerFactory.ChannelIDs()
}

// initSystemChannel scan for and initialize the system channel, if it exists.
func (r *Registrar) initSystemChannel(existingChannels []string) {
	for _, channelID := range existingChannels {
		rl, err := r.ledgerFactory.GetOrCreate(channelID)
		if err != nil {
			logger.Panicf("通道账本工厂报告了需要账本 channelID %s, 但没有找到账本: %s", channelID, err)
		}

		if rl.Height() == 0 {
			// At this point in the initialization flow the system channel cannot be with height==0 and a join-block.
			// Even when the system channel is joined via the channel participation API, on-boarding is performed
			// prior to this point. Therefore, this is an application channel.
			continue // Skip application channels
		}

		configTransaction := configTx(rl)
		if configTransaction == nil {
			logger.Panic("Programming error, configTransaction should never be nil here")
		}
		ledgerResources, err := r.newLedgerResources(configTransaction)
		if err != nil {
			logger.Panicf("Error creating ledger resources: %s", err)
		}
		channelID := ledgerResources.ConfigtxValidator().ChannelID()

		if _, ok := ledgerResources.ConsortiumsConfig(); !ok {
			continue // Skip application channels
		}

		if r.systemChannelID != "" {
			logger.Panicf("There appear to be two system channels %s and %s", r.systemChannelID, channelID)
		}

		chain, err := newChainSupport(r, ledgerResources, r.consenters, r.signer, r.blockcutterMetrics, r.bccsp)
		if err != nil {
			logger.Panicf("Error creating chain support: %s", err)
		}
		r.templator = msgprocessor.NewDefaultTemplator(chain, r.bccsp)
		chain.Processor = msgprocessor.NewSystemChannel(
			chain,
			r.templator,
			msgprocessor.CreateSystemChannelFilters(r.config, r, chain, chain.MetadataValidator),
			r.bccsp,
		)

		// Retrieve genesis block to log its hash. See FAB-5450 for the purpose
		genesisBlock := ledgerResources.Block(0)
		if genesisBlock == nil {
			logger.Panicf("Error reading genesis block of system channel '%s'", channelID)
		}
		logger.Infof(
			"Starting system channel '%s' with genesis block hash %x and orderer type %s",
			channelID,
			protoutil.BlockHeaderHash(genesisBlock.Header),
			chain.SharedConfig().ConsensusType(),
		)

		r.chains[channelID] = chain
		r.systemChannelID = channelID
		r.systemChannel = chain
	}
}

// initAppChannels initializes application channels, assuming that the system channel does NOT exist.
// This implies that the orderer is using the channel participation API for joins (channel creation).
func (r *Registrar) initAppChannels(existingChannels []string, channelsWithJoinBlock map[string]*cb.Block) {
	// init app channels with join-blocks
	for channelID, joinBlock := range channelsWithJoinBlock {
		ledgerRes, clusterConsenter, err := r.initLedgerResourcesClusterConsenter(joinBlock)
		if err != nil {
			logger.Panicf("Error: %s, channel: %s", err, channelID)
		}

		isMember, err := clusterConsenter.IsChannelMember(joinBlock)
		if err != nil {
			logger.Panicf("Failed to determine cluster membership from join-block, channel: %s, error: %s", channelID, err)
		}

		if joinBlock.Header.Number == 0 && isMember {
			if _, _, err := r.createAsMember(ledgerRes, joinBlock, channelID); err != nil {
				logger.Panicf("Failed to createAsMember, error: %s", err)
			}
			if err := r.removeJoinBlock(channelID); err != nil {
				logger.Panicf("Failed to remove join-block, channel: %s, error: %s", channelID, err)
			}
		} else {
			if _, _, err = r.createFollower(ledgerRes, clusterConsenter, joinBlock, channelID); err != nil {
				logger.Panicf("Failed to createFollower, error: %s", err)
			}
		}
	}

	// init app channels without join-blocks
	for _, channelID := range existingChannels {
		if _, withJoinBlock := channelsWithJoinBlock[channelID]; withJoinBlock {
			continue // Skip channels with join-blocks, since they were already initialized above.
		}

		rl, err := r.ledgerFactory.GetOrCreate(channelID)
		if err != nil {
			logger.Panicf("Ledger factory reported channelID %s but could not retrieve it: %s", channelID, err)
		}

		configBlock := ConfigBlockOrPanic(rl)
		configTx := protoutil.ExtractEnvelopeOrPanic(configBlock, 0)
		if configTx == nil {
			logger.Panic("Programming error, configTx should never be nil here")
		}
		ledgerRes, err := r.newLedgerResources(configTx)
		if err != nil {
			logger.Panicf("Error creating ledger resources: %s", err)
		}

		ordererConfig, _ := ledgerRes.OrdererConfig()
		consenter, foundConsenter := r.consenters[ordererConfig.ConsensusType()]
		if !foundConsenter {
			logger.Panicf("Failed to find a consenter for consensus type: %s", ordererConfig.ConsensusType())
		}

		clusterConsenter, ok := consenter.(consensus.ClusterConsenter)
		if !ok {
			logger.Panic("clusterConsenter is not a consensus.ClusterConsenter")
		}

		isMember, err := clusterConsenter.IsChannelMember(configBlock)
		if err != nil {
			logger.Panicf("Failed to determine cluster membership from config-block, error: %s", err)
		}

		if isMember {
			chainSupport, err := newChainSupport(r, ledgerRes, r.consenters, r.signer, r.blockcutterMetrics, r.bccsp)
			if err != nil {
				logger.Panicf("Failed to create chain support for channel '%s', error: %s", channelID, err)
			}
			r.chains[channelID] = chainSupport
		} else {
			_, _, err := r.createFollower(ledgerRes, clusterConsenter, nil, channelID)
			if err != nil {
				logger.Panicf("Failed to create follower for channel '%s', error: %s", channelID, err)
			}
		}
	}
}

// initAppChannelsWhenSystemChannelExists initializes application channels, assuming that the system channel exists.
// This implies that the channel participation API is not used for joins (channel creation). Therefore, there are no
// join-blocks, and follower.Chain(s) are never created. The call to newChainSupport creates a consensus.Chain of the
// appropriate type.
func (r *Registrar) initAppChannelsWhenSystemChannelExists(existingChannels []string) {
	for _, channelID := range existingChannels {
		if channelID == r.systemChannelID {
			continue // Skip system channel
		}
		rl, err := r.ledgerFactory.GetOrCreate(channelID)
		if err != nil {
			logger.Panicf("Ledger factory reported channelID %s but could not retrieve it: %s", channelID, err)
		}

		configTxEnv := configTx(rl)
		if configTxEnv == nil {
			logger.Panic("Programming error, configTxEnv should never be nil here")
		}
		ledgerRes, err := r.newLedgerResources(configTxEnv)
		if err != nil {
			logger.Panicf("Error creating ledger resources: %s", err)
		}

		chainSupport, err := newChainSupport(r, ledgerRes, r.consenters, r.signer, r.blockcutterMetrics, r.bccsp)
		if err != nil {
			logger.Panicf("Failed to create chain support for channel '%s', error: %s", channelID, err)
		}
		r.chains[channelID] = chainSupport
	}
}

func (r *Registrar) initLedgerResourcesClusterConsenter(configBlock *cb.Block) (*ledgerResources, consensus.ClusterConsenter, error) {
	configEnv, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed extracting config envelope from block")
	}

	ledgerRes, err := r.newLedgerResources(configEnv)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed creating ledger resources")
	}

	ordererConfig, _ := ledgerRes.OrdererConfig()
	consenter, foundConsenter := r.consenters[ordererConfig.ConsensusType()]
	if !foundConsenter {
		return nil, nil, errors.Errorf("failed to find a consenter for consensus type: %s", ordererConfig.ConsensusType())
	}

	clusterConsenter, ok := consenter.(consensus.ClusterConsenter)
	if !ok {
		return nil, nil, errors.New("failed cast: clusterConsenter is not a consensus.ClusterConsenter")
	}

	return ledgerRes, clusterConsenter, nil
}

// SystemChannelID returns the ChannelID for the system channel.
func (r *Registrar) SystemChannelID() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.systemChannelID
}

// SystemChannel returns the ChainSupport for the system channel.
func (r *Registrar) SystemChannel() *ChainSupport {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.systemChannel
}

// BroadcastChannelSupport returns the message channel header, whether the message is a config update
// and the channel resources for a message or an error if the message is not a message which can
// be processed directly (like CONFIG and ORDERER_TRANSACTION messages)
func (r *Registrar) BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, *ChainSupport, error) {
	chdr, err := protoutil.ChannelHeader(msg)
	if err != nil {
		return nil, false, nil, errors.WithMessage(err, "could not determine channel ID")
	}

	cs := r.GetChain(chdr.ChannelId)
	// New channel creation
	if cs == nil {
		sysChan := r.SystemChannel()
		if sysChan == nil {
			return nil, false, nil, errors.New("channel creation request not allowed because the orderer system channel is not defined")
		}
		cs = sysChan
	}

	isConfig := false
	switch cs.ClassifyMsg(chdr) {
	case msgprocessor.ConfigUpdateMsg:
		isConfig = true
	case msgprocessor.ConfigMsg:
		return chdr, false, nil, errors.New("message is of type that cannot be processed directly")
	default:
	}

	return chdr, isConfig, cs, nil
}

// GetConsensusChain retrieves the consensus.Chain of the channel, if it exists.
func (r *Registrar) GetConsensusChain(chainID string) consensus.Chain {
	r.lock.RLock()
	defer r.lock.RUnlock()

	cs, exists := r.chains[chainID]
	if !exists {
		return nil
	}

	return cs.Chain
}

// GetChain retrieves the chain support for a chain if it exists.
func (r *Registrar) GetChain(chainID string) *ChainSupport {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.chains[chainID]
}

// GetFollower retrieves the follower.Chain if it exists.
func (r *Registrar) GetFollower(chainID string) *follower.Chain {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.followers[chainID]
}

func (r *Registrar) newLedgerResources(configTx *cb.Envelope) (*ledgerResources, error) {
	payload, err := protoutil.UnmarshalPayload(configTx.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "error umarshaling envelope to payload")
	}

	if payload.Header == nil {
		return nil, errors.New("missing channel header")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling channel header")
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.WithMessage(err, "error umarshaling config envelope from payload data")
	}

	bundle, err := channelconfig.NewBundle(chdr.ChannelId, configEnvelope.Config, r.bccsp)
	if err != nil {
		return nil, errors.WithMessage(err, "error creating channelconfig bundle")
	}

	err = checkResources(bundle)
	if err != nil {
		return nil, errors.WithMessagef(err, "error checking bundle for channel: %s", chdr.ChannelId)
	}

	ledger, err := r.ledgerFactory.GetOrCreate(chdr.ChannelId)
	if err != nil {
		return nil, errors.WithMessagef(err, "error getting ledger for channel: %s", chdr.ChannelId)
	}

	return &ledgerResources{
		configResources: &configResources{
			mutableResources: channelconfig.NewBundleSource(bundle, r.callbacks...),
			bccsp:            r.bccsp,
		},
		ReadWriter: ledger,
	}, nil
}

// CreateChain makes the Registrar create a consensus.Chain with the given name.
func (r *Registrar) CreateChain(chainName string) {
	lf, err := r.ledgerFactory.GetOrCreate(chainName)
	if err != nil {
		logger.Panicf("Failed obtaining ledger factory for %s: %v", chainName, err)
	}
	chain := r.GetChain(chainName)
	if chain != nil {
		logger.Infof("A chain of type %T for channel %s already exists. "+
			"Halting it.", chain.Chain, chainName)
		r.lock.Lock()
		chain.Halt()
		delete(r.chains, chainName)
		r.lock.Unlock()
	}
	r.newChain(configTx(lf))
}

// 方法用于根据配置交易创建一个新的通道。
// 参数:
//   - configtx: 包含通道创建配置信息的交易信封。
func (r *Registrar) newChain(configtx *cb.Envelope) {
	// 加写锁以确保在创建通道过程中的并发安全性。
	r.lock.Lock()
	defer r.lock.Unlock()

	// 从配置交易中提取通道名称。
	channelName, err := channelNameFromConfigTx(configtx)
	if err != nil {
		// 如果提取通道名称失败，记录警告并返回。
		logger.Warnf("提取通道名称失败: %v", err)
		return
	}

	// 解决 https://github.com/hyperledger/fabric/issues/2931 问题。
	// 检查是否已存在同名通道。
	if existingChain, exists := r.chains[channelName]; exists {
		// 若存在且为Raft类型的通道，记录日志并跳过创建。
		if _, isRaftChain := existingChain.Chain.(*etcdraft.Chain); isRaftChain {
			logger.Infof("通道 %s 已经创建，跳过重复创建", channelName)
			return
		}
	}

	// 使用配置交易创建新通道的支持结构。
	cs := r.createNewChain(configtx)
	// 启动新创建的通道共识。
	cs.start()
	// 记录日志，表明新通道已创建并启动。
	logger.Infof("创建并启动了新通道 %s", cs.ChannelID())
}

// createNewChain 方法根据给定的配置交易创建一个新的通道支持结构（ChainSupport）。
// 参数:
//   - configtx: 包含通道创建配置信息的交易信封。
//
// 返回:
//   - *ChainSupport: 新创建的通道支持实例。
func (r *Registrar) createNewChain(configtx *cb.Envelope) *ChainSupport {
	// 使用配置交易创建账本资源。
	ledgerResources, err := r.newLedgerResources(configtx)
	if err != nil {
		// 如果创建账本资源失败，则记录错误信息并终止程序。
		logger.Panicf("创建账本资源时发生错误: %s", err)
	}

	// 如果账本当前没有区块（高度为0），则需要自行创建创世区块。
	if ledgerResources.Height() == 0 {
		// 使用配置交易创建下一个区块（实际上为创世区块）并附加到账本。
		if err := ledgerResources.Append(blockledger.CreateNextBlock(ledgerResources, []*cb.Envelope{configtx})); err != nil {
			// 创世区块附加失败时，记录错误信息并终止程序。
			logger.Panicf("将创世区块附加到账本时发生错误: %s", err)
		}
	}

	// 根据提供的资源和配置创建通道共识实例。
	cs, err := newChainSupport(r, ledgerResources, r.consenters, r.signer, r.blockcutterMetrics, r.bccsp)
	if err != nil {
		// 创建通道支持实例失败时，记录错误信息并终止程序。
		logger.Panicf("创建通道支持结构时发生错误: %s", err)
	}

	// 将新创建的通道支持实例添加到注册器的通道映射中。
	chainID := ledgerResources.ConfigtxValidator().ChannelID()
	r.chains[chainID] = cs

	// 成功创建并配置后，返回新的通道支持实例。
	return cs
}

// SwitchFollowerToChain creates a consensus.Chain from the tip of the ledger, and removes the follower.
// It is called when a follower detects a config block that indicates cluster membership and halts, transferring
// execution to the consensus.Chain.
func (r *Registrar) SwitchFollowerToChain(channelID string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	lf, err := r.ledgerFactory.GetOrCreate(channelID)
	if err != nil {
		logger.Panicf("Failed obtaining ledger factory for channel %s: %v", channelID, err)
	}

	if _, chainExists := r.chains[channelID]; chainExists {
		logger.Panicf("Programming error, channel already exists: %s", channelID)
	}

	delete(r.followers, channelID)
	logger.Debugf("Removed follower for channel %s", channelID)
	cs := r.createNewChain(configTx(lf))
	if err := r.removeJoinBlock(channelID); err != nil {
		logger.Panicf("Failed removing join-block for channel: %s: %v", channelID, err)
	}
	cs.start()
	logger.Infof("Created and started channel %s", cs.ChannelID())
}

// SwitchChainToFollower creates a follower.Chain from the tip of the ledger and removes the consensus.Chain.
// It is called when an etcdraft.Chain detects it was evicted from the cluster (i.e. removed from the consenters set)
// and halts, transferring execution to the follower.Chain.
func (r *Registrar) SwitchChainToFollower(channelName string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, chainExists := r.chains[channelName]; !chainExists {
		logger.Infof("Channel %s consenter was removed", channelName)
		return
	}

	if _, followerExists := r.followers[channelName]; followerExists {
		logger.Panicf("Programming error, both a follower.Chain and a consensus.Chain exist, channel: %s", channelName)
	}

	rl, err := r.ledgerFactory.GetOrCreate(channelName)
	if err != nil {
		logger.Panicf("Failed obtaining ledger factory for %s: %v", channelName, err)
	}

	configBlock := ConfigBlockOrPanic(rl)
	ledgerRes, clusterConsenter, err := r.initLedgerResourcesClusterConsenter(configBlock)
	if err != nil {
		logger.Panicf("Error initializing ledgerResources & clusterConsenter: %s", err)
	}

	delete(r.chains, channelName)
	logger.Debugf("Removed consensus.Chain for channel %s", channelName)

	fChain, _, err := r.createFollower(ledgerRes, clusterConsenter, nil, channelName)
	if err != nil {
		logger.Panicf("Failed to create follower.Chain for channel '%s', error: %s", channelName, err)
	}
	fChain.Start()

	logger.Infof("Created and started a follower.Chain for channel %s", channelName)
}

// ChannelsCount returns the count of the current total number of channels.
func (r *Registrar) ChannelsCount() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return len(r.chains) + len(r.followers)
}

// NewChannelConfig produces a new template channel configuration based on the system channel's current config.
func (r *Registrar) NewChannelConfig(envConfigUpdate *cb.Envelope) (channelconfig.Resources, error) {
	return r.templator.NewChannelConfig(envConfigUpdate)
}

// CreateBundle calls channelconfig.NewBundle
func (r *Registrar) CreateBundle(channelID string, config *cb.Config) (channelconfig.Resources, error) {
	return channelconfig.NewBundle(channelID, config, r.bccsp)
}

// ChannelList returns a slice of ChannelInfoShort containing all application channels (excluding the system
// channel), and ChannelInfoShort of the system channel (nil if does not exist).
// The URL fields are empty, and are to be completed by the caller.
func (r *Registrar) ChannelList() types.ChannelList {
	r.lock.RLock()
	defer r.lock.RUnlock()

	list := types.ChannelList{}

	if r.systemChannelID != "" {
		list.SystemChannel = &types.ChannelInfoShort{Name: r.systemChannelID}
	}
	for name := range r.chains {
		if name == r.systemChannelID {
			continue
		}
		list.Channels = append(list.Channels, types.ChannelInfoShort{Name: name})
	}
	for name := range r.followers {
		list.Channels = append(list.Channels, types.ChannelInfoShort{Name: name})
	}

	for c := range r.pendingRemoval {
		list.Channels = append(list.Channels, types.ChannelInfoShort{
			Name: c,
		})
	}

	return list
}

// ChannelInfo provides extended status information about a channel.
// The URL field is empty, and is to be completed by the caller.
func (r *Registrar) ChannelInfo(channelID string) (types.ChannelInfo, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	info := types.ChannelInfo{Name: channelID}

	if c, ok := r.chains[channelID]; ok {
		info.Height = c.Height()
		info.ConsensusRelation, info.Status = c.StatusReport()
		return info, nil
	}

	if f, ok := r.followers[channelID]; ok {
		info.Height = f.Height()
		info.ConsensusRelation, info.Status = f.StatusReport()
		return info, nil
	}

	status, ok := r.pendingRemoval[channelID]
	if ok {
		return types.ChannelInfo{
			Name:              channelID,
			ConsensusRelation: status.ConsensusRelation,
			Status:            status.Status,
		}, nil
	}

	return types.ChannelInfo{}, types.ErrChannelNotExist
}

// JoinChannel instructs the orderer to create a channel and join it with the provided config block.
// The URL field is empty, and is to be completed by the caller.
func (r *Registrar) JoinChannel(channelID string, configBlock *cb.Block, isAppChannel bool) (info types.ChannelInfo, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if status, ok := r.pendingRemoval[channelID]; ok {
		if status.Status == types.StatusFailed {
			return types.ChannelInfo{}, types.ErrChannelRemovalFailure
		}
		return types.ChannelInfo{}, types.ErrChannelPendingRemoval
	}

	if r.systemChannelID != "" {
		return types.ChannelInfo{}, types.ErrSystemChannelExists
	}

	if _, ok := r.chains[channelID]; ok {
		return types.ChannelInfo{}, types.ErrChannelAlreadyExists
	}

	if _, ok := r.followers[channelID]; ok {
		return types.ChannelInfo{}, types.ErrChannelAlreadyExists
	}

	if !isAppChannel && len(r.chains) > 0 {
		return types.ChannelInfo{}, types.ErrAppChannelsAlreadyExists
	}

	defer func() {
		if err != nil {
			if err2 := r.ledgerFactory.Remove(channelID); err2 != nil {
				logger.Warningf("Failed to cleanup ledger: %v", err2)
			}
		}
	}()
	ledgerRes, clusterConsenter, err := r.initLedgerResourcesClusterConsenter(configBlock)
	if err != nil {
		return types.ChannelInfo{}, err
	}

	blockBytes, err := proto.Marshal(configBlock)
	if err != nil {
		return types.ChannelInfo{}, errors.Wrap(err, "failed marshaling joinblock")
	}

	if err := r.joinBlockFileRepo.Save(channelID, blockBytes); err != nil {
		return types.ChannelInfo{}, errors.WithMessagef(err, "failed saving joinblock to file repo for channel %s", channelID)
	}
	defer func() {
		if err != nil {
			if err2 := r.removeJoinBlock(channelID); err2 != nil {
				logger.Warningf("Failed to cleanup joinblock for channel %s: %v", channelID, err2)
			}
		}
	}()

	if !isAppChannel {
		info, err := r.joinSystemChannel(ledgerRes, clusterConsenter, configBlock, channelID)
		return info, err
	}

	isMember, err := clusterConsenter.IsChannelMember(configBlock)
	if err != nil {
		return types.ChannelInfo{}, errors.WithMessage(err, "无法从join-block确定群集 Member 成员身份")
	}

	if configBlock.Header.Number == 0 && isMember {
		chain, info, err := r.createAsMember(ledgerRes, configBlock, channelID)
		if err == nil {
			if err := r.removeJoinBlock(channelID); err != nil {
				return types.ChannelInfo{}, err
			}
			chain.start()
		}
		return info, err
	}

	fChain, info, err := r.createFollower(ledgerRes, clusterConsenter, configBlock, channelID)
	if err != nil {
		return info, errors.WithMessage(err, "failed to create follower")
	}

	fChain.Start()
	logger.Infof("Joining channel: %v", info)
	return info, err
}

func (r *Registrar) createAsMember(ledgerRes *ledgerResources, configBlock *cb.Block, channelID string) (*ChainSupport, types.ChannelInfo, error) {
	if ledgerRes.Height() == 0 {
		if err := ledgerRes.Append(configBlock); err != nil {
			return nil, types.ChannelInfo{}, errors.WithMessage(err, "failed to append join block to the ledger")
		}
	}
	chain, err := newChainSupport(
		r,
		ledgerRes,
		r.consenters,
		r.signer,
		r.blockcutterMetrics,
		r.bccsp,
	)
	if err != nil {
		return nil, types.ChannelInfo{}, errors.WithMessage(err, "failed to create chain support")
	}

	info := types.ChannelInfo{
		Name:   channelID,
		URL:    "",
		Height: ledgerRes.Height(),
	}
	info.ConsensusRelation, info.Status = chain.StatusReport()
	r.chains[channelID] = chain

	logger.Infof("Joining channel: %v", info)
	return chain, info, nil
}

// createFollower created a follower.Chain, puts it in the map, but does not start it.
func (r *Registrar) createFollower(
	ledgerRes *ledgerResources,
	clusterConsenter consensus.ClusterConsenter,
	joinBlock *cb.Block,
	channelID string,
) (*follower.Chain, types.ChannelInfo, error) {
	fLog := flogging.MustGetLogger("orderer.commmon.follower")
	blockPullerCreator, err := follower.NewBlockPullerCreator(
		channelID, fLog, r.signer, r.clusterDialer, r.config.General.Cluster, r.bccsp)
	if err != nil {
		return nil, types.ChannelInfo{}, errors.WithMessagef(err, "failed to create BlockPullerFactory for channel %s", channelID)
	}

	fChain, err := follower.NewChain(
		ledgerRes,
		clusterConsenter,
		joinBlock,
		follower.Options{
			Logger: fLog,
		},
		blockPullerCreator,
		r,
		r.bccsp,
		r,
	)
	if err != nil {
		return nil, types.ChannelInfo{}, errors.WithMessagef(err, "failed to create follower for channel %s", channelID)
	}

	clusterRelation, status := fChain.StatusReport()
	info := types.ChannelInfo{
		Name:              channelID,
		URL:               "",
		Height:            ledgerRes.Height(),
		ConsensusRelation: clusterRelation,
		Status:            status,
	}

	r.followers[channelID] = fChain

	logger.Debugf("Created follower.Chain: %v", info)
	return fChain, info, nil
}

// Assumes the system channel join-block is saved to the file repo.
func (r *Registrar) joinSystemChannel(
	ledgerRes *ledgerResources,
	clusterConsenter consensus.ClusterConsenter,
	configBlock *cb.Block,
	channelID string,
) (types.ChannelInfo, error) {
	logger.Infof("Joining system channel '%s', with config block number: %d", channelID, configBlock.Header.Number)

	if configBlock.Header.Number == 0 {
		if err := ledgerRes.Append(configBlock); err != nil {
			return types.ChannelInfo{}, errors.WithMessage(err, "error appending config block to the ledger")
		}
	}

	// This is a degenerate ChainSupport holding an inactive.Chain, that will respond to a GET request with the info
	// returned below. This is an indication to the user/admin that the orderer needs a restart, and prevent
	// conflicting channel participation API actions on the orderer.
	cs, err := newOnBoardingChainSupport(ledgerRes, r.config, r.bccsp)
	if err != nil {
		return types.ChannelInfo{}, errors.WithMessage(err, "error creating onboarding chain support")
	}
	r.chains[channelID] = cs
	r.systemChannel = cs
	r.systemChannelID = channelID

	info := types.ChannelInfo{
		Name:   channelID,
		URL:    "",
		Height: ledgerRes.Height(),
	}
	info.ConsensusRelation, info.Status = r.systemChannel.StatusReport()

	logger.Infof("System channel creation pending: server requires restart! ChannelInfo: %v", info)

	return info, nil
}

// RemoveChannel instructs the orderer to remove a channel.
func (r *Registrar) RemoveChannel(channelID string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	status, ok := r.pendingRemoval[channelID]
	if ok && status.Status != types.StatusFailed {
		return types.ErrChannelPendingRemoval
	}

	if r.systemChannelID != "" {
		if channelID != r.systemChannelID {
			return types.ErrSystemChannelExists
		}
		return r.removeSystemChannel()
	}

	cs, ok := r.chains[channelID]
	if ok {
		cs.Halt()
		r.removeMember(channelID, cs)
		return nil
	}

	fChain, ok := r.followers[channelID]
	if ok {
		fChain.Halt()
		return r.removeFollower(channelID, fChain)
	}

	return types.ErrChannelNotExist
}

func (r *Registrar) removeMember(channelID string, cs *ChainSupport) {
	relation, status := cs.StatusReport()
	r.pendingRemoval[channelID] = consensus.StaticStatusReporter{ConsensusRelation: relation, Status: status}
	r.removeLedgerAsync(channelID)

	delete(r.chains, channelID)

	logger.Infof("Removed channel: %s", channelID)
}

func (r *Registrar) removeFollower(channelID string, follower *follower.Chain) error {
	// join block may still exist if the follower is:
	// 1) still onboarding
	// 2) active but not yet called registrar.SwitchFollowerToChain()
	// NOTE: if the join block does not exist, os.RemoveAll returns nil
	// so there is no harm attempting to remove a non-existent join block.
	if err := r.removeJoinBlock(channelID); err != nil {
		return err
	}

	relation, status := follower.StatusReport()
	r.pendingRemoval[channelID] = consensus.StaticStatusReporter{ConsensusRelation: relation, Status: status}
	r.removeLedgerAsync(channelID)

	delete(r.followers, channelID)

	logger.Infof("Removed channel: %s", channelID)

	return nil
}

func (r *Registrar) loadJoinBlocks() map[string]*cb.Block {
	channelToBlockMap := make(map[string]*cb.Block)
	if !r.config.ChannelParticipation.Enabled {
		return channelToBlockMap
	}
	channelsList, err := r.joinBlockFileRepo.List()
	if err != nil {
		logger.Panicf("列出准备加入通道的区块文件 join-blocks  时出错: %s", err)
	}

	logger.Debugf("正在加载准备加入通道的区块文件 join-blocks 来自 %d channels: %s", len(channelsList), channelsList)
	for _, fileName := range channelsList {
		channelName := r.joinBlockFileRepo.FileToBaseName(fileName)
		blockBytes, err := r.joinBlockFileRepo.Read(channelName)
		if err != nil {
			logger.Panicf("读取准备加入通道的区块文件时出错: '%s', error: %s", fileName, err)
		}
		block, err := protoutil.UnmarshalBlock(blockBytes)
		if err != nil {
			logger.Panicf("反序列化准备加入通道的区块文件时出错: '%s', error: %s", fileName, err)
		}
		channelToBlockMap[channelName] = block
	}

	logger.Debug("创建缺失的通道账本")
	for channelID := range channelToBlockMap {
		if _, err := r.ledgerFactory.GetOrCreate(channelID); err != nil {
			logger.Panicf("无法为通道创建通道账本: '%s', error: %s", channelID, err)
		}
	}

	return channelToBlockMap
}

func (r *Registrar) removeJoinBlock(channelID string) error {
	if err := r.joinBlockFileRepo.Remove(channelID); err != nil {
		return errors.WithMessagef(err, "failed removing joinblock for channel %s", channelID)
	}

	return nil
}

func (r *Registrar) removeSystemChannel() error {
	systemChannelID := r.systemChannelID
	consensusType := r.systemChannel.SharedConfig().ConsensusType()
	if consensusType != "etcdraft" {
		return errors.Errorf("cannot remove %s system channel: %s", consensusType, systemChannelID)
	}

	// halt the inactive chain registry
	consenter := r.consenters["etcdraft"].(consensus.ClusterConsenter)
	consenter.RemoveInactiveChainRegistry()

	// halt the system channel and remove it from the chains map
	r.systemChannel.Halt()
	delete(r.chains, systemChannelID)

	// remove system channel resources
	err := r.ledgerFactory.Remove(systemChannelID)
	if err != nil {
		return errors.WithMessagef(err, "failed removing ledger for system channel %s", r.systemChannelID)
	}

	// remove system channel references
	r.systemChannel = nil
	r.systemChannelID = ""
	logger.Infof("removed system channel: %s", systemChannelID)

	failedRemovals := []string{}

	// halt all application channels
	for channel, cs := range r.chains {
		cs.Halt()

		rl, err := r.ledgerFactory.GetOrCreate(channel)
		if err != nil {
			return errors.WithMessagef(err, "could not retrieve ledger for channel: %s", channel)
		}
		configBlock := ConfigBlockOrPanic(rl)
		isChannelMember, err := consenter.IsChannelMember(configBlock)
		if err != nil {
			return errors.WithMessagef(err, "failed to determine channel membership for channel: %s", channel)
		}
		if !isChannelMember {
			logger.Debugf("not a member of channel %s, removing it", channel)
			err := r.ledgerFactory.Remove(channel)
			if err != nil {
				logger.Errorf("failed removing ledger for channel %s, error: %v", channel, err)
				failedRemovals = append(failedRemovals, channel)
				continue
			}

			delete(r.chains, channel)
		}
	}

	if len(failedRemovals) > 0 {
		return fmt.Errorf("failed removing ledger for channel(s): %s", strings.Join(failedRemovals, ", "))
	}

	// reintialize the registrar to recreate every channel
	r.init(r.consenters)

	// restart every channel
	r.startChannels()

	return nil
}

func (r *Registrar) removeLedgerAsync(channelID string) {
	go func() {
		err := r.ledgerFactory.Remove(channelID)
		r.lock.Lock()
		defer r.lock.Unlock()
		if err != nil {
			r.pendingRemoval[channelID] = consensus.StaticStatusReporter{ConsensusRelation: r.pendingRemoval[channelID].ConsensusRelation, Status: types.StatusFailed}
			r.channelParticipationMetrics.reportStatus(channelID, types.StatusFailed)
			logger.Errorf("ledger factory failed to remove empty ledger '%s', error: %s", channelID, err)
			return
		}
		delete(r.pendingRemoval, channelID)
	}()
}

func (r *Registrar) ReportConsensusRelationAndStatusMetrics(channelID string, relation types.ConsensusRelation, status types.Status) {
	r.channelParticipationMetrics.reportConsensusRelation(channelID, relation)
	r.channelParticipationMetrics.reportStatus(channelID, status)
}

func channelNameFromConfigTx(configtx *cb.Envelope) (string, error) {
	payload, err := protoutil.UnmarshalPayload(configtx.Payload)
	if err != nil {
		return "", errors.WithMessage(err, "error umarshaling envelope to payload")
	}

	if payload.Header == nil {
		return "", errors.New("missing channel header")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", errors.WithMessage(err, "error unmarshalling channel header")
	}

	return chdr.ChannelId, nil
}
