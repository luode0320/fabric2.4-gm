/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"path"
	"reflect"
	"time"

	"code.cloudfoundry.org/clock"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/inactive"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

//go:generate counterfeiter -o mocks/inactive_chain_registry.go --fake-name InactiveChainRegistry . InactiveChainRegistry

// InactiveChainRegistry 接口用于注册和追踪那些处于非活动状态的链。
type InactiveChainRegistry interface {
	// TrackChain 方法用于追踪具有给定名称的链，并在该链应当被创建时调用给定的回调函数。
	// 这在链成员资格发生变化，例如节点被驱逐或链配置更新时特别有用，此时链暂时变为非活动状态，
	// 直到满足再次参与共识的条件，回调函数将被触发以重新创建链实例。
	TrackChain(chainName string, genesisBlock *common.Block, createChain func())

	// Stop 方法用于停止 InactiveChainRegistry 的运行。这在移除系统链时会被调用，
	// 因为系统链的移除意味着整个网络配置的变更，所有依赖于它的非活动链追踪也需要终止。
	Stop()
}

//go:generate counterfeiter -o mocks/chain_manager.go --fake-name ChainManager . ChainManager

// ChainManager 接口定义了Consenter组件从多通道注册表(multichannel.Registrar)中所需要的方法集合。
type ChainManager interface {
	// 获取指定通道ID对应的共识链实例。
	GetConsensusChain(channelID string) consensus.Chain
	// 创建一个新的共识链实例，通常在通道启动或重新配置时调用。
	CreateChain(channelID string)
	// 将指定通道的共识链切换到跟随者(Follower)模式，通常在网络分区或节点疑似被驱逐时调用。
	SwitchChainToFollower(channelID string)
	// 上报共识关系和状态的度量信息，用于监控和分析共识算法的工作状态。
	ReportConsensusRelationAndStatusMetrics(channelID string, relation types.ConsensusRelation, status types.Status)
}

// Config 结构体包含了etcdraft共识算法的相关配置参数。
type Config struct {
	WALDir               string // 指定WAL（Write-Ahead Log，预写式日志）数据的存储路径，每个通道的WAL数据将存放在WALDir目录下的相应子目录中。
	SnapDir              string // 指定快照数据的存储路径，每个通道的快照将存放在SnapDir目录下的相应子目录中。
	EvictionSuspicion    string // 定义了节点怀疑自己被从通道中驱逐的时间阈值，单位为时间持续期字符串（如"10s"表示10秒）。
	TickIntervalOverride string // 用于覆盖通道配置中指定的tick间隔，允许外部配置一个不同的tick间隔时间，单位同样为时间持续期字符串。
}

// Consenter 结构体实现了etcdraft共识者接口，用于处理和管理共识流程。
type Consenter struct {
	ChainManager          ChainManager             // 负责创建和管理链实例的管理器
	InactiveChainRegistry InactiveChainRegistry    // 用于跟踪和管理非活动链的注册表
	Dialer                *cluster.PredicateDialer // 分布式系统中用于节点间通信的拨号器
	Communication         cluster.Communicator     // 实现了通信接口，用于节点间的消息传递
	*Dispatcher                                    // 消息分发器，负责将接收到的消息转发至适当的处理程序
	Logger                *flogging.FabricLogger   // 日志记录器，用于记录共识过程中的信息和调试日志
	EtcdRaftConfig        Config                   // etcdraft共识算法的配置信息
	OrdererConfig         localconfig.TopLevel     // Orderer服务的顶层配置
	Cert                  []byte                   // 当前节点的证书，用于身份验证和加密通信
	Metrics               *Metrics                 // 监控指标，用于性能监控和故障诊断
	BCCSP                 bccsp.BCCSP              // 区块链密码服务提供者，用于加密和签名操作
}

// TargetChannel extracts the channel from the given proto.Message.
// Returns an empty string on failure.
func (c *Consenter) TargetChannel(message proto.Message) string {
	switch req := message.(type) {
	case *orderer.ConsensusRequest:
		return req.Channel
	case *orderer.SubmitRequest:
		return req.Channel
	default:
		return ""
	}
}

// ReceiverByChain returns the MessageReceiver for the given channelID or nil
// if not found.
func (c *Consenter) ReceiverByChain(channelID string) MessageReceiver {
	chain := c.ChainManager.GetConsensusChain(channelID)
	if chain == nil {
		return nil
	}
	if etcdRaftChain, isEtcdRaftChain := chain.(*Chain); isEtcdRaftChain {
		return etcdRaftChain
	}
	c.Logger.Warningf("Chain %s is of type %v and not etcdraft.Chain", channelID, reflect.TypeOf(chain))
	return nil
}

// detectSelfID 在共识者映射中查找并返回当前节点的Raft节点ID。
func (c *Consenter) detectSelfID(consenters map[uint64]*etcdraft.Consenter) (uint64, error) {
	// 将当前节点的证书转换为DER格式
	thisNodeCertAsDER, err := pemToDER(c.Cert, 0, "server", c.Logger)
	if err != nil {
		return 0, err
	}

	var serverCertificates []string // 用于存储所有共识者证书的字符串表示

	// 遍历共识者映射
	for nodeID, consenter := range consenters {
		// 将共识者的服务器TLS证书添加到证书列表中
		serverCertificates = append(serverCertificates, string(consenter.ServerTlsCert))

		// 将共识者的证书转换为DER格式
		certAsDER, err := pemToDER(consenter.ServerTlsCert, nodeID, "server", c.Logger)
		if err != nil {
			return 0, err
		}

		// 检查当前节点的证书公钥是否与共识者证书的公钥相同
		if crypto.CertificatesWithSamePublicKey(thisNodeCertAsDER, certAsDER) == nil {
			// 如果找到匹配，返回共识者映射中的节点ID
			return nodeID, nil
		}
	}

	// 如果没有找到匹配的共识者，日志警告并返回错误
	c.Logger.Warning("未找到", string(c.Cert), "在共识者列表中:", serverCertificates)
	return 0, cluster.ErrNotInChannel
}

// HandleChain 返回一个新的Chain实例或在失败时返回错误
// metadata就是最新区块
func (c *Consenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	m := &etcdraft.ConfigMetadata{}
	// 尝试反序列化共识元数据，如果失败则返回错误
	if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
		return nil, errors.Wrap(err, "反序列化共识元数据失败")
	}

	// 检查etcdraft配置选项是否为空
	if m.Options == nil {
		return nil, errors.New("etcdraft选项未提供")
	}

	// 判断是否为迁移场景（即元数据为空且链高度大于1）
	isMigration := (metadata == nil || len(metadata.Value) == 0) && (support.Height() > 1)
	if isMigration {
		c.Logger.Debugf("在块高度=%d处,区块元数据为空,这表示共识类型迁移", support.Height())
	}

	// 根据提供的元数据和配置元数据读取Raft元数据
	blockMetadata, err := ReadBlockMetadata(metadata, m)
	if err != nil {
		return nil, errors.Wrapf(err, "读取Raft元数据失败")
	}

	// 根据blockMetadata和配置元数据创建共识者映射表
	consenters := CreateConsentersMap(blockMetadata, m)

	// 检测并确定当前节点的ID
	id, err := c.detectSelfID(consenters)
	if err != nil {
		// 如果有系统通道，则使用InactiveChainRegistry跟踪应用通道的未来配置更新
		if c.InactiveChainRegistry != nil {
			c.InactiveChainRegistry.TrackChain(support.ChannelID(), support.Block(0), func() {
				// 回调创建一个新的共识链实例，通常在通道启动或重新配置时调用。
				c.ChainManager.CreateChain(support.ChannelID())
			})
			// 上报共识关系和状态的度量信息，用于监控和分析共识算法的工作状态。
			c.ChainManager.ReportConsensusRelationAndStatusMetrics(support.ChannelID(), types.ConsensusRelationConfigTracker, types.StatusInactive)
			return &inactive.Chain{Err: errors.Errorf("通道 %s 不由我服务", support.ChannelID())}, nil
		}

		// 如果没有系统通道，则应已创建跟随者
		return nil, errors.Wrap(err, "无系统通道，应已创建跟随者")
	}

	// 节点在这段时间内无领导的情况下开始怀疑自己被驱逐的阈值
	var evictionSuspicion time.Duration
	if c.EtcdRaftConfig.EvictionSuspicion == "" {
		c.Logger.Infof("未设置驱逐嫌疑，默认使用 %v : 节点在这段时间内无领导的情况下开始怀疑自己被驱逐的阈值", DefaultEvictionSuspicion)
		evictionSuspicion = DefaultEvictionSuspicion
	} else {
		evictionSuspicion, err = time.ParseDuration(c.EtcdRaftConfig.EvictionSuspicion)
		if err != nil {
			c.Logger.Panicf("解析 Consensus.EvictionSuspicion 失败: %s: %v", c.EtcdRaftConfig.EvictionSuspicion, err)
		}
	}

	// 解析心跳间隔时间，优先使用配置覆盖
	var tickInterval time.Duration
	if c.EtcdRaftConfig.TickIntervalOverride == "" {
		tickInterval, err = time.ParseDuration(m.Options.TickInterval)
		if err != nil {
			return nil, errors.Errorf("解析TickInterval (%s) 为时间间隔失败", m.Options.TickInterval)
		}
	} else {
		tickInterval, err = time.ParseDuration(c.EtcdRaftConfig.TickIntervalOverride)
		if err != nil {
			return nil, errors.WithMessage(err, "解析Consensus.TickIntervalOverride失败")
		}
		c.Logger.Infof("TickIntervalOverride已设置，覆盖通道配置的心跳间隔为 %v", tickInterval)
	}

	// 初始化Raft共识算法的选项配置
	// 这些配置项涵盖了RPC超时时间、Raft节点ID、时钟源、存储方式、日志记录器，
	// 以及Raft算法的具体参数，如选举超时倍数、心跳超时倍数、最大同时飞行中的区块数量、单条消息的最大尺寸、快照间隔大小等。
	// 此外，还包括了是否为迁移初始化、WAL和Snap目录的路径、节点驱逐嫌疑时间阈值等关键设置。
	opts := Options{ // 定义Raft共识算法的配置选项
		RPCTimeout:    c.OrdererConfig.General.Cluster.RPCTimeout, // RPC请求超时时间
		RaftID:        id,                                         // Raft节点的唯一ID
		Clock:         clock.NewClock(),                           // 创建新的时钟源，用于Raft算法中的时间计算
		MemoryStorage: raft.NewMemoryStorage(),                    // 使用内存存储方式，用于临时存储Raft状态
		Logger:        c.Logger,                                   // 日志记录器，用于记录Raft算法运行的日志信息

		// 下面的参数均为Raft算法的具体配置参数
		TickInterval:         tickInterval,                                                 // Tick事件的触发间隔时间
		ElectionTick:         int(m.Options.ElectionTick),                                  // 选举超时的Tick倍数
		HeartbeatTick:        int(m.Options.HeartbeatTick),                                 // 心跳超时的Tick倍数
		MaxInflightBlocks:    int(m.Options.MaxInflightBlocks),                             // 最大同时飞行中的区块数量
		MaxSizePerMsg:        uint64(support.SharedConfig().BatchSize().PreferredMaxBytes), // 单条消息的最大尺寸
		SnapshotIntervalSize: m.Options.SnapshotIntervalSize,                               // 快照间隔大小，决定何时生成快照

		// 下面的参数与共识状态和初始化相关
		BlockMetadata: blockMetadata, // 区块元数据
		Consenters:    consenters,    // 当前共识者列表

		// 下面的参数与迁移和持久化存储有关
		MigrationInit:     isMigration,                                              // 是否为迁移初始化
		WALDir:            path.Join(c.EtcdRaftConfig.WALDir, support.ChannelID()),  // WAL数据的存储目录
		SnapDir:           path.Join(c.EtcdRaftConfig.SnapDir, support.ChannelID()), // 快照数据的存储目录
		EvictionSuspicion: evictionSuspicion,                                        // 节点驱逐嫌疑时间阈值
		Cert:              c.Cert,                                                   // 当前节点的证书信息
		Metrics:           c.Metrics,                                                // 监控指标，用于性能监控
	}

	// 初始化RPC相关配置
	rpc := &cluster.RPC{
		Timeout:       c.OrdererConfig.General.Cluster.RPCTimeout,
		Logger:        c.Logger,
		Channel:       support.ChannelID(),
		Comm:          c.Communication,
		StreamsByType: cluster.NewStreamsByType(),
	}

	// 定义haltCallback，根据是否有系统通道，在检测到从集群中被驱逐后执行不同的回调操作
	var haltCallback func()
	if c.InactiveChainRegistry != nil {
		haltCallback = func() {
			c.InactiveChainRegistry.TrackChain(support.ChannelID(), nil, func() {
				// 回调创建一个新的共识链实例，通常在通道启动或重新配置时调用。
				c.ChainManager.CreateChain(support.ChannelID())
			})
			// 上报共识关系和状态的度量信息，用于监控和分析共识算法的工作状态。
			c.ChainManager.ReportConsensusRelationAndStatusMetrics(support.ChannelID(), types.ConsensusRelationConfigTracker, types.StatusInactive)
		}
	} else {
		// 将指定通道的共识链切换到跟随者(Follower)模式，通常在网络分区或节点疑似被驱逐时调用。
		haltCallback = func() { c.ChainManager.SwitchChainToFollower(support.ChannelID()) }
	}

	// 使用配置的参数创建新的Chain实例，用于处理特定通道上的交易和区块管理
	return NewChain(
		// 提供共识层支持接口，用于访问账本、状态数据库等底层服务
		support,
		// raft选项配置，可能包含一些高级配置项或回调函数
		opts,
		// 通信层，用于与网络中的其他节点进行通信
		c.Communication,
		// RPC服务，用于处理远程过程调用，提供外部访问接口
		rpc,
		// BCCSP实例，用于加密、签名和验证等安全相关操作
		c.BCCSP,
		// BlockPuller工厂函数，用于创建BlockPuller实例，负责从远程节点拉取区块数据
		func() (BlockPuller, error) {
			// 使用支持接口、标准Dialer、集群配置和BCCSP创建BlockPuller实例
			return NewBlockPuller(support, c.Dialer, c.OrdererConfig.General.Cluster, c.BCCSP)
		},
		// 停止回调函数，当链实例停止时会被调用，用于执行清理操作
		haltCallback,
		// 保留参数，可能用于未来扩展，目前设置为nil
		nil,
	)
}

func (c *Consenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	if joinBlock == nil {
		return false, errors.New("nil block")
	}
	envelopeConfig, err := protoutil.ExtractEnvelope(joinBlock, 0)
	if err != nil {
		return false, err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, c.BCCSP)
	if err != nil {
		return false, err
	}
	oc, exists := bundle.OrdererConfig()
	if !exists {
		return false, errors.New("no orderer config in bundle")
	}
	configMetadata := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), configMetadata); err != nil {
		return false, err
	}

	verifyOpts, err := createX509VerifyOptions(oc)
	if err != nil {
		return false, errors.Wrapf(err, "无法从 orderer 区块配置创建 x509 验证选项")
	}

	if err := VerifyConfigMetadata(configMetadata, verifyOpts); err != nil {
		return false, errors.Wrapf(err, "无法验证 orderer 配置的共识配置元数据")
	}

	consenters := make(map[uint64]*etcdraft.Consenter)
	for i, c := range configMetadata.Consenters {
		consenters[uint64(i+1)] = c // the IDs don't matter
	}

	if _, err := c.detectSelfID(consenters); err != nil {
		if err != cluster.ErrNotInChannel {
			return false, errors.Wrapf(err, "failed to detect self ID by comparing public keys")
		}
		return false, nil
	}

	return true, nil
}

// RemoveInactiveChainRegistry stops and removes the inactive chain registry.
// This is used when removing the system channel.
func (c *Consenter) RemoveInactiveChainRegistry() {
	if c.InactiveChainRegistry == nil {
		return
	}
	c.InactiveChainRegistry.Stop()
	c.InactiveChainRegistry = nil
}

// ReadBlockMetadata 尝试从区块元数据中读取Raft元数据，如果不可用，则从提供的配置元数据中读取。
func ReadBlockMetadata(blockMetadata *common.Metadata, configMetadata *etcdraft.ConfigMetadata) (*etcdraft.BlockMetadata, error) {
	// 首先检查区块元数据是否存在且非空
	if blockMetadata != nil && len(blockMetadata.Value) != 0 {
		// 将元数据反序列化为etcdraft.BlockMetadata结构体
		m := &etcdraft.BlockMetadata{}
		if err := proto.Unmarshal(blockMetadata.Value, m); err != nil {
			return nil, errors.Wrap(err, "反序列化区块元数据失败")
		}
		// 成功反序列化，直接返回
		return m, nil
	}

	// 如果区块元数据不存在或为空，则从配置元数据中初始化Raft元数据
	m := &etcdraft.BlockMetadata{
		NextConsenterId: 1,                                              // 初始化下一个共识者ID为1
		ConsenterIds:    make([]uint64, len(configMetadata.Consenters)), // 创建共识者ID数组
	}
	// 为每个共识者分配一个ID
	for i := range m.ConsenterIds {
		m.ConsenterIds[i] = m.NextConsenterId
		m.NextConsenterId++ // 增加下一个共识者ID
	}

	// 返回初始化后的Raft元数据
	return m, nil
}

// New creates a etcdraft Consenter
func New(
	clusterDialer *cluster.PredicateDialer,
	conf *localconfig.TopLevel,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	registrar ChainManager,
	icr InactiveChainRegistry,
	metricsProvider metrics.Provider,
	bccsp bccsp.BCCSP,
) *Consenter {
	logger := flogging.MustGetLogger("orderer.consensus.etcdraft")

	var cfg Config
	err := mapstructure.Decode(conf.Consensus, &cfg)
	if err != nil {
		logger.Panicf("Failed to decode etcdraft configuration: %s", err)
	}

	consenter := &Consenter{
		ChainManager:          registrar,
		Cert:                  srvConf.SecOpts.Certificate,
		Logger:                logger,
		EtcdRaftConfig:        cfg,
		OrdererConfig:         *conf,
		Dialer:                clusterDialer,
		Metrics:               NewMetrics(metricsProvider),
		InactiveChainRegistry: icr,
		BCCSP:                 bccsp,
	}
	consenter.Dispatcher = &Dispatcher{
		Logger:        logger,
		ChainSelector: consenter,
	}

	comm := createComm(clusterDialer, consenter, conf.General.Cluster, metricsProvider)
	consenter.Communication = comm
	svc := &cluster.Service{
		CertExpWarningThreshold:          conf.General.Cluster.CertExpirationWarningThreshold,
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		StreamCountReporter: &cluster.StreamCountReporter{
			Metrics: comm.Metrics,
		},
		StepLogger: flogging.MustGetLogger("orderer.common.cluster.step"),
		Logger:     flogging.MustGetLogger("orderer.common.cluster"),
		Dispatcher: comm,
	}
	orderer.RegisterClusterServer(srv.Server(), svc)

	if icr == nil {
		logger.Debug("Created an etcdraft consenter without a system channel, InactiveChainRegistry is nil")
	}

	return consenter
}

func createComm(clusterDialer *cluster.PredicateDialer, c *Consenter, config localconfig.Cluster, p metrics.Provider) *cluster.Comm {
	metrics := cluster.NewMetrics(p)
	logger := flogging.MustGetLogger("orderer.common.cluster")

	compareCert := cluster.CachePublicKeyComparisons(func(a, b []byte) bool {
		err := crypto.CertificatesWithSamePublicKey(a, b)
		if err != nil && err != crypto.ErrPubKeyMismatch {
			crypto.LogNonPubKeyMismatchErr(logger.Errorf, err, a, b)
		}
		return err == nil
	})

	comm := &cluster.Comm{
		MinimumExpirationWarningInterval: cluster.MinimumExpirationWarningInterval,
		CertExpWarningThreshold:          config.CertExpirationWarningThreshold,
		SendBufferSize:                   config.SendBufferSize,
		Logger:                           logger,
		Chan2Members:                     make(map[string]cluster.MemberMapping),
		Connections:                      cluster.NewConnectionStore(clusterDialer, metrics.EgressTLSConnectionCount),
		Metrics:                          metrics,
		ChanExt:                          c,
		H:                                c,
		CompareCertificate:               compareCert,
	}
	c.Communication = comm
	return comm
}
