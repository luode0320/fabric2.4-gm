/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"context"
	"encoding/pem"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/clock"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/types"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
)

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
)

const (
	// DefaultSnapshotCatchUpEntries 是默认的快照追加条目数量，当创建快照时，会在内存中保留一定数量的条目。
	// 这是为了让慢速跟随者能够追赶最新的状态，避免立即丢失所有未持久化的更改。
	DefaultSnapshotCatchUpEntries = uint64(4)

	// DefaultSnapshotIntervalSize 是默认的快照间隔大小，如果在通道配置选项中没有提供 SnapshotIntervalSize，
	// 将使用此值。它用于确保快照机制的激活，避免日志条目无限增长，降低存储压力。
	DefaultSnapshotIntervalSize = 16 * MEGABYTE

	// DefaultEvictionSuspicion 是节点在没有领导者的情况下开始怀疑自己可能被集群驱逐的阈值时间。
	// 如果节点在这段时间内未能检测到任何领导者活动，它将开始怀疑自己可能已被集群排除在外。
	DefaultEvictionSuspicion = time.Minute * 10

	// DefaultLeaderlessCheckInterval 是链实例检查自身领导状态的默认间隔时间。
	// 链实例会定期检查自己是否仍然是领导者，如果不是，它将采取适当的行动，比如放弃领导权或退出集群。
	DefaultLeaderlessCheckInterval = time.Second * 10

	// AbdicationMaxAttempts 确定了在移除自身参与重新配置的事务中，放弃领导权的最大重试次数。
	// 当节点尝试退出集群时，它可能会遇到一些问题，比如网络延迟或冲突，因此设置了重试机制。
	// 如果在规定的尝试次数内仍无法成功放弃领导权，节点将采取进一步的措施，比如进入故障状态或寻求外部干预。
	AbdicationMaxAttempts = 5
)

//go:generate counterfeiter -o mocks/configurator.go . Configurator

// Configurator 是一个接口，用于在链启动时配置通信层。
// 当链启动时，它会被用来更新通信层的配置，以反映最新的网络拓扑。
// 这通常涉及到添加或删除远程节点，以确保通信层能够正确地与当前的共识组成员通信。
type Configurator interface {
	// Configure 方法接收一个通道名和一个新节点的列表。
	// 它应该更新通信层的配置，以包括或排除这些节点。
	// 这样做是为了确保通信层能够与最新成员列表中的所有节点进行有效通信。
	Configure(channel string, newNodes []cluster.RemoteNode)
}

//go:generate counterfeiter -o mocks/mock_rpc.go . RPC

// RPC 接口用于在测试中模拟传输层的行为。
// 它定义了共识参与者之间通信的基本方法，允许我们发送共识请求和提交请求到远程节点。
type RPC interface {
	// SendConsensus 方法用于向指定的目标节点（dest）发送共识请求（msg）。
	// 这是共识算法（如Raft）中用于同步状态和达成一致决策的关键步骤之一。
	// 如果在发送过程中发生错误，该方法应返回错误。
	SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error

	// SendSubmit 方法用于向指定的目标节点（dest）发送提交请求（request）。
	// 此方法还接受一个回调函数（report）来报告任何在异步处理中发生的错误。
	// 这通常用于提交事务到共识组，等待最终确认。
	SendSubmit(dest uint64, request *orderer.SubmitRequest, report func(err error)) error
}

//go:generate counterfeiter -o mocks/mock_blockpuller.go . BlockPuller

// BlockPuller 接口定义了一个从其他 Orderer Service Node(OSN) 拉取区块的组件。
// 它提供了以下功能：
//
// PullBlock: 根据给定的序列号(seq)从网络中拉取对应的区块。
// HeightsByEndpoints: 返回一个映射，其中键是远程OSN的端点(endpoint)，值是该OSN上的最新区块高度。
// Close: 关闭BlockPuller资源，例如关闭网络连接等。
type BlockPuller interface {
	// 根据区块序列号拉取区块
	PullBlock(seq uint64) *common.Block
	// 获取每个远程OSN的最新区块高度
	HeightsByEndpoints() (map[string]uint64, error)
	// 关闭BlockPuller并释放相关资源
	Close()
}

// CreateBlockPuller 是一个函数类型，用于按需创建 BlockPuller 实例。
// 这个函数被传递给链初始化器，以便在测试中能够模拟 BlockPuller 的行为。
// BlockPuller 是用于从其他节点拉取区块的组件，CreateBlockPuller 函数允许系统动态生成这个组件，
// 提供了灵活性和可测试性，特别是在单元测试和集成测试中，可以使用假的（mock）BlockPuller 来替代真实的实现。
type CreateBlockPuller func() (BlockPuller, error)

// Options 结构体包含了与链raft相关的所有配置信息。
type Options struct {
	// RPCTimeout 是进行远程过程调用（RPC）时的超时时间。
	RPCTimeout time.Duration

	// RaftID 是当前节点在Raft共识算法中的唯一标识符。
	RaftID uint64

	// Clock 是时间处理的接口，允许在测试环境中模拟时间流逝。
	Clock clock.Clock

	// WALDir 是写入前日志（Write-Ahead Log）的存储目录。
	WALDir string

	// SnapDir 是快照文件的存储目录。
	SnapDir string

	// SnapshotIntervalSize 是触发快照创建的区块大小阈值。
	SnapshotIntervalSize uint32

	// SnapshotCatchUpEntries 是可配置的主要用于测试目的的参数。它表示在快照恢复时需要追加的条目数量。
	SnapshotCatchUpEntries uint64

	// MemoryStorage 是存储在内存中的数据结构，用于存储和检索Raft状态。
	MemoryStorage MemoryStorage

	// Logger 是用于记录日志的结构体。
	Logger *flogging.FabricLogger

	// TickInterval 是Raft算法中心跳和选举的周期。
	TickInterval time.Duration

	// ElectionTick 是触发选举的tick次数。
	ElectionTick int

	// HeartbeatTick 是发送心跳的tick次数。
	HeartbeatTick int

	// MaxSizePerMsg 是单个Raft消息的最大尺寸。
	MaxSizePerMsg uint64

	// MaxInflightBlocks 是同时处理的未决块的最大数量。
	MaxInflightBlocks int

	// BlockMetadata 和 Consenters 应在 raftMetadataLock 锁定下进行修改。
	// BlockMetadata 包含关于块元数据的信息。
	BlockMetadata *etcdraft.BlockMetadata

	// Consenters 是当前共识组的成员列表。
	Consenters map[uint64]*etcdraft.Consenter

	// MigrationInit 是在共识类型迁移后启动节点时设置的标志。
	MigrationInit bool

	// Metrics 是用于收集和报告共识层性能指标的结构体。
	Metrics *Metrics

	// Cert 是节点的证书，用于身份验证和加密通信。
	Cert []byte

	// EvictionSuspicion 是怀疑节点被驱逐的等待时间。
	EvictionSuspicion time.Duration

	// LeaderCheckInterval 是检查领导权状态的间隔时间。
	LeaderCheckInterval time.Duration
}

type submit struct {
	req    *orderer.SubmitRequest // 提交请求
	leader chan uint64            // 领导者通道
}

type gc struct {
	index uint64
	state raftpb.ConfState
	data  []byte
}

// Chain 结构体实现了共识层的 Chain 接口，它是 etcd/raft 状态机在特定通道上的具体实现。
type Chain struct {
	// configurator 用于配置和管理共识层的组件。
	configurator Configurator

	// rpc 是远程过程调用服务，用于处理外部请求和与其他节点的通信。
	rpc RPC

	// raftID 是 raft 状态机的唯一标识符，用于区分不同的 raft 群集。
	raftID uint64
	// channelID 是当前链所属的通道标识符，用于标识特定的业务逻辑和数据隔离。
	channelID string

	// lastKnownLeader 是上一个已知的领导者节点的 raftID，用于在领导者变更时的快速恢复。
	lastKnownLeader uint64
	// ActiveNodes 是一个原子变量，用于存储当前活跃节点的列表，方便在集群中快速查找和通知。
	ActiveNodes atomic.Value

	// 是提交通道，用于接收来自客户端的提案(收集到这里后才可以发送)。
	submitC chan *submit
	// 是应用通道，用于接收 raft 状态机应用的条目，触发本地状态的更新。
	applyC chan apply
	// 是观察者通道，用于通知外部观察者领导者的变更。
	observeC chan<- raft.SoftState
	// 是停止通道，用于信号告知协程链正在停止。
	haltC chan struct{}
	// 是完成通道，当链停止时关闭，用于等待链完全停止。
	doneC chan struct{}
	// 是启动通道，当节点启动时关闭，用于同步节点启动状态。
	startC chan struct{}
	// 是快照通道，用于通知 raft 状态机追赶快照。
	snapC chan *raftpb.Snapshot
	// 是垃圾回收通道，用于触发快照的生成和旧状态的清理。
	gcC chan *gc

	// errorCLock 和 errorC 用于控制和监控错误状态。
	errorCLock sync.RWMutex
	errorC     chan struct{}

	// raftMetadataLock 和 confChangeInProgress 用于控制配置变更的元数据。
	raftMetadataLock     sync.RWMutex
	confChangeInProgress *raftpb.ConfChange
	// justElected 和 configInflight 用于标记节点状态和配置变更的执行状态。
	justElected    bool
	configInflight bool
	// blockInflight 用于统计在飞行中的块的数量。
	blockInflight int

	// clock 用于提供时间服务，测试时可注入假时钟。
	clock clock.Clock

	// support 是共识层的支持接口，用于访问账本、状态数据库等底层服务。
	support consensus.ConsenterSupport

	// lastBlock 和 appliedIndex 用于追踪最后一个应用的区块和当前已应用的条目索引。
	lastBlock    *common.Block
	appliedIndex uint64

	// sizeLimit、accDataSize 和 lastSnapBlockNum 用于控制和追踪快照生成的时机和状态。
	sizeLimit        uint32
	accDataSize      uint32
	lastSnapBlockNum uint64
	// confState 用于存储 raft 状态机的配置状态，需在快照中持久化。
	confState raftpb.ConfState

	// createPuller 是一个函数，用于按需创建 BlockPuller 实例，实现区块的远程拉取。
	createPuller CreateBlockPuller

	// fresh 用于标记这是不是一个全新的 raft 节点，初始启动时使用, 由是否存在共识日志文件判断
	fresh bool

	// Node 是 raft 状态机的节点实例，对外暴露状态和操作。
	Node *node
	// opts 是raft链的选项配置，可能包含一些高级配置项或回调函数。
	opts Options

	// Metrics 是用于收集和报告链运行状态的指标。
	Metrics *Metrics
	// logger 是日志记录器，用于记录调试和错误信息。
	logger *flogging.FabricLogger

	// periodicChecker 用于周期性地检查链的状态和执行维护任务。
	periodicChecker *PeriodicCheck

	// haltCallback 是停止回调函数，当链实例停止时会被调用，用于执行清理操作。
	haltCallback func()

	// statusReportMutex、consensusRelation 和 status 用于控制和报告链的运行状态。
	statusReportMutex sync.Mutex
	consensusRelation types.ConsensusRelation
	status            types.Status

	// CryptoProvider 是 BCCSP 实例，用于加密、签名和验证等安全相关操作。
	CryptoProvider bccsp.BCCSP

	// leadershipTransferInProgress 用于标记领导权转移是否正在进行中。
	leadershipTransferInProgress uint32
}

// NewChain 构建了一个新的 Chain 对象，用于管理特定通道上的共识和通信。
func NewChain(
	support consensus.ConsenterSupport, // 支持共识操作的接口，提供通道和共识相关的方法
	rafts Options, // 配置选项，包括Raft ID、日志记录器、存储路径等
	conf Configurator, // 配置管理器，用于更新和管理共识配置
	rpc RPC, // 远程过程调用接口，用于与远程节点通信
	cryptoProvider bccsp.BCCSP, // BCCSP（Blockchain Crypto Service Provider）接口，提供加密服务
	f CreateBlockPuller, // 创建区块拉取器的函数，用于从其他节点拉取缺失的区块
	haltCallback func(), // 回调函数，用于在共识停止时执行一些清理操作
	observeC chan<- raft.SoftState, // 观察者通道，用于发送Raft软状态信息
) (*Chain, error) {
	// 创建带有通道ID和节点ID的日志记录器
	lg := rafts.Logger.With("channel", support.ChannelID(), "node", rafts.RaftID)

	// 检查WAL目录是否存在，用于判断是否是新链
	fresh := !wal.Exist(rafts.WALDir)
	storage, err := CreateStorage(lg, rafts.WALDir, rafts.SnapDir, rafts.MemoryStorage) // 创建存储，包括WAL和快照目录
	if err != nil {
		return nil, errors.Errorf("恢复持久化的Raft数据失败: %s", err) // 如果创建存储失败，返回错误
	}

	// 设置SnapshotCatchUpEntries和SnapshotIntervalSize的默认值, 它表示在快照恢复时需要追加的条目数量。
	if rafts.SnapshotCatchUpEntries == 0 {
		storage.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	} else {
		storage.SnapshotCatchUpEntries = rafts.SnapshotCatchUpEntries
	}

	// 触发快照创建的区块大小阈值。
	sizeLimit := rafts.SnapshotIntervalSize
	if sizeLimit == 0 {
		sizeLimit = DefaultSnapshotIntervalSize
	}

	// 获取最新快照中的区块编号
	var snapBlkNum uint64
	var cc raftpb.ConfState
	if s := storage.Snapshot(); !raft.IsEmptySnap(s) {
		b := protoutil.UnmarshalBlockOrPanic(s.Data)
		snapBlkNum = b.Header.Number
		cc = s.Metadata.ConfState
	}

	// 获取通道上最后一个区块
	b := support.Block(support.Height() - 1)
	if b == nil {
		return nil, errors.Errorf("获取最后一个区块失败")
	}
	// 创建 Chain 结构体实例，初始化其各个字段。
	c := &Chain{
		// 配置管理器和 RPC 服务初始化
		configurator: conf, // conf 是配置管理器，用于管理共识层的配置。
		rpc:          rpc,  // rpc 是远程过程调用服务，用于处理与外部的通信。

		// 通道和 Raft 相关标识初始化
		channelID: support.ChannelID(), // support.ChannelID() 返回当前链所属的通道标识符。
		raftID:    rafts.RaftID,        // rafts.RaftID 是当前节点在 Raft 群集中的唯一标识符。

		// 通道初始化，用于内部通信和状态控制
		submitC: make(chan *submit),          // submitC 是用于接收提交请求的通道。
		applyC:  make(chan apply),            // applyC 是用于接收应用结果的通道。
		haltC:   make(chan struct{}),         // haltC 用于发送停止信号给 Chain 的内部协程。
		doneC:   make(chan struct{}),         // doneC 在 Chain 停止后关闭，用于外部等待 Chain 完全停止。
		startC:  make(chan struct{}),         // startC 在 Chain 启动后关闭，用于外部等待 Chain 完全启动。
		snapC:   make(chan *raftpb.Snapshot), // snapC 用于通知 Chain 追赶快照。
		errorC:  make(chan struct{}),         // errorC 在发生错误时关闭，用于外部检测 Chain 是否出现错误。
		gcC:     make(chan *gc),              // gcC 用于通知 Chain 执行垃圾回收。

		// 观察者通道初始化
		observeC: observeC, // 用于向外部观察者发送软状态更新，如领导权变更。

		// 支持服务和新鲜度标志初始化
		support: support, // support 是共识层的支持服务接口，用于访问账本、状态数据库等底层服务。
		fresh:   fresh,   // fresh 标记 Chain 是否为新启动的。

		// 最后应用的区块和快照控制字段初始化
		appliedIndex:     rafts.BlockMetadata.RaftIndex, // raftIndex 是最后应用的区块在 Raft 日志中的索引。
		lastBlock:        b,                             // b 是最后应用的区块。
		sizeLimit:        sizeLimit,                     // sizeLimit 是快照生成的大小限制。
		lastSnapBlockNum: snapBlkNum,                    // snapBlkNum 是上次快照的区块编号。

		// 配置状态和拉取器初始化
		confState:    cc, // cc 是当前的配置状态。
		createPuller: f,  // f 是用于创建 BlockPuller 的函数，BlockPuller 用于从其他节点拉取区块。

		// 时间服务和停止回调初始化
		clock:        rafts.Clock,  // rafts.Clock 是时间服务，用于提供系统时钟。
		haltCallback: haltCallback, // haltCallback 是在 Chain 停止时被调用的回调函数。

		// 共识关系和状态初始化
		consensusRelation: types.ConsensusRelationConsenter, // ConsensusRelationConsenter 表示共识关系类型。
		status:            types.StatusActive,               // StatusActive 表示 Chain 当前处于活动状态。

		// 监控指标初始化
		Metrics: &Metrics{ // Metrics 是一组用于监控 Chain 运行状态的指标。
			// 下面是一系列 Prometheus 监控指标的初始化，每个指标都与共识层的特定方面相关联。
			ClusterSize:             rafts.Metrics.ClusterSize.With("channel", support.ChannelID()),             // ClusterSize 监控当前群集大小。
			IsLeader:                rafts.Metrics.IsLeader.With("channel", support.ChannelID()),                // IsLeader 监控当前节点是否是领导者。
			ActiveNodes:             rafts.Metrics.ActiveNodes.With("channel", support.ChannelID()),             // ActiveNodes 监控当前活跃节点数。
			CommittedBlockNumber:    rafts.Metrics.CommittedBlockNumber.With("channel", support.ChannelID()),    // CommittedBlockNumber 监控已提交的区块数量。
			SnapshotBlockNumber:     rafts.Metrics.SnapshotBlockNumber.With("channel", support.ChannelID()),     // SnapshotBlockNumber 监控快照的区块编号。
			LeaderChanges:           rafts.Metrics.LeaderChanges.With("channel", support.ChannelID()),           // LeaderChanges 监控领导权变更次数。
			ProposalFailures:        rafts.Metrics.ProposalFailures.With("channel", support.ChannelID()),        // ProposalFailures 监控提案失败次数。
			DataPersistDuration:     rafts.Metrics.DataPersistDuration.With("channel", support.ChannelID()),     // DataPersistDuration 监控数据持久化耗时。
			NormalProposalsReceived: rafts.Metrics.NormalProposalsReceived.With("channel", support.ChannelID()), // NormalProposalsReceived 监控接收到的常规提案数量。
			ConfigProposalsReceived: rafts.Metrics.ConfigProposalsReceived.With("channel", support.ChannelID()), // ConfigProposalsReceived 监控接收到的配置变更提案数量。
		},

		// 日志记录器和选项初始化
		logger: lg,    // lg 是用于记录日志的日志记录器。
		opts:   rafts, // rafts 是 Chain 的选项配置，可能包含高级配置项或回调函数。

		// 密码服务提供者初始化
		CryptoProvider: cryptoProvider, // cryptoProvider 是 BCCSP 实例，用于加密、签名和验证等安全相关操作。
	}

	// 设置监控指标的初始值
	c.Metrics.ClusterSize.Set(float64(len(c.opts.BlockMetadata.ConsenterIds))) // 设置群集大小，即参与共识的节点数。
	c.Metrics.IsLeader.Set(float64(0))                                         // 初始时所有节点都是跟随者，因此IsLeader设为0。
	c.Metrics.ActiveNodes.Set(float64(0))                                      // 初始时活动节点数为0。
	c.Metrics.CommittedBlockNumber.Set(float64(c.lastBlock.Header.Number))     // 设置已提交的区块数量，基于最后一个区块的编号。
	c.Metrics.SnapshotBlockNumber.Set(float64(c.lastSnapBlockNum))             // 设置快照的区块编号。

	// 配置Raft节点
	config := &raft.Config{
		// 设置Raft配置参数
		ID:                        c.raftID,                 // 当前节点在Raft群集中的唯一ID。
		ElectionTick:              c.opts.ElectionTick,      // 选举超时时间，单位为心跳周期数。
		HeartbeatTick:             c.opts.HeartbeatTick,     // 心跳周期，用于保持与群集中其他节点的联系。
		MaxSizePerMsg:             c.opts.MaxSizePerMsg,     // 单个消息的最大尺寸。
		MaxInflightMsgs:           c.opts.MaxInflightBlocks, // 允许同时在飞行（未确认）的消息数量。
		Logger:                    c.logger,                 // 日志记录器，用于记录Raft节点的操作和状态。
		Storage:                   c.opts.MemoryStorage,     // 存储实现，用于存储和检索Raft日志和状态。
		PreVote:                   true,                     // 预投票机制，防止重新连接的节点干扰网络。
		CheckQuorum:               true,                     // 在选举和心跳时检查是否达到法定人数。
		DisableProposalForwarding: true,                     // 禁止跟随者转发提案，防止意外提案区块。
	}

	// 创建Disseminator实例，用于在节点间传播信息,使用c.rpc作为RPC服务。
	disseminator := &Disseminator{RPC: c.rpc}
	disseminator.UpdateMetadata(nil) // 初始化Disseminator，可能涉及元数据的更新。
	c.ActiveNodes.Store([]uint64{})  // 初始化活动节点列表，使用原子存储确保线程安全。

	// 创建Node实例，即Raft节点
	c.Node = &node{
		chainID:      c.channelID,          // 当前节点所属的通道ID。
		chain:        c,                    // 当前的Chain实例，用于处理事务和状态变更。
		logger:       c.logger,             // 日志记录器，用于记录Node的操作和状态。
		metrics:      c.Metrics,            // 监控指标，用于监控Node的运行状态。
		storage:      storage,              // 存储实现，用于存储和检索区块链数据。
		rpc:          disseminator,         // Disseminator实例，用于节点间的信息传播。
		config:       config,               // Raft配置参数。
		tickInterval: c.opts.TickInterval,  // Raft心跳周期，单位为时间间隔。
		clock:        c.clock,              // 时间服务，用于提供系统时钟。
		metadata:     c.opts.BlockMetadata, // 区块元数据，包含共识层的配置和状态信息。
		tracker: &Tracker{
			id:     c.raftID,              // 节点ID，用于标识节点。
			sender: disseminator,          // 发送者，用于发送消息到其他节点。
			gauge:  c.Metrics.ActiveNodes, // 监控指标，用于监控活动节点的数量。
			active: &c.ActiveNodes,        // 活动节点列表，用于维护当前活动的节点集合。
			logger: c.logger,              // 日志记录器，用于记录Tracker的操作和状态。
		},
	}

	return c, nil // 返回构建完成的Chain实例
}

// Start 函数指示排序服务开始为链提供服务并保持其最新状态。
func (c *Chain) Start() {
	c.logger.Infof("启动 Raft 节点") // 日志记录，表明正在启动 Raft 节点。

	// 配置通信，如果配置失败，关闭 doneC 通道并返回，终止链的启动。
	if err := c.configureComm(); err != nil {
		c.logger.Errorf("启动链失败，正在中止: +%v", err)
		close(c.doneC)
		return
	}

	// 判断是否为加入现有链
	isJoin := c.support.Height() > 1
	if isJoin && c.opts.MigrationInit {
		// 如果是共识类型迁移，视为新节点启动而非加入。
		isJoin = false
		c.logger.Infof("检测到共识类型迁移, 正在现有通道上启动新的 Raft 节点, 高度=%d", c.support.Height())
	}

	// 调用 Node 的 start 方法，开始 Raft 节点的运行, 这里有接受其他节点消息的处理。
	c.Node.start(c.fresh, isJoin)

	// 关闭 startC 通道，表示链已经启动。
	close(c.startC)
	// 关闭 errorC 通道，表示链没有错误。
	close(c.errorC)

	// 启动垃圾回收协程，用于定期清理过期的快照和日志。
	go c.gc()
	// 启动主运行协程，负责处理提案、应用状态和通信等核心任务, 这里有发送给其他节点消息的处理。
	go c.run()

	// 创建一个 EvictionSuspector 实例，用于检测节点是否可能被驱逐。
	es := c.newEvictionSuspector()

	// 设置检查领导权状态的间隔，默认为 DefaultLeaderlessCheckInterval，可由选项覆盖。
	interval := DefaultLeaderlessCheckInterval
	if c.opts.LeaderCheckInterval != 0 {
		interval = c.opts.LeaderCheckInterval
	}

	// 创建并运行 PeriodicCheck 实例，定期检查并报告节点可能被驱逐的情况。
	c.periodicChecker = &PeriodicCheck{
		Logger:        c.logger,            // 日志记录器，用于记录 PeriodicCheck 的操作和状态。
		Report:        es.confirmSuspicion, // 当检测到可能被驱逐时，调用 es.confirmSuspicion 报告。
		ReportCleared: es.clearSuspicion,   // 当确定没有被驱逐风险时，调用 es.clearSuspicion 清除报警。
		CheckInterval: interval,            // 检查间隔，用于定期执行 Condition。
		Condition:     c.suspectEviction,   // 检测条件，用于判断节点是否可能被驱逐。
	}
	c.periodicChecker.Run() // 开始运行 PeriodicCheck，执行定期检查。
}

// Order 提交普通类型的事务以进行订购。
func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	c.Metrics.NormalProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// Configure 提交用于订购的配置类型事务。
func (c *Chain) Configure(env *common.Envelope, configSeq uint64) error {
	c.Metrics.ConfigProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// WaitReady 区块链时:
// - 正在使用快照赶上其他节点
//
// 在任何其他情况下，它立即返回。
func (c *Chain) WaitReady() error {
	if err := c.isRunning(); err != nil {
		return err
	}

	select {
	case c.submitC <- nil:
	case <-c.doneC:
		return errors.Errorf("链已停止")
	}

	return nil
}

// Errored 返回当链停止时关闭的通道。
func (c *Chain) Errored() <-chan struct{} {
	c.errorCLock.RLock()
	defer c.errorCLock.RUnlock()
	return c.errorC
}

// Halt 停止链条。
func (c *Chain) Halt() {
	c.stop()
}

// stop 尝试停止链并返回一个布尔值，表示链是否成功停止。
// 如果链尚未启动，函数会记录警告日志并返回 false。
// 如果链正在停止或已经停止，则会返回 true。
func (c *Chain) stop() bool {
	// 首先检查链是否已经启动。
	// 如果 startC 通道中还有值（即链还未开始启动），则尝试从通道接收，否则说明链已启动。
	select {
	case <-c.startC: // 链已启动，继续执行停止流程
	default:
		// 链尚未启动，记录警告日志并直接返回 false。
		c.logger.Warn("尝试停止一个尚未启动的链")
		return false
	}

	// 尝试向 haltC 通道发送一个空结构体，以触发停止流程。
	// 如果通道已关闭（链正在或已经停止），则会执行下面的 case 分支。
	select {
	case c.haltC <- struct{}{}: // 发送停止信号
	case <-c.doneC: // 如果链已完成停止，则直接返回 false，避免重复停止操作
		return false
	}

	// 等待 doneC 信号，确保链已经完成停止流程。
	<-c.doneC

	// 在互斥锁的保护下更新链的状态为不活动。
	c.statusReportMutex.Lock()
	defer c.statusReportMutex.Unlock()
	c.status = types.StatusInactive // 更新状态为不活动

	// 返回 true，表示链已成功停止。
	return true
}

// 停止链并调用 haltCallback 函数，该函数允许链在发现自身不再属于某个通道时，
// 将责任转移给跟随者节点或不活动链注册表。
func (c *Chain) halt() {
	// 首先尝试停止链。
	// stop 方法会返回一个布尔值，表示链是否已经停止。
	if stopped := c.stop(); !stopped {
		// 如果链已经被停止，那么不调用 haltCallback，并记录日志信息。
		c.logger.Info("此节点已被停止，haltCallback 将不会被调用")
		return
	}
	// 如果 haltCallback 不为空，则调用之。
	// 注意：调用 haltCallback 时不能持有任何内部锁，以避免死锁风险。
	if c.haltCallback != nil {
		c.haltCallback()

		// 在调用 haltCallback 后，确保在更新状态报告锁的保护下更新共识关系状态。
		c.statusReportMutex.Lock()
		defer c.statusReportMutex.Unlock()

		// 如果 haltCallback 将链注册到了不活动链注册表中（即系统通道存在），
		// 那么这是正确的共识关系类型。如果 haltCallback 将责任转移到了另一个跟随者节点的 Chain 上，
		// 则当前链即将被垃圾回收，而新的替代链将会报告正确的 StatusReport。
		c.consensusRelation = types.ConsensusRelationConfigTracker
	}

	// 链停止后，活跃节点的指标应当重置为零，避免指标冻结在链停止前的数值。
	c.Metrics.ActiveNodes.Set(float64(0))
}

// 检查通道共识是否已经启动
func (c *Chain) isRunning() error {
	// 检查链是否已经启动
	select {
	case <-c.startC:
	default:
		return errors.Errorf("通道共识尚未启动")
	}

	// 检查链是否已经停止
	select {
	case <-c.doneC:
		return errors.Errorf("通道共识已停止")
	default:
	}

	return nil
}

// Consensus 将给定的ConsensusRequest消息传递给raft.Node实例。
// 方法接收者：c（Chain类型的指针）
// 输入参数：
//   - req：ConsensusRequest消息。
//   - sender：发送者的ID。
//
// 返回值：
//   - error：如果在处理消息时出错，则返回错误。
func (c *Chain) Consensus(req *orderer.ConsensusRequest, sender uint64) error {
	// 检查链是否正在运行
	if err := c.isRunning(); err != nil {
		return err
	}

	// 修改为使用 Message 本身的序列化反式
	stepMsg := &raftpb.Message{}
	if err := stepMsg.Unmarshal(req.Payload); err != nil {
		return fmt.Errorf("无法将 StepRequest 有效负载反序列化为 raftpb.Message 消息: %s", err)
	}

	// 检查消息的目标节点是否与当前节点的raftID匹配
	if stepMsg.To != c.raftID {
		c.logger.Warnf("收到节点 %d 的消息, 此节点ID可能是错误的, 本节点为 %d ", stepMsg.To, c.raftID)
		c.halt()
		return nil
	}

	// 将消息传递给raft.Node实例进行处理
	if err := c.Node.Step(context.TODO(), *stepMsg); err != nil {
		return fmt.Errorf("Raft 步骤 Step 信息处理失败: %s", err)
	}

	// 如果ConsensusRequest的Metadata字段为空或发送者不是当前已知的leader，则忽略元数据
	if len(req.Metadata) == 0 || atomic.LoadUint64(&c.lastKnownLeader) != sender {
		return nil
	}

	// 解析ConsensusRequest的Metadata字段为ClusterMetadata
	clusterMetadata := &etcdraft.ClusterMetadata{}
	if err := proto.Unmarshal(req.Metadata, clusterMetadata); err != nil {
		return errors.Errorf("无法反序列化 etcdraft.ClusterMetadata: %s", err)
	}

	// 更新活跃节点的指标和存储
	c.Metrics.ActiveNodes.Set(float64(len(clusterMetadata.ActiveNodes)))
	c.ActiveNodes.Store(clusterMetadata.ActiveNodes)

	return nil
}

// Submit 方法将传入的请求转发至：
// - 如果当前节点是领导者，则转发至本地运行的 goroutine (跳过)
// - 如果当前节点不是领导者，则通过传输机制转发至实际领导者
// 如果尚未选举出领导者，则调用失败。
func (c *Chain) Submit(req *orderer.SubmitRequest, sender uint64) error {
	// 检查通道共识是否已经启动
	if err := c.isRunning(); err != nil {
		c.Metrics.ProposalFailures.Add(1)
		return err
	}

	leadC := make(chan uint64, 1)
	select {
	case c.submitC <- &submit{req, leadC}:
		lead := <-leadC
		if lead == raft.None {
			c.Metrics.ProposalFailures.Add(1)
			return errors.Errorf("没有 Raft 领导者")
		}

		if lead != c.raftID {
			// 转发给领导
			if err := c.forwardToLeader(lead, req); err != nil {
				return err
			}
		}

	case <-c.doneC:
		c.Metrics.ProposalFailures.Add(1)
		return errors.Errorf("通道共识已停止")
	}

	return nil
}

// 将交易转发至领导者
func (c *Chain) forwardToLeader(lead uint64, req *orderer.SubmitRequest) error {
	c.logger.Infof("将交易转发至领导者 %d", lead)
	timer := time.NewTimer(c.opts.RPCTimeout)
	defer timer.Stop()

	sentChan := make(chan struct{})
	atomicErr := &atomic.Value{}

	// report 函数用于处理发送请求后的结果
	report := func(err error) {
		if err != nil {
			atomicErr.Store(err.Error())
			c.Metrics.ProposalFailures.Add(1)
		}
		close(sentChan)
	}

	// 调用 RPC 发送请求至领导者
	c.rpc.SendSubmit(lead, req, report)

	select {
	case <-sentChan:
	case <-c.doneC:
		return errors.Errorf("链已停止")
	case <-timer.C:
		return errors.Errorf("转发至 %d 时超时 (%v)", lead, c.opts.RPCTimeout)
	}

	if atomicErr.Load() != nil {
		return errors.Errorf(atomicErr.Load().(string))
	}
	return nil
}

// raft的日志和状态结构体
type apply struct {
	entries []raftpb.Entry  // 用于存储 Raft 日志条目
	soft    *raft.SoftState // 用于存储 Raft 节点的软状态信息(节点id)
}

// 是否是候选者
func isCandidate(state raft.StateType) bool {
	return state == raft.StatePreCandidate || state == raft.StateCandidate
}

// 启动主运行协程，负责处理提案、应用状态和通信等核心任务, 这里有发送给其他节点消息的处理。
func (c *Chain) run() {
	// 初始化ticker状态和定时器
	// ticking 用来标记定时器是否正在运行
	ticking := false
	// 使用clock创建一个定时器，初始设定为1秒后触发
	timer := c.clock.NewTimer(time.Second)
	// 确保定时器在开始前处于停止状态，若已启动则等待其完成并消费掉触发的事件
	if !timer.Stop() {
		<-timer.C()
	}

	// 定义启动定时器的辅助函数
	// 若定时器未运行，则设置新的超时时间并标记为运行中
	startTimer := func() {
		if !ticking {
			ticking = true
			// 重置定时器，使用共识批次处理超时时间作为新的超时期限
			timer.Reset(c.support.SharedConfig().BatchTimeout())
		}
	}

	// 定义停止定时器的辅助函数
	// 停止定时器并清空已到期但未处理的事件，同时更新ticker状态为未运行
	stopTimer := func() {
		if !timer.Stop() && ticking {
			// 若定时器无法停止（意味着它已经触发但事件未被消费），则消费掉这个事件
			<-timer.C()
		}
		ticking = false
	}

	// 初始化关键变量
	// soft 用于记录Raft的软状态信息，如当前领导者等
	var soft raft.SoftState
	// submitC 是交易提交通道，用于接收待排序的交易
	submitC := c.submitC

	// 初始化区块创建器实例，用于构造新的区块
	var bc *blockCreator
	// 初始化提案通道和取消函数，用于管理向Raft提交区块的过程
	// 用于发送待提议的区块
	var propC chan<- *common.Block
	// cancelProp 用于取消正在进行的提案过程
	var cancelProp context.CancelFunc
	// 初始化cancelProp为一个空操作函数，避免未定义错误
	cancelProp = func() {}

	// 定义领导者转变函数
	becomeLeader := func() (chan<- *common.Block, context.CancelFunc) {
		// 设置当前节点为领导者状态的监控指标
		c.Metrics.IsLeader.Set(1)
		// 重置当前节点正在处理的区块数量（飞行中的区块）
		c.blockInflight = 0
		// 标记此节点刚刚当选为领导者
		c.justElected = true
		// 当前节点成为领导者后，不再直接从submitC接收交易，因此将其设为nil
		submitC = nil
		// 创建一个新的区块提案通道，容量限制为允许的最大并发区块数
		ch := make(chan *common.Block, c.opts.MaxInflightBlocks)

		// 如果存在未完成的配置变更请求（ConfChange），则尝试继续推进该变更集合。是否有新的raft节点, 或者排除的节点
		if cc := c.getInFlightConfChange(); cc != nil {
			// 异步尝试向Raft节点提议配置变更。这一步需要在goroutine中执行，以避免阻塞其他关键操作，
			// 特别是在网络负载较高时，领导者可能因失去领导地位而阻塞。
			go func() {
				if err := c.Node.ProposeConfChange(context.TODO(), *cc); err != nil {
					c.logger.Warnf("向Raft节点提议配置更新失败: %s", err)
				}
			}()

			// 标记有配置变更正在进行，并设置配置变更飞行标志
			c.confChangeInProgress = cc
			c.configInflight = true
		}

		// 以领导者身份异步提议新区块到Raft共识中
		// 使用带取消功能的上下文，确保在必要时能优雅地停止提议过程
		ctx, cancel := context.WithCancel(context.Background())
		go func(ctx context.Context, propC <-chan *common.Block) {
			for {
				select {
				// 从通道接收待提议的区块
				case b := <-propC:
					// 序列化区块数据，准备提交给Raft
					data := protoutil.MarshalOrPanic(b)
					// 提议区块到Raft，若失败则记录错误并返回
					if err := c.Node.Propose(ctx, data); err != nil {
						c.logger.Errorf("向Raft提议区块[%d]失败，丢弃队列中%d个区块: %s", b.Header.Number, len(propC), err)
						return
					}
					// 成功提议后记录日志
					c.logger.Debugf("向Raft共识提议了区块[%d]", b.Header.Number)

				// 上下文被取消时，退出提议循环
				case <-ctx.Done():
					c.logger.Debugf("停止提议区块，队列中丢弃了%d个区块", len(propC))
					return
				}
			}
		}(ctx, ch)

		// 返回新创建的提案通道和取消函数，以便外部控制提议流程
		return ch, cancel
	}

	// becomeFollower 定义了一个转换到跟随者状态的操作流程。
	// 当节点不再是Raft集群的领导者时，需要执行以下操作：
	becomeFollower := func() {
		// 1. 调用cancelProp取消正在进行的提案上下文，停止任何待处理的区块提议操作。
		cancelProp()
		// 2. 重置blockInflight计数器，表示当前没有区块正处于飞行中（等待Raft共识确认）。
		c.blockInflight = 0
		// 3. 调用BlockCutter的Cut方法尝试切割并处理当前累积的交易批次，这通常是为确保跟随者状态下交易的有序处理。
		_ = c.support.BlockCutter().Cut()
		// 4. 停止之前启动的定时器，避免在跟随者状态下不必要的定时任务执行。
		stopTimer()
		// 5. 将交易提交通道submitC重置为默认的提交通道，跟随者直接通过此通道接收交易请求。
		submitC = c.submitC
		// 6. 将区块创建器实例bc设置为nil，因为在跟随者状态下不需要创建新的区块。
		bc = nil
		// 7. 更新监控指标，将IsLeader指标设置为0，表明当前节点不再是领导者。
		c.Metrics.IsLeader.Set(0)
	}

	for {
		select {
		// 接受本地的raft消息, 此通道消息是由本地发送而来的, 本地必须是领导节点才会处理, 否则会跳过
		case s := <-submitC:
			// 检查是否有空消息，这可能是由`WaitReady`调用触发的轮询
			if s == nil {
				continue
			}

			// 如果当前Raft状态为预候选人或候选状态，说明尚未选举出领导者，通知请求者当前无领导者
			if soft.RaftState == raft.StatePreCandidate || soft.RaftState == raft.StateCandidate {
				s.leader <- raft.None
				continue
			}

			// 通知请求者当前已知的领导者ID
			s.leader <- soft.Lead
			// 如果当前节点不是领导者，则跳过后续处理
			if soft.Lead != c.raftID {
				continue
			}

			// 对请求进行排序，得到批次切片以及是否有挂起的请求标识
			batches, pending, err := c.ordered(s.req)
			if err != nil {
				c.logger.Errorf("排序消息失败, 记录错误并继续处理下一个请求: %s", err)
				continue
			}

			// 如果没有挂起的请求且批次为空，直接跳过
			if !pending && len(batches) == 0 {
				continue
			}

			// 根据是否有挂起请求来决定是否启动或停止定时器
			if pending {
				startTimer() // 定时器已启动则此操作无效果
			} else {
				stopTimer()
			}

			c.propose(propC, bc, batches...) // 将批次数据封装并发送至Raft处理管道

			// 如果有配置变更正在进行中，暂停接受新的事务
			if c.configInflight {
				c.logger.Info("检测到配置事务，暂停接受新事务直至当前配置事务提交完成")
				submitC = nil
			} else if c.blockInflight >= c.opts.MaxInflightBlocks {
				c.logger.Debugf("飞行中的区块数量(%d)已达上限(%d)，暂停接受新的事务",
					c.blockInflight, c.opts.MaxInflightBlocks)
				submitC = nil
			}

		case app := <-c.applyC: // 接受来自其他节点的消息
			if app.soft != nil {
				newLeader := atomic.LoadUint64(&app.soft.Lead) // 使用原子操作加载新领导者
				if newLeader != soft.Lead {
					c.logger.Infof("Raft 共识节点的 leader 领导者发生改变: 从 %d -> 到 %d", soft.Lead, newLeader)
					c.Metrics.LeaderChanges.Add(1)

					atomic.StoreUint64(&c.lastKnownLeader, newLeader)

					// 如果新选举的的lead是当前节点, 则改变当前节点的状态为lead
					if newLeader == c.raftID {
						// 定义角色转变函数
						propC, cancelProp = becomeLeader() // 成为领导者
					}

					if soft.Lead == c.raftID {
						becomeFollower() // 成为跟随者
					}
				}

				// 检查是否发现新的领导者或候选者退出
				// foundLeader 用于判断是否发现新的领导者，条件为当前领导者为空且新领导者不为空
				foundLeader := soft.Lead == raft.None && newLeader != raft.None
				// quitCandidate 用于判断是否候选者退出，条件为 当前节点为候选者 且 新节点不是候选者
				quitCandidate := isCandidate(soft.RaftState) && !isCandidate(app.soft.RaftState)

				if foundLeader || quitCandidate {
					c.errorCLock.Lock()            // 加锁以保证并发安全
					c.errorC = make(chan struct{}) // 创建一个新的错误通道
					c.errorCLock.Unlock()          // 解锁
				}

				// 处理 节点为候选者 或 新领导者为空 的情况
				if isCandidate(app.soft.RaftState) || newLeader == raft.None {
					// 使用原子操作将最后已知领导者设置为 None
					atomic.StoreUint64(&c.lastKnownLeader, raft.None)
					select {
					case <-c.errorC:
					default:
						nodeCount := len(c.opts.BlockMetadata.ConsenterIds)
						// 只有在集群大小大于 2 时才关闭错误通道（以向前端信号传播/传递共识后端错误）
						// 否则无法将大小为 1 的集群扩展为 2 节点。
						if nodeCount > 2 {
							close(c.errorC) // 关闭错误通道
						} else {
							c.logger.Warningf("没有领导者存在，集群大小为 %d", nodeCount) // 记录警告日志，表示没有领导者存在
						}
					}
				}

				soft = raft.SoftState{Lead: newLeader, RaftState: app.soft.RaftState}

				// 通知外部观察者
				select {
				case c.observeC <- soft:
				default:
				}
			}

			// 用于处理 Raft 日志条目的应用。根据不同类型的日志条目进行相应的处理，包括写入区块、应用配置更改、处理快照等操作
			c.apply(app.entries)

			// 如果当前节点刚刚被选举为领导者
			if c.justElected {
				// 检查是否有消息（区块）还在传输中（即尚未应用）
				msgInflight := c.Node.lastIndex() > c.appliedIndex
				if msgInflight {
					// 如果有消息在传输中，新领导者应暂时不处理请求
					c.logger.Debugf("存在正在传输的区块，新当选的领导者不应立即处理客户端请求")
					continue // 继续下一轮循环，不处理当前请求
				}

				// 检查是否有配置变更（ConfChange）正处于传输中
				if c.configInflight {
					c.logger.Debugf("存在配置变更或配置区块正在传输中，新当选的领导者不应立即处理客户端请求")
					continue // 继续下一轮循环，不处理当前请求
				}

				// 如果上述条件都不满足，新领导者开始接受Raft请求
				c.logger.Infof("领导者已就绪，开始接受Raft客户端请求，当前最新区块编号为 [%d]", c.lastBlock.Header.Number)
				// 初始化一个新的blockCreator实例，用于创建新区块
				bc = &blockCreator{
					hash:   protoutil.BlockHeaderHash(c.lastBlock.Header), // 设置前一区块的哈希值
					number: c.lastBlock.Header.Number,                     // 设置新区块的起始区块编号
					logger: c.logger,                                      // 设置日志记录器
				}
				// 允许提交通道用于接收新的交易请求
				submitC = c.submitC
				// 标记领导者选举状态为已处理
				c.justElected = false
			} else if c.configInflight {
				// 如果有配置变更正在处理中，暂停接受新的交易请求
				c.logger.Info("由于存在配置变更或配置区块传输中，暂停处理新的交易请求")
				// 关闭提交通道，拒绝新的交易
				submitC = nil
			} else if c.blockInflight < c.opts.MaxInflightBlocks {
				// 如果当前飞行中的区块数量小于最大允许数，开放提交通道以接受新交易
				submitC = c.submitC
			}

		// 等待定时器触发或超时
		case <-timer.C():
			ticking = false // 标记定时器已结束

			// 调用BlockCutter来切分当前累积的消息批次
			batch := c.support.BlockCutter().Cut()
			// 检查切分后的批次是否为空
			if len(batch) == 0 {
				// 如果批次为空，这可能意味着出现了意外情况（比如没有待处理请求）
				c.logger.Warningf("批次定时器到期，但没有待处理的请求，这可能表示存在一个bug")
				continue // 继续下一次循环，等待新的事件或超时
			}

			// 记录日志，表示批次定时器到期，将创建新区块
			c.logger.Debugf("批次定时器到期，开始创建区块")
			// 调用propose方法，将批次数据发送给Raft进行共识处理
			c.propose(propC, bc, batch) // 将批次数据封装并发送至Raft处理管道

		// 处理接收到的快照通知
		case sn := <-c.snapC:
			// 检查快照索引是否非零，零索引的快照可能是用于触发追赶同步的特殊快照
			if sn.Metadata.Index != 0 {
				// 若快照索引小于等于已应用的索引，说明该快照已过时，无需处理
				if sn.Metadata.Index <= c.appliedIndex {
					c.logger.Debugf("跳过在索引 %d 处拍摄的快照，因为它落后于当前已应用的索引 %d", sn.Metadata.Index, c.appliedIndex)
					break // 继续处理下一个事件
				}

				// 更新配置状态和已应用索引至快照的对应值
				c.confState = sn.Metadata.ConfState
				c.appliedIndex = sn.Metadata.Index
			} else {
				// 特殊处理零索引快照，通常用于指示需要进行追赶同步
				c.logger.Infof("收到用于触发追赶同步的人工快照")
			}

			// 尝试从接收到的快照进行追赶恢复
			if err := c.catchUp(sn); err != nil {
				// 如果从快照恢复失败，则记录严重错误并终止进程
				c.logger.Panicf("从任期 %d 和索引 %d 处拍摄的快照恢复失败: %s",
					sn.Metadata.Term, sn.Metadata.Index, err)
			}

		// 当接收到 doneC 信号，表示应该停止服务
		case <-c.doneC:
			// 停止计时器，防止资源泄露
			stopTimer()
			// 取消提议（Propose）操作，确保在退出前不再接受新的提议处理
			cancelProp()

			// 确保 errorC 通道优雅关闭，避免对已关闭通道再次关闭
			select {
			// 如果 errorC 已经关闭，则什么都不做，直接跳过
			case <-c.errorC:
			// 否则，关闭 errorC 通道，防止悬挂的发送者
			default:
				close(c.errorC)
			}

			// 记录日志，表明服务请求已停止
			c.logger.Infof("停止处理请求")
			// 停止周期性检查器，释放相关资源
			c.periodicChecker.Stop()
			// 返回，结束当前的 goroutine 或函数执行
			return
		}
	}
}

// 方法将指定的区块写入到区块链中，并处理与Raft索引同步相关的工作。
// 如果区块序号跳跃或重复，会根据情况进行错误处理或警告。
// 同时，它还会检查区块是否包含配置更新，并相应地调用专门的处理函数。
func (c *Chain) writeBlock(block *common.Block, index uint64) {
	// 检查接收到的区块序号是否直接紧随当前链的最后一个区块之后。
	// 如果大于预期（跳过了某个序号），则报错并终止程序。
	if block.Header.Number > c.lastBlock.Header.Number+1 {
		c.logger.Panicf("接收到的区块序号 [%d] 超出了期望值 [%d]，表明区块序列不连续", block.Header.Number, c.lastBlock.Header.Number+1)
	} else if block.Header.Number < c.lastBlock.Header.Number+1 {
		c.logger.Infof("接收到较旧的区块 [%d]，而当前期待的是序号 [%d] 的区块，此节点可能需要同步缺失的区块", block.Header.Number, c.lastBlock.Header.Number+1)
		return
	}

	// 如果当前有区块正在飞行（即等待处理中），减少飞行中的区块计数。
	// 这通常在领导节点处理完一个区块后发生。
	if c.blockInflight > 0 {
		c.blockInflight-- // 仅在领导者上减少
	}
	// 更新最后一个区块为当前处理的区块。
	c.lastBlock = block

	// 记录日志，表明区块已成功写入，同时记录对应的Raft索引。
	c.logger.Infof("成功写入区块 [%d] 至通道账本，对应Raft索引为: %d", block.Header.Number, index)

	// 检查此区块是否包含配置更新交易。
	// 如果是配置区块，则调用专门的方法处理配置更新。
	if protoutil.IsConfigBlock(block) {
		c.writeConfigBlock(block, index)
		return
	}

	// 对Raft元数据进行并发保护，更新其Raft索引，并序列化元数据。
	c.raftMetadataLock.Lock()
	c.opts.BlockMetadata.RaftIndex = index
	m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
	c.raftMetadataLock.Unlock()

	// 最后，通过支持接口（support）写入区块和相应的元数据到账本中。
	c.support.WriteBlock(block, m)
}

// 在 'msg' 内容中订购信封。提交请求。
// 返回
//
//	-- batches [][]*common.Envelope; 切分后的批次，
//	-- pending bool; 是否有待排序的信封，
//	-- err error; 遇到的错误，如果有的话。
//
// 处理配置消息以及如果配置序列已经提前，则重新验证消息。
func (c *Chain) ordered(msg *orderer.SubmitRequest) (batches [][]*common.Envelope, pending bool, err error) {
	// 配置序列号通常用于追踪和标识系统配置的更新历史，确保各节点间配置状态的一致性。
	seq := c.support.Sequence()

	// 方法用于判断给定的信封是否为配置消息。
	isconfig, err := c.isConfig(msg.Payload)
	if err != nil {
		return nil, false, errors.Errorf("错误消息: %s", err)
	}

	// 这是一个更换共识领导节点的配置步骤
	if isconfig {
		// ConfigMsg
		if msg.LastValidationSeq < seq {
			c.logger.Warnf("配置消息已经根据 %d 进行验证，尽管当前配置序列已经提前 (%d)", msg.LastValidationSeq, seq)
			// 调用`ProcessConfigUpdateMsg`以生成与原始消息相同类型的新配置消息。
			msg.Payload, _, err = c.support.ProcessConfigMsg(msg.Payload)
			if err != nil {
				c.Metrics.ProposalFailures.Add(1)
				return nil, true, errors.Errorf("错误的配置消息: %s", err)
			}
		}

		// 函数用于检查节点驱逐和证书轮换，如果请求中包含这些内容则返回 true，否则返回 false。
		if c.checkForEvictionNCertRotation(msg.Payload) {
			// 用于标记领导权转移是否正在进行中。
			if !atomic.CompareAndSwapUint32(&c.leadershipTransferInProgress, 0, 1) {
				c.logger.Warnf("重新配置事务已经在进行中，忽略后续事务")
				return
			}

			go func() {
				defer atomic.StoreUint32(&c.leadershipTransferInProgress, 0)
				// AbdicationMaxAttempts: 确定了在移除自身参与重新配置的事务中，放弃领导权的最大重试次数。
				for attempt := 1; attempt <= AbdicationMaxAttempts; attempt++ {
					// 选择一个最近处于活动状态的节点，并尝试将领导权转移给它。
					if err := c.Node.abdicateLeadership(); err != nil {
						// 如果没有领导者，中止并不重试。
						// 为了防止重新提交事务，提前返回
						if err == ErrNoLeader || err == ErrChainHalting {
							return
						}

						// 如果错误不是以下情况之一，则是编程错误，所以触发 panic。
						if err != ErrNoAvailableLeaderCandidate && err != ErrTimedOutLeaderTransfer {
							c.logger.Panicf("编程错误，abdicateLeader() 返回了意外错误: %v", err)
						}

						// 否则，是上述错误之一，所以我们重试。
						continue
					} else {
						// 否则，放弃领导权成功，我们提交事务（转发到领导者）
						if err := c.Submit(msg, 0); err != nil {
							c.logger.Warnf("重新配置事务转发失败，错误: %v", err)
						}
						return
					}
				}

				c.logger.Warnf("放弃领导权失败次数过多（%d），中止重试", AbdicationMaxAttempts)
			}()
			return nil, false, nil
		}

		// Cut 切割当前批次并开始一个新的批次，返回被切割的批次内容。
		batch := c.support.BlockCutter().Cut()
		batches = [][]*common.Envelope{}
		if len(batch) != 0 {
			batches = append(batches, batch)
		}
		batches = append(batches, []*common.Envelope{msg.Payload})
		return batches, false, nil
	}

	// 这是一个正常的共识消息
	if msg.LastValidationSeq < seq {
		c.logger.Warnf("普通消息已经根据 %d 进行验证，尽管当前配置序列已经提前 (%d)", msg.LastValidationSeq, seq)
		if _, err := c.support.ProcessNormalMsg(msg.Payload); err != nil {
			c.Metrics.ProposalFailures.Add(1)
			return nil, true, errors.Errorf("错误的正常消息: %s", err)
		}
	}

	// 应在消息被排序时依次调用, 这个并不是对交易排序, 只是对交易的大小切割为多个区块高度。
	batches, pending = c.support.BlockCutter().Ordered(msg.Payload)
	return batches, pending, nil
}

// 提交批次到 Raft 共识中
func (c *Chain) propose(propC chan<- *common.Block, bc *blockCreator, batches ...[]*common.Envelope) {
	for _, batch := range batches {
		// 结构体中的 createNextBlock 方法用于根据给定的一组交易信封（Envelope）创建下一个区块链块。
		b := bc.createNextBlock(batch)
		c.logger.Infof("本节点为领导者节点, 已创建的块高度 [%d], 准备推送共识同步到跟随着, 是否有正在传输中的块(如果有需要等待): %d 个", b.Header.Number, c.blockInflight)

		select {
		case propC <- b:
		default:
			c.logger.Panic("编程错误: 在传输中的块数量限制未正确生效或块由跟随者提议")
		}

		// 如果是配置区块，则应等待区块的提交
		if protoutil.IsConfigBlock(b) {
			c.configInflight = true
		}

		c.blockInflight++
	}
}

func (c *Chain) catchUp(snap *raftpb.Snapshot) error {
	b, err := protoutil.UnmarshalBlock(snap.Data)
	if err != nil {
		return errors.Errorf("failed to unmarshal snapshot data to block: %s", err)
	}

	if c.lastBlock.Header.Number >= b.Header.Number {
		c.logger.Warnf("Snapshot is at block [%d], local block number is %d, no sync needed", b.Header.Number, c.lastBlock.Header.Number)
		return nil
	} else if b.Header.Number == c.lastBlock.Header.Number+1 {
		c.logger.Infof("The only missing block [%d] is encapsulated in snapshot, committing it to shortcut catchup process", b.Header.Number)
		c.commitBlock(b)
		c.lastBlock = b
		return nil
	}

	puller, err := c.createPuller()
	if err != nil {
		return errors.Errorf("failed to create block puller: %s", err)
	}
	defer puller.Close()

	next := c.lastBlock.Header.Number + 1

	c.logger.Infof("Catching up with snapshot taken at block [%d], starting from block [%d]", b.Header.Number, next)

	for next <= b.Header.Number {
		block := puller.PullBlock(next)
		if block == nil {
			return errors.Errorf("failed to fetch block [%d] from cluster", next)
		}
		c.commitBlock(block)
		c.lastBlock = block
		next++
	}

	c.logger.Infof("Finished syncing with cluster up to and including block [%d]", b.Header.Number)
	return nil
}

func (c *Chain) commitBlock(block *common.Block) {
	// read consenters metadata to write into the replicated block
	blockMeta, err := protoutil.GetConsenterMetadataFromBlock(block)
	if err != nil {
		c.logger.Panicf("Failed to obtain metadata: %s", err)
	}

	if !protoutil.IsConfigBlock(block) {
		c.support.WriteBlock(block, blockMeta.Value)
		return
	}

	c.support.WriteConfigBlock(block, blockMeta.Value)

	configMembership := c.detectConfChange(block)

	if configMembership != nil && configMembership.Changed() {
		c.logger.Infof("Config block [%d] changes consenter set, communication should be reconfigured", block.Header.Number)

		c.raftMetadataLock.Lock()
		c.opts.BlockMetadata = configMembership.NewBlockMetadata
		c.opts.Consenters = configMembership.NewConsenters
		c.raftMetadataLock.Unlock()

		if err := c.configureComm(); err != nil {
			c.logger.Panicf("Failed to configure communication: %s", err)
		}
	}
}

// 检测Conf更改
func (c *Chain) detectConfChange(block *common.Block) *MembershipChanges {
	// 如果配置针对此通道，请检查共识者集并在添加/删除节点时提议 Raft ConfChange
	configMetadata := c.newConfigMetadata(block)

	if configMetadata == nil {
		return nil
	}

	if configMetadata.Options != nil &&
		configMetadata.Options.SnapshotIntervalSize != 0 &&
		configMetadata.Options.SnapshotIntervalSize != c.sizeLimit {
		c.logger.Infof("将快照间隔大小更新为 %d 字节（之前为 %d）",
			configMetadata.Options.SnapshotIntervalSize, c.sizeLimit)
		c.sizeLimit = configMetadata.Options.SnapshotIntervalSize
	}

	// 函数根据新共识者信息计算成员更新，返回两个切片：一个包含新增共识者，一个包含待移除的共识者。
	changes, err := ComputeMembershipChanges(c.opts.BlockMetadata, c.opts.Consenters, configMetadata.Consenters)
	if err != nil {
		c.logger.Panicf("检测到非法配置更改: %s", err)
	}

	if changes.Rotated() {
		c.logger.Infof("配置区块 [%d] 旋转了节点 %d 的 TLS 证书", block.Header.Number, changes.RotatedNode)
	}

	return changes
}

// 用于处理 Raft 日志条目的应用。根据不同类型的日志条目进行相应的处理，包括写入区块、应用配置更改、处理快照等操作
func (c *Chain) apply(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}

	if ents[0].Index > c.appliedIndex+1 {
		c.logger.Panicf("已提交条目的第一个索引[%d]应 <= 应用索引[%d]+1", ents[0].Index, c.appliedIndex)
	}

	var position int
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			// 正常进入
			if len(ents[i].Data) == 0 {
				break
			}

			position = i
			c.accDataSize += uint32(len(ents[i].Data))

			// 我们需要严格避免重新应用普通条目，否则会写入相同的区块两次。
			if ents[i].Index <= c.appliedIndex {
				c.logger.Debugf("接收到 Raft 索引为 (%d) <= 应用索引为 (%d) 的区块，跳过", ents[i].Index, c.appliedIndex)
				break
			}

			// 方法将指定的区块写入到区块链中，并处理与Raft索引同步相关的工作。
			block := protoutil.UnmarshalBlockOrPanic(ents[i].Data)
			c.writeBlock(block, ents[i].Index)
			c.Metrics.CommittedBlockNumber.Set(float64(block.Header.Number))

		// 处理配置变更条目
		case raftpb.EntryConfChange:
			// 解析配置变更数据
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				c.logger.Warnf("无法解码配置变更数据: %s", err)
				continue
			}

			// 应用配置变更到本地节点状态
			c.confState = *c.Node.ApplyConfChange(cc)

			// 根据配置变更类型记录日志
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				c.logger.Infof("应用配置变更以添加节点 %d, 当前通道节点列表: %+v", cc.NodeID, c.confState.Nodes)
			case raftpb.ConfChangeRemoveNode:
				c.logger.Infof("应用配置变更以移除节点 %d, 当前通道节点列表: %+v", cc.NodeID, c.confState.Nodes)
			default:
				c.logger.Panic("编程错误: 遇到了不支持的Raft配置变更类型")
			}

			// 如果当前配置变更与之前提交的匹配，则执行以下操作：
			var configureComm bool
			if c.confChangeInProgress != nil &&
				c.confChangeInProgress.NodeID == cc.NodeID &&
				c.confChangeInProgress.Type == cc.Type {

				configureComm = true
				c.confChangeInProgress = nil // 清除正在进行的配置变更标记
				c.configInflight = false     // 标记配置变更已应用
				// 报告新的集群大小
				c.Metrics.ClusterSize.Set(float64(len(c.opts.BlockMetadata.ConsenterIds)))
			}

			// 解除 `run` 协程的阻塞，以便它仍然可以消费 Raft 消息
			shouldHalt := cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == c.raftID
			// 异步处理后续操作：配置通信或停止节点
			go func() {
				if configureComm && !shouldHalt { // 仅在不停止节点时配置通信
					if err := c.configureComm(); err != nil {
						c.logger.Panicf("配置通信失败: %s", err)
					}
				}

				if shouldHalt { // 如果当前节点被移除
					c.logger.Infof("此节点正在从副本集中移除")
					c.halt() // 执行节点停止操作
					return
				}
			}()
		}

		// 更新已应用的最新条目索引
		if ents[i].Index > c.appliedIndex {
			c.appliedIndex = ents[i].Index
		}
	}

	// 检查累积的数据大小是否达到了设定的限制
	if c.accDataSize >= c.sizeLimit {
		// 解析当前位置的日志条目数据为完整的区块
		b := protoutil.UnmarshalBlockOrPanic(ents[position].Data)

		// 尝试发送信号至垃圾回收通道（gcC），以触发快照的创建
		select {
		case c.gcC <- &gc{ // 发送信号包含当前应用的索引、配置状态和待清理的日志条目数据
			index: c.appliedIndex,
			state: c.confState,
			data:  ents[position].Data,
		}:
			// 成功发送信号，记录日志信息
			c.logger.Infof("自上次快照以来累积了 %d 字节,超过大小限制 (%d 字节)，"+
				"因此将在区块 [%d] (索引: %d) 处进行快照,上次快照的区块编号为 %d，当前节点: %+v",
				c.accDataSize, c.sizeLimit, b.Header.Number, c.appliedIndex, c.lastSnapBlockNum, c.confState.Nodes)
			// 重置累积数据大小计数器
			c.accDataSize = 0
			// 更新最近一次快照对应的区块编号
			c.lastSnapBlockNum = b.Header.Number
			// 更新指标，记录快照对应的区块编号
			c.Metrics.SnapshotBlockNumber.Set(float64(b.Header.Number))
		default:
			// 如果无法立即发送（通道满），则提示可能是因为快照间隔或通道缓冲区大小设置过小
			c.logger.Warnf("无法立即执行快照，可能是因为已有快照操作正在进行中，或者快照间隔和通道配置太紧凑。")
		}
	}
}

// gc 函数负责执行垃圾回收任务，不断监听并处理来自 gcC 通道的快照生成请求或 doneC 通道的停止信号。
func (c *Chain) gc() {
	// 使用无限循环来持续监听通道事件。
	for {
		// 使用 select 语句等待来自两个不同通道的事件。
		select {
		// 当从 gcC 通道收到垃圾回收任务（快照请求）时。
		case g := <-c.gcC:
			// 调用 Node 的 takeSnapshot 方法，传入快照索引、状态和数据，
			// 以生成并保存快照，实现对历史数据的整理和内存占用的优化。
			c.Node.takeSnapshot(g.index, g.state, g.data)

		// 当从 doneC 通道接收到停止信号时。
		case <-c.doneC:
			// 记录日志，表明垃圾回收任务已停止。
			c.logger.Infof("停止垃圾回收")
			// 结束循环，退出垃圾回收函数。
			return
		}
	}
}

// 方法用于判断给定的信封是否为配置消息。
func (c *Chain) isConfig(env *common.Envelope) (bool, error) {
	// 从信封中提取通道头信息
	h, err := protoutil.ChannelHeader(env)
	if err != nil {
		// 如果提取通道头信息失败，则记录错误并返回 false 和错误信息
		c.logger.Errorf("从信封中提取通道头失败")
		return false, err
	}

	// 判断通道头中的消息类型是否为配置消息或订购者事务消息
	isConfigMsg := h.Type == int32(common.HeaderType_CONFIG) || h.Type == int32(common.HeaderType_ORDERER_TRANSACTION)

	// 返回判断结果和 nil 错误
	return isConfigMsg, nil
}

// configureComm 函数用于重新配置通信，例如在网络拓扑变化或节点加入/离开时。
func (c *Chain) configureComm() error {
	// 当通信被重新配置时，重置不可达节点的映射。
	c.Node.unreachableLock.Lock()
	c.Node.unreachable = make(map[uint64]struct{}) // 清空不可达节点的记录。
	c.Node.unreachableLock.Unlock()

	// 获取远程排序节点列表。
	nodes, err := c.remoteOrderers()
	if err != nil {
		return err // 如果获取远程对等节点失败，直接返回错误。
	}

	// 使用获取到的节点列表重新配置共识层。
	c.configurator.Configure(c.channelID, nodes)
	return nil // 重新配置成功，返回 nil。
}

// remoteOrderers 函数用于构建远程排序节点的信息列表。
func (c *Chain) remoteOrderers() ([]cluster.RemoteNode, error) {
	// 加读锁，确保在读取共识层元数据期间不会被写操作影响。
	c.raftMetadataLock.RLock()
	defer c.raftMetadataLock.RUnlock() // 释放读锁。

	var nodes []cluster.RemoteNode // 初始化远程节点列表。

	// 遍历共识层配置中的所有节点。
	for raftID, consenter := range c.opts.Consenters {
		// 跳过自身，无需了解自己的信息。
		if raftID == c.raftID {
			continue
		}

		// 将服务器 TLS 证书从 PEM 格式转换为 DER 格式。
		serverCertAsDER, err := pemToDER(consenter.ServerTlsCert, raftID, "server", c.logger)
		if err != nil {
			return nil, errors.WithStack(err) // 如果转换失败，返回错误。
		}

		// 将客户端 TLS 证书从 PEM 格式转换为 DER 格式。
		clientCertAsDER, err := pemToDER(consenter.ClientTlsCert, raftID, "client", c.logger)
		if err != nil {
			return nil, errors.WithStack(err) // 如果转换失败，返回错误。
		}

		// 构建并添加远程节点信息至列表。
		nodes = append(nodes, cluster.RemoteNode{
			ID:            raftID,                                               // 节点的 Raft ID。
			Endpoint:      fmt.Sprintf("%s:%d", consenter.Host, consenter.Port), // 构建节点的通信端点。
			ServerTLSCert: serverCertAsDER,                                      // 服务器的 TLS 证书。
			ClientTLSCert: clientCertAsDER,                                      // 客户端的 TLS 证书。
		})
	}

	return nodes, nil // 返回构建好的远程节点列表。
}

// pemToDER 函数用于将 PEM 格式的证书转换为 DER 编码的字节序列。
func pemToDER(pemBytes []byte, id uint64, certType string, logger *flogging.FabricLogger) ([]byte, error) {
	// 解码 PEM 格式的证书。
	bl, _ := pem.Decode(pemBytes)
	if bl == nil {
		// 如果解码失败，记录错误信息并返回错误。
		logger.Errorf("拒绝节点 %d 的 %s TLS 证书的 PEM 块，违规的 PEM 内容为: %s", id, certType, string(pemBytes))
		return nil, errors.Errorf("无效的 PEM 块")
	}
	// 返回 DER 编码的证书字节。
	return bl.Bytes, nil
}

// writeConfigBlock 将配置区块写入账本，同时提取有关 Raft 副本集的更新，如果有更改，则更新集群成员资格
func (c *Chain) writeConfigBlock(block *common.Block, index uint64) {
	// 一个配置区块，并返回其中包含的配置信封的头类型，
	hdr, err := ConfigChannelHeader(block)
	if err != nil {
		c.logger.Panicf("无法从配置区块中获取配置头类型: %s", err)
	}

	c.configInflight = false

	switch common.HeaderType(hdr.Type) {
	case common.HeaderType_CONFIG:
		// 检测Conf更改, 返回新的的共识配置
		configMembership := c.detectConfChange(block)

		c.raftMetadataLock.Lock()
		c.opts.BlockMetadata.RaftIndex = index
		if configMembership != nil {
			c.opts.BlockMetadata = configMembership.NewBlockMetadata
			c.opts.Consenters = configMembership.NewConsenters
		}
		c.raftMetadataLock.Unlock()

		// 写入带有元数据的区块
		blockMetadataBytes := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
		c.support.WriteConfigBlock(block, blockMetadataBytes)

		if configMembership == nil {
			return
		}

		// update membership
		if configMembership.ConfChange != nil {
			// We need to propose conf change in a go routine, because it may be blocked if raft node
			// becomes leaderless, and we should not block `run` so it can keep consuming applyC,
			// otherwise we have a deadlock.
			go func() {
				// ProposeConfChange returns error only if node being stopped.
				// This proposal is dropped by followers because DisableProposalForwarding is enabled.
				if err := c.Node.ProposeConfChange(context.TODO(), *configMembership.ConfChange); err != nil {
					c.logger.Warnf("Failed to propose configuration update to Raft node: %s", err)
				}
			}()

			c.confChangeInProgress = configMembership.ConfChange

			switch configMembership.ConfChange.Type {
			case raftpb.ConfChangeAddNode:
				c.logger.Infof("Config block just committed adds node %d, pause accepting transactions till config change is applied", configMembership.ConfChange.NodeID)
			case raftpb.ConfChangeRemoveNode:
				c.logger.Infof("Config block just committed removes node %d, pause accepting transactions till config change is applied", configMembership.ConfChange.NodeID)
			default:
				c.logger.Panic("Programming error, encountered unsupported raft config change")
			}

			c.configInflight = true
		} else {
			if err := c.configureComm(); err != nil {
				c.logger.Panicf("Failed to configure communication: %s", err)
			}
		}

	case common.HeaderType_ORDERER_TRANSACTION:
		// If this config is channel creation, no extra inspection is needed
		c.raftMetadataLock.Lock()
		c.opts.BlockMetadata.RaftIndex = index
		m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
		c.raftMetadataLock.Unlock()

		c.support.WriteConfigBlock(block, m)

	default:
		c.logger.Panicf("Programming error: unexpected config type: %s", common.HeaderType(hdr.Type))
	}
}

// getInFlightConfChange 返回当前正在进行中的配置变更(ConfChange)，如果有任意一个的话。
// 如果存在正在处理中的配置变更(confChangeInProgress)，则直接返回它。
// 否则，从最新提交的区块中尝试获取配置变更信息（该配置变更可能为nil）。
func (c *Chain) getInFlightConfChange() *raftpb.ConfChange {
	// 首先检查是否存在正在进行中的配置变更
	if c.confChangeInProgress != nil {
		return c.confChangeInProgress
	}

	// 如果链刚刚启动，即最后一个区块编号为0，表明没有需要故障切换的情况
	if c.lastBlock.Header.Number == 0 {
		return nil
	}

	// 检查最后一个区块是否为配置块，如果不是则无需进行配置变更处理
	if !protoutil.IsConfigBlock(c.lastBlock) {
		return nil
	}

	// 应用一个空的配置变更到Raft节点上，目的是为了获取当前的配置状态
	confState := c.Node.ApplyConfChange(raftpb.ConfChange{})

	// 比较当前Raft配置状态中的节点数量与区块链元数据中ConsenterIds的数量
	if len(confState.Nodes) == len(c.opts.BlockMetadata.ConsenterIds) {
		// Raft配置变更每次只能增加或移除一个节点。如果当前Raft配置的节点数
		// 等于区块元数据中存储的成员数，说明配置已同步，无需提出新的配置更新。
		return nil
	}

	// 如果配置不一致，则根据区块元数据和当前Raft配置状态构建一个新的配置变更提议
	return ConfChange(c.opts.BlockMetadata, confState)
}

// newMetadata extract config metadata from the configuration block
func (c *Chain) newConfigMetadata(block *common.Block) *etcdraft.ConfigMetadata {
	metadata, err := ConsensusMetadataFromConfigBlock(block)
	if err != nil {
		c.logger.Panicf("error reading consensus metadata: %s", err)
	}
	return metadata
}

// ValidateConsensusMetadata determines the validity of a
// ConsensusMetadata update during config updates on the channel.
func (c *Chain) ValidateConsensusMetadata(oldOrdererConfig, newOrdererConfig channelconfig.Orderer, newChannel bool) error {
	if newOrdererConfig == nil {
		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil new channel config")
		return nil
	}

	// metadata was not updated
	if newOrdererConfig.ConsensusMetadata() == nil {
		return nil
	}

	if oldOrdererConfig == nil {
		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil old channel config")
		return nil
	}

	if oldOrdererConfig.ConsensusMetadata() == nil {
		c.logger.Panic("Programming Error: ValidateConsensusMetadata called with nil old metadata")
		return nil
	}

	oldMetadata := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(oldOrdererConfig.ConsensusMetadata(), oldMetadata); err != nil {
		c.logger.Panicf("Programming Error: Failed to unmarshal old etcdraft consensus metadata: %v", err)
	}

	newMetadata := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(newOrdererConfig.ConsensusMetadata(), newMetadata); err != nil {
		return errors.Wrap(err, "failed to unmarshal new etcdraft metadata configuration")
	}

	verifyOpts, err := createX509VerifyOptions(newOrdererConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to create x509 verify options from old and new orderer config")
	}

	if err := VerifyConfigMetadata(newMetadata, verifyOpts); err != nil {
		return errors.Wrap(err, "invalid new config metadata")
	}

	if newChannel {
		// check if the consenters are a subset of the existing consenters (system channel consenters)
		set := ConsentersToMap(oldMetadata.Consenters)
		for _, c := range newMetadata.Consenters {
			if !set.Exists(c) {
				return errors.New("new channel has consenter that is not part of system consenter set")
			}
		}
		return nil
	}

	// create the dummy parameters for ComputeMembershipChanges
	c.raftMetadataLock.RLock()
	dummyOldBlockMetadata := proto.Clone(c.opts.BlockMetadata).(*etcdraft.BlockMetadata)
	c.raftMetadataLock.RUnlock()

	dummyOldConsentersMap := CreateConsentersMap(dummyOldBlockMetadata, oldMetadata)
	changes, err := ComputeMembershipChanges(dummyOldBlockMetadata, dummyOldConsentersMap, newMetadata.Consenters)
	if err != nil {
		return err
	}

	// new config metadata was verified above. Additionally need to check new consenters for certificates expiration
	for _, c := range changes.AddedNodes {
		if err := validateConsenterTLSCerts(c, verifyOpts, false); err != nil {
			return errors.Wrapf(err, "consenter %s:%d has invalid certificates", c.Host, c.Port)
		}
	}

	active := c.ActiveNodes.Load().([]uint64)
	if changes.UnacceptableQuorumLoss(active) {
		return errors.Errorf("%d out of %d nodes are alive, configuration will result in quorum loss", len(active), len(dummyOldConsentersMap))
	}

	return nil
}

// StatusReport returns the ConsensusRelation & Status
func (c *Chain) StatusReport() (types.ConsensusRelation, types.Status) {
	c.statusReportMutex.Lock()
	defer c.statusReportMutex.Unlock()

	return c.consensusRelation, c.status
}

func (c *Chain) suspectEviction() bool {
	if c.isRunning() != nil {
		return false
	}

	return atomic.LoadUint64(&c.lastKnownLeader) == uint64(0)
}

func (c *Chain) newEvictionSuspector() *evictionSuspector {
	consenterCertificate := &ConsenterCertificate{
		Logger:               c.logger,
		ConsenterCertificate: c.opts.Cert,
		CryptoProvider:       c.CryptoProvider,
	}

	return &evictionSuspector{
		amIInChannel:               consenterCertificate.IsConsenterOfChannel,
		evictionSuspicionThreshold: c.opts.EvictionSuspicion,
		writeBlock:                 c.support.Append,
		createPuller:               c.createPuller,
		height:                     c.support.Height,
		triggerCatchUp:             c.triggerCatchup,
		logger:                     c.logger,
		halt: func() {
			c.halt()
		},
	}
}

func (c *Chain) triggerCatchup(sn *raftpb.Snapshot) {
	select {
	case c.snapC <- sn:
	case <-c.doneC:
	}
}

// 函数用于检查节点驱逐和证书轮换，如果请求中包含这些内容则返回 true，否则返回 false。
func (c *Chain) checkForEvictionNCertRotation(env *common.Envelope) bool {
	// 从信封中提取有效载荷
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		c.logger.Warnf("从配置信封中提取有效载荷失败: %s", err)
		return false
	}

	// 解析有效载荷中的配置更新
	configUpdate, err := configtx.UnmarshalConfigUpdateFromPayload(payload)
	if err != nil {
		c.logger.Warnf("无法读取配置更新: %s", err)
		return false
	}

	// 从配置更新中提取配置元数据
	configMeta, err := MetadataFromConfigUpdate(configUpdate)
	if err != nil || configMeta == nil {
		c.logger.Warnf("无法读取配置元数据: %s", err)
		return false
	}

	// 计算共识成员变更, 不支持一次更新多个共识 consenter 节点
	membershipUpdates, err := ComputeMembershipChanges(c.opts.BlockMetadata, c.opts.Consenters, configMeta.Consenters)
	if err != nil {
		c.logger.Warnf("检测到非法配置更改: %s", err)
		return false
	}

	// 检查是否发生了我们节点的证书轮换
	if membershipUpdates.RotatedNode == c.raftID {
		c.logger.Infof("检测到我们节点的证书轮换")
		return true
	}

	// 检查是否发生了我们节点被驱逐出配置
	if _, exists := membershipUpdates.NewConsenters[c.raftID]; !exists {
		c.logger.Infof("检测到我们节点被驱逐出配置")
		return true
	}

	// 节点仍然是共识者集合的一部分
	c.logger.Debugf("节点 %d 仍然是共识者集合的一部分", c.raftID)
	return false
}
