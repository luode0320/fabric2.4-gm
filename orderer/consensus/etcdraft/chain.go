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
	req    *orderer.SubmitRequest
	leader chan uint64
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

	// submitC 是提交通道，用于接收来自客户端的提案。
	submitC chan *submit
	// applyC 是应用通道，用于接收 raft 状态机应用的条目，触发本地状态的更新。
	applyC chan apply
	// observeC 是观察者通道，用于通知外部观察者领导者的变更。
	observeC chan<- raft.SoftState
	// haltC 是停止通道，用于信号告知协程链正在停止。
	haltC chan struct{}
	// doneC 是完成通道，当链停止时关闭，用于等待链完全停止。
	doneC chan struct{}
	// startC 是启动通道，当节点启动时关闭，用于同步节点启动状态。
	startC chan struct{}
	// snapC 是快照通道，用于通知 raft 状态机追赶快照。
	snapC chan *raftpb.Snapshot
	// gcC 是垃圾回收通道，用于触发快照的生成和旧状态的清理。
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

	// fresh 用于标记这是不是一个全新的 raft 节点，初始启动时使用。
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
		observeC: observeC, // observeC 用于向外部观察者发送软状态更新，如领导权变更。

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
		isJoin = false // 如果是共识类型迁移，视为新节点启动而非加入。
		c.logger.Infof("检测到共识类型迁移，正在现有通道上启动新的 Raft 节点；高度=%d", c.support.Height())
	}

	// 调用 Node 的 start 方法，开始 Raft 节点的运行。
	c.Node.start(c.fresh, isJoin)

	// 关闭 startC 通道，表示链已经启动。
	close(c.startC)
	// 关闭 errorC 通道，表示链没有错误。
	close(c.errorC)

	// 启动垃圾回收协程，用于定期清理过期的快照和日志。
	go c.gc()
	// 启动主运行协程，负责处理提案、应用状态和通信等核心任务。
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

// Order submits normal type transactions for ordering.
func (c *Chain) Order(env *common.Envelope, configSeq uint64) error {
	c.Metrics.NormalProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// Configure submits config type transactions for ordering.
func (c *Chain) Configure(env *common.Envelope, configSeq uint64) error {
	c.Metrics.ConfigProposalsReceived.Add(1)
	return c.Submit(&orderer.SubmitRequest{LastValidationSeq: configSeq, Payload: env, Channel: c.channelID}, 0)
}

// WaitReady blocks when the chain:
// - is catching up with other nodes using snapshot
//
// In any other case, it returns right away.
func (c *Chain) WaitReady() error {
	if err := c.isRunning(); err != nil {
		return err
	}

	select {
	case c.submitC <- nil:
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	}

	return nil
}

// Errored returns a channel that closes when the chain stops.
func (c *Chain) Errored() <-chan struct{} {
	c.errorCLock.RLock()
	defer c.errorCLock.RUnlock()
	return c.errorC
}

// Halt stops the chain.
func (c *Chain) Halt() {
	c.stop()
}

func (c *Chain) stop() bool {
	select {
	case <-c.startC:
	default:
		c.logger.Warn("Attempted to halt a chain that has not started")
		return false
	}

	select {
	case c.haltC <- struct{}{}:
	case <-c.doneC:
		return false
	}
	<-c.doneC

	c.statusReportMutex.Lock()
	defer c.statusReportMutex.Unlock()
	c.status = types.StatusInactive

	return true
}

// halt stops the chain and calls the haltCallback function, which allows the
// chain to transfer responsibility to a follower or the inactive chain registry when a chain
// discovers it is no longer a member of a channel.
func (c *Chain) halt() {
	if stopped := c.stop(); !stopped {
		c.logger.Info("This node was stopped, the haltCallback will not be called")
		return
	}
	if c.haltCallback != nil {
		c.haltCallback() // Must be invoked WITHOUT any internal lock

		c.statusReportMutex.Lock()
		defer c.statusReportMutex.Unlock()

		// If the haltCallback registers the chain in to the inactive chain registry (i.e., system channel exists) then
		// this is the correct consensusRelation. If the haltCallback transfers responsibility to a follower.Chain, then
		// this chain is about to be GC anyway. The new follower.Chain replacing this one will report the correct
		// StatusReport.
		c.consensusRelation = types.ConsensusRelationConfigTracker
	}

	// active nodes metric shouldn't be frozen once a channel is stopped.
	c.Metrics.ActiveNodes.Set(float64(0))
}

func (c *Chain) isRunning() error {
	select {
	case <-c.startC:
	default:
		return errors.Errorf("chain is not started")
	}

	select {
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
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

// Submit forwards the incoming request to:
// - the local run goroutine if this is leader
// - the actual leader via the transport mechanism
// The call fails if there's no leader elected yet.
func (c *Chain) Submit(req *orderer.SubmitRequest, sender uint64) error {
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
			return errors.Errorf("no Raft leader")
		}

		if lead != c.raftID {
			if err := c.forwardToLeader(lead, req); err != nil {
				return err
			}
		}

	case <-c.doneC:
		c.Metrics.ProposalFailures.Add(1)
		return errors.Errorf("chain is stopped")
	}

	return nil
}

func (c *Chain) forwardToLeader(lead uint64, req *orderer.SubmitRequest) error {
	c.logger.Infof("Forwarding transaction to the leader %d", lead)
	timer := time.NewTimer(c.opts.RPCTimeout)
	defer timer.Stop()

	sentChan := make(chan struct{})
	atomicErr := &atomic.Value{}

	report := func(err error) {
		if err != nil {
			atomicErr.Store(err.Error())
			c.Metrics.ProposalFailures.Add(1)
		}
		close(sentChan)
	}

	c.rpc.SendSubmit(lead, req, report)

	select {
	case <-sentChan:
	case <-c.doneC:
		return errors.Errorf("chain is stopped")
	case <-timer.C:
		return errors.Errorf("timed out (%v) waiting on forwarding to %d", c.opts.RPCTimeout, lead)
	}

	if atomicErr.Load() != nil {
		return errors.Errorf(atomicErr.Load().(string))
	}
	return nil
}

type apply struct {
	entries []raftpb.Entry
	soft    *raft.SoftState
}

func isCandidate(state raft.StateType) bool {
	return state == raft.StatePreCandidate || state == raft.StateCandidate
}

func (c *Chain) run() {
	ticking := false
	timer := c.clock.NewTimer(time.Second)
	// we need a stopped timer rather than nil,
	// because we will be select waiting on timer.C()
	if !timer.Stop() {
		<-timer.C()
	}

	// if timer is already started, this is a no-op
	startTimer := func() {
		if !ticking {
			ticking = true
			timer.Reset(c.support.SharedConfig().BatchTimeout())
		}
	}

	stopTimer := func() {
		if !timer.Stop() && ticking {
			// we only need to drain the channel if the timer expired (not explicitly stopped)
			<-timer.C()
		}
		ticking = false
	}

	var soft raft.SoftState
	submitC := c.submitC
	var bc *blockCreator

	var propC chan<- *common.Block
	var cancelProp context.CancelFunc
	cancelProp = func() {} // no-op as initial value

	becomeLeader := func() (chan<- *common.Block, context.CancelFunc) {
		c.Metrics.IsLeader.Set(1)

		c.blockInflight = 0
		c.justElected = true
		submitC = nil
		ch := make(chan *common.Block, c.opts.MaxInflightBlocks)

		// if there is unfinished ConfChange, we should resume the effort to propose it as
		// new leader, and wait for it to be committed before start serving new requests.
		if cc := c.getInFlightConfChange(); cc != nil {
			// The reason `ProposeConfChange` should be called in go routine is documented in `writeConfigBlock` method.
			go func() {
				if err := c.Node.ProposeConfChange(context.TODO(), *cc); err != nil {
					c.logger.Warnf("Failed to propose configuration update to Raft node: %s", err)
				}
			}()

			c.confChangeInProgress = cc
			c.configInflight = true
		}

		// Leader should call Propose in go routine, because this method may be blocked
		// if node is leaderless (this can happen when leader steps down in a heavily
		// loaded network). We need to make sure applyC can still be consumed properly.
		ctx, cancel := context.WithCancel(context.Background())
		go func(ctx context.Context, ch <-chan *common.Block) {
			for {
				select {
				case b := <-ch:
					data := protoutil.MarshalOrPanic(b)
					if err := c.Node.Propose(ctx, data); err != nil {
						c.logger.Errorf("Failed to propose block [%d] to raft and discard %d blocks in queue: %s", b.Header.Number, len(ch), err)
						return
					}
					c.logger.Debugf("Proposed block [%d] to raft consensus", b.Header.Number)

				case <-ctx.Done():
					c.logger.Debugf("Quit proposing blocks, discarded %d blocks in the queue", len(ch))
					return
				}
			}
		}(ctx, ch)

		return ch, cancel
	}

	becomeFollower := func() {
		cancelProp()
		c.blockInflight = 0
		_ = c.support.BlockCutter().Cut()
		stopTimer()
		submitC = c.submitC
		bc = nil
		c.Metrics.IsLeader.Set(0)
	}

	for {
		select {
		case s := <-submitC:
			if s == nil {
				// polled by `WaitReady`
				continue
			}

			if soft.RaftState == raft.StatePreCandidate || soft.RaftState == raft.StateCandidate {
				s.leader <- raft.None
				continue
			}

			s.leader <- soft.Lead
			if soft.Lead != c.raftID {
				continue
			}

			batches, pending, err := c.ordered(s.req)
			if err != nil {
				c.logger.Errorf("Failed to order message: %s", err)
				continue
			}

			if !pending && len(batches) == 0 {
				continue
			}

			if pending {
				startTimer() // no-op if timer is already started
			} else {
				stopTimer()
			}

			c.propose(propC, bc, batches...)

			if c.configInflight {
				c.logger.Info("已接收配置事务, 暂停接受事务直到提交")
				submitC = nil
			} else if c.blockInflight >= c.opts.MaxInflightBlocks {
				c.logger.Debugf("Number of in-flight blocks (%d) reaches limit (%d), pause accepting transaction",
					c.blockInflight, c.opts.MaxInflightBlocks)
				submitC = nil
			}

		case app := <-c.applyC:
			if app.soft != nil {
				newLeader := atomic.LoadUint64(&app.soft.Lead) // etcdraft requires atomic access
				if newLeader != soft.Lead {
					c.logger.Infof("Raft 共识节点的 leader 改变: 从%d -> 到%d", soft.Lead, newLeader)
					c.Metrics.LeaderChanges.Add(1)

					atomic.StoreUint64(&c.lastKnownLeader, newLeader)

					if newLeader == c.raftID {
						propC, cancelProp = becomeLeader()
					}

					if soft.Lead == c.raftID {
						becomeFollower()
					}
				}

				foundLeader := soft.Lead == raft.None && newLeader != raft.None
				quitCandidate := isCandidate(soft.RaftState) && !isCandidate(app.soft.RaftState)

				if foundLeader || quitCandidate {
					c.errorCLock.Lock()
					c.errorC = make(chan struct{})
					c.errorCLock.Unlock()
				}

				if isCandidate(app.soft.RaftState) || newLeader == raft.None {
					atomic.StoreUint64(&c.lastKnownLeader, raft.None)
					select {
					case <-c.errorC:
					default:
						nodeCount := len(c.opts.BlockMetadata.ConsenterIds)
						// Only close the error channel (to signal the broadcast/deliver front-end a consensus backend error)
						// If we are a cluster of size 3 or more, otherwise we can't expand a cluster of size 1 to 2 nodes.
						if nodeCount > 2 {
							close(c.errorC)
						} else {
							c.logger.Warningf("No leader is present, cluster size is %d", nodeCount)
						}
					}
				}

				soft = raft.SoftState{Lead: newLeader, RaftState: app.soft.RaftState}

				// notify external observer
				select {
				case c.observeC <- soft:
				default:
				}
			}

			c.apply(app.entries)

			if c.justElected {
				msgInflight := c.Node.lastIndex() > c.appliedIndex
				if msgInflight {
					c.logger.Debugf("There are in flight blocks, new leader should not serve requests")
					continue
				}

				if c.configInflight {
					c.logger.Debugf("There is config block in flight, new leader should not serve requests")
					continue
				}

				c.logger.Infof("Start accepting requests as Raft leader at block [%d]", c.lastBlock.Header.Number)
				bc = &blockCreator{
					hash:   protoutil.BlockHeaderHash(c.lastBlock.Header),
					number: c.lastBlock.Header.Number,
					logger: c.logger,
				}
				submitC = c.submitC
				c.justElected = false
			} else if c.configInflight {
				c.logger.Info("Config block or ConfChange in flight, pause accepting transaction")
				submitC = nil
			} else if c.blockInflight < c.opts.MaxInflightBlocks {
				submitC = c.submitC
			}

		case <-timer.C():
			ticking = false

			batch := c.support.BlockCutter().Cut()
			if len(batch) == 0 {
				c.logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}

			c.logger.Debugf("Batch timer expired, creating block")
			c.propose(propC, bc, batch) // we are certain this is normal block, no need to block

		case sn := <-c.snapC:
			if sn.Metadata.Index != 0 {
				if sn.Metadata.Index <= c.appliedIndex {
					c.logger.Debugf("Skip snapshot taken at index %d, because it is behind current applied index %d", sn.Metadata.Index, c.appliedIndex)
					break
				}

				c.confState = sn.Metadata.ConfState
				c.appliedIndex = sn.Metadata.Index
			} else {
				c.logger.Infof("Received artificial snapshot to trigger catchup")
			}

			if err := c.catchUp(sn); err != nil {
				c.logger.Panicf("Failed to recover from snapshot taken at Term %d and Index %d: %s",
					sn.Metadata.Term, sn.Metadata.Index, err)
			}

		case <-c.doneC:
			stopTimer()
			cancelProp()

			select {
			case <-c.errorC: // avoid closing closed channel
			default:
				close(c.errorC)
			}

			c.logger.Infof("Stop serving requests")
			c.periodicChecker.Stop()
			return
		}
	}
}

func (c *Chain) writeBlock(block *common.Block, index uint64) {
	if block.Header.Number > c.lastBlock.Header.Number+1 {
		c.logger.Panicf("Got block [%d], expect block [%d]", block.Header.Number, c.lastBlock.Header.Number+1)
	} else if block.Header.Number < c.lastBlock.Header.Number+1 {
		c.logger.Infof("Got block [%d], expect block [%d], this node was forced to catch up", block.Header.Number, c.lastBlock.Header.Number+1)
		return
	}

	if c.blockInflight > 0 {
		c.blockInflight-- // only reduce on leader
	}
	c.lastBlock = block

	c.logger.Infof("写入块 [%d] (Raft 索引: %d) 至通道账本", block.Header.Number, index)

	if protoutil.IsConfigBlock(block) {
		c.writeConfigBlock(block, index)
		return
	}

	c.raftMetadataLock.Lock()
	c.opts.BlockMetadata.RaftIndex = index
	m := protoutil.MarshalOrPanic(c.opts.BlockMetadata)
	c.raftMetadataLock.Unlock()

	c.support.WriteBlock(block, m)
}

// Orders the envelope in the `msg` content. SubmitRequest.
// Returns
//
//	-- batches [][]*common.Envelope; the batches cut,
//	-- pending bool; if there are envelopes pending to be ordered,
//	-- err error; the error encountered, if any.
//
// It takes care of config messages as well as the revalidation of messages if the config sequence has advanced.
func (c *Chain) ordered(msg *orderer.SubmitRequest) (batches [][]*common.Envelope, pending bool, err error) {
	seq := c.support.Sequence()

	isconfig, err := c.isConfig(msg.Payload)
	if err != nil {
		return nil, false, errors.Errorf("bad message: %s", err)
	}

	if isconfig {
		// ConfigMsg
		if msg.LastValidationSeq < seq {
			c.logger.Warnf("Config message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
			msg.Payload, _, err = c.support.ProcessConfigMsg(msg.Payload)
			if err != nil {
				c.Metrics.ProposalFailures.Add(1)
				return nil, true, errors.Errorf("bad config message: %s", err)
			}
		}

		if c.checkForEvictionNCertRotation(msg.Payload) {

			if !atomic.CompareAndSwapUint32(&c.leadershipTransferInProgress, 0, 1) {
				c.logger.Warnf("A reconfiguration transaction is already in progress, ignoring a subsequent transaction")
				return
			}

			go func() {
				defer atomic.StoreUint32(&c.leadershipTransferInProgress, 0)

				for attempt := 1; attempt <= AbdicationMaxAttempts; attempt++ {
					if err := c.Node.abdicateLeadership(); err != nil {
						// If there is no leader, abort and do not retry.
						// Return early to prevent re-submission of the transaction
						if err == ErrNoLeader || err == ErrChainHalting {
							return
						}

						// If the error isn't any of the below, it's a programming error, so panic.
						if err != ErrNoAvailableLeaderCandidate && err != ErrTimedOutLeaderTransfer {
							c.logger.Panicf("Programming error, abdicateLeader() returned with an unexpected error: %v", err)
						}

						// Else, it's one of the errors above, so we retry.
						continue
					} else {
						// Else, abdication succeeded, so we submit the transaction (which forwards to the leader)
						if err := c.Submit(msg, 0); err != nil {
							c.logger.Warnf("Reconfiguration transaction forwarding failed with error: %v", err)
						}
						return
					}
				}

				c.logger.Warnf("Abdication failed too many times consecutively (%d), aborting retries", AbdicationMaxAttempts)
			}()
			return nil, false, nil
		}

		batch := c.support.BlockCutter().Cut()
		batches = [][]*common.Envelope{}
		if len(batch) != 0 {
			batches = append(batches, batch)
		}
		batches = append(batches, []*common.Envelope{msg.Payload})
		return batches, false, nil
	}
	// it is a normal message
	if msg.LastValidationSeq < seq {
		c.logger.Warnf("Normal message was validated against %d, although current config seq has advanced (%d)", msg.LastValidationSeq, seq)
		if _, err := c.support.ProcessNormalMsg(msg.Payload); err != nil {
			c.Metrics.ProposalFailures.Add(1)
			return nil, true, errors.Errorf("bad normal message: %s", err)
		}
	}
	batches, pending = c.support.BlockCutter().Ordered(msg.Payload)
	return batches, pending, nil
}

func (c *Chain) propose(ch chan<- *common.Block, bc *blockCreator, batches ...[]*common.Envelope) {
	for _, batch := range batches {
		b := bc.createNextBlock(batch)
		c.logger.Infof("已创建的块 [%d], 有 %d 个块在传输中", b.Header.Number, c.blockInflight)

		select {
		case ch <- b:
		default:
			c.logger.Panic("Programming error: limit of in-flight blocks does not properly take effect or block is proposed by follower")
		}

		// if it is config block, then we should wait for the commit of the block
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

func (c *Chain) detectConfChange(block *common.Block) *MembershipChanges {
	// If config is targeting THIS channel, inspect consenter set and
	// propose raft ConfChange if it adds/removes node.
	configMetadata := c.newConfigMetadata(block)

	if configMetadata == nil {
		return nil
	}

	if configMetadata.Options != nil &&
		configMetadata.Options.SnapshotIntervalSize != 0 &&
		configMetadata.Options.SnapshotIntervalSize != c.sizeLimit {
		c.logger.Infof("Update snapshot interval size to %d bytes (was %d)",
			configMetadata.Options.SnapshotIntervalSize, c.sizeLimit)
		c.sizeLimit = configMetadata.Options.SnapshotIntervalSize
	}

	changes, err := ComputeMembershipChanges(c.opts.BlockMetadata, c.opts.Consenters, configMetadata.Consenters)
	if err != nil {
		c.logger.Panicf("illegal configuration change detected: %s", err)
	}

	if changes.Rotated() {
		c.logger.Infof("Config block [%d] rotates TLS certificate of node %d", block.Header.Number, changes.RotatedNode)
	}

	return changes
}

func (c *Chain) apply(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}

	if ents[0].Index > c.appliedIndex+1 {
		c.logger.Panicf("first index of committed entry[%d] should <= appliedIndex[%d]+1", ents[0].Index, c.appliedIndex)
	}

	var position int
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}

			position = i
			c.accDataSize += uint32(len(ents[i].Data))

			// We need to strictly avoid re-applying normal entries,
			// otherwise we are writing the same block twice.
			if ents[i].Index <= c.appliedIndex {
				c.logger.Debugf("Received block with raft index (%d) <= applied index (%d), skip", ents[i].Index, c.appliedIndex)
				break
			}

			block := protoutil.UnmarshalBlockOrPanic(ents[i].Data)
			c.writeBlock(block, ents[i].Index)
			c.Metrics.CommittedBlockNumber.Set(float64(block.Header.Number))

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				c.logger.Warnf("Failed to unmarshal ConfChange data: %s", err)
				continue
			}

			c.confState = *c.Node.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				c.logger.Infof("已应用配置更改以添加节点 %d, 通道中的当前节点: %+v", cc.NodeID, c.confState.Nodes)
			case raftpb.ConfChangeRemoveNode:
				c.logger.Infof("应用配置更改以删除节点 %d, 通道中的当前节点: %+v", cc.NodeID, c.confState.Nodes)
			default:
				c.logger.Panic("Programming error, encountered unsupported raft config change")
			}

			// This ConfChange was introduced by a previously committed config block,
			// we can now unblock submitC to accept envelopes.
			var configureComm bool
			if c.confChangeInProgress != nil &&
				c.confChangeInProgress.NodeID == cc.NodeID &&
				c.confChangeInProgress.Type == cc.Type {

				configureComm = true
				c.confChangeInProgress = nil
				c.configInflight = false
				// report the new cluster size
				c.Metrics.ClusterSize.Set(float64(len(c.opts.BlockMetadata.ConsenterIds)))
			}

			shouldHalt := cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == c.raftID
			// unblock `run` go routine so it can still consume Raft messages
			go func() {
				if configureComm && !shouldHalt { // no need to configure comm if this node is going to halt
					if err := c.configureComm(); err != nil {
						c.logger.Panicf("Failed to configure communication: %s", err)
					}
				}

				if shouldHalt {
					c.logger.Infof("This node is being removed from replica set")
					c.halt()
					return
				}
			}()
		}

		if ents[i].Index > c.appliedIndex {
			c.appliedIndex = ents[i].Index
		}
	}

	if c.accDataSize >= c.sizeLimit {
		b := protoutil.UnmarshalBlockOrPanic(ents[position].Data)

		select {
		case c.gcC <- &gc{index: c.appliedIndex, state: c.confState, data: ents[position].Data}:
			c.logger.Infof("Accumulated %d bytes since last snapshot, exceeding size limit (%d bytes), "+
				"taking snapshot at block [%d] (index: %d), last snapshotted block number is %d, current nodes: %+v",
				c.accDataSize, c.sizeLimit, b.Header.Number, c.appliedIndex, c.lastSnapBlockNum, c.confState.Nodes)
			c.accDataSize = 0
			c.lastSnapBlockNum = b.Header.Number
			c.Metrics.SnapshotBlockNumber.Set(float64(b.Header.Number))
		default:
			c.logger.Warnf("Snapshotting is in progress, it is very likely that SnapshotIntervalSize is too small")
		}
	}
}

func (c *Chain) gc() {
	for {
		select {
		case g := <-c.gcC:
			c.Node.takeSnapshot(g.index, g.state, g.data)
		case <-c.doneC:
			c.logger.Infof("Stop garbage collecting")
			return
		}
	}
}

func (c *Chain) isConfig(env *common.Envelope) (bool, error) {
	h, err := protoutil.ChannelHeader(env)
	if err != nil {
		c.logger.Errorf("failed to extract channel header from envelope")
		return false, err
	}

	return h.Type == int32(common.HeaderType_CONFIG) || h.Type == int32(common.HeaderType_ORDERER_TRANSACTION), nil
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

// writeConfigBlock writes configuration blocks into the ledger in
// addition extracts updates about raft replica set and if there
// are changes updates cluster membership as well
func (c *Chain) writeConfigBlock(block *common.Block, index uint64) {
	hdr, err := ConfigChannelHeader(block)
	if err != nil {
		c.logger.Panicf("Failed to get config header type from config block: %s", err)
	}

	c.configInflight = false

	switch common.HeaderType(hdr.Type) {
	case common.HeaderType_CONFIG:
		configMembership := c.detectConfChange(block)

		c.raftMetadataLock.Lock()
		c.opts.BlockMetadata.RaftIndex = index
		if configMembership != nil {
			c.opts.BlockMetadata = configMembership.NewBlockMetadata
			c.opts.Consenters = configMembership.NewConsenters
		}
		c.raftMetadataLock.Unlock()

		blockMetadataBytes := protoutil.MarshalOrPanic(c.opts.BlockMetadata)

		// write block with metadata
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

// getInFlightConfChange returns ConfChange in-flight if any.
// It returns confChangeInProgress if it is not nil. Otherwise
// it returns ConfChange from the last committed block (might be nil).
func (c *Chain) getInFlightConfChange() *raftpb.ConfChange {
	if c.confChangeInProgress != nil {
		return c.confChangeInProgress
	}

	if c.lastBlock.Header.Number == 0 {
		return nil // nothing to failover just started the chain
	}

	if !protoutil.IsConfigBlock(c.lastBlock) {
		return nil
	}

	// extracting current Raft configuration state
	confState := c.Node.ApplyConfChange(raftpb.ConfChange{})

	if len(confState.Nodes) == len(c.opts.BlockMetadata.ConsenterIds) {
		// Raft configuration change could only add one node or
		// remove one node at a time, if raft conf state size is
		// equal to membership stored in block metadata field,
		// that means everything is in sync and no need to propose
		// config update.
		return nil
	}

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

// checkForEvictionNCertRotation checks for node eviction and
// certificate rotation, return true if request includes it
// otherwise returns false
func (c *Chain) checkForEvictionNCertRotation(env *common.Envelope) bool {
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		c.logger.Warnf("failed to extract payload from config envelope: %s", err)
		return false
	}

	configUpdate, err := configtx.UnmarshalConfigUpdateFromPayload(payload)
	if err != nil {
		c.logger.Warnf("could not read config update: %s", err)
		return false
	}

	configMeta, err := MetadataFromConfigUpdate(configUpdate)
	if err != nil || configMeta == nil {
		c.logger.Warnf("无法读取配置元数据: %s", err)
		return false
	}

	membershipUpdates, err := ComputeMembershipChanges(c.opts.BlockMetadata, c.opts.Consenters, configMeta.Consenters)
	if err != nil {
		c.logger.Warnf("illegal configuration change detected: %s", err)
		return false
	}

	if membershipUpdates.RotatedNode == c.raftID {
		c.logger.Infof("Detected certificate rotation of our node")
		return true
	}

	if _, exists := membershipUpdates.NewConsenters[c.raftID]; !exists {
		c.logger.Infof("Detected eviction of ourselves from the configuration")
		return true
	}

	c.logger.Debugf("Node %d is still part of the consenters set", c.raftID)
	return false
}
