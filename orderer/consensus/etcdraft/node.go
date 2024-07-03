/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"context"
	"crypto/sha256"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/hyperledger/fabric/bccsp/factory"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/clock"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// 错误常量定义
var (
	ErrChainHalting               = errors.New("链停止过程正在进行中")        // 链正在停止的错误
	ErrNoAvailableLeaderCandidate = errors.New("领导权转移未能找到接收转移的候选者") // 领导权转移失败，找不到合适的接替节点
	ErrTimedOutLeaderTransfer     = errors.New("领导权转移超时")           // 领导权转移操作超时
	ErrNoLeader                   = errors.New("没有领导节点")            // 当前没有领导节点的错误
)

// node 结构体定义，代表一个Raft节点
type node struct {
	chainID string                 // 链的ID
	logger  *flogging.FabricLogger // 日志记录器，用于记录日志信息
	metrics *Metrics               // 用于跟踪和记录性能指标

	// 保护不可达节点集合的读写锁和集合本身
	unreachableLock sync.RWMutex
	unreachable     map[uint64]struct{} // 存储不可达的节点ID

	tracker *Tracker // 跟踪器，可能用于跟踪节点状态或其他相关信息

	storage *RaftStorage // Raft存储组件，负责持久化Raft状态和日志
	config  *raft.Config // Raft配置，定义了Raft节点的配置信息

	rpc RPC // RPC接口，用于节点间的通信

	chain *Chain // 链实例，包含了链相关的操作和数据

	tickInterval time.Duration // Raft心跳间隔时间
	clock        clock.Clock   // 时钟接口，用于获取当前时间

	metadata *etcdraft.BlockMetadata // 区块元数据，可能包含共识相关的附加信息

	// 使用原子值存储领导变更订阅信息，以便于并发安全地更新
	leaderChangeSubscription atomic.Value

	// 继承自底层Raft库的Node接口，提供了Raft节点的核心功能
	raft.Node
}

// 开始 Raft 节点的运行, 这里有接受其他节点消息的处理
// fresh: 用于标记这是不是一个全新的 raft 节点，初始启动时使用, 由是否存在共识日志文件判断
// join: 判断是否为全新启动或加入现有通道
func (n *node) start(fresh, join bool) {
	// 根据共识者的ID列表创建RaftPeers
	raftNodes := RaftNodes(n.metadata.ConsenterIds)
	n.logger.Debugf("正在启动 Raft 节点：节点数：%v", len(raftNodes))

	// 是否发起竞选成为领导者
	var campaign bool
	if fresh {
		// 判断是否为全新启动或加入现有通道
		if join {
			// 若加入现有通道，则不使用初始的raft列表
			raftNodes = nil
			n.logger.Info("正在启动 Raft 节点以加入现有通道")
		} else {
			n.logger.Info("正在启动 Raft 节点作为新通道的一部分")

			// 为了决定哪个节点应该发起竞选（成为第一个领导者），计算：
			// hash(通道ID) % 群集大小 + 1，选择该ID的节点作为竞选节点
			// 注意：此逻辑根据加密算法进行了分支处理，支持国密SM3

			// todo luode 进行国密sm3的改造
			switch factory.FactoryName {
			case factory.GuomiBasedFactoryName:
				sha := sm3.Sm3Sum([]byte(n.chainID))
				number, _ := proto.DecodeVarint(sha[24:])
				if n.config.ID == number%uint64(len(raftNodes))+1 {
					campaign = true
				}
			default:
				sha := sha256.Sum256([]byte(n.chainID))
				number, _ := proto.DecodeVarint(sha[24:])
				if n.config.ID == number%uint64(len(raftNodes))+1 {
					campaign = true
				}
			}
		}
		// 根据配置和节点列表启动或新建Raft节点
		n.Node = raft.StartNode(n.config, raftNodes)
	} else {
		n.logger.Info("重新启动raft节点")
		// 重启已有节点
		// RestartNode类似于StartNode ，但不采用对等体列表。
		// 将从存储中还原群集的当前成员身份。如果调用方具有现有状态机，则传入应用到它的最后一个日志索引; 否则使用零。
		n.Node = raft.RestartNode(n.config)
	}

	// 运行raft, 持久化raft共识的数据, 和发送/接受消息给raft的其他节点
	go n.run(campaign)
}

// 运行raft, 持久化raft共识的数据, 和发送/接受消息给raft的其他节点
// campaign: 是否发起竞选成为领导者
func (n *node) run(campaign bool) {
	// 计算选举超时时间
	electionTimeout := n.tickInterval.Seconds() * float64(n.config.ElectionTick)
	// 计算选举超时时间的一半
	halfElectionTimeout := electionTimeout / 2
	// 创建一个 Raft 时钟
	raftTicker := n.clock.NewTicker(n.tickInterval)
	// Snapshot: 返回存储在 内存中 的最新快照, 如果存储中存在快照
	if s := n.storage.Snapshot(); !raft.IsEmptySnap(s) {
		n.chain.snapC <- &s // 程序启动时将最新的一个 内存中 的快照发送到 n.chain.snapC 通道
	}

	// 创建一个无缓冲的通道，用于通知选举结果
	elected := make(chan struct{})
	if campaign {
		n.logger.Infof("选取此节点参与选举领导者以启动活动")
		go func() {
			// 每隔两个 HeartbeatTimeout 心跳超时的时间尝试进行选举活动，直到出现领导者 - 要么此节点成功获取领导权，要么在此节点启动时已经存在其他领导者。
			// 我们可以更懒惰地执行此操作，并在转换为候选人状态后退出主动竞选（不是预候选人，因为其他节点可能尚未启动，在这种情况下，预投票消息将在接收方被丢弃）。
			// 但是目前没有明显的原因要懒惰。
			//
			// 使用 2*HeartbeatTick 心跳滴答是为了在网络延迟显著时避免过多的竞选活动，并且在这种极端情况下 Raft 术语保持前进。
			campaignTicker := n.clock.NewTicker(n.tickInterval * time.Duration(n.config.HeartbeatTick) * 2)
			defer campaignTicker.Stop()

			for {
				select {
				case <-campaignTicker.C():
					// Campaign使节点转换到候选状态并开始竞选以成为领导者
					n.Campaign(context.TODO()) // 发起选举活动
				case <-elected:
					return // 当选举结果通道收到信号时，退出协程
				case <-n.chain.doneC:
					return // 当链结束时，退出协程
				}
			}
		}()
	}

	for {
		select {
		case <-raftTicker.C():
			// 在触发 Raft 时钟时执行以下操作：

			// 在进行 Raft 时钟的下一次滴答之前，获取 Raft 状态，以便 `RecentActive` 属性尚未重置。
			status := n.Status()

			// 滴答将节点的内部逻辑时钟增加一个滴答心跳。选举超时和心跳超时以滴答为单位。
			n.Tick()                 // 执行 Raft 的滴答操作
			n.tracker.Check(&status) // 检查 Raft 的状态. 更新活跃节点的本地内存映射

		// 读取raft共识消息, 返回当前时间点状态的通道
		case rd := <-n.Ready():
			// Ready返回返回当前时间点状态的通道
			// 当从 `n.Ready()` 通道接收到数据时执行以下操作：

			// 记录开始存储的时间: 作用是检测持久化共识快照是否太慢
			startStoring := n.clock.Now()

			// 如果存储失败，则记录错误并终止程序
			// Entries: 条目指定在发送消息之前要保存到稳定存储的数据实体
			// HardState: 在发送消息之前要保存到稳定存储的节点的当前状态
			// Snapshot: 快照指定要保存到稳定存储的快照
			if err := n.storage.Store(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
				n.logger.Panicf("etcd/raft 数据持久化失败: %s", err)
			}

			// 计算存储持续的时间
			duration := n.clock.Since(startStoring).Seconds()
			// 记录存储持续时间的指标
			n.metrics.DataPersistDuration.Observe(float64(duration))
			if duration > halfElectionTimeout {
				n.logger.Warningf("共识 WAL 同步花费了 %v 秒, 并且网络配置为在 %v 秒后开始选举. 您的磁盘速度太慢, 可能会导致法定人数丢失并触发领导选举.", duration, electionTimeout)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				n.chain.snapC <- &rd.Snapshot // 读取共识的快照不为空，则将其发送到 n.chain.snapC 通道
			}

			// 加载 leaderChangeSubscription 领导者更改订阅
			lcs := n.leaderChangeSubscription.Load()

			if lcs != nil && rd.SoftState != nil {
				// 调用 leaderChangeSubscription 函数，传递当前的领导者 ID
				if l := atomic.LoadUint64(&rd.SoftState.Lead); l != raft.None {
					subscription := lcs.(func(uint64))
					subscription(l)
				}
			}

			// 跳过空的应用
			if len(rd.CommittedEntries) != 0 || rd.SoftState != nil {
				n.chain.applyC <- apply{rd.CommittedEntries, rd.SoftState} // 读取raft共识消息, 已提交的条目 + 当前节点的状态
			}

			// 当前节点是否正在进行选举, SoftState = 当前节点的状态
			if campaign && rd.SoftState != nil {
				// 使用原子操作获取 SoftState 中的 Lead 字段，即当前节点的领导者 ID
				leader := atomic.LoadUint64(&rd.SoftState.Lead)
				// 如果 leader 不等于 raft.None，表示当前节点已经有了领导者
				if leader != raft.None {
					n.logger.Infof("raft领导者 %d 存在, 退出选举活动", leader)
					campaign = false
					close(elected)
				}
			}

			// 已经处理完当前的 Ready 事件，可以继续处理下一个 Ready 事件
			n.Advance()

			// 将 Ready() 中的消息发送给其他节点。如果本节点是leader节点, 就会发送给其他所有跟随着
			// 如果本节点是跟随着, 就会发送给leader节点
			n.send(rd.Messages)

		case <-n.chain.haltC:
			// 当 `n.chain.haltC` 通道收到信号时执行以下操作：

			raftTicker.Stop()              // 停止 Raft 时钟
			n.Stop()                       // 停止 Raft 节点
			n.storage.Close()              // 关闭存储
			n.logger.Infof("Raft 共识节点已停止") // 打印日志，表示 Raft 节点已停止
			close(n.chain.doneC)           // 关闭 n.chain.doneC 通道，表示链已完成
			return                         // 退出函数
		}
	}
}

// 将 Ready() 中的消息发送给其他节点。如果本节点是leader节点, 就会发送给其他所有跟随着
// 如果本节点是跟随着, 就会发送给leader节点
func (n *node) send(msgs []raftpb.Message) {
	n.unreachableLock.RLock()         // 加读锁
	defer n.unreachableLock.RUnlock() // 解读锁

	for _, msg := range msgs {
		if msg.To == 0 {
			continue // 如果消息的目标节点为 0，则跳过
		}

		status := raft.SnapshotFinish
		bytes, _ := msg.Marshal() // 序列化消息

		//n.logger.Infof("测试发送 %v -> %v", msg.From, msg.To, msg.Index)
		// 发送共识消息到目标节点, 如果本节点是leader节点, 就会发送给其他所有跟随着, 如果本节点是跟随着, 就会发送给leader节点
		err := n.rpc.SendConsensus(msg.To, &orderer.ConsensusRequest{Channel: n.chainID, Payload: bytes})
		if err != nil {
			n.ReportUnreachable(msg.To)   // 报告目标节点不可达
			n.logSendFailure(msg.To, err) // 记录发送失败日志

			status = raft.SnapshotFailure // 设置快照发送状态为失败
		} else if _, ok := n.unreachable[msg.To]; ok {
			n.logger.Infof("尝试失败后, 已成功将 StepRequest 发送到 %d", msg.To) // 在尝试失败后成功发送消息时记录日志
			delete(n.unreachable, msg.To)                            // 从不可达节点列表中删除目标节点
		}

		if msg.Type == raftpb.MsgSnap {
			n.ReportSnapshot(msg.To, status) // 报告快照发送状态
		}
	}
}

// abdicateLeadership 方法选择一个最近处于活动状态的节点，并尝试将领导权转移给它。
// 阻塞，直到领导权转移发生或超时。失败时返回错误。
func (n *node) abdicateLeadership() error {
	start := time.Now()
	defer func() {
		n.logger.Infof("退位领导者采取了 %v", time.Since(start))
	}()

	// 返回raft状态机的当前状态
	status := n.Status()

	if status.Lead == raft.None {
		n.logger.Warn("没有领导者，无法转移领导权")
		return ErrNoLeader
	}

	if status.Lead != n.config.ID {
		n.logger.Warn("自要求转移领导权以来，领导者已更改")
		return nil
	}

	// 注册领导变更
	notifyC, unsubscribe := n.subscribeToLeaderChange()
	defer unsubscribe()

	var transferee uint64
	for id, pr := range status.Progress {
		if id == status.ID {
			continue // 跳过自身
		}

		// 是否已暂停向此节点发送日志条目
		if pr.RecentActive && !pr.IsPaused() {
			transferee = id
			break
		}

		n.logger.Debugf("节点 %d 不符合转移接收者的条件，因为它要么暂停了要么不活跃", id)
	}

	if transferee == raft.None {
		n.logger.Errorf("没有合格的跟随者作为接收者，放弃领导权转移")
		return ErrNoAvailableLeaderCandidate
	}

	n.logger.Infof("将领导权转移给 %d", transferee)

	timeToWait := time.Duration(n.config.ElectionTick) * n.tickInterval
	n.logger.Infof("将等待 %v 的时间来放弃领导权", timeToWait)
	ctx, cancel := context.WithTimeout(context.TODO(), timeToWait)
	defer cancel()

	// 试图将领导权转移给给定的受让人
	n.TransferLeadership(ctx, status.ID, transferee)

	timer := n.clock.NewTimer(timeToWait)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
			n.logger.Warn("领导权转移超时")
			return ErrTimedOutLeaderTransfer
		case l := <-notifyC:
			n.logger.Infof("领导权已从 %d 转移给 %d", n.config.ID, l)
			return nil
		case <-n.chain.doneC:
			n.logger.Infof("由于链正在停止，提前返回")
			return ErrChainHalting
		}
	}
}

// 注册领导变更
func (n *node) subscribeToLeaderChange() (chan uint64, func()) {
	notifyC := make(chan uint64, 1)
	subscriptionActive := uint32(1)
	unsubscribe := func() {
		atomic.StoreUint32(&subscriptionActive, 0)
	}

	// subscription 函数用于处理领导者变更通知
	subscription := func(leader uint64) {
		if atomic.LoadUint32(&subscriptionActive) == 0 {
			return
		}
		if leader != n.config.ID {
			select {
			case notifyC <- leader:
			default:
				// 如果 notifyC 已满
			}
		}
	}

	// 将 subscription 函数存储到 leaderChangeSubscription 中
	n.leaderChangeSubscription.Store(subscription)

	return notifyC, unsubscribe
}

func (n *node) logSendFailure(dest uint64, err error) {
	if _, ok := n.unreachable[dest]; ok {
		n.logger.Debugf("发送 StepRequest 到 %d 失败, 因为: %s", dest, err)
		return
	}

	n.logger.Errorf("发送 StepRequest 到 %d 失败, 因为: %s", dest, err)
	n.unreachable[dest] = struct{}{}
}

// takeSnapshot 方法用于在给定的节点上创建一个快照。
// 快照包含指定索引处的Raft状态机状态，以及相关的配置状态和数据快照。
func (n *node) takeSnapshot(index uint64, cs raftpb.ConfState, data []byte) {
	// 尝试使用节点的存储组件 storage 来创建一个快照。
	// 传入参数包括快照的索引位置、配置状态（ConfState）及快照数据本身。
	if err := n.storage.TakeSnapshot(index, cs, data); err != nil {
		// 如果在创建快照过程中遇到错误，则通过节点的日志组件记录错误信息。
		// 错误信息会显示失败的快照索引位置以及具体的错误原因。
		n.logger.Errorf("在索引 %d 处创建快照失败: %s", index, err)
	}
}

func (n *node) lastIndex() uint64 {
	i, _ := n.storage.ram.LastIndex()
	return i
}
