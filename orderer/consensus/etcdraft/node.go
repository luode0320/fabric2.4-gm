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

var (
	ErrChainHalting               = errors.New("chain halting is in progress")
	ErrNoAvailableLeaderCandidate = errors.New("leadership transfer failed to identify transferee")
	ErrTimedOutLeaderTransfer     = errors.New("leadership transfer timed out")
	ErrNoLeader                   = errors.New("no leader")
)

type node struct {
	chainID string
	logger  *flogging.FabricLogger
	metrics *Metrics

	unreachableLock sync.RWMutex
	unreachable     map[uint64]struct{}

	tracker *Tracker

	storage *RaftStorage
	config  *raft.Config

	rpc RPC

	chain *Chain

	tickInterval time.Duration
	clock        clock.Clock

	metadata *etcdraft.BlockMetadata

	leaderChangeSubscription atomic.Value

	raft.Node
}

func (n *node) start(fresh, join bool) {
	raftPeers := RaftPeers(n.metadata.ConsenterIds)
	n.logger.Debugf("Starting raft node: #peers: %v", len(raftPeers))

	var campaign bool
	if fresh {
		if join {
			raftPeers = nil
			n.logger.Info("Starting raft node to join an existing channel")
		} else {
			n.logger.Info("Starting raft node as part of a new channel")

			// determine the node to start campaign by selecting the node with ID equals to:
			//                hash(channelID) % cluster_size + 1

			// todo luode 进行国密sm3的改造
			switch factory.FactoryName {
			case factory.GuomiBasedFactoryName:
				sha := sm3.Sm3Sum([]byte(n.chainID))
				number, _ := proto.DecodeVarint(sha[24:])
				if n.config.ID == number%uint64(len(raftPeers))+1 {
					campaign = true
				}
			default:
				sha := sha256.Sum256([]byte(n.chainID))
				number, _ := proto.DecodeVarint(sha[24:])
				if n.config.ID == number%uint64(len(raftPeers))+1 {
					campaign = true
				}
			}

		}
		n.Node = raft.StartNode(n.config, raftPeers)
	} else {
		n.logger.Info("重新启动raft节点")
		n.Node = raft.RestartNode(n.config)
	}

	go n.run(campaign)
}

func (n *node) run(campaign bool) {
	// 计算选举超时时间
	electionTimeout := n.tickInterval.Seconds() * float64(n.config.ElectionTick)
	// 计算选举超时时间的一半
	halfElectionTimeout := electionTimeout / 2
	// 创建一个 Raft 时钟
	raftTicker := n.clock.NewTicker(n.tickInterval)
	// 如果存储中存在快照
	if s := n.storage.Snapshot(); !raft.IsEmptySnap(s) {
		// 将快照发送到 n.chain.snapC 通道
		n.chain.snapC <- &s
	}

	// 创建一个无缓冲的通道，用于通知选举结果
	elected := make(chan struct{})
	if campaign {
		n.logger.Infof("选取此节点以启动活动")
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

			n.Tick()                 // 执行 Raft 的滴答操作
			n.tracker.Check(&status) // 检查 Raft 的状态
		case rd := <-n.Ready():
			// 当从 `n.Ready()` 通道接收到数据时执行以下操作：

			// 记录开始存储的时间
			startStoring := n.clock.Now()

			// 如果存储失败，则记录错误并终止程序
			if err := n.storage.Store(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
				n.logger.Panicf("etcd/raft 数据持久化失败: %s", err)
			}

			// 计算存储持续的时间
			duration := n.clock.Since(startStoring).Seconds()
			// 记录存储持续时间的指标
			n.metrics.DataPersistDuration.Observe(float64(duration))
			if duration > halfElectionTimeout {
				n.logger.Warningf("WAL sync 同步花费了 %v 秒，并且网络配置为在 %v 秒后开始选举. 您的磁盘速度太慢，可能会导致法定人数丢失并触发领导选举.", duration, electionTimeout)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				// 如果快照不为空，则将其发送到 n.chain.snapC 通道
				n.chain.snapC <- &rd.Snapshot
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
				// 将应用的数据发送到 n.chain.applyC 通道
				n.chain.applyC <- apply{rd.CommittedEntries, rd.SoftState}
			}

			// 当前节点是否正在进行选举, SoftState = 当前节点的状态
			if campaign && rd.SoftState != nil {
				// 使用原子操作获取 SoftState 中的 Lead 字段，即当前节点的领导者 ID
				leader := atomic.LoadUint64(&rd.SoftState.Lead)
				// 如果 leader 不等于 raft.None，表示当前节点已经有了领导者
				if leader != raft.None {
					n.logger.Infof("领导 %d 存在, 退出活动", leader)
					campaign = false
					close(elected)
				}
			}

			// 将当前节点的状态推进到 Ready() 中的最新状态。这样可以通知 Raft 模块当前节点已经处理了 Ready() 中的数据
			n.Advance()

			// TODO(jay_guo) leader可以与复制并行地写入磁盘
			// 给追随者和他们写到他们的磁盘。在论文中检查10.2.1
			// 将 Ready() 中的消息发送给其他节点。rd.Messages 是一个消息列表，其中包含了需要发送给其他节点的消息。这些消息可能包括附加日志条目、请求投票等
			n.send(rd.Messages)

		case <-n.chain.haltC:
			// 当 `n.chain.haltC` 通道收到信号时执行以下操作：

			raftTicker.Stop()            // 停止 Raft 时钟
			n.Stop()                     // 停止 Raft 节点
			n.storage.Close()            // 关闭存储
			n.logger.Infof("Raft 节点已停止") // 打印日志，表示 Raft 节点已停止
			close(n.chain.doneC)         // 关闭 n.chain.doneC 通道，表示链已完成
			return                       // 退出函数
		}
	}
}

func (n *node) send(msgs []raftpb.Message) {
	n.unreachableLock.RLock()
	defer n.unreachableLock.RUnlock()

	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}

		status := raft.SnapshotFinish
		bytes, _ := msg.Marshal()
		err := n.rpc.SendConsensus(msg.To, &orderer.ConsensusRequest{Channel: n.chainID, Payload: bytes})
		if err != nil {
			n.ReportUnreachable(msg.To)
			n.logSendFailure(msg.To, err)

			status = raft.SnapshotFailure
		} else if _, ok := n.unreachable[msg.To]; ok {
			n.logger.Infof("尝试失败后, 已成功将 StepRequest 发送到 %d", msg.To)
			delete(n.unreachable, msg.To)
		}

		if msg.Type == raftpb.MsgSnap {
			n.ReportSnapshot(msg.To, status)
		}
	}
}

// abdicateLeadership 选择一个最近处于活动状态的节点，并尝试将领导权转移给它。
// 阻塞，直到领导权转移发生或超时。
// 失败时返回错误。
func (n *node) abdicateLeadership() error {
	start := time.Now()
	defer func() {
		n.logger.Infof("abdicateLeader took %v", time.Since(start))
	}()

	status := n.Status()

	if status.Lead == raft.None {
		n.logger.Warn("No leader, cannot transfer leadership")
		return ErrNoLeader
	}

	if status.Lead != n.config.ID {
		n.logger.Warn("Leader has changed since asked to transfer leadership")
		return nil
	}

	// 登记领导变更
	notifyC, unsubscribe := n.subscribeToLeaderChange()
	defer unsubscribe()

	var transferee uint64
	for id, pr := range status.Progress {
		if id == status.ID {
			continue // skip self
		}

		if pr.RecentActive && !pr.IsPaused() {
			transferee = id
			break
		}

		n.logger.Debugf("Node %d is not qualified as transferee because it's either paused or not active", id)
	}

	if transferee == raft.None {
		n.logger.Errorf("No follower is qualified as transferee, abort leader transfer")
		return ErrNoAvailableLeaderCandidate
	}

	n.logger.Infof("Transferring leadership to %d", transferee)

	timeToWait := time.Duration(n.config.ElectionTick) * n.tickInterval
	n.logger.Infof("Will wait %v time to abdicate", timeToWait)
	ctx, cancel := context.WithTimeout(context.TODO(), timeToWait)
	defer cancel()

	n.TransferLeadership(ctx, status.ID, transferee)

	timer := n.clock.NewTimer(timeToWait)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
			n.logger.Warn("Leader transfer timed out")
			return ErrTimedOutLeaderTransfer
		case l := <-notifyC:
			n.logger.Infof("Leader has been transferred from %d to %d", n.config.ID, l)
			return nil
		case <-n.chain.doneC:
			n.logger.Infof("Returning early because chain is halting")
			return ErrChainHalting
		}
	}
}

func (n *node) subscribeToLeaderChange() (chan uint64, func()) {
	notifyC := make(chan uint64, 1)
	subscriptionActive := uint32(1)
	unsubscribe := func() {
		atomic.StoreUint32(&subscriptionActive, 0)
	}
	subscription := func(leader uint64) {
		if atomic.LoadUint32(&subscriptionActive) == 0 {
			return
		}
		if leader != n.config.ID {
			select {
			case notifyC <- leader:
			default:
				// In case notifyC is full
			}
		}
	}
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

func (n *node) takeSnapshot(index uint64, cs raftpb.ConfState, data []byte) {
	if err := n.storage.TakeSnapshot(index, cs, data); err != nil {
		n.logger.Errorf("Failed to create snapshot at index %d: %s", index, err)
	}
}

func (n *node) lastIndex() uint64 {
	i, _ := n.storage.ram.LastIndex()
	return i
}
