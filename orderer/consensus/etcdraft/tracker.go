/*
 Copyright IBM Corp All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"sync/atomic"

	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/protoutil"
	"go.etcd.io/etcd/raft"
)

// Tracker periodically poll Raft Status, and update disseminator
// so that status is populated to followers.
type Tracker struct {
	id     uint64
	sender *Disseminator
	gauge  metrics.Gauge
	active *atomic.Value

	counter int

	logger *flogging.FabricLogger
}

// Check 方法根据 Raft 状态更新活跃节点追踪信息。
func (t *Tracker) Check(status *raft.Status) {
	// 如果当前集群无领导者
	if status.Lead == raft.None {
		t.gauge.Set(0)             // 设置活跃节点计数为0
		t.active.Store([]uint64{}) // 清空活跃节点存储
		return
	}

	// 如果当前节点为跟随者，直接返回，无需更新
	if status.RaftState == raft.StateFollower {
		return
	}

	// 处理领导者逻辑
	// 初始化当前活跃节点列表，包含自己
	current := []uint64{t.id}

	// 遍历进度信息，寻找活跃的节点
	for id, progress := range status.Progress {
		// 跳过自身，因为领导者在当前etcd/raft实现中不标记为最近活跃，但为了未来兼容性做预防处理
		if id == t.id {
			continue
		}

		// 如果节点近期活跃，则加入当前活跃节点列表
		if progress.RecentActive {
			current = append(current, id)
		}
	}

	// 获取上次记录的活跃节点列表
	last := t.active.Load().([]uint64)

	// 更新活跃节点存储
	t.active.Store(current)

	// 检查活跃节点列表是否有变化
	if len(current) != len(last) {
		// 列表长度变化，重置计数器
		t.counter = 0
		return
	}

	// 确保活跃节点列表连续三次相同，认为是稳定的，避免因选举周期导致的短暂波动
	if t.counter < 3 {
		t.counter++
		return
	}

	// 计数器达到稳定条件，重置计数器
	t.counter = 0
	// 记录日志，展示当前集群的活跃节点
	t.logger.Debugf("集群当前的活跃节点为: %+v", current)

	// 更新活跃节点计数的Gauge指标
	t.gauge.Set(float64(len(current)))

	// 序列化活跃节点信息为元数据
	metadata := protoutil.MarshalOrPanic(&etcdraft.ClusterMetadata{ActiveNodes: current})

	// 通过sender更新元数据，通知其他组件或节点
	t.sender.UpdateMetadata(metadata)
}
