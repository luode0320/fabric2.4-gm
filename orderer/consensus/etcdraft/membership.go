/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// MembershipByCert 函数将共识者映射转换为以客户端 TLS 证书为键的集合封装的映射。
func MembershipByCert(consenters map[uint64]*etcdraft.Consenter) map[string]uint64 {
	set := map[string]uint64{}

	// 遍历共识者映射，将客户端 TLS 证书作为键，节点 ID 作为值存入集合中
	for nodeID, c := range consenters {
		set[string(c.ClientTlsCert)] = nodeID
	}

	return set
}

// MembershipChanges 结构体用于保存在配置更新期间引入的成员变更信息。
type MembershipChanges struct {
	NewBlockMetadata *etcdraft.BlockMetadata        // 新的区块元数据
	NewConsenters    map[uint64]*etcdraft.Consenter // 新的共识者映射
	AddedNodes       []*etcdraft.Consenter          // 新增的共识者列表
	RemovedNodes     []*etcdraft.Consenter          // 移除的共识者列表
	ConfChange       *raftpb.ConfChange             // Raft 配置变更
	RotatedNode      uint64                         // 被轮换的节点 ID
}

// ComputeMembershipChanges 函数根据新共识者信息计算成员更新，返回两个切片：一个包含新增共识者，一个包含待移除的共识者。
func ComputeMembershipChanges(oldMetadata *etcdraft.BlockMetadata, oldConsenters map[uint64]*etcdraft.Consenter, newConsenters []*etcdraft.Consenter) (mc *MembershipChanges, err error) {
	// 初始化结果结构, 保存在配置更新期间引入的成员变更信息。
	result := &MembershipChanges{
		NewConsenters:    map[uint64]*etcdraft.Consenter{},                   // 新的共识者映射
		NewBlockMetadata: proto.Clone(oldMetadata).(*etcdraft.BlockMetadata), // 新的区块元数据
		AddedNodes:       []*etcdraft.Consenter{},                            // 新增的共识者列表
		RemovedNodes:     []*etcdraft.Consenter{},                            // 移除的共识者列表
	}

	result.NewBlockMetadata.ConsenterIds = make([]uint64, len(newConsenters))

	var addedNodeIndex int
	// 将共识者映射转换为以客户端 TLS 证书为键的集合封装的映射。
	currentConsentersSet := MembershipByCert(oldConsenters)

	// 遍历新共识者列表
	for i, c := range newConsenters {
		// 使用共识者证书判断是否是同一个共识节点
		if nodeID, exists := currentConsentersSet[string(c.ClientTlsCert)]; exists {
			// 新的区块元数据
			result.NewBlockMetadata.ConsenterIds[i] = nodeID
			// 新的共识者映射
			result.NewConsenters[nodeID] = c
			continue
		}
		addedNodeIndex = i
		// 新增的共识者列表
		result.AddedNodes = append(result.AddedNodes, c)
	}

	var deletedNodeID uint64
	// 将共识者映射为以客户端 TLS 证书为键的集合。
	newConsentersSet := ConsentersToMap(newConsenters)

	// 检查旧共识者列表中是否有被移除的共识者
	for nodeID, c := range oldConsenters {
		if !newConsentersSet.Exists(c) {
			// 移除的共识者列表
			result.RemovedNodes = append(result.RemovedNodes, c)
			deletedNodeID = nodeID
		}
	}

	switch {
	case len(result.AddedNodes) == 1 && len(result.RemovedNodes) == 1:
		// 如果只有一个新节点被添加且只有一个现有节点被移除，则认为证书被轮换
		result.RotatedNode = deletedNodeID
		result.NewBlockMetadata.ConsenterIds[addedNodeIndex] = deletedNodeID
		result.NewConsenters[deletedNodeID] = result.AddedNodes[0]
	case len(result.AddedNodes) == 1 && len(result.RemovedNodes) == 0:
		// 新节点
		nodeID := result.NewBlockMetadata.NextConsenterId
		result.NewConsenters[nodeID] = result.AddedNodes[0]
		result.NewBlockMetadata.ConsenterIds[addedNodeIndex] = nodeID
		result.NewBlockMetadata.NextConsenterId++
		result.ConfChange = &raftpb.ConfChange{
			NodeID: nodeID,
			Type:   raftpb.ConfChangeAddNode,
		}
	case len(result.AddedNodes) == 0 && len(result.RemovedNodes) == 1:
		// 移除节点
		nodeID := deletedNodeID
		result.ConfChange = &raftpb.ConfChange{
			NodeID: nodeID,
			Type:   raftpb.ConfChangeRemoveNode,
		}
		delete(result.NewConsenters, nodeID)
	case len(result.AddedNodes) == 0 && len(result.RemovedNodes) == 0:
		// 没有变化
	default:
		// len(result.AddedNodes) > 1 || len(result.RemovedNodes) > 1 {
		return nil, errors.Errorf("不支持一次更新多个共识 consenter 节点, 请求更改: %s", result)
	}

	return result, nil
}

// Stringer implements fmt.Stringer interface
func (mc *MembershipChanges) String() string {
	return fmt.Sprintf("add %d node(s), remove %d node(s)", len(mc.AddedNodes), len(mc.RemovedNodes))
}

// Changed indicates whether these changes actually do anything
func (mc *MembershipChanges) Changed() bool {
	return len(mc.AddedNodes) > 0 || len(mc.RemovedNodes) > 0
}

// Rotated indicates whether the change was a rotation
func (mc *MembershipChanges) Rotated() bool {
	return len(mc.AddedNodes) == 1 && len(mc.RemovedNodes) == 1
}

// UnacceptableQuorumLoss returns true if membership change will result in avoidable quorum loss,
// given current number of active nodes in cluster. Avoidable means that more nodes can be started
// to prevent quorum loss. Sometimes, quorum loss is inevitable, for example expanding 1-node cluster.
func (mc *MembershipChanges) UnacceptableQuorumLoss(active []uint64) bool {
	activeMap := make(map[uint64]struct{})
	for _, i := range active {
		activeMap[i] = struct{}{}
	}

	isCFT := len(mc.NewConsenters) > 2 // if resulting cluster cannot tolerate any fault, quorum loss is inevitable
	quorum := len(mc.NewConsenters)/2 + 1

	switch {
	case mc.ConfChange != nil && mc.ConfChange.Type == raftpb.ConfChangeAddNode: // Add
		return isCFT && len(active) < quorum

	case mc.RotatedNode != raft.None: // Rotate
		delete(activeMap, mc.RotatedNode)
		return isCFT && len(activeMap) < quorum

	case mc.ConfChange != nil && mc.ConfChange.Type == raftpb.ConfChangeRemoveNode: // Remove
		delete(activeMap, mc.ConfChange.NodeID)
		return len(activeMap) < quorum

	default: // No change
		return false
	}
}
