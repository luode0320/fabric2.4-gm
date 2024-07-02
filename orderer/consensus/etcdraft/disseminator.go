/*
 Copyright IBM Corp All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"sync"

	"github.com/hyperledger/fabric-protos-go/orderer"
)

// Disseminator 结构体用于在网络通信中附加集群元数据。
// 它会将可用的元数据附着到传出的消息上，以实现元数据的传播。
type Disseminator struct {
	RPC // RPC 接口，用于实际的远程过程调用

	l        sync.Mutex      // 互斥锁，用于同步对sent和metadata的访问
	sent     map[uint64]bool // 已经发送过元数据的目标节点映射
	metadata []byte          // 集群元数据，将被附加到消息中
}

// SendConsensus 方法用于发送共识请求。最终会调用到rpc.SendConsensus发送
// 在发送之前，它会检查是否已经向目标节点发送过元数据。
// 如果尚未发送，并且存在元数据，它会将元数据附加到消息的Metadata字段。
// 然后，使用RPC接口的SendConsensus方法发送更新后的消息。
func (d *Disseminator) SendConsensus(dest uint64, msg *orderer.ConsensusRequest) error {
	d.l.Lock()
	defer d.l.Unlock()

	// 如果还没有向目标节点发送过元数据，并且存在元数据
	if !d.sent[dest] && len(d.metadata) != 0 {
		// 将元数据附加到消息中
		msg.Metadata = d.metadata
		// 标记为目标节点已经接收到元数据
		d.sent[dest] = true
	}

	// 使用RPC接口发送共识请求
	return d.RPC.SendConsensus(dest, msg)
}

// UpdateMetadata 方法用于更新Disseminator的元数据。
// 同时，它会重置sent映射，确保所有节点都需要再次接收新的元数据。
func (d *Disseminator) UpdateMetadata(m []byte) {
	d.l.Lock()
	defer d.l.Unlock()

	// 重置sent映射
	d.sent = make(map[uint64]bool)
	// 更新元数据
	d.metadata = m
}
