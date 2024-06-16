/*
版权所有 IBM Corp. 2017保留所有权利。

SPDX-License-Identifier: Apache-2.0
*/

package consensus // 导入共识相关的包

import "github.com/hyperledger/fabric/orderer/common/types" // 引入Hyperledger Fabric中orderer组件的common类型定义

// StatusReporter 接口是由集群类型的Chain实现的。
// 它使得节点能够报告其在集群中的关系及其在该关系中的状态。
// 这些信息用于生成响应特定通道“列表”请求的channelparticipation.ChannelInfo。
//
// 不是所有的链都需要实现这个接口，特别是非集群类型（如solo、kafka）的链在构造时会被分配一个StaticStatusReporter。
type StatusReporter interface {
	// StatusReport 提供集群关系及状态信息。
	// 详情请参阅: channelparticipation.ChannelInfo。
	StatusReport() (types.ConsensusRelation, types.Status)
}

// StaticStatusReporter 旨在为不实现StatusReporter接口的链提供服务。
type StaticStatusReporter struct {
	ConsensusRelation types.ConsensusRelation // 集群中的共识关系
	Status            types.Status            // 当前状态
}

// StatusReport 方法返回StaticStatusReporter结构体中预先设定的集群关系和状态信息。
func (s StaticStatusReporter) StatusReport() (types.ConsensusRelation, types.Status) {
	return s.ConsensusRelation, s.Status
}
