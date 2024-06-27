/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

// ErrorResponse carries the error response an HTTP request.
// This is marshaled into the body of the HTTP response.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ChannelList carries the response to an HTTP request to List all the channels.
// This is marshaled into the body of the HTTP response.
// swagger:model channelList
type ChannelList struct {
	// The system channel info, nil if it doesn't exist.
	SystemChannel *ChannelInfoShort `json:"systemChannel"`
	// Application channels only, nil or empty if no channels defined.
	Channels []ChannelInfoShort `json:"channels"`
}

// ChannelInfoShort carries a short info of a single channel.
type ChannelInfoShort struct {
	// The channel name.
	Name string `json:"name"`
	// The channel relative URL (no Host:Port, only path), e.g.: "/participation/v1/channels/my-channel".
	URL string `json:"url"`
}

// ConsensusRelation 表示排序者和通道的共识集群之间的关系。
type ConsensusRelation string

const (
	// ConsensusRelationConsenter 表示Orderer是特定通道中集群共识协议（例如etcdraft）的集群共识者。
	// 即，Orderer位于通道的共识者集合中，直接参与共识决策。
	ConsensusRelationConsenter ConsensusRelation = "consenter"

	// ConsensusRelationFollower 表示Orderer通过从其他Orderer拉取区块的方式跟随集群共识协议。
	// Orderer并不在通道的共识者集合中，而是作为跟随者参与。
	ConsensusRelationFollower ConsensusRelation = "follower"

	// ConsensusRelationConfigTracker 表示Orderer不在通道的共识者集合中，而是仅跟踪（轮询）通道的最后一个配置区块，
	// 以便检测何时被添加到通道中，通常用于准备加入共识组的初始阶段。
	ConsensusRelationConfigTracker ConsensusRelation = "config-tracker"

	// ConsensusRelationOther 表示Orderer运行的是非集群型共识算法，如Solo或Kafka共识。
	// 这类Orderer不参与集群共识决策。
	ConsensusRelationOther ConsensusRelation = "other"
)

// Status 类型表示Orderer在加入通道后（无论是作为共识者还是跟随者）相对于集群其他成员的追赶程度。
type Status string

const (
	// StatusActive 表示Orderer在通道的共识协议中处于活跃状态，或者作为跟随者与集群同步，
	// 且区块高度大于加入区块的编号。（高度为最后一个区块编号+1）
	StatusActive Status = "active"

	// StatusOnBoarding 表示Orderer正在通过从其他Orderer拉取区块的方式追赶集群，
	// 且区块高度小于等于加入区块的编号。
	StatusOnBoarding Status = "onboarding"

	// StatusInactive 表示Orderer并未为此通道存储任何区块。
	StatusInactive Status = "inactive"

	// StatusFailed 表示上一次Orderer对通道的操作失败。
	StatusFailed Status = "failed"
)

// ChannelInfo carries the response to an HTTP request to List a single channel.
// This is marshaled into the body of the HTTP response.
// swagger:model channelInfo
type ChannelInfo struct {
	// The channel name.
	Name string `json:"name"`
	// The channel relative URL (no Host:Port, only path), e.g.: "/participation/v1/channels/my-channel".
	URL string `json:"url"`
	// Whether the orderer is a “consenter”, ”follower”, or "config-tracker" of
	// the cluster for this channel.
	// For non cluster consensus types (solo, kafka) it is "other".
	// Possible values:  “consenter”, ”follower”, "config-tracker", "other".
	ConsensusRelation ConsensusRelation `json:"consensusRelation"`
	// Whether the orderer is ”onboarding”, ”active”, or "inactive", for this channel.
	// For non cluster consensus types (solo, kafka) it is "active".
	// Possible values:  “onboarding”, ”active”, "inactive".
	Status Status `json:"status"`
	// Current block height.
	Height uint64 `json:"height"`
}
