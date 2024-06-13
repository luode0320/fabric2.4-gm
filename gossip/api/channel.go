/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package api

import (
	"github.com/hyperledger/fabric/gossip/common"
)

//go:generate mockery -dir . -name SecurityAdvisor -case underscore -output mocks/

// SecurityAdvisor 定义提供安全和身份相关功能的外部辅助对象
type SecurityAdvisor interface {
	// OrgByPeerIdentity 返回给定对等标识的OrgIdentityType。
	// 如果发生任何错误，则返回nil。
	// 此方法不验证peerIdentity。 应该在执行流程期间适当地完成此验证。
	OrgByPeerIdentity(PeerIdentityType) OrgIdentityType
}

// ChannelNotifier is implemented by the gossip component and is used for the peer
// layer to notify the gossip component of a JoinChannel event
type ChannelNotifier interface {
	JoinChannel(joinMsg JoinChannelMessage, channelID common.ChannelID)
}

// JoinChannelMessage is the message that asserts a creation or mutation
// of a channel's membership list, and is the message that is gossipped
// among the peers
type JoinChannelMessage interface {

	// SequenceNumber returns the sequence number of the configuration block
	// the JoinChannelMessage originated from
	SequenceNumber() uint64

	// Members returns the organizations of the channel
	Members() []OrgIdentityType

	// AnchorPeersOf returns the anchor peers of the given organization
	AnchorPeersOf(org OrgIdentityType) []AnchorPeer
}

// AnchorPeer is an anchor peer's certificate and endpoint (host:port)
type AnchorPeer struct {
	Host string // Host is the hostname/ip address of the remote peer
	Port int    // Port is the port the remote peer is listening on
}

// OrgIdentityType 定义组织的身份
type OrgIdentityType []byte
