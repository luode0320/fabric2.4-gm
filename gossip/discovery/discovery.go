/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"fmt"

	protolib "github.com/golang/protobuf/proto"
	proto "github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/protoext"
)

// CryptoService is an interface that the discovery expects to be implemented and passed on creation
type CryptoService interface {
	// ValidateAliveMsg validates that an Alive message is authentic
	ValidateAliveMsg(message *protoext.SignedGossipMessage) bool

	// SignMessage signs a message
	SignMessage(m *proto.GossipMessage, internalEndpoint string) *proto.Envelope
}

// EnvelopeFilter may or may not remove part of the Envelope
// that the given SignedGossipMessage originates from.
type EnvelopeFilter func(message *protoext.SignedGossipMessage) *proto.Envelope

// Sieve defines the messages that are allowed to be sent to some remote peer,
// based on some criteria.
// Returns whether the sieve permits sending a given message.
type Sieve func(message *protoext.SignedGossipMessage) bool

// DisclosurePolicy defines which messages a given remote peer
// is eligible of knowing about, and also what is it eligible
// to know about out of a given SignedGossipMessage.
// Returns:
//  1. A Sieve for a given remote peer.
//     The Sieve is applied for each peer in question and outputs
//     whether the message should be disclosed to the remote peer.
//  2. A EnvelopeFilter for a given SignedGossipMessage, which may remove
//     part of the Envelope the SignedGossipMessage originates from
type DisclosurePolicy func(remotePeer *NetworkMember) (Sieve, EnvelopeFilter)

// CommService 是发现期望实现并传递给创建的接口
type CommService interface {
	// Gossip 消息
	Gossip(msg *protoext.SignedGossipMessage)

	// SendToPeer 向给定的对等方发送消息。
	// 由于通信模块本身处理随机数，因此随机数可以是任何内容
	SendToPeer(peer *NetworkMember, msg *protoext.SignedGossipMessage)

	// Ping 探测远程对等点并返回它是否有响应
	Ping(peer *NetworkMember) bool

	// Accept 返回从远程对等方发送的成员资格消息的只读通道
	Accept() <-chan protoext.ReceivedMessage

	// PresumedDead 为假定已死亡的对等点返回只读通道
	PresumedDead() <-chan common.PKIidType

	// CloseConn 命令关闭与某个对等点的连接
	CloseConn(peer *NetworkMember)

	// Forward 将消息发送到下一跃点，不包括最初接收消息的跃点
	Forward(msg protoext.ReceivedMessage)

	// IdentitySwitch 返回有关身份更改事件的只读通道
	IdentitySwitch() <-chan common.PKIidType
}

// AnchorPeerTracker 是传递给发现以检查端点是否为锚点对等方的接口
type AnchorPeerTracker interface {
	IsAnchorPeer(endpoint string) bool
}

// NetworkMember 是对等体的成员。
type NetworkMember struct {
	Endpoint         string            // 对等体的端点地址
	Metadata         []byte            // 对等体的元数据
	PKIid            common.PKIidType  // 对等体的 PKI ID
	InternalEndpoint string            // 对等体的内部端点地址
	Properties       *proto.Properties // 对等体的属性
	*proto.Envelope                    // 对等体的消息信封
}

// Clone clones the NetworkMember
func (n NetworkMember) Clone() NetworkMember {
	pkiIDClone := make(common.PKIidType, len(n.PKIid))
	copy(pkiIDClone, n.PKIid)
	nmClone := NetworkMember{
		Endpoint:         n.Endpoint,
		Metadata:         n.Metadata,
		InternalEndpoint: n.InternalEndpoint,
		PKIid:            pkiIDClone,
	}

	if n.Properties != nil {
		nmClone.Properties = protolib.Clone(n.Properties).(*proto.Properties)
	}

	if n.Envelope != nil {
		nmClone.Envelope = protolib.Clone(n.Envelope).(*proto.Envelope)
	}

	return nmClone
}

// String returns a string representation of the NetworkMember
func (n NetworkMember) String() string {
	return fmt.Sprintf("Endpoint: %s, InternalEndpoint: %s, PKI-ID: %s, Metadata: %x", n.Endpoint, n.InternalEndpoint, n.PKIid, n.Metadata)
}

// PreferredEndpoint computes the endpoint to connect to,
// while preferring internal endpoint over the standard
// endpoint
func (n NetworkMember) PreferredEndpoint() string {
	if n.InternalEndpoint != "" {
		return n.InternalEndpoint
	}
	return n.Endpoint
}

// PeerIdentification encompasses a remote peer's
// PKI-ID and whether its in the same org as the current
// peer or not
type PeerIdentification struct {
	ID      common.PKIidType
	SelfOrg bool
}

type identifier func() (*PeerIdentification, error)

// Discovery 是表示发现模块的接口
type Discovery interface {
	// Lookup 返回网络成员，如果未找到，则返回nil
	Lookup(PKIID common.PKIidType) *NetworkMember

	// Self 返回此实例的成员资格信息
	Self() NetworkMember

	// UpdateMetadata 更新此实例的元数据
	UpdateMetadata([]byte)

	// UpdateEndpoint 更新此实例的端点
	UpdateEndpoint(string)

	// Stop 关闭此实例
	Stop()

	// GetMembership 返回视图中的活动成员
	GetMembership() []NetworkMember

	// InitiateSync 使实例向给定数量的对等方询问其成员资格信息
	InitiateSync(peerNum int)

	// Connect 使此实例连接到远程实例
	// 标识符param是一个可用于标识peer的函数，
	// 并断言其pki-id，无论其是否在对等方的组织中，
	// 以及操作是否成功
	Connect(member NetworkMember, id identifier)
}

// Members represents an aggregation of NetworkMembers
type Members []NetworkMember

// ByID returns a mapping from the PKI-IDs (in string form)
// to NetworkMember
func (members Members) ByID() map[string]NetworkMember {
	res := make(map[string]NetworkMember, len(members))
	for _, peer := range members {
		res[string(peer.PKIid)] = peer
	}
	return res
}

// Intersect returns the intersection of 2 Members
func (members Members) Intersect(otherMembers Members) Members {
	var res Members
	m := otherMembers.ByID()
	for _, member := range members {
		if _, exists := m[string(member.PKIid)]; exists {
			res = append(res, member)
		}
	}
	return res
}

// Filter returns only members that satisfy the given filter
func (members Members) Filter(filter func(member NetworkMember) bool) Members {
	var res Members
	for _, member := range members {
		if filter(member) {
			res = append(res, member)
		}
	}
	return res
}

// Map invokes the given function to every NetworkMember among the Members
func (members Members) Map(f func(member NetworkMember) NetworkMember) Members {
	var res Members
	for _, m := range members {
		res = append(res, f(m))
	}
	return res
}

// HaveExternalEndpoints selects network members that have external endpoints
func HasExternalEndpoint(member NetworkMember) bool {
	return member.Endpoint != ""
}
