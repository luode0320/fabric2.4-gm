/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	proto "github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/gossip/msgstore"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/pkg/errors"
)

const (
	DefAliveTimeInterval            = 5 * time.Second
	DefAliveExpirationTimeout       = 5 * DefAliveTimeInterval
	DefAliveExpirationCheckInterval = DefAliveExpirationTimeout / 10
	DefReconnectInterval            = DefAliveExpirationTimeout
	DefMsgExpirationFactor          = 20
	DefMaxConnectionAttempts        = 120
)

type timestamp struct {
	incTime  time.Time
	seqNum   uint64
	lastSeen time.Time
}

func (ts *timestamp) String() string {
	return fmt.Sprintf("%v, %v", ts.incTime.UnixNano(), ts.seqNum)
}

// 发现服务
type gossipDiscoveryImpl struct {
	incTime                      uint64                        // 时间增量，用于生成消息的时间戳
	seqNum                       uint64                        // 消息序列号，用于生成消息的序列号
	self                         NetworkMember                 // 当前节点的 NetworkMember 表示
	deadLastTS                   map[string]*timestamp         // 最后一次收到死亡消息的时间戳
	aliveLastTS                  map[string]*timestamp         // 最后一次收到存活消息的时间戳
	id2Member                    map[string]*NetworkMember     // 根据 PKI ID 映射 NetworkMember
	aliveMembership              *util.MembershipStore         // 存活成员的成员关系存储
	deadMembership               *util.MembershipStore         // 死亡成员的成员关系存储
	selfAliveMessage             *protoext.SignedGossipMessage // 活着的消息
	msgStore                     *aliveMsgStore                // 存活消息存储
	comm                         CommService                   // 通信服务
	crypt                        CryptoService                 // 加密服务
	lock                         *sync.RWMutex                 // 读写锁，用于保护共享数据结构的并发访问
	toDieChan                    chan struct{}                 // 用于通知发现服务停止的通道
	port                         int                           // 端口
	logger                       util.Logger                   // 日志记录器
	disclosurePolicy             DisclosurePolicy              // 披露策略
	pubsub                       *util.PubSub                  // 发布-订阅模式的事件处理器
	aliveTimeInterval            time.Duration                 // 存活时间间隔
	aliveExpirationTimeout       time.Duration                 // 存活过期超时时间
	aliveExpirationCheckInterval time.Duration                 // 存活过期检查间隔
	reconnectInterval            time.Duration                 // 重新连接间隔
	msgExpirationFactor          int                           // 消息过期因子
	maxConnectionAttempts        int                           // 最大连接尝试次数
	bootstrapPeers               []string                      // 引导节点列表
	anchorPeerTracker            AnchorPeerTracker             // 锚节点追踪器
}

// DiscoveryConfig 包含了发现模块的配置信息。
type DiscoveryConfig struct {
	AliveTimeInterval            time.Duration // 存活时间间隔，表示节点发送存活消息的时间间隔
	AliveExpirationTimeout       time.Duration // 存活过期超时时间，表示节点被认为已停止的超时时间
	AliveExpirationCheckInterval time.Duration // 存活过期检查间隔，表示检查存活过期的时间间隔
	ReconnectInterval            time.Duration // 重新连接间隔，表示节点重新连接的时间间隔
	MaxConnectionAttempts        int           // 最大连接尝试次数，表示节点尝试连接的最大次数
	MsgExpirationFactor          int           // 消息过期因子，表示消息过期时间的因子
	BootstrapPeers               []string      // 引导节点列表，表示用于引导发现的初始节点列表
}

// NewDiscoveryService 使用传递的 comm 模块和 crypto 服务创建一个新的发现服务。
//
// 参数：
//   - self：当前节点的 NetworkMember 表示。
//   - comm：通信服务，用于与其他节点进行通信。
//   - crypt：加密服务，用于对消息进行加密和解密。
//   - disPol：披露策略，用于确定哪些节点应该被披露给其他节点。
//   - config：发现模块的配置信息。
//   - anchorPeerTracker：锚节点追踪器，用于追踪锚节点的变化。
//   - logger：日志记录器，用于记录日志信息。
//
// 返回值：
//   - Discovery：新创建的发现服务。
func NewDiscoveryService(self NetworkMember, comm CommService, crypt CryptoService, disPol DisclosurePolicy,
	config DiscoveryConfig, anchorPeerTracker AnchorPeerTracker, logger util.Logger) Discovery {
	d := &gossipDiscoveryImpl{
		self:             self,                            // 当前节点的 NetworkMember 表示
		incTime:          uint64(time.Now().UnixNano()),   // 时间增量，用于生成消息的时间戳
		seqNum:           uint64(0),                       // 消息序列号，用于生成消息的序列号
		deadLastTS:       make(map[string]*timestamp),     // 最后一次收到死亡消息的时间戳
		aliveLastTS:      make(map[string]*timestamp),     // 最后一次收到存活消息的时间戳
		id2Member:        make(map[string]*NetworkMember), // 根据 PKI ID 映射 NetworkMember
		aliveMembership:  util.NewMembershipStore(),       // 存活成员的成员关系存储
		deadMembership:   util.NewMembershipStore(),       // 死亡成员的成员关系存储
		crypt:            crypt,                           // 加密服务
		comm:             comm,                            // 通信服务
		lock:             &sync.RWMutex{},                 // 读写锁，用于保护共享数据结构的并发访问
		toDieChan:        make(chan struct{}),             // 用于通知发现服务停止的通道
		logger:           logger,                          // 日志记录器
		disclosurePolicy: disPol,                          // 披露策略
		pubsub:           util.NewPubSub(),                // 发布-订阅模式的事件处理器

		aliveTimeInterval:            config.AliveTimeInterval,            // 存活时间间隔
		aliveExpirationTimeout:       config.AliveExpirationTimeout,       // 存活过期超时时间
		aliveExpirationCheckInterval: config.AliveExpirationCheckInterval, // 存活过期检查间隔
		reconnectInterval:            config.ReconnectInterval,            // 重新连接间隔
		maxConnectionAttempts:        config.MaxConnectionAttempts,        // 最大连接尝试次数
		msgExpirationFactor:          config.MsgExpirationFactor,          // 消息过期因子

		bootstrapPeers:    config.BootstrapPeers, // 引导节点列表
		anchorPeerTracker: anchorPeerTracker,     // 锚节点追踪器
	}

	d.validateSelfConfig()           // 验证当前节点的配置信息
	d.msgStore = newAliveMsgStore(d) // 创建存活消息存储

	go d.periodicalSendAlive()       // 启动定期发送存活消息的协程
	go d.periodicalCheckAlive()      // 启动定期检查存活消息的协程
	go d.handleMessages()            // 启动处理消息的协程
	go d.periodicalReconnectToDead() // 启动定期重新连接死亡节点的协程
	go d.handleEvents()              // 启动处理事件的协程

	return d // 返回新创建的发现服务
}

// Lookup returns a network member, or nil if not found
func (d *gossipDiscoveryImpl) Lookup(PKIID common.PKIidType) *NetworkMember {
	if bytes.Equal(PKIID, d.self.PKIid) {
		return &d.self
	}
	d.lock.RLock()
	defer d.lock.RUnlock()
	return copyNetworkMember(d.id2Member[string(PKIID)])
}

func (d *gossipDiscoveryImpl) Connect(member NetworkMember, id identifier) {
	for _, endpoint := range []string{member.InternalEndpoint, member.Endpoint} {
		if d.isMyOwnEndpoint(endpoint) {
			d.logger.Debug("Skipping connecting to myself")
			return
		}
	}

	d.logger.Debug("Entering", member)
	defer d.logger.Debug("Exiting")
	go func() {
		for i := 0; i < d.maxConnectionAttempts && !d.toDie(); i++ {
			id, err := id()
			if err != nil {
				if d.toDie() {
					return
				}
				d.logger.Warningf("无法连接到 %v : %v", member, err)
				time.Sleep(d.reconnectInterval)
				continue
			}
			peer := &NetworkMember{
				InternalEndpoint: member.InternalEndpoint,
				Endpoint:         member.Endpoint,
				PKIid:            id.ID,
			}
			m, err := d.createMembershipRequest(id.SelfOrg)
			if err != nil {
				d.logger.Warningf("Failed creating membership request: %+v", errors.WithStack(err))
				continue
			}
			req, err := protoext.NoopSign(m)
			if err != nil {
				d.logger.Warningf("Failed creating SignedGossipMessage: %+v", errors.WithStack(err))
				continue
			}
			req.Nonce = util.RandomUInt64()
			req, err = protoext.NoopSign(req.GossipMessage)
			if err != nil {
				d.logger.Warningf("Failed adding NONCE to SignedGossipMessage %+v", errors.WithStack(err))
				continue
			}
			go d.sendUntilAcked(peer, req)
			return
		}
	}()
}

func (d *gossipDiscoveryImpl) isMyOwnEndpoint(endpoint string) bool {
	return endpoint == fmt.Sprintf("127.0.0.1:%d", d.port) || endpoint == fmt.Sprintf("localhost:%d", d.port) ||
		endpoint == d.self.InternalEndpoint || endpoint == d.self.Endpoint
}

// 验证当前节点的配置信息
func (d *gossipDiscoveryImpl) validateSelfConfig() {
	endpoint := d.self.InternalEndpoint
	if len(endpoint) == 0 {
		d.logger.Panic("内部端点为空:", endpoint)
	}

	internalEndpointSplit := strings.Split(endpoint, ":")
	if len(internalEndpointSplit) != 2 {
		d.logger.Panicf("自身端点 %s 的格式错误: 'host:port'", endpoint)
	}
	myPort, err := strconv.ParseInt(internalEndpointSplit[1], 10, 64)
	if err != nil {
		d.logger.Panicf("自端点 %s 没有有效端口, %+v", endpoint, errors.WithStack(err))
	}

	if myPort > int64(math.MaxUint16) {
		d.logger.Panicf("自端点 %s 的端口占用超过 16 位", endpoint)
	}

	d.port = int(myPort)
}

func (d *gossipDiscoveryImpl) sendUntilAcked(peer *NetworkMember, message *protoext.SignedGossipMessage) {
	nonce := message.Nonce
	for i := 0; i < d.maxConnectionAttempts && !d.toDie(); i++ {
		sub := d.pubsub.Subscribe(fmt.Sprintf("%d", nonce), time.Second*5)
		d.comm.SendToPeer(peer, message)
		if _, timeoutErr := sub.Listen(); timeoutErr == nil {
			return
		}
		time.Sleep(d.reconnectInterval)
	}
}

func (d *gossipDiscoveryImpl) InitiateSync(peerNum int) {
	if d.toDie() {
		return
	}
	var peers2SendTo []*NetworkMember

	d.lock.RLock()

	n := d.aliveMembership.Size()
	k := peerNum
	if k > n {
		k = n
	}

	aliveMembersAsSlice := d.aliveMembership.ToSlice()
	for _, i := range util.GetRandomIndices(k, n-1) {
		pulledPeer := aliveMembersAsSlice[i].GetAliveMsg().Membership
		var internalEndpoint string
		if aliveMembersAsSlice[i].Envelope.SecretEnvelope != nil {
			internalEndpoint = protoext.InternalEndpoint(aliveMembersAsSlice[i].Envelope.SecretEnvelope)
		}
		netMember := &NetworkMember{
			Endpoint:         pulledPeer.Endpoint,
			Metadata:         pulledPeer.Metadata,
			PKIid:            pulledPeer.PkiId,
			InternalEndpoint: internalEndpoint,
		}
		peers2SendTo = append(peers2SendTo, netMember)
	}

	d.lock.RUnlock()

	if len(peers2SendTo) == 0 {
		d.logger.Debugf("No peers to send to, aborting membership sync")
		return
	}

	m, err := d.createMembershipRequest(true)
	if err != nil {
		d.logger.Warningf("Failed creating membership request: %+v", errors.WithStack(err))
		return
	}
	memReq, err := protoext.NoopSign(m)
	if err != nil {
		d.logger.Warningf("Failed creating SignedGossipMessage: %+v", errors.WithStack(err))
		return
	}

	for _, netMember := range peers2SendTo {
		d.comm.SendToPeer(netMember, memReq)
	}
}

func (d *gossipDiscoveryImpl) handleEvents() {
	defer d.logger.Debug("Stopped")

	for {
		select {
		case deadPeer := <-d.comm.PresumedDead():
			if d.isAlive(deadPeer) {
				d.expireDeadMembers([]common.PKIidType{deadPeer})
			}
		case changedPKIID := <-d.comm.IdentitySwitch():
			// If a peer changed its PKI-ID, purge the old PKI-ID
			d.purge(changedPKIID)
		case <-d.toDieChan:
			return
		}
	}
}

func (d *gossipDiscoveryImpl) isAlive(pkiID common.PKIidType) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()
	_, alive := d.aliveLastTS[string(pkiID)]
	return alive
}

func (d *gossipDiscoveryImpl) handleMessages() {
	defer d.logger.Debug("Stopped")

	in := d.comm.Accept()
	for {
		select {
		case m := <-in:
			d.handleMsgFromComm(m)
		case <-d.toDieChan:
			return
		}
	}
}

func (d *gossipDiscoveryImpl) handleMsgFromComm(msg protoext.ReceivedMessage) {
	if msg == nil {
		return
	}
	m := msg.GetGossipMessage()
	if m.GetAliveMsg() == nil && m.GetMemRes() == nil && m.GetMemReq() == nil {
		d.logger.Warning("Got message with wrong type (expected Alive or MembershipResponse or MembershipRequest message):", m.GossipMessage)
		return
	}

	d.logger.Debug("Got message:", m)
	defer d.logger.Debug("Exiting")

	if memReq := m.GetMemReq(); memReq != nil {
		selfInfoGossipMsg, err := protoext.EnvelopeToGossipMessage(memReq.SelfInformation)
		if err != nil {
			d.logger.Warningf("Failed deserializing GossipMessage from envelope: %+v", errors.WithStack(err))
			return
		}

		if !d.crypt.ValidateAliveMsg(selfInfoGossipMsg) {
			return
		}

		if d.msgStore.CheckValid(selfInfoGossipMsg) {
			d.handleAliveMessage(selfInfoGossipMsg)
		}

		var internalEndpoint string
		if memReq.SelfInformation.SecretEnvelope != nil {
			internalEndpoint = protoext.InternalEndpoint(memReq.SelfInformation.SecretEnvelope)
		}

		// Sending a membership response to a peer may block this routine
		// in case the sending is deliberately slow (i.e attack).
		// will keep this async until I'll write a timeout detector in the comm layer
		go d.sendMemResponse(selfInfoGossipMsg.GetAliveMsg().Membership, internalEndpoint, m.Nonce)
		return
	}

	if protoext.IsAliveMsg(m.GossipMessage) {
		if !d.msgStore.CheckValid(m) || !d.crypt.ValidateAliveMsg(m) {
			return
		}
		// If the message was sent by me, ignore it and don't forward it further
		if d.isSentByMe(m) {
			return
		}

		d.msgStore.Add(m)
		d.handleAliveMessage(m)
		d.comm.Forward(msg)
		return
	}

	if memResp := m.GetMemRes(); memResp != nil {
		d.pubsub.Publish(fmt.Sprintf("%d", m.Nonce), m.Nonce)
		for _, env := range memResp.Alive {
			am, err := protoext.EnvelopeToGossipMessage(env)
			if err != nil {
				d.logger.Warningf("Membership response contains an invalid message from an online peer:%+v", errors.WithStack(err))
				return
			}
			if !protoext.IsAliveMsg(am.GossipMessage) {
				d.logger.Warning("Expected alive message, got", am, "instead")
				return
			}

			if d.msgStore.CheckValid(am) && d.crypt.ValidateAliveMsg(am) {
				d.handleAliveMessage(am)
			}
		}

		for _, env := range memResp.Dead {
			dm, err := protoext.EnvelopeToGossipMessage(env)
			if err != nil {
				d.logger.Warningf("Membership response contains an invalid message from an offline peer %+v", errors.WithStack(err))
				return
			}

			// Newer alive message exists or the message isn't authentic
			if !d.msgStore.CheckValid(dm) || !d.crypt.ValidateAliveMsg(dm) {
				continue
			}

			newDeadMembers := []*protoext.SignedGossipMessage{}
			d.lock.RLock()
			if _, known := d.id2Member[string(dm.GetAliveMsg().Membership.PkiId)]; !known {
				newDeadMembers = append(newDeadMembers, dm)
			}
			d.lock.RUnlock()
			d.learnNewMembers([]*protoext.SignedGossipMessage{}, newDeadMembers)
		}
	}
}

func (d *gossipDiscoveryImpl) sendMemResponse(targetMember *proto.Member, internalEndpoint string, nonce uint64) {
	d.logger.Debug("Entering", protoext.MemberToString(targetMember))

	targetPeer := &NetworkMember{
		Endpoint:         targetMember.Endpoint,
		Metadata:         targetMember.Metadata,
		PKIid:            targetMember.PkiId,
		InternalEndpoint: internalEndpoint,
	}

	var aliveMsg *protoext.SignedGossipMessage
	var err error
	d.lock.RLock()
	aliveMsg = d.selfAliveMessage
	d.lock.RUnlock()
	if aliveMsg == nil {
		aliveMsg, err = d.createSignedAliveMessage(true)
		if err != nil {
			d.logger.Warningf("Failed creating alive message: %+v", errors.WithStack(err))
			return
		}
	}
	memResp := d.createMembershipResponse(aliveMsg, targetPeer)
	if memResp == nil {
		errMsg := `Got a membership request from a peer that shouldn't have sent one: %v, closing connection to the peer as a result.`
		d.logger.Warningf(errMsg, targetMember)
		d.comm.CloseConn(targetPeer)
		return
	}

	defer d.logger.Debug("Exiting, replying with", protoext.MembershipResponseToString(memResp))

	msg, err := protoext.NoopSign(&proto.GossipMessage{
		Tag:   proto.GossipMessage_EMPTY,
		Nonce: nonce,
		Content: &proto.GossipMessage_MemRes{
			MemRes: memResp,
		},
	})
	if err != nil {
		err = errors.WithStack(err)
		d.logger.Warningf("Failed creating SignedGossipMessage: %+v", errors.WithStack(err))
		return
	}
	d.comm.SendToPeer(targetPeer, msg)
}

func (d *gossipDiscoveryImpl) createMembershipResponse(aliveMsg *protoext.SignedGossipMessage, targetMember *NetworkMember) *proto.MembershipResponse {
	shouldBeDisclosed, omitConcealedFields := d.disclosurePolicy(targetMember)

	if !shouldBeDisclosed(aliveMsg) {
		return nil
	}

	d.lock.RLock()
	defer d.lock.RUnlock()

	deadPeers := []*proto.Envelope{}

	for _, dm := range d.deadMembership.ToSlice() {

		if !shouldBeDisclosed(dm) {
			continue
		}
		deadPeers = append(deadPeers, omitConcealedFields(dm))
	}

	var aliveSnapshot []*proto.Envelope
	for _, am := range d.aliveMembership.ToSlice() {
		if !shouldBeDisclosed(am) {
			continue
		}
		aliveSnapshot = append(aliveSnapshot, omitConcealedFields(am))
	}

	return &proto.MembershipResponse{
		Alive: append(aliveSnapshot, omitConcealedFields(aliveMsg)),
		Dead:  deadPeers,
	}
}

func (d *gossipDiscoveryImpl) handleAliveMessage(m *protoext.SignedGossipMessage) {
	d.logger.Debug("Entering", m)
	defer d.logger.Debug("Exiting")

	if d.isSentByMe(m) {
		return
	}

	pkiID := m.GetAliveMsg().Membership.PkiId

	ts := m.GetAliveMsg().Timestamp

	d.lock.RLock()
	_, known := d.id2Member[string(pkiID)]
	d.lock.RUnlock()

	if !known {
		d.learnNewMembers([]*protoext.SignedGossipMessage{m}, []*protoext.SignedGossipMessage{})
		return
	}

	d.lock.RLock()
	_, isAlive := d.aliveLastTS[string(pkiID)]
	lastDeadTS, isDead := d.deadLastTS[string(pkiID)]
	d.lock.RUnlock()

	if !isAlive && !isDead {
		d.logger.Panicf("Member %s is known but not found neither in alive nor in dead lastTS maps, isAlive=%v, isDead=%v", m.GetAliveMsg().Membership.Endpoint, isAlive, isDead)
		return
	}

	if isAlive && isDead {
		d.logger.Panicf("Member %s is both alive and dead at the same time", m.GetAliveMsg().Membership)
		return
	}

	if isDead {
		if before(lastDeadTS, ts) {
			// resurrect peer
			d.resurrectMember(m, *ts)
		} else if !same(lastDeadTS, ts) {
			d.logger.Debug("got old alive message about dead peer ", protoext.MemberToString(m.GetAliveMsg().Membership), "lastDeadTS:", lastDeadTS, "but got ts:", ts)
		}
		return
	}

	d.lock.RLock()
	lastAliveTS, isAlive := d.aliveLastTS[string(pkiID)]
	d.lock.RUnlock()

	if isAlive {
		if before(lastAliveTS, ts) {
			d.learnExistingMembers([]*protoext.SignedGossipMessage{m})
		} else if !same(lastAliveTS, ts) {
			d.logger.Debug("got old alive message about alive peer ", protoext.MemberToString(m.GetAliveMsg().Membership), "lastAliveTS:", lastAliveTS, "but got ts:", ts)
		}
	}
	// else, ignore the message because it is too old
}

func (d *gossipDiscoveryImpl) purge(id common.PKIidType) {
	d.logger.Infof("Purging %s from membership", id)
	d.lock.Lock()
	defer d.lock.Unlock()
	d.aliveMembership.Remove(id)
	d.deadMembership.Remove(id)
	delete(d.id2Member, string(id))
	delete(d.deadLastTS, string(id))
	delete(d.aliveLastTS, string(id))
}

func (d *gossipDiscoveryImpl) isSentByMe(m *protoext.SignedGossipMessage) bool {
	pkiID := m.GetAliveMsg().Membership.PkiId
	if !equalPKIid(pkiID, d.self.PKIid) {
		return false
	}
	d.logger.Debug("Got alive message about ourselves,", m)
	d.lock.RLock()
	diffExternalEndpoint := d.self.Endpoint != m.GetAliveMsg().Membership.Endpoint
	d.lock.RUnlock()
	var diffInternalEndpoint bool
	secretEnvelope := m.GetSecretEnvelope()
	if secretEnvelope != nil {
		internalEndpoint := protoext.InternalEndpoint(secretEnvelope)
		if internalEndpoint != "" {
			diffInternalEndpoint = internalEndpoint != d.self.InternalEndpoint
		}
	}
	if diffInternalEndpoint || diffExternalEndpoint {
		d.logger.Error("Bad configuration detected: Received AliveMessage from a peer with the same PKI-ID as myself:", m.GossipMessage)
	}
	return true
}

func (d *gossipDiscoveryImpl) resurrectMember(am *protoext.SignedGossipMessage, t proto.PeerTime) {
	d.logger.Debug("Entering, AliveMessage:", am, "t:", t)
	defer d.logger.Debug("Exiting")
	d.lock.Lock()
	defer d.lock.Unlock()

	member := am.GetAliveMsg().Membership
	pkiID := member.PkiId
	d.aliveLastTS[string(pkiID)] = &timestamp{
		lastSeen: time.Now(),
		seqNum:   t.SeqNum,
		incTime:  tsToTime(t.IncNum),
	}

	var internalEndpoint string
	if prevNetMem := d.id2Member[string(pkiID)]; prevNetMem != nil {
		internalEndpoint = prevNetMem.InternalEndpoint
	}
	if am.Envelope.SecretEnvelope != nil {
		internalEndpoint = protoext.InternalEndpoint(am.Envelope.SecretEnvelope)
	}

	d.id2Member[string(pkiID)] = &NetworkMember{
		Endpoint:         member.Endpoint,
		Metadata:         member.Metadata,
		PKIid:            member.PkiId,
		InternalEndpoint: internalEndpoint,
	}

	delete(d.deadLastTS, string(pkiID))
	d.deadMembership.Remove(pkiID)
	d.aliveMembership.Put(pkiID, &protoext.SignedGossipMessage{GossipMessage: am.GossipMessage, Envelope: am.Envelope})
}

func (d *gossipDiscoveryImpl) periodicalReconnectToDead() {
	defer d.logger.Debug("Stopped")

	for !d.toDie() {
		wg := &sync.WaitGroup{}

		for _, member := range d.copyLastSeen(d.deadLastTS) {
			wg.Add(1)
			go func(member NetworkMember) {
				defer wg.Done()
				if d.comm.Ping(&member) {
					d.logger.Debug(member, "is responding, sending membership request")
					d.sendMembershipRequest(&member, true)
				} else {
					d.logger.Debug(member, "is still dead")
				}
			}(member)
		}

		wg.Wait()
		d.logger.Debug("Sleeping", d.reconnectInterval)
		time.Sleep(d.reconnectInterval)
	}
}

func (d *gossipDiscoveryImpl) sendMembershipRequest(member *NetworkMember, includeInternalEndpoint bool) {
	m, err := d.createMembershipRequest(includeInternalEndpoint)
	if err != nil {
		d.logger.Warningf("Failed creating membership request: %+v", errors.WithStack(err))
		return
	}
	req, err := protoext.NoopSign(m)
	if err != nil {
		d.logger.Errorf("Failed creating SignedGossipMessage: %+v", errors.WithStack(err))
		return
	}
	d.comm.SendToPeer(member, req)
}

func (d *gossipDiscoveryImpl) createMembershipRequest(includeInternalEndpoint bool) (*proto.GossipMessage, error) {
	am, err := d.createSignedAliveMessage(includeInternalEndpoint)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	req := &proto.MembershipRequest{
		SelfInformation: am.Envelope,
		// TODO: sending the known peers is not secure because the remote peer might shouldn't know
		// TODO: about the known peers. I'm deprecating this until a secure mechanism will be implemented.
		// TODO: See FAB-2570 for tracking this issue.
		Known: [][]byte{},
	}
	return &proto.GossipMessage{
		Tag:   proto.GossipMessage_EMPTY,
		Nonce: uint64(0),
		Content: &proto.GossipMessage_MemReq{
			MemReq: req,
		},
	}, nil
}

func (d *gossipDiscoveryImpl) copyLastSeen(lastSeenMap map[string]*timestamp) []NetworkMember {
	d.lock.RLock()
	defer d.lock.RUnlock()

	res := []NetworkMember{}
	for pkiIDStr := range lastSeenMap {
		res = append(res, *(d.id2Member[pkiIDStr]))
	}
	return res
}

func (d *gossipDiscoveryImpl) periodicalCheckAlive() {
	defer d.logger.Debug("Stopped")

	for !d.toDie() {
		time.Sleep(d.aliveExpirationCheckInterval)
		dead := d.getDeadMembers()
		if len(dead) > 0 {
			d.logger.Debugf("Got %v dead members: %v", len(dead), dead)
			d.expireDeadMembers(dead)
		}
	}
}

func (d *gossipDiscoveryImpl) expireDeadMembers(dead []common.PKIidType) {
	d.logger.Warning("Entering", dead)
	defer d.logger.Warning("Exiting")

	var deadMembers2Expire []NetworkMember

	d.lock.Lock()

	for _, pkiID := range dead {
		if _, isAlive := d.aliveLastTS[string(pkiID)]; !isAlive {
			continue
		}
		deadMembers2Expire = append(deadMembers2Expire, d.id2Member[string(pkiID)].Clone())
		// move lastTS from alive to dead
		lastTS, hasLastTS := d.aliveLastTS[string(pkiID)]
		if hasLastTS {
			d.deadLastTS[string(pkiID)] = lastTS
			delete(d.aliveLastTS, string(pkiID))
		}

		if am := d.aliveMembership.MsgByID(pkiID); am != nil {
			d.deadMembership.Put(pkiID, am)
			d.aliveMembership.Remove(pkiID)
		}
	}

	d.lock.Unlock()

	for _, member2Expire := range deadMembers2Expire {
		d.logger.Warning("Closing connection to", member2Expire)
		d.comm.CloseConn(&member2Expire)
	}
}

func (d *gossipDiscoveryImpl) getDeadMembers() []common.PKIidType {
	d.lock.RLock()
	defer d.lock.RUnlock()

	dead := []common.PKIidType{}
	for id, last := range d.aliveLastTS {
		elapsedNonAliveTime := time.Since(last.lastSeen)
		if elapsedNonAliveTime > d.aliveExpirationTimeout {
			d.logger.Warning("Haven't heard from", []byte(id), "for", elapsedNonAliveTime)
			dead = append(dead, common.PKIidType(id))
		}
	}
	return dead
}

func (d *gossipDiscoveryImpl) periodicalSendAlive() {
	defer d.logger.Debug("Stopped")

	for !d.toDie() {
		d.logger.Debug("Sleeping", d.aliveTimeInterval)
		time.Sleep(d.aliveTimeInterval)
		if d.aliveMembership.Size() == 0 {
			d.logger.Debugf("存活成员的成员 aliveMembership 为空, 没有人可以向其发送心跳")
			continue
		}
		msg, err := d.createSignedAliveMessage(true)
		if err != nil {
			d.logger.Warningf("创建活动消息失败: %+v", errors.WithStack(err))
			return
		}
		d.lock.Lock()
		d.selfAliveMessage = msg
		d.lock.Unlock()
		d.comm.Gossip(msg)
	}
}

func (d *gossipDiscoveryImpl) aliveMsgAndInternalEndpoint() (*proto.GossipMessage, string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.seqNum++
	seqNum := d.seqNum
	endpoint := d.self.Endpoint
	meta := d.self.Metadata
	pkiID := d.self.PKIid
	internalEndpoint := d.self.InternalEndpoint
	msg := &proto.GossipMessage{
		Tag: proto.GossipMessage_EMPTY,
		Content: &proto.GossipMessage_AliveMsg{
			AliveMsg: &proto.AliveMessage{
				Membership: &proto.Member{
					Endpoint: endpoint,
					Metadata: meta,
					PkiId:    pkiID,
				},
				Timestamp: &proto.PeerTime{
					IncNum: d.incTime,
					SeqNum: seqNum,
				},
			},
		},
	}
	return msg, internalEndpoint
}

func (d *gossipDiscoveryImpl) createSignedAliveMessage(includeInternalEndpoint bool) (*protoext.SignedGossipMessage, error) {
	msg, internalEndpoint := d.aliveMsgAndInternalEndpoint()
	envp := d.crypt.SignMessage(msg, internalEndpoint)
	if envp == nil {
		return nil, errors.New("Failed signing message")
	}
	signedMsg := &protoext.SignedGossipMessage{
		GossipMessage: msg,
		Envelope:      envp,
	}

	if !includeInternalEndpoint {
		signedMsg.Envelope.SecretEnvelope = nil
	}

	return signedMsg, nil
}

func (d *gossipDiscoveryImpl) learnExistingMembers(aliveArr []*protoext.SignedGossipMessage) {
	d.logger.Debugf("Entering: learnedMembers={%v}", aliveArr)
	defer d.logger.Debug("Exiting")

	d.lock.Lock()
	defer d.lock.Unlock()

	for _, m := range aliveArr {
		am := m.GetAliveMsg()
		if am == nil {
			d.logger.Warning("Expected alive message, got instead:", m)
			return
		}
		d.logger.Debug("updating", protoext.AliveMessageToString(am))

		var internalEndpoint string
		if prevNetMem := d.id2Member[string(am.Membership.PkiId)]; prevNetMem != nil {
			internalEndpoint = prevNetMem.InternalEndpoint
		}
		if m.Envelope.SecretEnvelope != nil {
			internalEndpoint = protoext.InternalEndpoint(m.Envelope.SecretEnvelope)
		}

		// update member's data
		member := d.id2Member[string(am.Membership.PkiId)]
		member.Endpoint = am.Membership.Endpoint
		member.Metadata = am.Membership.Metadata
		member.InternalEndpoint = internalEndpoint

		if _, isKnownAsDead := d.deadLastTS[string(am.Membership.PkiId)]; isKnownAsDead {
			d.logger.Warning(am.Membership, "has already expired")
			continue
		}

		if _, isKnownAsAlive := d.aliveLastTS[string(am.Membership.PkiId)]; !isKnownAsAlive {
			d.logger.Warning(am.Membership, "has already expired")
			continue
		} else {
			d.logger.Debug("Updating aliveness data:", protoext.AliveMessageToString(am))
			// update existing aliveness data
			alive := d.aliveLastTS[string(am.Membership.PkiId)]
			alive.incTime = tsToTime(am.Timestamp.IncNum)
			alive.lastSeen = time.Now()
			alive.seqNum = am.Timestamp.SeqNum

			if am := d.aliveMembership.MsgByID(m.GetAliveMsg().Membership.PkiId); am != nil {
				d.logger.Debug("Replacing", am, "in aliveMembership")
				am.GossipMessage = m.GossipMessage
				am.Envelope = m.Envelope
			}
		}
	}
}

func (d *gossipDiscoveryImpl) learnNewMembers(aliveMembers []*protoext.SignedGossipMessage, deadMembers []*protoext.SignedGossipMessage) {
	d.logger.Debugf("Entering: learnedMembers={%v}, deadMembers={%v}", aliveMembers, deadMembers)
	defer d.logger.Debugf("Exiting")

	d.lock.Lock()
	defer d.lock.Unlock()

	for _, am := range aliveMembers {
		if equalPKIid(am.GetAliveMsg().Membership.PkiId, d.self.PKIid) {
			continue
		}
		d.aliveLastTS[string(am.GetAliveMsg().Membership.PkiId)] = &timestamp{
			incTime:  tsToTime(am.GetAliveMsg().Timestamp.IncNum),
			lastSeen: time.Now(),
			seqNum:   am.GetAliveMsg().Timestamp.SeqNum,
		}

		d.aliveMembership.Put(am.GetAliveMsg().Membership.PkiId, &protoext.SignedGossipMessage{GossipMessage: am.GossipMessage, Envelope: am.Envelope})
		d.logger.Debugf("Learned about a new alive member: %v", am)
	}

	for _, dm := range deadMembers {
		if equalPKIid(dm.GetAliveMsg().Membership.PkiId, d.self.PKIid) {
			continue
		}
		d.deadLastTS[string(dm.GetAliveMsg().Membership.PkiId)] = &timestamp{
			incTime:  tsToTime(dm.GetAliveMsg().Timestamp.IncNum),
			lastSeen: time.Now(),
			seqNum:   dm.GetAliveMsg().Timestamp.SeqNum,
		}

		d.deadMembership.Put(dm.GetAliveMsg().Membership.PkiId, &protoext.SignedGossipMessage{GossipMessage: dm.GossipMessage, Envelope: dm.Envelope})
		d.logger.Debugf("Learned about a new dead member: %v", dm)
	}

	// update the member in any case
	for _, a := range [][]*protoext.SignedGossipMessage{aliveMembers, deadMembers} {
		for _, m := range a {
			member := m.GetAliveMsg()
			if member == nil {
				d.logger.Warning("Expected alive message, got instead:", m)
				return
			}

			var internalEndpoint string
			if m.Envelope.SecretEnvelope != nil {
				internalEndpoint = protoext.InternalEndpoint(m.Envelope.SecretEnvelope)
			}

			if prevNetMem := d.id2Member[string(member.Membership.PkiId)]; prevNetMem != nil {
				internalEndpoint = prevNetMem.InternalEndpoint
			}

			d.id2Member[string(member.Membership.PkiId)] = &NetworkMember{
				Endpoint:         member.Membership.Endpoint,
				Metadata:         member.Membership.Metadata,
				PKIid:            member.Membership.PkiId,
				InternalEndpoint: internalEndpoint,
			}
		}
	}
}

func (d *gossipDiscoveryImpl) GetMembership() []NetworkMember {
	if d.toDie() {
		return []NetworkMember{}
	}
	d.lock.RLock()
	defer d.lock.RUnlock()

	response := []NetworkMember{}
	for _, m := range d.aliveMembership.ToSlice() {
		member := m.GetAliveMsg()
		response = append(response, NetworkMember{
			PKIid:            member.Membership.PkiId,
			Endpoint:         member.Membership.Endpoint,
			Metadata:         member.Membership.Metadata,
			InternalEndpoint: d.id2Member[string(m.GetAliveMsg().Membership.PkiId)].InternalEndpoint,
			Envelope:         m.Envelope,
		})
	}
	return response
}

func tsToTime(ts uint64) time.Time {
	return time.Unix(int64(0), int64(ts))
}

func (d *gossipDiscoveryImpl) UpdateMetadata(md []byte) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.self.Metadata = md
}

func (d *gossipDiscoveryImpl) UpdateEndpoint(endpoint string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.self.Endpoint = endpoint
}

func (d *gossipDiscoveryImpl) Self() NetworkMember {
	var env *proto.Envelope
	msg, _ := d.aliveMsgAndInternalEndpoint()
	sMsg, err := protoext.NoopSign(msg)
	if err != nil {
		d.logger.Warning("Failed creating SignedGossipMessage:", err)
	} else {
		env = sMsg.Envelope
	}
	mem := msg.GetAliveMsg().Membership
	return NetworkMember{
		Endpoint: mem.Endpoint,
		Metadata: mem.Metadata,
		PKIid:    mem.PkiId,
		Envelope: env,
	}
}

func (d *gossipDiscoveryImpl) toDie() bool {
	select {
	case <-d.toDieChan:
		return true
	default:
		return false
	}
}

func (d *gossipDiscoveryImpl) Stop() {
	select {
	case <-d.toDieChan:
	default:
		close(d.toDieChan)
		defer d.logger.Info("Stopped")
		d.logger.Info("Stopping")
		d.msgStore.Stop()
	}
}

func copyNetworkMember(member *NetworkMember) *NetworkMember {
	if member == nil {
		return nil
	} else {
		copiedNetworkMember := &NetworkMember{}
		*copiedNetworkMember = *member
		return copiedNetworkMember
	}
}

func equalPKIid(a, b common.PKIidType) bool {
	return bytes.Equal(a, b)
}

func same(a *timestamp, b *proto.PeerTime) bool {
	return uint64(a.incTime.UnixNano()) == b.IncNum && a.seqNum == b.SeqNum
}

func before(a *timestamp, b *proto.PeerTime) bool {
	return (uint64(a.incTime.UnixNano()) == b.IncNum && a.seqNum < b.SeqNum) ||
		uint64(a.incTime.UnixNano()) < b.IncNum
}

type aliveMsgStore struct {
	msgstore.MessageStore
}

func newAliveMsgStore(d *gossipDiscoveryImpl) *aliveMsgStore {
	policy := protoext.NewGossipMessageComparator(0)
	trigger := func(m interface{}) {}
	aliveMsgTTL := d.aliveExpirationTimeout * time.Duration(d.msgExpirationFactor)
	externalLock := func() { d.lock.Lock() }
	externalUnlock := func() { d.lock.Unlock() }
	callback := func(m interface{}) {
		msg := m.(*protoext.SignedGossipMessage)
		if !protoext.IsAliveMsg(msg.GossipMessage) {
			return
		}
		membership := msg.GetAliveMsg().Membership
		id := membership.PkiId
		endpoint := membership.Endpoint
		internalEndpoint := protoext.InternalEndpoint(msg.SecretEnvelope)
		if util.Contains(endpoint, d.bootstrapPeers) || util.Contains(internalEndpoint, d.bootstrapPeers) ||
			d.anchorPeerTracker.IsAnchorPeer(endpoint) || d.anchorPeerTracker.IsAnchorPeer(internalEndpoint) {
			// 切勿删除引导对等点或锚定对等点
			d.logger.Debugf("不要从成员资格中删除引导或锚定对等端点 %s", endpoint)
			return
		}
		d.logger.Infof("正在删除成员: 端点: %s, 内部端点: %s, PKIID: %x", endpoint, internalEndpoint, id)
		d.aliveMembership.Remove(id)
		d.deadMembership.Remove(id)
		delete(d.id2Member, string(id))
		delete(d.deadLastTS, string(id))
		delete(d.aliveLastTS, string(id))
	}

	s := &aliveMsgStore{
		MessageStore: msgstore.NewMessageStoreExpirable(policy, trigger, aliveMsgTTL, externalLock, externalUnlock, callback),
	}
	return s
}

func (s *aliveMsgStore) Add(msg interface{}) bool {
	m := msg.(*protoext.SignedGossipMessage)
	if !protoext.IsAliveMsg(m.GossipMessage) {
		panic(fmt.Sprint("Msg ", msg, " is not AliveMsg"))
	}
	return s.MessageStore.Add(msg)
}

func (s *aliveMsgStore) CheckValid(msg interface{}) bool {
	m := msg.(*protoext.SignedGossipMessage)
	if !protoext.IsAliveMsg(m.GossipMessage) {
		panic(fmt.Sprint("Msg ", msg, " is not AliveMsg"))
	}
	return s.MessageStore.CheckValid(msg)
}
