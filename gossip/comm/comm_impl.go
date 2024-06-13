/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/identity"
	"github.com/hyperledger/fabric/gossip/metrics"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	handshakeTimeout = time.Second * 10
	DefDialTimeout   = time.Second * 3
	DefConnTimeout   = time.Second * 2
	DefRecvBuffSize  = 20
	DefSendBuffSize  = 20
)

var errProbe = errors.New("probe")

// SecurityAdvisor defines an external auxiliary object
// that provides security and identity related capabilities
type SecurityAdvisor interface {
	// OrgByPeerIdentity returns the organization identity of the given PeerIdentityType
	OrgByPeerIdentity(api.PeerIdentityType) api.OrgIdentityType
}

func (c *commImpl) SetDialOpts(opts ...grpc.DialOption) {
	if len(opts) == 0 {
		c.logger.Warning("Given an empty set of grpc.DialOption, aborting")
		return
	}
	c.opts = opts
}

// NewCommInstance 创建一个新的通信实例，将其自身绑定到给定的 gRPC 服务器
//
// 输入参数：
//   - s：gRPC 服务器。
//   - certs：TLS 证书。
//   - idStore：身份映射器。
//   - peerIdentity：对等节点的身份类型。
//   - secureDialOpts：对等节点安全拨号选项。
//   - sa：安全性顾问。
//   - commMetrics：通信指标。
//   - config：通信配置。
//   - dialOpts：可选的 gRPC 拨号选项。
//
// 返回值：
//   - Comm：创建的 Comm 实例。
//   - error：如果创建过程中发生错误，则返回相应的错误。
func NewCommInstance(s *grpc.Server, certs *common.TLSCertificates, idStore identity.Mapper,
	peerIdentity api.PeerIdentityType, secureDialOpts api.PeerSecureDialOpts, sa api.SecurityAdvisor,
	commMetrics *metrics.CommMetrics, config CommConfig, dialOpts ...grpc.DialOption) (Comm, error) {

	// 是 Comm 接口的实现。
	commInst := &commImpl{
		sa:              sa,                                       // 安全性顾问
		pubSub:          util.NewPubSub(),                         // 发布-订阅模式的工具
		PKIID:           idStore.GetPKIidOfCert(peerIdentity),     // PKI ID
		idMapper:        idStore,                                  // 身份映射器
		logger:          util.GetLogger(util.CommLogger, ""),      // 日志记录器
		peerIdentity:    peerIdentity,                             // 对等节点的身份类型
		opts:            dialOpts,                                 // gRPC 拨号选项
		secureDialOpts:  secureDialOpts,                           // 安全拨号选项
		msgPublisher:    NewChannelDemultiplexer(),                // 消息发布器
		lock:            &sync.Mutex{},                            // 互斥锁
		deadEndpoints:   make(chan common.PKIidType, 100),         // 已断开连接的远程节点的 PKI ID 通道
		identityChanges: make(chan common.PKIidType, 1),           // 身份变更的远程节点的 PKI ID 通道
		stopping:        int32(0),                                 // 停止标志
		exitChan:        make(chan struct{}),                      // 退出通道
		subscriptions:   make([]chan protoext.ReceivedMessage, 0), // 订阅的消息通道列表
		tlsCerts:        certs,                                    // TLS 证书
		metrics:         commMetrics,                              // 通信指标
		dialTimeout:     config.DialTimeout,                       // 拨号超时时间
		connTimeout:     config.ConnTimeout,                       // 连接超时时间
		recvBuffSize:    config.RecvBuffSize,                      // 接收缓冲区大小
		sendBuffSize:    config.SendBuffSize,                      // 发送缓冲区大小
	}

	// 是初始化新 conn 所需的配置, 发生和解释的大小
	connConfig := ConnConfig{
		RecvBuffSize: config.RecvBuffSize, // 接收缓冲区大小
		SendBuffSize: config.SendBuffSize, // 发送缓冲区大小
	}

	// 一个连接存储结构，用于管理与远程对等节点的连接。
	commInst.connStore = newConnStore(commInst, commInst.logger, connConfig)

	// 注册gossip服务器。将服务及其实现注册到 gRPC 服务器。
	// 它是从 IDL 生成的代码中调用的。这必须在之前调用服务。
	// 它并不直接启动 Gossip，而是将 Gossip 服务与 gRPC 服务器进行绑定，以便可以通过 gRPC 接口进行通信。
	// 要启动 Gossip，您需要在适当的时机调用 gRPC 服务器的 Serve 方法，以便开始监听和处理来自客户端的请求。
	// 例如，可以使用 s.Serve(listener) 来启动 gRPC 服务器并监听指定的网络地址。
	proto.RegisterGossipServer(s, commInst)

	return commInst, nil
}

// CommConfig 是否需要配置来初始化新的通道
type CommConfig struct {
	DialTimeout  time.Duration // 拨号超时 (单位: 秒)
	ConnTimeout  time.Duration // 连接超时 (单位: 秒)
	RecvBuffSize int           // 收到消息的缓冲区大小
	SendBuffSize int           // 发送消息的缓冲区大小
}

// commImpl 是 Comm 接口的实现。
type commImpl struct {
	sa              api.SecurityAdvisor             // 安全性顾问
	tlsCerts        *common.TLSCertificates         // TLS 证书
	pubSub          *util.PubSub                    // 发布-订阅模式的工具
	peerIdentity    api.PeerIdentityType            // 对等节点的身份类型
	idMapper        identity.Mapper                 // 身份映射器
	logger          util.Logger                     // 日志记录器
	opts            []grpc.DialOption               // gRPC 拨号选项
	secureDialOpts  func() []grpc.DialOption        // 安全拨号选项
	connStore       *connectionStore                // 连接存储结构
	PKIID           []byte                          // PKI ID
	deadEndpoints   chan common.PKIidType           // 已断开连接的远程节点的 PKI ID 通道
	identityChanges chan common.PKIidType           // 身份变更的远程节点的 PKI ID 通道
	msgPublisher    *ChannelDeMultiplexer           // 消息发布器
	lock            *sync.Mutex                     // 互斥锁
	exitChan        chan struct{}                   // 退出通道
	stopWG          sync.WaitGroup                  // 停止等待组
	subscriptions   []chan protoext.ReceivedMessage // 订阅的消息通道列表
	stopping        int32                           // 停止标志
	metrics         *metrics.CommMetrics            // 通信指标
	dialTimeout     time.Duration                   // 拨号超时时间
	connTimeout     time.Duration                   // 连接超时时间
	recvBuffSize    int                             // 接收缓冲区大小
	sendBuffSize    int                             // 发送缓冲区大小
}

func (c *commImpl) createConnection(endpoint string, expectedPKIID common.PKIidType) (*connection, error) {
	var err error
	var cc *grpc.ClientConn
	var stream proto.Gossip_GossipStreamClient
	var pkiID common.PKIidType
	var connInfo *protoext.ConnectionInfo
	var dialOpts []grpc.DialOption

	c.logger.Debug("Entering", endpoint, expectedPKIID)
	defer c.logger.Debug("Exiting")

	if c.isStopping() {
		return nil, errors.New("Stopping")
	}
	dialOpts = append(dialOpts, c.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())
	dialOpts = append(dialOpts, c.opts...)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()
	cc, err = grpc.DialContext(ctx, endpoint, dialOpts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cl := proto.NewGossipClient(cc)

	ctx, cancel = context.WithTimeout(context.Background(), c.connTimeout)
	defer cancel()
	if _, err = cl.Ping(ctx, &proto.Empty{}); err != nil {
		cc.Close()
		return nil, errors.WithStack(err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	if stream, err = cl.GossipStream(ctx); err == nil {
		connInfo, err = c.authenticateRemotePeer(stream, true, false)
		if err == nil {
			pkiID = connInfo.ID
			// PKIID is nil when we don't know the remote PKI id's
			if expectedPKIID != nil && !bytes.Equal(pkiID, expectedPKIID) {
				actualOrg := c.sa.OrgByPeerIdentity(connInfo.Identity)
				// If the identity isn't present, it's nil - therefore OrgByPeerIdentity would
				// return nil too and thus would be different than the actual organization
				identity, _ := c.idMapper.Get(expectedPKIID)
				oldOrg := c.sa.OrgByPeerIdentity(identity)
				if !bytes.Equal(actualOrg, oldOrg) {
					c.logger.Warning("Remote endpoint claims to be a different peer, expected", expectedPKIID, "but got", pkiID)
					cc.Close()
					cancel()
					return nil, errors.New("authentication failure")
				} else {
					c.logger.Infof("Peer %s changed its PKI-ID from %s to %s", endpoint, expectedPKIID, pkiID)
					c.identityChanges <- expectedPKIID
				}
			}
			connConfig := ConnConfig{
				RecvBuffSize: c.recvBuffSize,
				SendBuffSize: c.sendBuffSize,
			}
			conn := newConnection(cl, cc, stream, c.metrics, connConfig)
			conn.pkiID = pkiID
			conn.info = connInfo
			conn.logger = c.logger
			conn.cancel = cancel

			h := func(m *protoext.SignedGossipMessage) {
				c.logger.Debug("Got message:", m)
				c.msgPublisher.DeMultiplex(&ReceivedMessageImpl{
					conn:                conn,
					SignedGossipMessage: m,
					connInfo:            connInfo,
				})
			}
			conn.handler = interceptAcks(h, connInfo.ID, c.pubSub)
			return conn, nil
		}
		c.logger.Warningf("Authentication failed: %+v", err)
	}
	cc.Close()
	cancel()
	return nil, errors.WithStack(err)
}

func (c *commImpl) Send(msg *protoext.SignedGossipMessage, peers ...*RemotePeer) {
	if c.isStopping() || len(peers) == 0 {
		return
	}
	c.logger.Debug("Entering, sending", msg, "to ", len(peers), "peers")

	for _, peer := range peers {
		go func(peer *RemotePeer, msg *protoext.SignedGossipMessage) {
			c.sendToEndpoint(peer, msg, nonBlockingSend)
		}(peer, msg)
	}
}

func (c *commImpl) sendToEndpoint(peer *RemotePeer, msg *protoext.SignedGossipMessage, shouldBlock blockingBehavior) {
	if c.isStopping() {
		return
	}
	c.logger.Debug("Entering, Sending to", peer.Endpoint, ", msg:", msg)
	defer c.logger.Debug("Exiting")
	var err error

	conn, err := c.connStore.getConnection(peer)
	if err == nil {
		disConnectOnErr := func(err error) {
			c.logger.Warningf("%v isn't responsive: %v", peer, err)
			c.disconnect(peer.PKIID)
			conn.close()
		}
		conn.send(msg, disConnectOnErr, shouldBlock)
		return
	}
	c.logger.Warningf("Failed obtaining connection for %v reason: %v", peer, err)
	c.disconnect(peer.PKIID)
}

func (c *commImpl) isStopping() bool {
	return atomic.LoadInt32(&c.stopping) == int32(1)
}

func (c *commImpl) Probe(remotePeer *RemotePeer) error {
	var dialOpts []grpc.DialOption
	endpoint := remotePeer.Endpoint
	pkiID := remotePeer.PKIID
	if c.isStopping() {
		return errors.New("stopping")
	}
	c.logger.Debug("Entering, endpoint:", endpoint, "PKIID:", pkiID)
	dialOpts = append(dialOpts, c.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())
	dialOpts = append(dialOpts, c.opts...)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()
	cc, err := grpc.DialContext(ctx, remotePeer.Endpoint, dialOpts...)
	if err != nil {
		c.logger.Debugf("Returning %v", err)
		return err
	}
	defer cc.Close()
	cl := proto.NewGossipClient(cc)
	ctx, cancel = context.WithTimeout(context.Background(), c.connTimeout)
	defer cancel()
	_, err = cl.Ping(ctx, &proto.Empty{})
	c.logger.Debugf("Returning %v", err)
	return err
}

// Handshake 与远程对等节点进行握手。
// 方法接收者：c（commImpl类型的指针）
// 输入参数：
//   - remotePeer：RemotePeer类型，表示远程对等节点。
//
// 返回值：
//   - api.PeerIdentityType：表示对等节点的身份类型。
//   - error：如果握手过程中出错，则返回错误。
func (c *commImpl) Handshake(remotePeer *RemotePeer) (api.PeerIdentityType, error) {
	// 创建 DialOption 列表
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, c.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())
	dialOpts = append(dialOpts, c.opts...)

	// 创建上下文，并设置超时时间
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	// 拨号到远程对等节点的终结点，并传入拨号选项数组
	cc, err := grpc.DialContext(ctx, remotePeer.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	// 创建一个GossipClient客户端对象，用于与远程对等节点进行通信
	cl := proto.NewGossipClient(cc)
	// 新的上下文对象ctx，并设置连接超时时间
	ctx, cancel = context.WithTimeout(context.Background(), c.connTimeout)
	defer cancel()

	// 使用Ping方法向远程对等节点发送一个空的Ping请求，以检查与远程对等节点的连接是否正常
	if _, err = cl.Ping(ctx, &proto.Empty{}); err != nil {
		return nil, err
	}

	// 新的上下文对象ctx，并设置握手超时时间
	ctx, cancel = context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()

	// 使用GossipStream方法创建一个与远程对等节点的流式通信
	stream, err := cl.GossipStream(ctx)
	if err != nil {
		return nil, err
	}

	// 使用authenticateRemotePeer方法对远程对等节点进行身份验证，并获取连接信息
	connInfo, err := c.authenticateRemotePeer(stream, true, true)
	if err != nil {
		c.logger.Warningf("身份验证失败: %v", err)
		return nil, err
	}

	// 如果远程对等节点的PKI-ID存在且与连接信息中的PKI-ID不匹配，则返回错误
	if len(remotePeer.PKIID) > 0 && !bytes.Equal(connInfo.ID, remotePeer.PKIID) {
		return nil, errors.New("远程 peer 的pki-id与预期的pki-id不匹配")
	}

	// 返回连接信息中的身份标识
	return connInfo.Identity, nil
}

func (c *commImpl) Accept(acceptor common.MessageAcceptor) <-chan protoext.ReceivedMessage {
	genericChan := c.msgPublisher.AddChannel(acceptor)
	specificChan := make(chan protoext.ReceivedMessage, 10)

	if c.isStopping() {
		c.logger.Warning("Accept() called but comm module is stopping, returning empty channel")
		return specificChan
	}

	c.lock.Lock()
	c.subscriptions = append(c.subscriptions, specificChan)
	c.lock.Unlock()

	c.stopWG.Add(1)
	go func() {
		defer c.logger.Debug("Exiting Accept() loop")

		defer c.stopWG.Done()

		for {
			select {
			case msg, channelOpen := <-genericChan:
				if !channelOpen {
					return
				}
				select {
				case specificChan <- msg.(*ReceivedMessageImpl):
				case <-c.exitChan:
					return
				}
			case <-c.exitChan:
				return
			}
		}
	}()
	return specificChan
}

func (c *commImpl) PresumedDead() <-chan common.PKIidType {
	return c.deadEndpoints
}

func (c *commImpl) IdentitySwitch() <-chan common.PKIidType {
	return c.identityChanges
}

func (c *commImpl) CloseConn(peer *RemotePeer) {
	c.logger.Debug("Closing connection for", peer)
	c.connStore.closeConnByPKIid(peer.PKIID)
}

func (c *commImpl) closeSubscriptions() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, ch := range c.subscriptions {
		close(ch)
	}
}

func (c *commImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopping, 0, int32(1)) {
		return
	}
	c.logger.Info("Stopping")
	defer c.logger.Info("Stopped")
	c.connStore.shutdown()
	c.logger.Debug("Shut down connection store, connection count:", c.connStore.connNum())
	c.msgPublisher.Close()
	close(c.exitChan)
	c.stopWG.Wait()
	c.closeSubscriptions()
}

func (c *commImpl) GetPKIid() common.PKIidType {
	return c.PKIID
}

func extractRemoteAddress(stream stream) string {
	var remoteAddress string
	p, ok := peer.FromContext(stream.Context())
	if ok {
		if address := p.Addr; address != nil {
			remoteAddress = address.String()
		}
	}
	return remoteAddress
}

func (c *commImpl) authenticateRemotePeer(stream stream, initiator, isProbe bool) (*protoext.ConnectionInfo, error) {
	ctx := stream.Context()
	remoteAddress := extractRemoteAddress(stream)
	remoteCertHash := extractCertificateHashFromContext(ctx)
	var err error
	var cMsg *protoext.SignedGossipMessage
	useTLS := c.tlsCerts != nil
	var selfCertHash []byte

	if useTLS {
		certReference := c.tlsCerts.TLSServerCert
		if initiator {
			certReference = c.tlsCerts.TLSClientCert
		}
		selfCertHash = certHashFromRawCert(certReference.Load().(*tls.Certificate).Certificate[0])
	}

	signer := func(msg []byte) ([]byte, error) {
		return c.idMapper.Sign(msg)
	}

	// TLS已启用，但在另一侧未检测到
	if useTLS && len(remoteCertHash) == 0 {
		c.logger.Warningf("%s 未发送TLS证书", remoteAddress)
		return nil, errors.New("无TLS证书")
	}

	cMsg, err = c.createConnectionMsg(c.PKIID, selfCertHash, c.peerIdentity, signer, isProbe)
	if err != nil {
		return nil, err
	}

	c.logger.Debug("Sending", cMsg, "to", remoteAddress)
	stream.Send(cMsg.Envelope)
	m, err := readWithTimeout(stream, c.connTimeout, remoteAddress)
	if err != nil {
		c.logger.Warningf("从 %s 读取消息失败, 原因: % v", remoteAddress, err)
		return nil, err
	}
	receivedMsg := m.GetConn()
	if receivedMsg == nil {
		c.logger.Warning("预期的连接消息来自节点", remoteAddress, "但是得到了消息", receivedMsg)
		return nil, errors.New("类型错误")
	}

	if receivedMsg.PkiId == nil {
		c.logger.Warningf("%s 节点未发送PKI-ID", remoteAddress)
		return nil, errors.New("节点未发送PKI-ID")
	}

	c.logger.Debug("已收到消息", receivedMsg, "从节点", remoteAddress)
	err = c.idMapper.Put(receivedMsg.PkiId, receivedMsg.Identity)
	if err != nil {
		c.logger.Warningf("身份信息存储被拒绝 %s: %v", remoteAddress, err)
		return nil, err
	}

	connInfo := &protoext.ConnectionInfo{
		ID:       receivedMsg.PkiId,
		Identity: receivedMsg.Identity,
		Endpoint: remoteAddress,
		Auth: &protoext.AuthInfo{
			Signature:  m.Signature,
			SignedData: m.Payload,
		},
	}

	// 如果启用并检测到TLS，请验证远程对等体
	if useTLS {
		// 如果远程对等方发送了其TLS证书，请确保它实际上与TLS证书匹配
		// 对等方使用的。
		if !bytes.Equal(remoteCertHash, receivedMsg.TlsCertHash) {
			return nil, errors.Errorf("TLS证书的远程哈希中需要 %v, 但获得了 %v", remoteCertHash, receivedMsg.TlsCertHash)
		}
	}
	// 最后一步-验证连接消息本身上的签名
	verifier := func(peerIdentity []byte, signature, message []byte) error {
		pkiID := c.idMapper.GetPKIidOfCert(peerIdentity)
		return c.idMapper.Verify(pkiID, signature, message)
	}
	err = m.Verify(receivedMsg.Identity, verifier)
	if err != nil {
		c.logger.Errorf("验证来自 %s 的签名失败: %v", remoteAddress, err)
		return nil, err
	}

	c.logger.Debug("已验证", remoteAddress)

	if receivedMsg.Probe {
		return connInfo, errProbe
	}

	return connInfo, nil
}

// SendWithAck sends a message to remote peers, waiting for acknowledgement from minAck of them, or until a certain timeout expires
func (c *commImpl) SendWithAck(msg *protoext.SignedGossipMessage, timeout time.Duration, minAck int, peers ...*RemotePeer) AggregatedSendResult {
	if len(peers) == 0 {
		return nil
	}
	var err error

	// Roll a random NONCE to be used as a send ID to differentiate
	// between different invocations
	msg.Nonce = util.RandomUInt64()
	// Replace the envelope in the message to update the NONCE
	msg, err = protoext.NoopSign(msg.GossipMessage)

	if c.isStopping() || err != nil {
		if err == nil {
			err = errors.New("comm is stopping")
		}
		results := []SendResult{}
		for _, p := range peers {
			results = append(results, SendResult{
				error:      err,
				RemotePeer: *p,
			})
		}
		return results
	}
	c.logger.Debug("Entering, sending", msg, "to ", len(peers), "peers")
	sndFunc := func(peer *RemotePeer, msg *protoext.SignedGossipMessage) {
		c.sendToEndpoint(peer, msg, blockingSend)
	}
	// Subscribe to acks
	subscriptions := make(map[string]func() error)
	for _, p := range peers {
		topic := topicForAck(msg.Nonce, p.PKIID)
		sub := c.pubSub.Subscribe(topic, timeout)
		subscriptions[string(p.PKIID)] = func() error {
			msg, err := sub.Listen()
			if err != nil {
				return err
			}
			if msg, isAck := msg.(*proto.Acknowledgement); !isAck {
				return errors.Errorf("received a message of type %s, expected *proto.Acknowledgement", reflect.TypeOf(msg))
			} else {
				if msg.Error != "" {
					return errors.New(msg.Error)
				}
			}
			return nil
		}
	}
	waitForAck := func(p *RemotePeer) error {
		return subscriptions[string(p.PKIID)]()
	}
	ackOperation := newAckSendOperation(sndFunc, waitForAck)
	return ackOperation.send(msg, minAck, peers...)
}

func (c *commImpl) GossipStream(stream proto.Gossip_GossipStreamServer) error {
	if c.isStopping() {
		return errors.New("正在关闭")
	}
	connInfo, err := c.authenticateRemotePeer(stream, false, false)

	if err == errProbe {
		c.logger.Infof("peer节点 %s (%s) 探测到我们", connInfo.ID, connInfo.Endpoint)
		return nil
	}

	if err != nil {
		c.logger.Errorf("身份验证失败: %v", err)
		return err
	}
	c.logger.Debug("Servicing", extractRemoteAddress(stream))

	conn := c.connStore.onConnected(stream, connInfo, c.metrics)

	h := func(m *protoext.SignedGossipMessage) {
		c.msgPublisher.DeMultiplex(&ReceivedMessageImpl{
			conn:                conn,
			SignedGossipMessage: m,
			connInfo:            connInfo,
		})
	}

	conn.handler = interceptAcks(h, connInfo.ID, c.pubSub)

	defer func() {
		c.logger.Debug("Client", extractRemoteAddress(stream), " disconnected")
		c.connStore.closeConnByPKIid(connInfo.ID)
	}()

	return conn.serviceConnection()
}

func (c *commImpl) Ping(context.Context, *proto.Empty) (*proto.Empty, error) {
	return &proto.Empty{}, nil
}

func (c *commImpl) disconnect(pkiID common.PKIidType) {
	select {
	case c.deadEndpoints <- pkiID:
	case <-c.exitChan:
		return
	}

	c.connStore.closeConnByPKIid(pkiID)
}

func readWithTimeout(stream stream, timeout time.Duration, address string) (*protoext.SignedGossipMessage, error) {
	incChan := make(chan *protoext.SignedGossipMessage, 1)
	errChan := make(chan error, 1)
	go func() {
		if m, err := stream.Recv(); err == nil {
			msg, err := protoext.EnvelopeToGossipMessage(m)
			if err != nil {
				errChan <- err
				return
			}
			incChan <- msg
		}
	}()
	select {
	case <-time.After(timeout):
		return nil, errors.Errorf("timed out waiting for connection message from %s", address)
	case m := <-incChan:
		return m, nil
	case err := <-errChan:
		return nil, errors.WithStack(err)
	}
}

func (c *commImpl) createConnectionMsg(pkiID common.PKIidType, certHash []byte, cert api.PeerIdentityType, signer protoext.Signer, isProbe bool) (*protoext.SignedGossipMessage, error) {
	m := &proto.GossipMessage{
		Tag:   proto.GossipMessage_EMPTY,
		Nonce: 0,
		Content: &proto.GossipMessage_Conn{
			Conn: &proto.ConnEstablish{
				TlsCertHash: certHash,
				Identity:    cert,
				PkiId:       pkiID,
				Probe:       isProbe,
			},
		},
	}
	sMsg := &protoext.SignedGossipMessage{
		GossipMessage: m,
	}
	_, err := sMsg.Sign(signer)
	return sMsg, errors.WithStack(err)
}

type stream interface {
	Send(envelope *proto.Envelope) error
	Recv() (*proto.Envelope, error)
	Context() context.Context
}

func topicForAck(nonce uint64, pkiID common.PKIidType) string {
	return fmt.Sprintf("%d %s", nonce, hex.EncodeToString(pkiID))
}
