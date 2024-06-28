/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"github.com/hyperledger/fabric/bccsp/common"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	// MinimumExpirationWarningInterval is the default minimum time interval
	// between consecutive warnings about certificate expiration.
	MinimumExpirationWarningInterval = time.Minute * 5
)

var (
	errOverflow = errors.New("send queue overflown")
	errAborted  = errors.New("aborted")
	errTimeout  = errors.New("rpc timeout expired")
)

// ChannelExtractor extracts the channel of a given message,
// or returns an empty string if that's not possible
type ChannelExtractor interface {
	TargetChannel(message proto.Message) string
}

//go:generate mockery -dir . -name Handler -case underscore -output ./mocks/

// Handler handles Step() and Submit() requests and returns a corresponding response
type Handler interface {
	OnConsensus(channel string, sender uint64, req *orderer.ConsensusRequest) error
	OnSubmit(channel string, sender uint64, req *orderer.SubmitRequest) error
}

// RemoteNode 结构体代表了集群中的一个成员节点。
type RemoteNode struct {
	// ID 是该成员节点在集群中的唯一标识符，且不能为 0。
	ID uint64

	// Endpoint 是节点的通信端点，格式为 "%s:%d"，其中 %s 表示主机名或 IP 地址，%d 表示端口号。
	Endpoint string

	// ServerTLSCert 是节点的 TLS 服务器证书，以 DER 编码的字节序列形式存储。
	ServerTLSCert []byte

	// ClientTLSCert 是节点的 TLS 客户端证书，同样以 DER 编码的字节序列形式存储。
	ClientTLSCert []byte
}

// String returns a string representation of this RemoteNode
func (rm RemoteNode) String() string {
	return fmt.Sprintf("ID: %d,\nEndpoint: %s,\nServerTLSCert:%s, ClientTLSCert:%s",
		rm.ID, rm.Endpoint, DERtoPEM(rm.ServerTLSCert), DERtoPEM(rm.ClientTLSCert))
}

//go:generate mockery -dir . -name Communicator -case underscore -output ./mocks/

// Communicator 接口定义了共识者进行通信的标准方式和能力。
type Communicator interface {
	// Remote 方法为给定的通道名称和远程节点ID创建或返回一个 RemoteContext 对象，
	// 如果因无法建立连接或通道未在配置中找到，则返回错误。
	// RemoteContext 对象封装了与远程节点通信所需的上下文和功能，
	// 包括但不限于发送消息、接收响应以及管理底层的网络连接。
	Remote(channel string, id uint64) (*RemoteContext, error)

	// Configure 方法允许重新配置通信器，使其连接到所有指定的成员，
	// 并断开与那些不再属于成员列表的节点的连接。
	// 这在共识组的成员资格发生变化时非常关键，例如当有新节点加入或现有节点离开时，
	// 确保通信器总是与当前有效的共识组成员保持通信。
	Configure(channel string, members []RemoteNode)

	// Shutdown 方法用于关闭通信器，释放所有相关的网络资源，
	// 包括但不限于网络连接、线程池和缓存区。
	// 这个方法确保了在共识者退出或系统关闭时，所有资源都能够被优雅地回收，
	// 避免资源泄露和潜在的系统不稳定。
	Shutdown()
}

// MembersByChannel 是来自通道名称的映射到MemberMapping
type MembersByChannel map[string]MemberMapping

// Comm 结构体实现了 Communicator 接口，用于管理与多个节点的通信。
type Comm struct {
	// MinimumExpirationWarningInterval 是一个时间持续值，表示在证书到期前发出警告的最短间隔。
	MinimumExpirationWarningInterval time.Duration

	// CertExpWarningThreshold 是一个时间持续值，表示在证书到期前多少时间开始发出警告。
	CertExpWarningThreshold time.Duration

	// shutdownSignal 是一个通道，用于通知 Comm 实例停止所有正在进行的通信并释放资源。
	shutdownSignal chan struct{}

	// shutdown 是一个布尔值，表示 Comm 是否已经被关闭。
	shutdown bool

	// SendBufferSize 是一个整数值，表示发送缓冲区的大小。
	SendBufferSize int

	// Lock 是一个读写锁，用于保护 Comm 的内部状态，确保线程安全。
	Lock sync.RWMutex

	// Logger 是一个日志记录器，用于记录 Comm 的操作和状态。
	Logger *flogging.FabricLogger

	// ChanExt 是一个 ChannelExtractor 接口实例，用于从消息中提取通道信息。
	ChanExt ChannelExtractor

	// H 是一个 Handler 接口实例，用于处理来自远程节点的消息。
	H Handler

	// Connections 是一个 ConnectionStore 指针，用于存储和管理与远程节点的连接。
	Connections *ConnectionStore

	// Chan2Members 是一个 MembersByChannel 映射，用于存储每个通道的成员信息。
	Chan2Members MembersByChannel

	// Metrics 是一个 Metrics 指针，用于收集和报告 Comm 的性能指标。
	Metrics *Metrics

	// CompareCertificate 是一个 CertificateComparator 接口实例，用于比较证书的有效性。
	CompareCertificate CertificateComparator
}

type requestContext struct {
	channel string
	sender  uint64
}

// DispatchSubmit identifies the channel and sender of the submit request and passes it
// to the underlying Handler
func (c *Comm) DispatchSubmit(ctx context.Context, request *orderer.SubmitRequest) error {
	reqCtx, err := c.requestContext(ctx, request)
	if err != nil {
		return err
	}
	return c.H.OnSubmit(reqCtx.channel, reqCtx.sender, request)
}

// DispatchConsensus identifies the channel and sender of the step request and passes it
// to the underlying Handler
func (c *Comm) DispatchConsensus(ctx context.Context, request *orderer.ConsensusRequest) error {
	reqCtx, err := c.requestContext(ctx, request)
	if err != nil {
		return err
	}
	return c.H.OnConsensus(reqCtx.channel, reqCtx.sender, request)
}

// requestContext identifies the sender and channel of the request and returns
// it wrapped in a requestContext
func (c *Comm) requestContext(ctx context.Context, msg proto.Message) (*requestContext, error) {
	channel := c.ChanExt.TargetChannel(msg)
	if channel == "" {
		return nil, errors.Errorf("badly formatted message, cannot extract channel")
	}

	c.Lock.RLock()
	mapping, exists := c.Chan2Members[channel]
	c.Lock.RUnlock()

	if !exists {
		return nil, errors.Errorf("channel %s doesn't exist", channel)
	}

	cert := util.ExtractRawCertificateFromContext(ctx)
	if len(cert) == 0 {
		return nil, errors.Errorf("no TLS certificate sent")
	}

	stub := mapping.LookupByClientCert(cert)
	if stub == nil {
		return nil, errors.Errorf("certificate extracted from TLS connection isn't authorized")
	}
	return &requestContext{
		channel: channel,
		sender:  stub.ID,
	}, nil
}

// Remote 方法从给定的通道上下文中获取与目标节点关联的 RemoteContext。
// 这个方法首先锁定通信器的状态，确保在读取或修改成员映射时的一致性。
// 如果通信器已被关闭，立即返回错误。
// 然后，它查找指定通道的成员映射，如果通道不存在，返回错误。
// 接下来，根据节点ID查找成员，如果找不到对应的成员，返回错误。
// 如果找到的成员是活动的，直接返回其 RemoteContext。
// 如果成员处于非活动状态，尝试激活它，并创建一个新的 RemoteContext。
// 如果激活过程中出现错误，返回错误；否则，返回新创建的 RemoteContext。
func (c *Comm) Remote(channel string, id uint64) (*RemoteContext, error) {
	c.Lock.RLock()         // 加读锁以安全地访问成员映射
	defer c.Lock.RUnlock() // 在方法结束时释放读锁

	if c.shutdown { // 检查通信器是否已关闭
		return nil, errors.New("通信已经关闭") // 如果已关闭，返回错误
	}

	mapping, exists := c.Chan2Members[channel] // 查找通道的成员映射
	if !exists {                               // 如果通道不存在
		return nil, errors.Errorf("通道 %s 不存在", channel) // 返回错误
	}
	stub := mapping.ByID(id) // 根据节点ID查找成员
	if stub == nil {         // 如果找不到成员
		return nil, errors.Errorf("节点 %d 在通道 %s 的成员列表中不存在", id, channel) // 返回错误
	}

	if stub.Active() { // 如果成员是活动的
		return stub.RemoteContext, nil // 直接返回 RemoteContext
	}

	err := stub.Activate(c.createRemoteContext(stub, channel)) // 尝试激活成员并创建 RemoteContext
	if err != nil {                                            // 如果激活失败
		return nil, errors.WithStack(err) // 返回错误
	}
	return stub.RemoteContext, nil // 返回新创建的 RemoteContext
}

// Configure 函数用于使用给定的 RemoteNode 列表来配置指定的通道。
func (c *Comm) Configure(channel string, newNodes []RemoteNode) {
	// 遍历新节点列表，打印日志信息，显示即将加入的节点详情。
	for _, node := range newNodes {
		c.Logger.Infof("加入共识成功, 通道: %s, 节点: [ID:%d,Endpoint:%s]", channel, node.ID, node.Endpoint)
	}
	// 打印退出日志信息。
	defer c.Logger.Infof("退出")

	// 获取写锁，确保在配置过程中不会有其他并发写操作影响。
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// 如果需要，创建关闭信号。
	c.createShutdownSignalIfNeeded()

	// 检查是否已触发关闭，如果是，则直接返回。
	if c.shutdown {
		return
	}

	// 记录配置变更前正在使用的服务器证书。
	beforeConfigChange := c.serverCertsInUse()

	// 更新通道级别的成员映射，使用新的节点列表。
	c.applyMembershipConfig(channel, newNodes)

	// 清理不再新成员列表中的节点的连接。
	c.cleanUnusedConnections(beforeConfigChange)
}

func (c *Comm) createShutdownSignalIfNeeded() {
	if c.shutdownSignal == nil {
		c.shutdownSignal = make(chan struct{})
	}
}

// Shutdown shuts down the instance
func (c *Comm) Shutdown() {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.createShutdownSignalIfNeeded()
	if !c.shutdown {
		close(c.shutdownSignal)
	}

	c.shutdown = true
	for _, members := range c.Chan2Members {
		members.Foreach(func(id uint64, stub *Stub) {
			c.Connections.Disconnect(stub.ServerTLSCert)
		})
	}
}

// cleanUnusedConnections 函数断开所有在调用时刻未被使用的连接。
func (c *Comm) cleanUnusedConnections(serverCertsBeforeConfig StringSet) {
	// 重新配置后，扫描所有节点。
	serverCertsAfterConfig := c.serverCertsInUse()

	// 筛选出在重新配置后仍存在的证书，后面将过滤掉这些不再需要的证书。
	serverCertsBeforeConfig.subtract(serverCertsAfterConfig)

	// 断开与这些节点的所有连接，因为它们现在不应该再被使用。
	for serverCertificate := range serverCertsBeforeConfig {
		c.Connections.Disconnect([]byte(serverCertificate))
	}
}

// serverCertsInUse 函数返回当前正在使用的服务器证书，表示为字符串集合。
func (c *Comm) serverCertsInUse() StringSet {
	// 初始化一个空的字符串集合，用于存储正在使用的服务器证书。
	endpointsInUse := make(StringSet)

	// 遍历通道到成员的映射，收集所有正在使用的服务器证书。
	for _, mapping := range c.Chan2Members {
		// 调用 ServerCertificates 方法，获取当前映射下的所有服务器证书。
		endpointsInUse.union(mapping.ServerCertificates())
	}

	// 返回收集到的正在使用的服务器证书集合。
	return endpointsInUse
}

// applyMembershipConfig 为给定通道设置给定的RemoteNodes
func (c *Comm) applyMembershipConfig(channel string, newNodes []RemoteNode) {
	mapping := c.getOrCreateMapping(channel)
	newNodeIDs := make(map[uint64]struct{})

	for _, node := range newNodes {
		newNodeIDs[node.ID] = struct{}{}
		c.updateStubInMapping(channel, mapping, node)
	}

	// 删除所有没有对应节点的存根
	// 在新节点中
	mapping.Foreach(func(id uint64, stub *Stub) {
		if _, exists := newNodeIDs[id]; exists {
			c.Logger.Debug(id, "存在于通道的新旧成员中", channel, ", 正在跳过其停用")
			return
		}
		c.Logger.Info("已停用节点", id, "谁是端点", stub.Endpoint, "因为它已从会员中删除")
		mapping.Remove(id)
		stub.Deactivate()
	})
}

// updateStubInMapping 更新给定的RemoteNode并将其添加到MemberMapping
func (c *Comm) updateStubInMapping(channel string, mapping MemberMapping, node RemoteNode) {
	stub := mapping.ByID(node.ID)
	if stub == nil {
		c.Logger.Info("节点", node.ID, "分配", node.Endpoint, "用于通道", channel)
		stub = &Stub{}
	}

	// 检查节点的TLS服务器证书是否被替换
	// 如果是这样-那么停用存根，以触发
	// 重新创建其gRPC连接
	if !bytes.Equal(stub.ServerTLSCert, node.ServerTLSCert) {
		c.Logger.Info("停用节点", node.ID, "在通道中", channel,
			"的端点", node.Endpoint, "由于TLS证书更改")
		stub.Deactivate()
	}

	// 用新数据覆盖存根节点数据
	stub.RemoteNode = node

	// 将存根放入映射中
	mapping.Put(stub)

	// 检查存根是否需要激活。
	if stub.Active() {
		return
	}

	// 激活存根
	stub.Activate(c.createRemoteContext(stub, channel))
}

// createRemoteContext 函数返回一个创建 RemoteContext 的函数。
// 这个函数被作为参数传递给 Stub.Activate() 方法，用于原子性地激活一个 stub。
func (c *Comm) createRemoteContext(stub *Stub, channel string) func() (*RemoteContext, error) {
	return func() (*RemoteContext, error) {
		// 解析服务器 TLS 证书
		cert, err := common.ParseCertificate(stub.ServerTLSCert)
		if err != nil {
			// 如果解析失败，编码证书为 PEM 格式并记录错误日志
			pemString := string(pem.EncodeToMemory(&pem.Block{Bytes: stub.ServerTLSCert}))
			c.Logger.Errorf("通道 %s 的无效 DER，端点 %s，ID %d: %v", channel, stub.Endpoint, stub.ID, pemString)
			return nil, errors.Wrap(err, "无效的证书 DER")
		}

		// 输出正在连接的远程节点和频道信息
		c.Logger.Debug("正在连接到", stub.RemoteNode, "用于通道", channel)

		// 从 Connections 中获取与指定端点的连接
		conn, err := c.Connections.Connection(stub.Endpoint, stub.ServerTLSCert)
		if err != nil {
			// 如果获取连接失败，记录警告日志并返回错误
			c.Logger.Warningf("无法获取 %d(%s) 的连接（通道 %s）: %v", stub.ID, stub.Endpoint, channel, err)
			return nil, err
		}

		// 定义一个探查连接状态的函数
		probeConnection := func(conn *grpc.ClientConn) error {
			connState := conn.GetState()
			// 如果连接状态为 Connecting，返回错误
			if connState == connectivity.Connecting {
				return errors.Errorf("与 %d(%s) 的连接处于 %s 状态", stub.ID, stub.Endpoint, connState)
			}
			return nil
		}

		// 创建 ClusterClient 实例
		clusterClient := orderer.NewClusterClient(conn)

		// 初始化 workerCountReporter 实例
		workerCountReporter := workerCountReporter{
			channel: channel,
		}

		// 创建并初始化 RemoteContext 实例
		rc := &RemoteContext{
			expiresAt:                        cert.NotAfter,                      // 证书过期时间
			minimumExpirationWarningInterval: c.MinimumExpirationWarningInterval, // 最小过期警告间隔
			certExpWarningThreshold:          c.CertExpWarningThreshold,          // 证书过期警告阈值
			workerCountReporter:              workerCountReporter,                // 工作线程计数报告器
			Channel:                          channel,                            // 频道名称
			Metrics:                          c.Metrics,                          // 监控指标
			SendBuffSize:                     c.SendBufferSize,                   // 发送缓冲区大小
			shutdownSignal:                   c.shutdownSignal,                   // 关闭信号
			endpoint:                         stub.Endpoint,                      // 远程节点端点
			Logger:                           c.Logger,                           // 日志记录器
			ProbeConn:                        probeConnection,                    // 探查连接状态的函数
			conn:                             conn,                               // gRPC 连接
			Client:                           clusterClient,                      // ClusterClient 实例
		}
		// 返回初始化完成的 RemoteContext 实例
		return rc, nil
	}
}

// getOrCreateMapping creates a MemberMapping for the given channel
// or returns the existing one.
func (c *Comm) getOrCreateMapping(channel string) MemberMapping {
	// Lazily create a mapping if it doesn't already exist
	mapping, exists := c.Chan2Members[channel]
	if !exists {
		mapping = MemberMapping{
			id2stub:       make(map[uint64]*Stub),
			SamePublicKey: c.CompareCertificate,
		}
		c.Chan2Members[channel] = mapping
	}
	return mapping
}

// Stub holds all information about the remote node,
// including the RemoteContext for it, and serializes
// some operations on it.
type Stub struct {
	lock sync.RWMutex
	RemoteNode
	*RemoteContext
}

// Active returns whether the Stub
// is active or not
func (stub *Stub) Active() bool {
	stub.lock.RLock()
	defer stub.lock.RUnlock()
	return stub.isActive()
}

// Active returns whether the Stub
// is active or not.
func (stub *Stub) isActive() bool {
	return stub.RemoteContext != nil
}

// Deactivate deactivates the Stub and
// ceases all communication operations
// invoked on it.
func (stub *Stub) Deactivate() {
	stub.lock.Lock()
	defer stub.lock.Unlock()
	if !stub.isActive() {
		return
	}
	stub.RemoteContext.Abort()
	stub.RemoteContext = nil
}

// Activate 函数使用给定的回调函数 `createRemoteContext` 以原子方式创建远程上下文。
// 如果在 Stub 上并行调用了两个激活请求，只会执行一次 `createRemoteContext` 的调用。
func (stub *Stub) Activate(createRemoteContext func() (*RemoteContext, error)) error {
	// 加锁以确保原子操作，防止多个 goroutine 并发修改 Stub 的状态。
	stub.lock.Lock()
	defer stub.lock.Unlock()

	// 检查在等待锁的过程中 Stub 是否已被激活。
	if stub.isActive() {
		return nil // 如果 Stub 已经激活，则直接返回无错误。
	}

	// 调用回调函数 `createRemoteContext` 来创建远程上下文。
	remoteStub, err := createRemoteContext()
	if err != nil {
		// 如果创建远程上下文时出现错误，返回带有堆栈信息的错误。
		return errors.WithStack(err)
	}

	// 将创建的远程上下文赋值给 Stub 的 RemoteContext 字段。
	stub.RemoteContext = remoteStub
	return nil // 成功创建远程上下文，返回无错误。
}

// RemoteContext interacts with remote cluster
// nodes. Every call can be aborted via call to Abort()
type RemoteContext struct {
	expiresAt                        time.Time
	minimumExpirationWarningInterval time.Duration
	certExpWarningThreshold          time.Duration
	Metrics                          *Metrics
	Channel                          string
	SendBuffSize                     int
	shutdownSignal                   chan struct{}
	Logger                           *flogging.FabricLogger
	endpoint                         string
	Client                           orderer.ClusterClient
	ProbeConn                        func(conn *grpc.ClientConn) error
	conn                             *grpc.ClientConn
	nextStreamID                     uint64
	streamsByID                      streamsMapperReporter
	workerCountReporter              workerCountReporter
}

// Stream 用于与远程集群成员发送/接收消息。
type Stream struct {
	abortChan <-chan struct{} // 用于中止流的通道
	sendBuff  chan struct {   // 发送缓冲通道，包含请求和报告函数
		request *orderer.StepRequest
		report  func(error)
	}
	commShutdown               chan struct{}               // 通信关闭通道
	abortReason                *atomic.Value               // 中止原因的原子值
	metrics                    *Metrics                    // 指标
	ID                         uint64                      // 流的唯一标识
	Channel                    string                      // 通道名称
	NodeName                   string                      // 节点名称
	Endpoint                   string                      // 终端点
	Logger                     *flogging.FabricLogger      // 日志记录器
	Timeout                    time.Duration               // 超时时间
	orderer.Cluster_StepClient                             // 集群步骤客户端
	Cancel                     func(error)                 // 取消函数
	canceled                   *uint32                     // 取消标志
	expCheck                   *certificateExpirationCheck // 证书过期检查
}

// StreamOperation denotes an operation done by a stream, such a Send or Receive.
type StreamOperation func() (*orderer.StepResponse, error)

// Canceled returns whether the stream was canceled.
func (stream *Stream) Canceled() bool {
	return atomic.LoadUint32(stream.canceled) == uint32(1)
}

// Send sends the given request to the remote cluster member.
func (stream *Stream) Send(request *orderer.StepRequest) error {
	return stream.SendWithReport(request, func(_ error) {})
}

// SendWithReport 方法向远程集群成员发送给定请求，并在发送结果上调用 report 函数。
func (stream *Stream) SendWithReport(request *orderer.StepRequest, report func(error)) error {
	if stream.Canceled() {
		return errors.New(stream.abortReason.Load().(string))
	}
	var allowDrop bool
	// 如果请求是共识请求，我们希望在远程节点跟不上时丢弃共识交易，
	// 否则会拖慢整个有限状态机的速度。
	if request.GetConsensusRequest() != nil {
		allowDrop = true
	}

	return stream.sendOrDrop(request, allowDrop, report)
}

// sendOrDrop 方法向远程集群成员发送给定请求，或者在共识请求且队列已满时丢弃请求。
func (stream *Stream) sendOrDrop(request *orderer.StepRequest, allowDrop bool, report func(error)) error {
	msgType := "transaction"
	if allowDrop {
		msgType = "consensus"
	}

	// 报告队列占用情况
	stream.metrics.reportQueueOccupancy(stream.Endpoint, msgType, stream.Channel, len(stream.sendBuff), cap(stream.sendBuff))

	// 如果允许丢弃并且队列已满，则取消流并报告消息被丢弃
	if allowDrop && len(stream.sendBuff) == cap(stream.sendBuff) {
		stream.Cancel(errOverflow)
		stream.metrics.reportMessagesDropped(stream.Endpoint, stream.Channel)
		return errOverflow
	}

	select {
	case <-stream.abortChan:
		return errors.Errorf("流 %d 已中止", stream.ID)
	// 这里将消息雪茹到sendBuff通道, 由之前创建的流发送 sendMessage()
	case stream.sendBuff <- struct {
		request *orderer.StepRequest
		report  func(error)
	}{request: request, report: report}:
		return nil
	case <-stream.commShutdown:
		return nil
	}
}

// 将请求发送到流中
func (stream *Stream) sendMessage(request *orderer.StepRequest, report func(error)) {
	start := time.Now() // 记录开始时间
	var err error
	defer func() {
		message := fmt.Sprintf("发送 %s 到 %s(%s) 花费 %v",
			requestAsString(request), stream.NodeName, stream.Endpoint, time.Since(start)) // 构建消息字符串
		if err != nil {
			stream.Logger.Warnf("%s 但由于 %s 失败", message, err.Error()) // 记录警告日志
		} else {
			stream.Logger.Debug(message) // 记录调试日志
		}
	}()

	// 在流上执行给定操作，并阻塞直到超时到期。
	f := func() (*orderer.StepResponse, error) {
		startSend := time.Now()
		stream.expCheck.checkExpiration(startSend, stream.Channel)                               // 检查证书过期
		err := stream.Cluster_StepClient.Send(request)                                           // 发送请求
		stream.metrics.reportMsgSendTime(stream.Endpoint, stream.Channel, time.Since(startSend)) // 报告消息发送时间
		return nil, err
	}

	// 使用超时操作发送消息, 内部调用f()执行发送
	_, err = stream.operateWithTimeout(f, report)
}

// 服务流, 这里是发送请求的核心位置
func (stream *Stream) serviceStream() {
	streamStartTime := time.Now() // 记录流开始时间
	defer func() {
		stream.Cancel(errAborted) // 在函数结束时取消流
		stream.Logger.Debugf("流 %d 到 (%s) 终止，总生命周期为 %s",
			stream.ID, stream.Endpoint, time.Since(streamStartTime)) // 记录流的总生命周期
	}()

	for {
		select {
		case reqReport := <-stream.sendBuff: // 从发送缓冲通道接收请求和报告函数
			stream.sendMessage(reqReport.request, reqReport.report) // 处理发送消息
		case <-stream.abortChan: // 如果中止通道有信号，则结束循环
			return
		case <-stream.commShutdown: // 如果通信关闭通道有信号，则结束循环
			return
		}
	}
}

// Recv 接收来自远程集群成员的消息。
func (stream *Stream) Recv() (*orderer.StepResponse, error) {
	start := time.Now()
	defer func() {
		if !stream.Logger.IsEnabledFor(zap.DebugLevel) {
			return
		}
		stream.Logger.Debugf("Receive from %s(%s) took %v", stream.NodeName, stream.Endpoint, time.Since(start))
	}()

	f := func() (*orderer.StepResponse, error) {
		return stream.Cluster_StepClient.Recv()
	}

	return stream.operateWithTimeout(f, func(_ error) {})
}

// operateWithTimeout 在流上执行给定操作，并阻塞直到超时到期。
func (stream *Stream) operateWithTimeout(invoke StreamOperation, report func(error)) (*orderer.StepResponse, error) {
	timer := time.NewTimer(stream.Timeout) // 创建定时器
	defer timer.Stop()

	var operationEnded sync.WaitGroup
	operationEnded.Add(1)

	responseChan := make(chan struct {
		res *orderer.StepResponse
		err error
	}, 1)

	// 启动一个 goroutine 执行操作
	go func() {
		defer operationEnded.Done()
		// 执行发送/接受消息到raft
		res, err := invoke()
		responseChan <- struct {
			res *orderer.StepResponse
			err error
		}{res: res, err: err}
	}()

	select {
	case r := <-responseChan: // 接收操作结果
		// 回调
		report(r.err)
		if r.err != nil {
			stream.Cancel(r.err)
		}
		return r.res, r.err
	case <-timer.C: // 超时处理
		// 回调
		report(errTimeout)
		stream.Logger.Warningf("流 %d 到 %s(%s) 因超时 (%v) 被强制终止",
			stream.ID, stream.NodeName, stream.Endpoint, stream.Timeout)
		stream.Cancel(errTimeout)
		// 等待操作 goroutine 结束
		operationEnded.Wait()
		return nil, errTimeout
	}
}

func requestAsString(request *orderer.StepRequest) string {
	switch t := request.GetPayload().(type) {
	case *orderer.StepRequest_SubmitRequest:
		if t.SubmitRequest == nil || t.SubmitRequest.Payload == nil {
			return fmt.Sprintf("Empty SubmitRequest: %v", t.SubmitRequest)
		}
		return fmt.Sprintf("SubmitRequest for channel %s with payload of size %d",
			t.SubmitRequest.Channel, len(t.SubmitRequest.Payload.Payload))
	case *orderer.StepRequest_ConsensusRequest:
		return fmt.Sprintf("ConsensusRequest for channel %s with payload of size %d",
			t.ConsensusRequest.Channel, len(t.ConsensusRequest.Payload))
	default:
		return fmt.Sprintf("unknown type: %v", request)
	}
}

// NewStream 创建一个新流, 这里是发送请求的核心位置。
// 它不是线程安全的，Send() 或 Recv() 只会阻塞直到超时到期。
func (rc *RemoteContext) NewStream(timeout time.Duration) (*Stream, error) {
	// 检查连接是否可用
	if err := rc.ProbeConn(rc.conn); err != nil {
		return nil, err
	}

	// 创建上下文和流
	ctx, cancel := context.WithCancel(context.TODO())
	stream, err := rc.Client.Step(ctx)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	// 生成流ID和节点名称
	streamID := atomic.AddUint64(&rc.nextStreamID, 1)
	nodeName := commonNameFromContext(stream.Context())

	var canceled uint32

	// 初始化中止通道和中止原因
	abortChan := make(chan struct{})
	abortReason := &atomic.Value{}

	once := &sync.Once{}

	// 带原因取消函数
	cancelWithReason := func(err error) {
		once.Do(func() {
			abortReason.Store(err.Error())
			cancel()
			rc.streamsByID.Delete(streamID)
			rc.Metrics.reportEgressStreamCount(rc.Channel, atomic.LoadUint32(&rc.streamsByID.size))
			rc.Logger.Debugf("流 %d 到 %s(%s) 已中止", streamID, nodeName, rc.endpoint)
			atomic.StoreUint32(&canceled, 1)
			close(abortChan)
		})
	}

	// 日志记录器
	logger := flogging.MustGetLogger("orderer.common.cluster.step")
	stepLogger := logger.WithOptions(zap.AddCallerSkip(1))

	// 创建流对象
	s := &Stream{
		Channel:     rc.Channel,  // 设置通道名称
		metrics:     rc.Metrics,  // 设置指标
		abortReason: abortReason, // 设置中止原因
		abortChan:   abortChan,   // 设置中止通道
		sendBuff: make(chan struct { // 创建发送缓冲通道
			request *orderer.StepRequest
			report  func(error)
		}, rc.SendBuffSize), // 设置缓冲通道大小
		commShutdown:       rc.shutdownSignal, // 设置通信关闭通道
		NodeName:           nodeName,          // 设置节点名称
		Logger:             stepLogger,        // 设置日志记录器
		ID:                 streamID,          // 设置流ID
		Endpoint:           rc.endpoint,       // 设置终端点
		Timeout:            timeout,           // 设置超时时间
		Cluster_StepClient: stream,            // 设置集群步骤客户端
		Cancel:             cancelWithReason,  // 设置取消函数
		canceled:           &canceled,         // 设置取消标志
	}

	// 证书过期检查
	s.expCheck = &certificateExpirationCheck{
		minimumExpirationWarningInterval: rc.minimumExpirationWarningInterval, // 设置最小过期警告间隔
		expirationWarningThreshold:       rc.certExpWarningThreshold,          // 设置过期警告阈值
		expiresAt:                        rc.expiresAt,                        // 设置过期时间
		endpoint:                         s.Endpoint,                          // 设置终端点
		nodeName:                         s.NodeName,                          // 设置节点名称
		alert: func(template string, args ...interface{}) { // 设置警告函数
			s.Logger.Warningf(template, args...)
		},
	}

	rc.Logger.Debugf("创建到 %s 的新流，ID 为 %d，缓冲区大小为 %d",
		rc.endpoint, streamID, cap(s.sendBuff))

	rc.streamsByID.Store(streamID, s)
	rc.Metrics.reportEgressStreamCount(rc.Channel, atomic.LoadUint32(&rc.streamsByID.size))

	// 启动服务流程
	go func() {
		rc.workerCountReporter.increment(s.metrics)
		// 服务流, 这里是发送请求的核心位置
		s.serviceStream()
		rc.workerCountReporter.decrement(s.metrics)
	}()

	return s, nil
}

// Abort aborts the contexts the RemoteContext uses, thus effectively
// causes all operations that use this RemoteContext to terminate.
func (rc *RemoteContext) Abort() {
	rc.streamsByID.Range(func(_, value interface{}) bool {
		value.(*Stream).Cancel(errAborted)
		return false
	})
}

func commonNameFromContext(ctx context.Context) string {
	cert := util.ExtractCertificateFromContext(ctx)
	if cert == nil {
		return "unidentified node"
	}
	return cert.Subject.CommonName
}

type streamsMapperReporter struct {
	size uint32
	sync.Map
}

func (smr *streamsMapperReporter) Delete(key interface{}) {
	smr.Map.Delete(key)
	atomic.AddUint32(&smr.size, ^uint32(0))
}

func (smr *streamsMapperReporter) Store(key, value interface{}) {
	smr.Map.Store(key, value)
	atomic.AddUint32(&smr.size, 1)
}

type workerCountReporter struct {
	channel     string
	workerCount uint32
}

func (wcr *workerCountReporter) increment(m *Metrics) {
	count := atomic.AddUint32(&wcr.workerCount, 1)
	m.reportWorkerCount(wcr.channel, count)
}

func (wcr *workerCountReporter) decrement(m *Metrics) {
	// ^0 flips all zeros to ones, which means
	// 2^32 - 1, and then we add this number wcr.workerCount.
	// It follows from commutativity of the unsigned integers group
	// that wcr.workerCount + 2^32 - 1 = wcr.workerCount - 1 + 2^32
	// which is just wcr.workerCount - 1.
	count := atomic.AddUint32(&wcr.workerCount, ^uint32(0))
	m.reportWorkerCount(wcr.channel, count)
}
