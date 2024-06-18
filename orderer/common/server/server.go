/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/orderer/common/broadcast"
	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

type broadcastSupport struct {
	*multichannel.Registrar
}

func (bs broadcastSupport) BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, broadcast.ChannelSupport, error) {
	return bs.Registrar.BroadcastChannelSupport(msg)
}

type deliverSupport struct {
	*multichannel.Registrar
}

func (ds deliverSupport) GetChain(chainID string) deliver.Chain {
	chain := ds.Registrar.GetChain(chainID)
	if chain == nil {
		return nil
	}
	return chain
}

// server 结构体整合了处理区块广播和交付的功能，同时也包含了调试设置和多通道注册器。
type server struct {
	// bh 是一个处理区块广播请求的处理器实例。
	bh *broadcast.Handler

	// dh 是一个负责区块交付给客户端的处理器实例。
	dh *deliver.Handler

	// debug 包含了从配置中加载的调试设置，用于控制服务器的调试行为。
	debug *localconfig.Debug

	// Registrar 是一个多通道注册器的实例，它负责管理不同通道上的操作和资源。
	*multichannel.Registrar
}

type responseSender struct {
	ab.AtomicBroadcast_DeliverServer
}

func (rs *responseSender) SendStatusResponse(status cb.Status) error {
	reply := &ab.DeliverResponse{
		Type: &ab.DeliverResponse_Status{Status: status},
	}
	return rs.Send(reply)
}

// SendBlockResponse sends block data and ignores pvtDataMap.
func (rs *responseSender) SendBlockResponse(
	block *cb.Block,
	channelID string,
	chain deliver.Chain,
	signedData *protoutil.SignedData,
) error {
	response := &ab.DeliverResponse{
		Type: &ab.DeliverResponse_Block{Block: block},
	}
	return rs.Send(response)
}

func (rs *responseSender) DataType() string {
	return "block"
}

// NewServer 根据提供的参数创建一个新的ab.AtomicBroadcastServer实例，用于处理原子广播服务请求。
// 该服务器结合了广播和交付功能，支持多通道注册和配置化的调试选项、时间窗口、双向TLS设置及证书有效期检查控制。
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func NewServer(
	r *multichannel.Registrar, // 多通道注册器，用于注册和管理不同通道的服务
	metricsProvider metrics.Provider, // 度量指标提供者，用于创建监控指标
	debug *localconfig.Debug, // 调试配置，包含日志级别等调试相关设定
	timeWindow time.Duration, // 交付服务中消息重放时间窗口的持续时间
	mutualTLS bool, // 是否启用双向TLS认证
	expirationCheckDisabled bool, // 是否禁用证书过期检查
) ab.AtomicBroadcastServer {
	// 初始化server实例
	s := &server{
		// 初始化交付处理器，传入支持注册器、时间窗口、双向TLS配置、度量指标以及证书有效期检查标志
		dh: deliver.NewHandler(
			deliverSupport{Registrar: r},
			timeWindow,
			mutualTLS,
			deliver.NewMetrics(metricsProvider),
			expirationCheckDisabled,
		),
		// 初始化广播处理器，配置支持注册器和度量指标
		bh: &broadcast.Handler{
			SupportRegistrar: broadcastSupport{Registrar: r},
			Metrics:          broadcast.NewMetrics(metricsProvider),
		},
		// 设置调试配置和注册器
		debug:     debug,
		Registrar: r,
	}

	// 返回创建好的服务器实例
	return s
}

type msgTracer struct {
	function string
	debug    *localconfig.Debug
}

func (mt *msgTracer) trace(traceDir string, msg *cb.Envelope, err error) {
	if err != nil {
		return
	}

	now := time.Now().UnixNano()
	path := fmt.Sprintf("%s%c%d_%p.%s", traceDir, os.PathSeparator, now, msg, mt.function)
	logger.Debugf("Writing %s request trace to %s", mt.function, path)
	go func() {
		pb, err := proto.Marshal(msg)
		if err != nil {
			logger.Debugf("Error marshaling trace msg for %s: %s", path, err)
			return
		}
		err = ioutil.WriteFile(path, pb, 0o660)
		if err != nil {
			logger.Debugf("Error writing trace msg for %s: %s", path, err)
		}
	}()
}

type broadcastMsgTracer struct {
	ab.AtomicBroadcast_BroadcastServer
	msgTracer
}

func (bmt *broadcastMsgTracer) Recv() (*cb.Envelope, error) {
	msg, err := bmt.AtomicBroadcast_BroadcastServer.Recv()
	if traceDir := bmt.debug.BroadcastTraceDir; traceDir != "" {
		bmt.trace(bmt.debug.BroadcastTraceDir, msg, err)
	}
	return msg, err
}

type deliverMsgTracer struct {
	deliver.Receiver
	msgTracer
}

func (dmt *deliverMsgTracer) Recv() (*cb.Envelope, error) {
	msg, err := dmt.Receiver.Recv()
	if traceDir := dmt.debug.DeliverTraceDir; traceDir != "" {
		dmt.trace(traceDir, msg, err)
	}
	return msg, err
}

// Broadcast 从客户端接收用于排序的消息流
//
// @Author: luode
// @Date: 2023/11/15
func (s *server) Broadcast(srv ab.AtomicBroadcast_BroadcastServer) error {
	logger.Debugf("启动新的广播处理程序")
	defer func() {
		if r := recover(); r != nil {
			logger.Criticalf("广播客户端触发了死机: % s\n%s", r, debug.Stack())
		}
		logger.Debugf("正在关闭广播流")
	}()
	return s.bh.Handle(&broadcastMsgTracer{
		AtomicBroadcast_BroadcastServer: srv,
		msgTracer: msgTracer{
			debug:    s.debug,
			function: "Broadcast",
		},
	})
}

// Deliver 为gRPC的Deliver服务提供处理函数，用于处理来自客户端的区块交付请求。
//
// @Author: luode
// @Date: 2023/11/15
func (s *server) Deliver(srv ab.AtomicBroadcast_DeliverServer) error {
	logger.Debugf("开始一个新的Deliver处理程序")
	defer func() {
		// 如果在处理过程中发生panic，恢复并记录严重错误日志，同时打印堆栈跟踪信息。
		if r := recover(); r != nil {
			logger.Criticalf("Deliver客户端触发恐慌: %s\n%s", r, debug.Stack())
		}
		logger.Debugf("关闭Deliver流")
	}()

	// 定义一个策略检查器，用于验证接收到的信封是否符合通道的访问策略。
	policyChecker := func(env *cb.Envelope, channelID string) error {
		// 获取指定通道的链实例。
		chain := s.GetChain(channelID)
		if chain == nil {
			// 如果找不到指定通道，返回错误。
			return errors.Errorf("通道 %s 未找到", channelID)
		}
		// 在维护模式下，要求信封满足/Channel/Orderer/Readers策略，这通常会拒绝来自只满足/Channel/Readers策略的对等节点的交付请求。
		sf := msgprocessor.NewSigFilter(policies.ChannelReaders, policies.ChannelOrdererReaders, chain)
		return sf.Apply(env)
	}

	// 创建Deliver服务实例，配置策略检查器、消息接收器（带追踪功能）以及响应发送器。
	deliverServer := &deliver.Server{
		PolicyChecker: deliver.PolicyCheckerFunc(policyChecker),
		Receiver: &deliverMsgTracer{
			Receiver: srv,
			msgTracer: msgTracer{
				debug:    s.debug,
				function: "Deliver",
			},
		},
		ResponseSender: &responseSender{
			AtomicBroadcast_DeliverServer: srv,
		},
	}

	// 调用底层的deliver.Handler来处理Deliver请求。
	return s.dh.Handle(srv.Context(), deliverServer)
}
