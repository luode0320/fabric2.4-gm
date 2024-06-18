/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package broadcast

import (
	"io"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("orderer.common.broadcast")

//go:generate counterfeiter -o mock/channel_support_registrar.go --fake-name ChannelSupportRegistrar . ChannelSupportRegistrar

// ChannelSupportRegistrar 为程序提供了一种查找通道支持的方法
type ChannelSupportRegistrar interface {
	// BroadcastChannelSupport 返回消息通道标头，无论消息是否为配置更新
	// 如果消息不是可以直接处理的消息(如CONFIG和ORDERER_TRANSACTION消息)，则为消息或错误提供通道资源
	BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, ChannelSupport, error)
}

//go:generate counterfeiter -o mock/channel_support.go --fake-name ChannelSupport . ChannelSupport

// ChannelSupport 提供支持在通道上广播所需的后台资源
type ChannelSupport interface {
	msgprocessor.Processor
	Consenter
}

// Consenter 提供通过共识发送消息的方法
type Consenter interface {
	// Order 方法接收信息，处理之后返回一个错误信息
	// 通常会完成 consensus.Chain interface
	Order(env *cb.Envelope, configSeq uint64) error

	// Configure 接受重新配置或返回指示失败原因的错误
	// 通常会完成 consensus.Chain interface
	Configure(config *cb.Envelope, configSeq uint64) error

	// WaitReady 等待同意者准备好接受新消息的块。
	// 当同意者需要暂时阻止进入消息时，这是有用的
	// 可以使用飞行中的消息。如果同意者处于错误状态，它可能返回错误。如果不需要这种阻塞行为，则consenter可以简单地返回nil。
	WaitReady() error
}

// Handler 负责处理通过 Broadcast AB gRPC 服务接收到的消息。
type Handler struct {
	// SupportRegistrar 是一个通道支持注册器，它提供了访问通道相关信息和处理器的接口，以支持消息处理过程。
	SupportRegistrar ChannelSupportRegistrar

	// Metrics 包含了一组度量指标，用于监控和统计消息处理的性能和操作情况。
	Metrics *Metrics
}

// Handle 从Broadcast流读取请求，处理请求，并将响应返回到流
func (bh *Handler) Handle(srv ab.AtomicBroadcast_BroadcastServer) error {
	// 从接收到的广播内容上下文中，解析出发送信息的远端地址
	addr := util.ExtractRemoteAddress(srv.Context())
	logger.Debugf("开始 %s 的新广播循环", addr)
	for {
		// 循环接受来自客户端gprc的请求, 如果没有会阻塞线程
		msg, err := srv.Recv()
		if err == io.EOF {
			logger.Debugf("收到来自 %s 的EOF，挂断", addr)
			return nil
		}
		if err != nil {
			logger.Warningf("从流读取时出错 %s: %s", addr, err)
			return err
		}

		// 验证单个消息，并将其放入共识队列中进行处理。
		resp := bh.ProcessMessage(msg, addr)
		err = srv.Send(resp)
		if resp.Status != cb.Status_SUCCESS {
			return err
		}

		if err != nil {
			logger.Warningf("发送到 %s 时出错: %s", addr, err)
			return err
		}
	}
}

type MetricsTracker struct {
	ValidateStartTime time.Time
	EnqueueStartTime  time.Time
	ValidateDuration  time.Duration
	ChannelID         string
	TxType            string
	Metrics           *Metrics
}

func (mt *MetricsTracker) Record(resp *ab.BroadcastResponse) {
	labels := []string{
		"status", resp.Status.String(),
		"channel", mt.ChannelID,
		"type", mt.TxType,
	}

	if mt.ValidateDuration == 0 {
		mt.EndValidate()
	}
	mt.Metrics.ValidateDuration.With(labels...).Observe(mt.ValidateDuration.Seconds())

	if mt.EnqueueStartTime != (time.Time{}) {
		enqueueDuration := time.Since(mt.EnqueueStartTime)
		mt.Metrics.EnqueueDuration.With(labels...).Observe(enqueueDuration.Seconds())
	}

	mt.Metrics.ProcessedCount.With(labels...).Add(1)
}

func (mt *MetricsTracker) BeginValidate() {
	mt.ValidateStartTime = time.Now()
}

func (mt *MetricsTracker) EndValidate() {
	mt.ValidateDuration = time.Since(mt.ValidateStartTime)
}

func (mt *MetricsTracker) BeginEnqueue() {
	mt.EnqueueStartTime = time.Now()
}

// ProcessMessage 验证单个消息，并将其放入共识队列中进行处理。
func (bh *Handler) ProcessMessage(msg *cb.Envelope, addr string) (resp *ab.BroadcastResponse) {
	// 初始化指标追踪器，初始设置为未知通道ID和交易类型
	tracker := &MetricsTracker{
		ChannelID: "unknown",
		TxType:    "unknown",
		Metrics:   bh.Metrics,
	}
	defer func() {
		// 使用匿名函数确保在函数结束时使用最终的resp值更新追踪器
		tracker.Record(resp)
	}()

	// 开始验证阶段的指标追踪
	tracker.BeginValidate()

	// 通过SupportRegistrar获取通道处理器，判断消息类型，以及是否为配置更新消息
	chdr, isConfig, processor, err := bh.SupportRegistrar.BroadcastChannelSupport(msg)
	if chdr != nil {
		// 设置追踪器中的通道ID和交易类型
		tracker.ChannelID = chdr.ChannelId
		tracker.TxType = cb.HeaderType(chdr.Type).String()
	}
	if err != nil {
		// 获取处理器失败，记录警告并返回错误响应
		logger.Warningf("[channel: %s] 无法为服务 %s 获取消息处理器: %s", tracker.ChannelID, addr, err)
		return &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}
	}

	// 处理普通消息
	if !isConfig {
		logger.Debugf("[channel: %s] 广播正在处理来自 %s 的常规消息，txid为'%s'，类型为 %s", chdr.ChannelId, addr, chdr.TxId, cb.HeaderType_name[chdr.Type])

		// 处理常规消息并获取配置序列号
		configSeq, err := processor.ProcessNormalMsg(msg)
		if err != nil {
			// 消息处理失败，记录警告并返回错误响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的常规消息广播，因为错误: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
		}
		tracker.EndValidate()

		// 开始入队指标追踪
		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			// 等待处理器就绪失败，记录警告并返回服务不可用响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的消息广播，状态为SERVICE_UNAVAILABLE: 被共识者拒绝: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
		}

		// 将消息加入共识队列
		err = processor.Order(msg, configSeq)
		if err != nil {
			// 加入共识队列失败，记录警告并返回服务不可用响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的常规消息广播，状态为SERVICE_UNAVAILABLE: 被Order拒绝: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
		}
	} else { // 处理配置更新消息
		logger.Debugf("[channel: %s] 广播正在处理来自 %s 的配置更新消息", chdr.ChannelId, addr)

		// 处理配置更新消息并获取新配置及配置序列号
		config, configSeq, err := processor.ProcessConfigUpdateMsg(msg)
		if err != nil {
			// 配置更新消息处理失败，记录警告并返回错误响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的配置消息广播，因为错误: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
		}
		tracker.EndValidate()

		// 开始入队指标追踪
		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			// 等待处理器就绪失败，记录警告并返回服务不可用响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的消息广播，状态为SERVICE_UNAVAILABLE: 被共识者拒绝: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
		}

		// 应用配置更新
		err = processor.Configure(config, configSeq)
		if err != nil {
			// 配置应用失败，记录警告并返回服务不可用响应
			logger.Warningf("[channel: %s] 拒绝来自 %s 的配置消息广播，状态为SERVICE_UNAVAILABLE: 被Configure拒绝: %s", chdr.ChannelId, addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
		}
	}

	// 成功入队消息后记录调试信息
	logger.Debugf("[channel: %s] 广播已成功入队来自 %s 的类型为 %s 的消息", chdr.ChannelId, addr, cb.HeaderType_name[chdr.Type])

	// 所有处理成功，返回成功响应
	return &ab.BroadcastResponse{Status: cb.Status_SUCCESS}
}

// ClassifyError converts an error type into a status code.
func ClassifyError(err error) cb.Status {
	switch errors.Cause(err) {
	case msgprocessor.ErrChannelDoesNotExist:
		return cb.Status_NOT_FOUND
	case msgprocessor.ErrPermissionDenied:
		return cb.Status_FORBIDDEN
	case msgprocessor.ErrMaintenanceMode:
		return cb.Status_SERVICE_UNAVAILABLE
	default:
		return cb.Status_BAD_REQUEST
	}
}
