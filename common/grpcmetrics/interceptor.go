/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package grpcmetrics

import (
	"context"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type UnaryMetrics struct {
	RequestDuration   metrics.Histogram
	RequestsReceived  metrics.Counter
	RequestsCompleted metrics.Counter
}

// UnaryServerInterceptor 函数返回一个用于拦截一元RPC的GRPC服务器拦截器。打印请求耗时。
//
// 参数：
//   - um: UnaryMetrics类型，表示一元RPC的度量指标。
//
// 返回值：
//   - grpc.UnaryServerInterceptor: 表示一元RPC的服务器拦截器。
func UnaryServerInterceptor(um *UnaryMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 从完整的方法名中提取出服务名和方法名
		service, method := serviceMethod(info.FullMethod)
		// 使用 um.RequestsReceived 计数器，记录接收到的请求数量，并使用标签指定了服务名和方法名
		um.RequestsReceived.With("service", service, "method", method).Add(1)

		// 记录了处理请求的开始时间
		startTime := time.Now()
		// 调用了实际的 gRPC 处理程序来处理请求
		resp, err := handler(ctx, req)
		// 从错误中提取出 gRPC 状态
		st, _ := status.FromError(err)
		// 计算了处理请求所花费的时间
		duration := time.Since(startTime)

		// 使用 um.RequestDuration 指标，记录请求的持续时间，并使用标签指定了服务名、方法名和响应状态码
		um.RequestDuration.With(
			"service", service, "method", method, "code", st.Code().String(),
		).Observe(duration.Seconds())
		// 使用 um.RequestsCompleted 计数器，记录已完成的请求数量，并使用标签指定了服务名、方法名和响应状态码
		um.RequestsCompleted.With("service", service, "method", method, "code", st.Code().String()).Add(1)

		return resp, err
	}
}

type StreamMetrics struct {
	RequestDuration   metrics.Histogram
	RequestsReceived  metrics.Counter
	RequestsCompleted metrics.Counter
	MessagesSent      metrics.Counter
	MessagesReceived  metrics.Counter
}

// StreamServerInterceptor 函数构造并返回一个针对gRPC流式RPC的服务器拦截器。
// 该拦截器的主要功能是收集和记录流式RPC的度量数据，如请求数量、消息收发统计、请求处理耗时及请求完成状态等。
//
// 参数：
//   - sm (*StreamMetrics): 一个指向StreamMetrics结构体的指针，用于存储流式RPC的性能度量信息。
//
// 返回值：
//   - grpc.StreamServerInterceptor: 返回一个gRPC流式RPC服务器拦截器，该拦截器会在每个流式调用前后执行，用于度量和记录数据。
func StreamServerInterceptor(sm *StreamMetrics) grpc.StreamServerInterceptor {
	// 定义拦截器的具体逻辑
	return func(svc interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// 确保sm变量在闭包中可用（此处实际是冗余操作，因为sm已经在闭包的外部作用域中）
		sm := sm

		// 解析gRPC服务和方法名称
		service, method := serviceMethod(info.FullMethod)

		// 增加收到的请求计数
		sm.RequestsReceived.With("service", service, "method", method).Add(1)

		// 包装原始的ServerStream为一个带度量统计的serverStream
		wrappedStream := &serverStream{
			ServerStream:     stream,                                                         // 保留原始的ServerStream接口
			messagesSent:     sm.MessagesSent.With("service", service, "method", method),     // 发送消息计数器
			messagesReceived: sm.MessagesReceived.With("service", service, "method", method), // 接收消息计数器
		}

		// 记录请求开始时间
		startTime := time.Now()

		// 调用原始的流处理器处理请求
		err := handler(svc, wrappedStream)

		// 从错误中提取gRPC状态码，如果转换失败则使用默认值
		st, _ := status.FromError(err)

		// 计算请求处理的总耗时
		duration := time.Since(startTime)

		// 记录请求处理的持续时间
		sm.RequestDuration.With("service", service, "method", method, "code", st.Code().String()).Observe(duration.Seconds())

		// 记录请求完成计数，包括成功或失败的状态
		sm.RequestsCompleted.With("service", service, "method", method, "code", st.Code().String()).Add(1)

		// 返回处理结果，保持错误传递
		return err
	}
}

func serviceMethod(fullMethod string) (service, method string) {
	normalizedMethod := strings.Replace(fullMethod, ".", "_", -1)
	parts := strings.SplitN(normalizedMethod, "/", -1)
	if len(parts) != 3 {
		return "unknown", "unknown"
	}
	return parts[1], parts[2]
}

type serverStream struct {
	grpc.ServerStream
	messagesSent     metrics.Counter
	messagesReceived metrics.Counter
}

func (ss *serverStream) SendMsg(msg interface{}) error {
	ss.messagesSent.Add(1)
	return ss.ServerStream.SendMsg(msg)
}

func (ss *serverStream) RecvMsg(msg interface{}) error {
	err := ss.ServerStream.RecvMsg(msg)
	if err == nil {
		ss.messagesReceived.Add(1)
	}
	return err
}
