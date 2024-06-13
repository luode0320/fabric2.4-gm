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

// StreamServerInterceptor 函数返回一个用于拦截流式RPC的GRPC服务器拦截器。
//
// 参数：
//   - sm: StreamMetrics类型，表示流式RPC的度量指标。
//
// 返回值：
//   - grpc.StreamServerInterceptor: 表示流式RPC的服务器拦截器。
func StreamServerInterceptor(sm *StreamMetrics) grpc.StreamServerInterceptor {
	return func(svc interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		sm := sm
		service, method := serviceMethod(info.FullMethod)
		sm.RequestsReceived.With("service", service, "method", method).Add(1)

		// 创建一个包装的 ServerStream
		wrappedStream := &serverStream{
			ServerStream:     stream,
			messagesSent:     sm.MessagesSent.With("service", service, "method", method),
			messagesReceived: sm.MessagesReceived.With("service", service, "method", method),
		}

		startTime := time.Now()
		err := handler(svc, wrappedStream)
		st, _ := status.FromError(err)
		duration := time.Since(startTime)

		// 记录请求的持续时间
		sm.RequestDuration.With(
			"service", service, "method", method, "code", st.Code().String(),
		).Observe(duration.Seconds())
		// 记录已完成的请求
		sm.RequestsCompleted.With("service", service, "method", method, "code", st.Code().String()).Add(1)

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
