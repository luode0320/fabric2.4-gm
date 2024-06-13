/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package grpclogging

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Leveler returns a zap level to use when logging from a grpc interceptor.
type Leveler interface {
	Level(ctx context.Context, fullMethod string) zapcore.Level
}

// PayloadLeveler gets the level to use when logging grpc message payloads.
type PayloadLeveler interface {
	PayloadLevel(ctx context.Context, fullMethod string) zapcore.Level
}

//go:generate counterfeiter -o fakes/leveler.go --fake-name Leveler . LevelerFunc

type LevelerFunc func(ctx context.Context, fullMethod string) zapcore.Level

func (l LevelerFunc) Level(ctx context.Context, fullMethod string) zapcore.Level {
	return l(ctx, fullMethod)
}

func (l LevelerFunc) PayloadLevel(ctx context.Context, fullMethod string) zapcore.Level {
	return l(ctx, fullMethod)
}

// DefaultPayloadLevel is default level to use when logging payloads
const DefaultPayloadLevel = zapcore.Level(zapcore.DebugLevel - 1)

type options struct {
	Leveler
	PayloadLeveler
}

type Option func(o *options)

func WithLeveler(l Leveler) Option {
	return func(o *options) { o.Leveler = l }
}

func WithPayloadLeveler(l PayloadLeveler) Option {
	return func(o *options) { o.PayloadLeveler = l }
}

func applyOptions(opts ...Option) *options {
	o := &options{
		Leveler:        LevelerFunc(func(context.Context, string) zapcore.Level { return zapcore.InfoLevel }),
		PayloadLeveler: LevelerFunc(func(context.Context, string) zapcore.Level { return DefaultPayloadLevel }),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// UnaryServerInterceptor 函数将需要调平人员，并应提供完整的方法信息, 返回一个用于拦截一元RPC的GRPC服务器拦截器。
//
// 参数：
//   - logger: zap.Logger类型，表示日志记录器。
//   - opts: Option类型的可选参数，表示拦截器选项。
//
// 返回值：
//   - grpc.UnaryServerInterceptor: 表示一元RPC的服务器拦截器。
func UnaryServerInterceptor(logger *zap.Logger, opts ...Option) grpc.UnaryServerInterceptor {
	o := applyOptions(opts...)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		logger := logger
		// 记录了处理请求的开始时间
		startTime := time.Now()

		// 获取上下文和方法的字段
		fields := getFields(ctx, info.FullMethod)
		// 调用 With 方法创建一个新的日志记录器
		logger = logger.With(fields...)
		ctx = WithFields(ctx, fields)

		// 创建用于记录负载的日志记录器
		payloadLogger := logger.Named("payload")
		payloadLevel := o.PayloadLevel(ctx, info.FullMethod)
		if ce := payloadLogger.Check(payloadLevel, "接收单次请求: "); ce != nil {
			ce.Write(ProtoMessage("message", req))
		}

		// 调用实际的 gRPC 处理程序处理请求
		resp, err := handler(ctx, req)

		// 记录发送响应的负载
		if ce := payloadLogger.Check(payloadLevel, "发送单次响应: "); ce != nil && err == nil {
			ce.Write(ProtoMessage("message", resp))
		}

		// 记录请求完成的日志
		if ce := logger.Check(o.Level(ctx, info.FullMethod), "单次调用已完成: "); ce != nil {
			st, _ := status.FromError(err)
			ce.Write(
				Error(err),
				zap.Stringer("grpc.code", st.Code()),
				zap.Duration("grpc.call_duration", time.Since(startTime)),
			)
		}

		return resp, err
	}
}

func StreamServerInterceptor(logger *zap.Logger, opts ...Option) grpc.StreamServerInterceptor {
	o := applyOptions(opts...)

	return func(service interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		logger := logger
		ctx := stream.Context()
		startTime := time.Now()

		fields := getFields(ctx, info.FullMethod)
		logger = logger.With(fields...)
		ctx = WithFields(ctx, fields)

		wrappedStream := &serverStream{
			ServerStream:  stream,
			context:       ctx,
			payloadLogger: logger.Named("payload"),
			payloadLevel:  o.PayloadLevel(ctx, info.FullMethod),
		}

		err := handler(service, wrappedStream)
		if ce := logger.Check(o.Level(ctx, info.FullMethod), "流调用已完成: "); ce != nil {
			st, _ := status.FromError(err)
			ce.Write(
				Error(err),
				zap.Stringer("grpc.code", st.Code()),
				zap.Duration("grpc.call_duration", time.Since(startTime)),
			)
		}
		return err
	}
}

func getFields(ctx context.Context, method string) []zapcore.Field {
	var fields []zap.Field
	if parts := strings.Split(method, "/"); len(parts) == 3 {
		fields = append(fields, zap.String("grpc.service", parts[1]), zap.String("grpc.method", parts[2]))
	}
	if deadline, ok := ctx.Deadline(); ok {
		fields = append(fields, zap.Time("grpc.request_deadline", deadline))
	}
	if p, ok := peer.FromContext(ctx); ok {
		fields = append(fields, zap.String("grpc.peer_address", p.Addr.String()))
		// TODO luode 将认证信息转换为TLS连接信息, 这里国密的tls是否可以验证
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			if len(ti.State.PeerCertificates) > 0 {
				cert := ti.State.PeerCertificates[0]
				fields = append(fields, zap.String("grpc.peer_subject", cert.Subject.String()))
			}
		}
	}
	return fields
}

type serverStream struct {
	grpc.ServerStream
	context       context.Context
	payloadLogger *zap.Logger
	payloadLevel  zapcore.Level
}

func (ss *serverStream) Context() context.Context {
	return ss.context
}

func (ss *serverStream) SendMsg(msg interface{}) error {
	if ce := ss.payloadLogger.Check(ss.payloadLevel, "sending stream message"); ce != nil {
		ce.Write(ProtoMessage("message", msg))
	}
	return ss.ServerStream.SendMsg(msg)
}

func (ss *serverStream) RecvMsg(msg interface{}) error {
	err := ss.ServerStream.RecvMsg(msg)
	if ce := ss.payloadLogger.Check(ss.payloadLevel, "received stream message"); ce != nil {
		ce.Write(ProtoMessage("message", msg))
	}
	return err
}
