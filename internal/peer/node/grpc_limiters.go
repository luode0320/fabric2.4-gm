/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"context"
	"strings"

	"github.com/hyperledger/fabric/common/semaphore"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// initGrpcSemaphores 函数根据配置信息初始化并返回一个包含GRPC服务的信号量映射。
//
// 参数：
//   - config: peer.Config类型，表示Peer节点的配置信息。
//
// 返回值：
//   - map[string]semaphore.Semaphore: 表示GRPC服务的信号量
func initGrpcSemaphores(config *peer.Config) map[string]semaphore.Semaphore {
	semaphores := make(map[string]semaphore.Semaphore)
	endorserConcurrency := config.LimitsConcurrencyEndorserService
	deliverConcurrency := config.LimitsConcurrencyDeliverService
	gatewayConcurrency := config.LimitsConcurrencyGatewayService

	// 目前对代言人服务、投递服务和网关服务进行并发限制。
	// 这些服务在fabric-protos和fabric-protos-go (从fabric-protos生成) 中定义。
	// 下面的服务名称必须与其定义匹配。
	if endorserConcurrency != 0 {
		logger.Infof("背书节点的并发限制 peer.limits.concurrency.endorserService 为 %d", endorserConcurrency)
		semaphores["/protos.Endorser"] = semaphore.New(endorserConcurrency)
	}
	if deliverConcurrency != 0 {
		logger.Infof("提交节点的并发限制 peer.limits.concurrency.deliverService 为 %d", deliverConcurrency)
		semaphores["/protos.Deliver"] = semaphore.New(deliverConcurrency)
	}
	if gatewayConcurrency != 0 {
		logger.Infof("Gateway网关的并发限制 peer.limits.concurrency.gatewayService 为 %d", gatewayConcurrency)
		semaphores["/gateway.Gateway"] = semaphore.New(gatewayConcurrency)
	}

	return semaphores
}

// unaryGrpcLimiter 返回一个gRPC的一元拦截器，用于限制并发请求。
// 方法接收者：无
// 输入参数：
//   - semaphores：服务名称到信号量的映射。
//
// 返回值：
//   - grpc.UnaryServerInterceptor：gRPC的一元拦截器。
func unaryGrpcLimiter(semaphores map[string]semaphore.Semaphore) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 获取服务名称
		serviceName := getServiceName(info.FullMethod)

		// 从信号量映射中获取对应的信号量
		sema, ok := semaphores[serviceName]
		if !ok {
			// 如果找不到对应的信号量，则直接调用处理函数
			return handler(ctx, req)
		}

		// 尝试获取信号量，如果获取失败，则表示超过并发限制
		if !sema.TryAcquire() {
			logger.Errorf("%s 的请求太多, 已超过并发限制 (%d)", serviceName, cap(sema))
			return nil, errors.Errorf("%s 的请求太多, 已超过并发限制 (%d)", serviceName, cap(sema))
		}

		// 在函数执行完毕后释放信号量
		defer sema.Release()

		// 调用处理函数
		return handler(ctx, req)
	}
}

// streamGrpcLimiter 返回一个gRPC的流拦截器，用于限制并发请求。
// 方法接收者：无
// 输入参数：
//   - semaphores：服务名称到信号量的映射。
//
// 返回值：
//   - grpc.StreamServerInterceptor：gRPC的流拦截器。
func streamGrpcLimiter(semaphores map[string]semaphore.Semaphore) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// 获取服务名称
		serviceName := getServiceName(info.FullMethod)

		// 从信号量映射中获取对应的信号量
		sema, ok := semaphores[serviceName]
		if !ok {
			// 如果找不到对应的信号量，则直接调用处理函数
			return handler(srv, ss)
		}

		// 尝试获取信号量，如果获取失败，则表示超过并发限制
		if !sema.TryAcquire() {
			logger.Errorf("%s 的请求太多，已超过并发限制 (%d)", serviceName, cap(sema))
			return errors.Errorf("%s 的请求太多，已超过并发限制 (%d)", serviceName, cap(sema))
		}

		// 在函数执行完毕后释放信号量
		defer sema.Release()

		// 调用处理函数
		return handler(srv, ss)
	}
}

// getServiceName 返回方法名称中的服务名称。
// 方法接收者：无
// 输入参数：
//   - methodName：方法名称。
//
// 返回值：
//   - string：服务名称。
func getServiceName(methodName string) string {
	// 查找最后一个斜杠的索引
	index := strings.LastIndex(methodName, "/")

	// 返回斜杠之前的部分，即服务名称
	return methodName[:index]
}
