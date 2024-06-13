/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package accesscontrol

import (
	"fmt"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	"google.golang.org/grpc"
)

// interceptor 结构体定义了一个链码拦截器。
type interceptor struct {
	next pb.ChaincodeSupportServer // 下一个 ChaincodeSupportServer 链码连接器实例
	auth authorization             // 授权对象
}

// ChaincodeStream 定义了一个用于 发送和接收 链码消息的 gRPC 流接口。
type ChaincodeStream interface {
	// Send 发送一个链码消息
	Send(*pb.ChaincodeMessage) error
	// Recv 接收一个链码消息
	Recv() (*pb.ChaincodeMessage, error)
}

// 授权处理器
type authorization func(message *pb.ChaincodeMessage, stream grpc.ServerStream) error

// newInterceptor 函数用于创建一个新的拦截器。
// 参数：
//   - srv pb.ChaincodeSupportServer：原始的 ChaincodeSupportServer 实例。
//   - auth authorization：授权对象，用于验证和处理授权逻辑。
//
// 返回值：
//   - pb.ChaincodeSupportServer：创建的拦截器实例。
func newInterceptor(srv pb.ChaincodeSupportServer, auth authorization) pb.ChaincodeSupportServer {
	return &interceptor{
		next: srv,
		auth: auth,
	}
}

// Register 方法使拦截器实现 ChaincodeSupportServer 接口。
// 参数：
//   - stream pb.ChaincodeSupport_RegisterServer：用于注册的 gRPC 流。
//
// 返回值：
//   - error：如果注册过程中出现错误，则返回错误。
func (i *interceptor) Register(stream pb.ChaincodeSupport_RegisterServer) error {
	// 创建一个被拦截的流对象
	is := &interceptedStream{
		incMessages:  make(chan *pb.ChaincodeMessage, 1),
		stream:       stream,
		ServerStream: stream,
		auth:         i.auth,
	}

	// 接收第一个链码消息
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("Recv() 错误: %v, 正在关闭连接", err)
	}

	// 进行授权验证处理
	err = is.auth(msg, is.ServerStream)
	if err != nil {
		return err
	}

	// 将接收到的消息放入消息通道
	is.incMessages <- msg
	close(is.incMessages)

	// 调用下一个 ChaincodeSupportServer 的 Register 方法
	return i.next.Register(is)
}

// interceptedStream 结构体定义了一个被拦截的流。
type interceptedStream struct {
	incMessages       chan *pb.ChaincodeMessage // 接收到的链码消息通道
	stream            ChaincodeStream           // ChaincodeStream 实例
	grpc.ServerStream                           // gRPC ServerStream 实例
	auth              authorization             // 授权对象
}

// Send 方法用于发送一个链码消息。
// 参数：
//   - msg *pb.ChaincodeMessage：要发送的链码消息。
//
// 返回值：
//   - error：如果发送过程中出现错误，则返回错误。
func (is *interceptedStream) Send(msg *pb.ChaincodeMessage) error {
	return is.stream.Send(msg)
}

// Recv 方法用于接收一个链码消息。
// 返回值：
//   - *pb.ChaincodeMessage：接收到的链码消息。
//   - error：如果接收过程中出现错误，则返回错误。
func (is *interceptedStream) Recv() (*pb.ChaincodeMessage, error) {
	msg, ok := <-is.incMessages
	if !ok {
		// 如果消息通道已关闭，则调用底层的 ChaincodeStream 实例的 Recv 方法接收消息
		return is.stream.Recv()
	}
	return msg, nil
}
