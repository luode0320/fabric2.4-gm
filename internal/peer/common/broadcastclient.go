/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/pkg/errors"
)

type BroadcastClient interface {
	// Send 将数据发送到 orderer
	Send(env *cb.Envelope) error
	Close() error
}

type BroadcastGRPCClient struct {
	Client ab.AtomicBroadcast_BroadcastClient
}

// GetBroadcastClient 创建并返回一个BroadcastClient接口的实例
// 该函数通过环境变量配置初始化OrdererClient，进而获取Broadcast客户端
func GetBroadcastClient() (BroadcastClient, error) {
	// 使用从环境变量加载的配置信息创建OrdererClient实例
	oc, err := NewOrdererClientFromEnv()
	if err != nil {
		// 如果创建OrdererClient时发生错误，则直接返回错误
		return nil, err
	}
	// 通过OrdererClient获取Broadcast服务客户端
	bc, err := oc.Broadcast()
	if err != nil {
		// 如果创建Broadcast客户端失败，同样返回错误
		return nil, err
	}

	// 包装获取到的gRPC广播客户端为BroadcastGRPCClient，并作为BroadcastClient接口返回
	return &BroadcastGRPCClient{Client: bc}, nil
}

// getAck 从gRPC流中接收响应，检查并验证是否收到成功状态的确认。
// 如果接收时发生错误或状态非成功，则返回相应的错误信息。
func (s *BroadcastGRPCClient) getAck() error {
	// 尝试从排序服务接收响应消息
	msg, err := s.Client.Recv()
	if err != nil {
		// 接收时发生错误，直接返回错误
		return err
	}
	// 检查响应消息的状态是否为成功
	if msg.Status != cb.Status_SUCCESS {
		// 如果状态不是成功，构造并返回错误信息
		return errors.Errorf("收到意外的状态: %v -- %s", msg.Status, msg.Info)
	}
	// 成功则返回nil表示无错误
	return nil
}

// Send 将给定的Envelope数据发送至排序服务。
// 成功发送后等待并处理确认响应，如有错误则返回。
func (s *BroadcastGRPCClient) Send(env *cb.Envelope) error {
	// 尝试通过gRPC客户端发送Envelope
	if err := s.Client.Send(env); err != nil {
		// 发送失败，附带错误信息返回
		return errors.WithMessage(err, "无法发送至排序服务节点")
	}

	// 发送后等待并获取确认响应
	err := s.getAck()

	// 返回确认响应的错误结果
	return err
}

// Close 关闭与排序服务的gRPC发送流。
func (s *BroadcastGRPCClient) Close() error {
	// 调用gRPC客户端的CloseSend方法关闭发送流
	return s.Client.CloseSend()
}
