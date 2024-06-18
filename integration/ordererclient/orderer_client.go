/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ordererclient

import (
	"context"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/integration/nwo"
	"github.com/pkg/errors"
)

// Broadcast 向指定排序服务发送给定的Envelope至其广播API。
func Broadcast(n *nwo.Network, o *nwo.Orderer, env *common.Envelope) (*orderer.BroadcastResponse, error) {
	// 建立与指定排序服务的连接
	conn := n.OrdererClientConn(o)
	defer conn.Close() // 确保连接在函数结束时关闭

	// 创建一个原子广播客户端，用于向排序服务发送消息
	broadcaster, err := orderer.NewAtomicBroadcastClient(conn).Broadcast(context.Background())
	if err != nil {
		// 如果创建广播客户端时出现错误，则返回错误
		return nil, err
	}

	// 尝试发送封装的消息
	err = broadcaster.Send(env)
	if err != nil {
		// 发送失败则返回错误
		return nil, err
	}

	// 接收排序服务的响应
	resp, err := broadcaster.Recv()
	if err != nil {
		// 如果接收响应时出错，返回错误
		return nil, err
	}

	// 成功接收响应后返回
	return resp, nil
}

// Deliver 将给定的Envelope发送到指定排序服务的Deliver API，用于获取新区块或交易信息。
func Deliver(n *nwo.Network, o *nwo.Orderer, env *common.Envelope) (*common.Block, error) {
	// 建立与指定排序服务的gRPC连接
	conn := n.OrdererClientConn(o)
	defer conn.Close() // 在函数执行完毕后确保连接关闭

	// 创建一个用于Deliver服务的客户端实例
	deliverer, err := orderer.NewAtomicBroadcastClient(conn).Deliver(context.Background())
	if err != nil {
		// 创建客户端时若发生错误则立即返回
		return nil, err
	}

	// 尝试发送Envelope请求到排序服务的Deliver API
	err = deliverer.Send(env)
	if err != nil {
		// 发送请求失败则返回错误
		return nil, err
	}

	// 从Deliver流接收响应
	resp, err := deliverer.Recv()
	if err != nil {
		// 若接收响应时出错，则返回错误
		return nil, err
	}

	// 解析响应中的区块数据
	blk := resp.GetBlock()
	if blk == nil {
		// 如果响应中未找到区块，则返回特定错误
		return nil, errors.Errorf("未找到区块")
	}

	// 成功获取区块，返回区块数据
	return blk, nil
}
