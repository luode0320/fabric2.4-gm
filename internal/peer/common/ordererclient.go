/*
Copyright IBM Corp. 2016-2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package common

import (
	"context"
	"crypto/tls"

	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/pkg/errors"
)

// OrdererClient represents a client for communicating with an ordering
// service
type OrdererClient struct {
	*CommonClient
}

// NewOrdererClientFromEnv 方法从全局的Viper实例中创建一个 OrdererClient 实例。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：无
// 返回值：
//   - *OrdererClient：表示Orderer客户端的指针。
//   - error：如果从环境中创建OrdererClient时出错，则返回错误。
func NewOrdererClientFromEnv() (*OrdererClient, error) {
	// 从全局Viper实例中获取 Orderer 的地址和客户端配置
	address, clientConfig, err := configFromEnv("orderer")
	if err != nil {
		return nil, errors.WithMessage(err, "无法加载 OrdererClient 的配置")
	}

	// 使用地址和客户端配置创建CommonClient实例
	cc, err := newCommonClient(address, clientConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "无法从配置创建 OrdererClient")
	}

	// 创建OrdererClient实例，并将CommonClient实例赋值给其CommonClient字段
	return &OrdererClient{CommonClient: cc}, nil
}

// Broadcast 返回 AtomicBroadcast 服务的原子广播客户端
// 该函数建立与排序服务的连接，用于广播交易至网络。
// 成功时，返回一个可用于发送交易的BroadcastClient；失败则返回错误。
func (oc *OrdererClient) Broadcast() (ab.AtomicBroadcast_BroadcastClient, error) {
	conn, err := oc.CommonClient.clientConfig.Dial(oc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "orderer 客户端无法连接到地址 %s", oc.address)
	}
	// todo：在返回BroadcastClient前，考虑是否需要进一步处理可能的错误情况
	return ab.NewAtomicBroadcastClient(conn).Broadcast(context.TODO())
}

// Deliver 返回 AtomicBroadcast 服务的传递客户端
// 该函数同样建立与排序服务的连接，但目的是接收已排序的区块数据流。
// 成功建立连接后，返回一个DeliverClient用于拉取新区块；如果连接失败，则返回错误。
func (oc *OrdererClient) Deliver() (ab.AtomicBroadcast_DeliverClient, error) {
	conn, err := oc.CommonClient.clientConfig.Dial(oc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "orderer 客户端连接到 %s 失败", oc.address)
	}
	// todo：在返回DeliverClient之前，评估是否需要实现错误处理逻辑
	return ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
}

// Certificate 返回TLS客户端证书 (如果可用)
func (oc *OrdererClient) Certificate() tls.Certificate {
	return oc.CommonClient.Certificate()
}
