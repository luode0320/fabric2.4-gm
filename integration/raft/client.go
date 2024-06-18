/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package raft

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/integration/nwo"
	"github.com/hyperledger/fabric/integration/ordererclient"
	"github.com/hyperledger/fabric/protoutil"
	. "github.com/onsi/gomega"
)

// FetchBlock 从指定排序服务中根据序列号和通道名称获取区块。
func FetchBlock(n *nwo.Network, o *nwo.Orderer, seq uint64, channel string) *common.Block {
	// 创建一个用于Deliver请求的Envelope，指定区块序列号和通道
	denv := CreateDeliverEnvelope(n, o, seq, channel)
	// 断言确保Envelope不为空
	Expect(denv).NotTo(BeNil())

	// 声明一个变量用于存储获取到的区块
	var blk *common.Block

	// 使用Eventually函数等待直到成功获取区块，或直至超时
	Eventually(func() error {
		// 尝试通过Deliver调用获取区块
		var err error
		blk, err = ordererclient.Deliver(n, o, denv)
		// 返回此次尝试的错误状态
		return err
		// 设置超时时间为网络配置的EventuallyTimeout
	}, n.EventuallyTimeout).ShouldNot(HaveOccurred()) // 确保在超时时间内没有发生错误

	// 返回获取到的区块
	return blk
}

// CreateBroadcastEnvelope 为给定的实体（如Peer或Orderer的管理员）在指定通道上创建一个包含数据的签名广播Envelope。
func CreateBroadcastEnvelope(n *nwo.Network, entity interface{}, channel string, data []byte) *common.Envelope {
	// 根据传入实体的类型（Peer或Orderer），确定用于签署Envelope的身份
	var signer *nwo.SigningIdentity
	switch creator := entity.(type) {
	case *nwo.Peer:
		// 如果实体是Peer，则获取该Peer的管理员签名者
		signer = n.PeerUserSigner(creator, "Admin")
	case *nwo.Orderer:
		// 如果实体是Orderer，则获取该Orderer的管理员签名者
		signer = n.OrdererUserSigner(creator, "Admin")
	}
	// 确保签名者不为空
	Expect(signer).NotTo(BeNil())

	// 使用protoutil库创建一个已签名的Envelope
	// 参数包括：消息类型、通道名、签名者、被封装的数据、txID的随机数部分、epoch
	env, err := protoutil.CreateSignedEnvelope(
		common.HeaderType_MESSAGE,
		channel,
		signer,
		// 注意：此处直接将data封装进另一个Envelope作为Payload是不太常见的用法，通常Payload会是更高层次的协议消息
		&common.Envelope{Payload: data},
		0,
		0,
	)
	// 确保创建Envelope过程中无错误
	Expect(err).NotTo(HaveOccurred())

	// 成功创建并返回Envelope
	return env
}

// CreateDeliverEnvelope 用于创建一个特定于区块的deliver envelope，以便从指定通道中查找指定编号的区块。
func CreateDeliverEnvelope(n *nwo.Network, o *nwo.Orderer, blkNum uint64, channel string) *common.Envelope {
	// 定义查询起始和结束位置为指定区块编号
	specified := &orderer.SeekPosition{
		Type: &orderer.SeekPosition_Specified{
			Specified: &orderer.SeekSpecified{Number: blkNum},
		},
	}

	// 使用protoutil创建一个签名的Envelope，类型为DELIVER_SEEK_INFO，用于向Orderer发起区块查询请求
	// 参数包括：消息头类型、通道ID、签名者（此处为Orderer的管理员）、SeekInfo（包含查询起止位置及行为）、TxID的随机数部分、epoch
	env, err := protoutil.CreateSignedEnvelope(
		common.HeaderType_DELIVER_SEEK_INFO,
		channel,
		n.OrdererUserSigner(o, "Admin"), // 使用Orderer的管理员身份进行签名
		&orderer.SeekInfo{
			Start:    specified,                          // 查询起始位置为指定区块
			Stop:     specified,                          // 查询结束位置也为指定区块，意味着仅查询该单一区块
			Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY, // 查询行为设置为等待直到目标区块可提供
		},
		0,
		0,
	)
	// 使用Gomega的Expect进行错误检查，确保Envelope创建过程中无异常
	Expect(err).NotTo(HaveOccurred())

	// 成功创建后返回Envelope实例
	return env
}
