/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
)

// blockCreator 结构体用于存储最新区块的哈希和区块编号，
// 以便基于此信息创建下一个区块。
type blockCreator struct {
	// hash 存储最新区块的哈希值，这是一个字节切片类型。
	// 它作为新块创建时计算新区块头哈希的依据。
	hash []byte

	// number 表示最新区块的编号（区块高度），是一个无符号64位整数。
	// 每创建一个新块，该值会递增，用以维持区块链的顺序。
	number uint64

	// logger 是一个 Fabric 日志记录器实例，
	// 用于在创建区块过程中输出日志信息，便于调试和监控。
	logger *flogging.FabricLogger
}

// 结构体中的 createNextBlock 方法用于根据给定的一组交易信封（Envelope）创建下一个区块链块。
func (bc *blockCreator) createNextBlock(envs []*cb.Envelope) *cb.Block {
	// 初始化一个新的 BlockData 结构，用于存放交易数据。
	data := &cb.BlockData{
		// 分配与交易信封数量相等的字节切片，用于存储序列化后的交易数据。
		Data: make([][]byte, len(envs)),
	}

	// 遍历所有交易信封
	var err error
	for i, env := range envs {
		// 将交易信封序列化为字节流，并存入 BlockData 的 Data 字段对应位置。
		data.Data[i], err = proto.Marshal(env)
		if err != nil {
			// 若序列化过程中发生错误，则通过 logger 记录严重错误并终止程序。
			bc.logger.Panicf("序列化信封时发生错误: %s", err)
		}
	}

	// 增加区块编号（高度）
	bc.number++

	// 使用当前区块编号和前一区块的哈希值创建新的区块头。
	block := protoutil.NewBlock(bc.number, bc.hash)
	// 计算 BlockData 的哈希值，并将其设置为新区块头的 DataHash 字段。
	block.Header.DataHash = protoutil.BlockDataHash(data)
	// 将准备好的 BlockData 赋值给新区块的数据部分。
	block.Data = data

	// 计算新生成区块头的哈希值，并更新 bc.hash 为新的区块头哈希，为创建下一个区块做准备。
	bc.hash = protoutil.BlockHeaderHash(block.Header)
	// 返回创建好的新区块。
	return block
}
