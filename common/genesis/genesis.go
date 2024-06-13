/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package genesis

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	msgVersion = int32(1)

	// 这些值对于genesis块是固定的。
	epoch = 0
)

// Factory 促进创世纪块的创建。
type Factory interface {
	// Block 返回给定通道ID的创世块。
	Block(channelID string) *cb.Block
}

type factory struct {
	channelGroup *cb.ConfigGroup
}

// NewFactoryImpl 创建一个新工厂。
func NewFactoryImpl(channelGroup *cb.ConfigGroup) Factory {
	return &factory{channelGroup: channelGroup}
}

// Block 方法用于构建并返回给定通道ID的创世区块。
// 方法接收者：factory 结构体的指针。
//
// 输入参数：
//   - channelID：通道的ID，表示要构建创世区块的通道。
//
// 返回值：
//   - *cb.Block：构建的创世区块对象，包含了给定通道ID的相关信息。
func (f *factory) Block(channelID string) *cb.Block {
	// 创建通道头部
	payloadChannelHeader := protoutil.MakeChannelHeader(cb.HeaderType_CONFIG, msgVersion, channelID, epoch)
	// 创建签名头部
	payloadSignatureHeader := protoutil.MakeSignatureHeader(nil, protoutil.CreateNonceOrPanic())
	// 设置交易ID。根据提供的签名头生成事务id
	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)
	// 创建有效载荷头部
	payloadHeader := protoutil.MakePayloadHeader(payloadChannelHeader, payloadSignatureHeader)
	// 创建有效载荷
	payload := &cb.Payload{Header: payloadHeader, Data: protoutil.MarshalOrPanic(&cb.ConfigEnvelope{Config: &cb.Config{ChannelGroup: f.channelGroup}})}
	// 创建信封(区块数据)
	envelope := &cb.Envelope{Payload: protoutil.MarshalOrPanic(payload), Signature: nil}

	// 创建区块, 构造一个区块号为0, 上 一个区块hash为nil的新区块
	block := protoutil.NewBlock(0, nil)
	// 设置区块数据
	block.Data = &cb.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(envelope)}}
	// 设置区块数据哈希
	block.Header.DataHash = protoutil.BlockDataHash(block.Data)
	// 设置区块元数据中的最后配置
	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: protoutil.MarshalOrPanic(&cb.LastConfig{Index: 0}),
	})
	// 设置区块元数据中的签名
	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
			LastConfig: &cb.LastConfig{Index: 0},
		}),
	})
	return block
}
