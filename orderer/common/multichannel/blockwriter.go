/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"sync"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	newchannelconfig "github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
)

type blockWriterSupport interface {
	identity.SignerSerializer
	blockledger.ReadWriter
	configtx.Validator
	Update(*newchannelconfig.Bundle)
	CreateBundle(channelID string, config *cb.Config) (*newchannelconfig.Bundle, error)
	SharedConfig() newchannelconfig.Orderer
}

// BlockWriter 有效地将区块链数据写入磁盘。
// 为了安全地使用BlockWriter，应仅允许一个线程与其交互。
// BlockWriter 会自行启动额外的提交goroutine并处理锁定机制，
// 以此确保这些后台goroutine能与调用它的主线程安全地协同工作。
type BlockWriter struct {
	support            blockWriterSupport // 提供区块写入所需的基础支持接口
	registrar          *Registrar         // 注册器，管理通道和资源
	lastConfigBlockNum uint64             // 记录最近一次配置更新的区块编号
	lastConfigSeq      uint64             // 记录最近一次配置更新的序列号
	lastBlock          *cb.Block          // 指向最近处理过的区块的指针
	committingBlock    sync.Mutex         // 互斥锁，确保区块提交过程中的线程安全
}

// 函数根据最后一个区块、注册器和区块写入支持者创建一个新的 BlockWriter 实例。
// 参数:
//   - lastBlock: 上一个区块的指针，用于初始化 BlockWriter。
//   - r: 注册器实例，提供通道相关的注册功能。
//   - support: blockWriterSupport 接口的实现，提供了区块写入所需的支持功能。
//
// 返回:
//   - *BlockWriter: 初始化后的新 BlockWriter 实例。
func newBlockWriter(lastBlock *cb.Block, r *Registrar, support blockWriterSupport) *BlockWriter {
	// 初始化 BlockWriter 结构体实例
	bw := &BlockWriter{
		support:       support,            // 设置支持功能
		lastConfigSeq: support.Sequence(), // 获取并设置最后配置序列号
		lastBlock:     lastBlock,          // 记录最后一个区块
		registrar:     r,                  // 设置注册器引用
	}

	// 如果这不是创世区块（区块编号不为0）
	if lastBlock.Header.Number != 0 {
		// 尝试从区块元数据中提取最后配置块的信息
		var err error
		bw.lastConfigBlockNum, err = protoutil.GetLastConfigIndexFromBlock(lastBlock)
		if err != nil {
			// 提取失败时，记录错误并终止程序
			logger.Panicf("[channel: %s] 从区块元数据中提取最后配置块时发生错误: %s", support.ChannelID(), err)
		}
	}

	// 打印调试信息，包括通道ID、当前区块编号、最后配置块编号和最后配置序列号
	logger.Debugf("[channel: %s] 为链的末端创建区块写入器 (blockNumber=%d, lastConfigBlockNum=%d, lastConfigSeq=%d)", support.ChannelID(), lastBlock.Header.Number, bw.lastConfigBlockNum, bw.lastConfigSeq)

	// 完成初始化后返回 BlockWriter 实例
	return bw
}

// CreateNextBlock 根据提供的消息内容创建一个新的区块，区块号为当前最大区块号加1。
func (bw *BlockWriter) CreateNextBlock(messages []*cb.Envelope) *cb.Block {
	// 计算上一个区块的哈希值
	previousBlockHash := protoutil.BlockHeaderHash(bw.lastBlock.Header)

	// 初始化新区块的数据部分，长度与传入消息列表相同
	data := &cb.BlockData{
		Data: make([][]byte, len(messages)),
	}

	// 遍历消息列表，将每个消息序列化并存入新区块的数据部分
	var err error
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		// 如果消息序列化失败，则通过日志记录错误并终止程序
		if err != nil {
			logger.Panicf("无法序列化信封: %s", err)
		}
	}

	// 使用上一个区块的哈希和当前区块号创建新区块的头部
	// 新区块号为上一个区块号加1
	block := protoutil.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	// 计算并设置新区块数据的哈希值
	block.Header.DataHash = protoutil.BlockDataHash(data)
	// 设置新区块的数据部分
	block.Data = data

	// 返回新创建的区块
	return block
}

// WriteConfigBlock should be invoked for blocks which contain a config transaction.
// This call will block until the new config has taken effect, then will return
// while the block is written asynchronously to disk.
func (bw *BlockWriter) WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte) {
	ctx, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		logger.Panicf("Told to write a config block, but could not get configtx: %s", err)
	}

	payload, err := protoutil.UnmarshalPayload(ctx.Payload)
	if err != nil {
		logger.Panicf("Told to write a config block, but configtx payload is invalid: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Told to write a config block, but configtx payload header is missing")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Told to write a config block with an invalid channel header: %s", err)
	}

	switch chdr.Type {
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		newChannelConfig, err := protoutil.UnmarshalEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config update embedded: %s", err)
		}
		bw.registrar.newChain(newChannelConfig)

	case int32(cb.HeaderType_CONFIG):
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config envelope encoded: %s", err)
		}

		err = bw.support.Validate(configEnvelope)
		if err != nil {
			logger.Panicf("Told to write a config block with new config, but could not apply it: %s", err)
		}

		bundle, err := bw.support.CreateBundle(chdr.ChannelId, configEnvelope.Config)
		if err != nil {
			logger.Panicf("Told to write a config block with a new config, but could not convert it to a bundle: %s", err)
		}

		oc, ok := bundle.OrdererConfig()
		if !ok {
			logger.Panicf("[channel: %s] OrdererConfig missing from bundle", bw.support.ChannelID())
		}

		currentType := bw.support.SharedConfig().ConsensusType()
		nextType := oc.ConsensusType()
		if currentType != nextType {
			encodedMetadataValue = nil
			logger.Debugf("[channel: %s] Consensus-type migration: maintenance mode, change from %s to %s, setting metadata to nil",
				bw.support.ChannelID(), currentType, nextType)
		}

		// Avoid Bundle update before the go-routine in WriteBlock() finished writing the previous block.
		// We do this (in particular) to prevent bw.support.Sequence() from advancing before the go-routine reads it.
		// In general, this prevents the StableBundle from changing before the go-routine in WriteBlock() finishes.
		bw.committingBlock.Lock()
		bw.committingBlock.Unlock()
		bw.support.Update(bundle)
	default:
		logger.Panicf("Told to write a config block with unknown header type: %v", chdr.Type)
	}

	bw.WriteBlockSync(block, encodedMetadataValue)
}

// WriteBlock 应用于包含常规交易的区块。
// 它将目标区块设为即将提交的下一个区块，并在提交前返回。
// 在返回前，它会获取提交锁，并启动一个goroutine来完成以下操作：
// 给区块添加元数据和签名，然后将区块写入账本，最后释放锁。
// 这样允许调用线程在提交阶段完成前开始组装下一个区块，提高了处理效率。
func (bw *BlockWriter) WriteBlock(block *cb.Block, encodedMetadataValue []byte) {
	// 加锁以确保在提交区块期间其他写操作不会干扰
	bw.committingBlock.Lock()
	// 设置当前区块为最后一个区块，准备进行提交流程
	bw.lastBlock = block

	// 启动一个新的goroutine来异步处理区块提交
	go func() {
		// 使用defer确保在goroutine执行完毕后解锁，即使发生panic也不例外
		defer bw.committingBlock.Unlock()
		// 调用commitBlock完成区块的元数据添加、签名及账本写入操作
		bw.commitBlock(encodedMetadataValue)
	}()
}

// WriteBlockSync is same as WriteBlock, but commits block synchronously.
// Note: WriteConfigBlock should use WriteBlockSync instead of WriteBlock.
//
//	If the block contains a transaction that remove the node from consenters,
//	the node will switch to follower and pull blocks from other nodes.
//	Suppose writing block asynchronously, the block maybe not persist to disk
//	when the follower chain starts working. The follower chain will read a block
//	before the config block, in which the node is still a consenter, so the follower
//	chain will switch to the consensus chain. That's a dead loop!
//	So WriteConfigBlock should use WriteBlockSync instead of WriteBlock.
func (bw *BlockWriter) WriteBlockSync(block *cb.Block, encodedMetadataValue []byte) {
	bw.committingBlock.Lock()
	bw.lastBlock = block

	defer bw.committingBlock.Unlock()
	bw.commitBlock(encodedMetadataValue)
}

// commitBlock 方法应当仅在持有 bw.committingBlock 锁定的情况下被调用，
// 这样做是为了确保编码的配置序列号保持同步。
func (bw *BlockWriter) commitBlock(encodedMetadataValue []byte) {
	// 在区块中添加最后一次配置更新的信息
	bw.addLastConfig(bw.lastBlock)
	// 为区块添加签名及元数据
	bw.addBlockSignature(bw.lastBlock, encodedMetadataValue)

	// 尝试将区块追加到区块链账本中
	err := bw.support.Append(bw.lastBlock)
	if err != nil {
		// 如果追加区块失败，则通过panic记录严重错误
		logger.Panicf("[通道: %s] 无法追加区块: %s", bw.support.ChannelID(), err)
	}
	// 成功追加区块后，记录调试信息
	logger.Debugf("[通道: %s] 写入区块 [%d]", bw.support.ChannelID(), bw.lastBlock.GetHeader().Number)
}

// 为区块添加签名元数据。
// 这包括构造签名头、签名值（包含最后配置索引和共识者元数据），并计算签名，
// 然后将签名元数据加入到区块的元数据中。
func (bw *BlockWriter) addBlockSignature(block *cb.Block, consenterMetadata []byte) {
	// 构造签名头，使用当前支持的签名者信息生成SignatureHeader
	blockSignature := &cb.MetadataSignature{
		SignatureHeader: protoutil.MarshalOrPanic(protoutil.NewSignatureHeaderOrPanic(bw.support)),
	}

	// 构造签名值部分，包含最后配置索引和共识者元数据
	blockSignatureValue := protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
		LastConfig:        &cb.LastConfig{Index: bw.lastConfigBlockNum},                     // 最后配置块的索引
		ConsenterMetadata: protoutil.MarshalOrPanic(&cb.Metadata{Value: consenterMetadata}), // 共识者的附加元数据
	})

	// 计算签名，对签名值、签名头及区块头进行拼接后签名
	blockSignature.Signature = protoutil.SignOrPanic(
		bw.support,
		util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, protoutil.BlockHeaderBytes(block.Header)),
	)

	// 将签名元数据结构体序列化，并加入到区块的SIGNATURES元数据索引中
	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: blockSignatureValue,
		Signatures: []*cb.MetadataSignature{
			blockSignature,
		},
	})
}

// 用于在区块元数据中添加最新的配置序列信息。
// 当检测到配置序列号(configSeq)有更新时，会更新lastConfigBlockNum和lastConfigSeq的值，
// 并在当前区块的元数据中设置LAST_CONFIG以记录最新的配置块编号。
func (bw *BlockWriter) addLastConfig(block *cb.Block) {
	// 获取当前配置序列号
	configSeq := bw.support.Sequence()

	// 检查并更新最后的配置序列号和对应的区块号
	if configSeq > bw.lastConfigSeq {
		logger.Debugf("[通道: %s] 检测到lastConfigSeq从%d变换到%d，将lastConfigBlockNum从%d更新为%d",
			bw.support.ChannelID(), bw.lastConfigSeq, configSeq, bw.lastConfigBlockNum, block.Header.Number)
		bw.lastConfigBlockNum = block.Header.Number
		bw.lastConfigSeq = configSeq
	}

	// 序列化最新的LastConfig信息
	lastConfigValue := protoutil.MarshalOrPanic(&cb.LastConfig{Index: bw.lastConfigBlockNum})
	logger.Debugf("[通道: %s] 即将写入区块，设置其LAST_CONFIG为%d", bw.support.ChannelID(), bw.lastConfigBlockNum)

	// 在区块元数据的LAST_CONFIG索引位置设置最新的配置信息
	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: lastConfigValue,
	})
}
