/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver 定义了一个有序广播消息的接收器接口。
type Receiver interface {
	// Ordered 应在消息被排序时依次调用, 这个并不是对交易排序, 只是对交易的大小切割为多个区块高度。
	// `msg`接收的消息本身。
	// `messageBatches`中的每个批次都将被打包进一个区块中。
	// `pending`表示接收器中是否仍有待处理的消息。
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut 切割当前批次并开始一个新的批次，返回被切割的批次内容。
	Cut() []*cb.Envelope
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope
	pendingBatchSizeBytes uint32

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, metrics *Metrics) Receiver {
	return &receiver{
		sharedConfigFetcher: sharedConfigFetcher,
		Metrics:             metrics,
		ChannelID:           channelID,
	}
}

// Ordered 方法应该随着消息的有序到达而被顺序调用。
//
// messageBatches 长度及 pending 参数可能的情况如下：
//
// - messageBatches 长度: 0, pending: false
//   - 不可能发生，因为我们刚接收到一条消息。
//
// - messageBatches 长度: 0, pending: true
//   - 没有切割批次，且有待处理的消息。
//
// - messageBatches 长度: 1, pending: false
//   - 消息计数达到了 BatchSize.MaxMessageCount 的限制。
//
// - messageBatches 长度: 1, pending: true
//   - 当前消息将导致待处理批次的字节大小超过 BatchSize.PreferredMaxBytes。
//
// - messageBatches 长度: 2, pending: false
//   - 当前消息的字节大小超过了 BatchSize.PreferredMaxBytes，因此被单独放在一个批次中。
//
// - messageBatches 长度: 2, pending: true
//   - 不可能发生。
//
// 注意：messageBatches 的长度不能大于 2。
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	if len(r.pendingBatch) == 0 {
		// 开始新批次时记录时间
		r.PendingBatchStartTime = time.Now()
	}

	// 获取排序服务配置
	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("无法检索排序服务配置以查询批次参数，无法进行区块切割")
	}

	// 这个batchSize就是通道配置中的batchSize
	batchSize := ordererConfig.BatchSize()

	// 计算当前消息的字节大小
	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("当前消息大小为 %v 字节，超过了首选最大批次大小 %v 字节，将被单独隔离。", messageSizeBytes, batchSize.PreferredMaxBytes)

		// 如果有待处理的消息，则先切割现有批次
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// 创建只包含当前消息的新批次
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// 记录该批次填充时间为0
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	// 检查消息是否会超出当前批次的预设最大字节大小
	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes
	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("当前消息大小为 %v 字节，将使待处理批次大小 %v 字节溢出。", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("如果添加当前消息，待处理批次将会溢出，现在进行批次切割。")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now() // 重置批次开始时间
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("将消息加入到批次中")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	// 检查消息数量是否达到最大限制
	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("达到批次消息数量上限，进行批次切割")
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut 方法返回当前批次并开始一个新的批次。
func (r *receiver) Cut() []*cb.Envelope {
	if r.pendingBatch != nil {
		// 记录当前批次填充耗时（从开始构建批次到切割的时间）
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	}
	// 重置批次开始时间
	r.PendingBatchStartTime = time.Time{}
	// 保存当前批次并清空，准备下一个批次
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0 // 重置批次字节大小计数
	return batch                // 返回已切割的批次
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
