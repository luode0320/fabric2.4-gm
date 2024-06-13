/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"time"

	"github.com/hyperledger/fabric/common/metrics"
)

// stats 是一个统计信息的结构体。
type stats struct {
	blockchainHeight       metrics.Gauge     // 区块链高度
	blockstorageCommitTime metrics.Histogram // 区块存储提交时间
}

func newStats(metricsProvider metrics.Provider) *stats {
	stats := &stats{}
	stats.blockchainHeight = metricsProvider.NewGauge(blockchainHeightOpts)
	stats.blockstorageCommitTime = metricsProvider.NewHistogram(blockstorageCommitTimeOpts)
	return stats
}

// ledgerStats 定义了区块链统计信息，适用于排序服务和节点。
type ledgerStats struct {
	stats    *stats // 统计信息
	ledgerid string // 区块链ID
}

// 定义了区块链统计信息，适用于排序服务和节点
func (s *stats) ledgerStats(ledgerid string) *ledgerStats {
	return &ledgerStats{
		s, ledgerid,
	}
}

// updateBlockchainHeight 更新区块链高度统计信息。
// 方法接收者：s（ledgerStats类型的指针）
// 输入参数：
//   - height：区块链高度。
//
// 返回值：无
func (s *ledgerStats) updateBlockchainHeight(height uint64) {
	// 将uint64类型的高度转换为float64类型，以保证数字的精度，范围为0到9,007,199,254,740,992 (1<<53)
	// 由于我们不期望在短期内出现这样规模的区块链，所以目前使用这种转换方式。
	s.stats.blockchainHeight.With("channel", s.ledgerid).Set(float64(height))
}

func (s *ledgerStats) updateBlockstorageCommitTime(timeTaken time.Duration) {
	s.stats.blockstorageCommitTime.With("channel", s.ledgerid).Observe(timeTaken.Seconds())
}

var (
	blockchainHeightOpts = metrics.GaugeOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "blockchain_height",
		Help:         "Height of the chain in blocks.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
	}

	blockstorageCommitTimeOpts = metrics.HistogramOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "blockstorage_commit_time",
		Help:         "Time taken in seconds for committing the block to storage.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}
)
