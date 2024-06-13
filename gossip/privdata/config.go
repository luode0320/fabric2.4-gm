/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

const (
	reconcileSleepIntervalDefault         = time.Minute
	reconcileBatchSizeDefault             = 10
	implicitCollectionMaxPeerCountDefault = 1
)

// PrivdataConfig 是定义Gossip Privdata配置的结构。
type PrivdataConfig struct {
	// ReconcileSleepInterval 确定协调程序从交互结束到下一次协调迭代开始的睡眠时间。
	ReconcileSleepInterval time.Duration
	// ReconcileBatchSize 确定将在单次迭代中协调的缺失专用数据的最大批处理大小。
	ReconcileBatchSize int
	// ReconciliationEnabled 是指示是否启用私有数据协调的标志。
	ReconciliationEnabled bool
	// ImplicitCollectionDisseminationPolicy 指定对等方自己的隐式集合的传播策略。
	ImplicitCollDisseminationPolicy ImplicitCollectionDisseminationPolicy
}

// ImplicitCollectionDisseminationPolicy 指定对等方自己的隐式集合的传播策略。
// 不适用于其他组织的隐式集合的私有数据。
type ImplicitCollectionDisseminationPolicy struct {
	// RequiredPeerCount 定义每个认可对等方必须成功的合格对等方的最小数量
	// 为自己的隐式集合传播私有数据。默认值为0。
	RequiredPeerCount int
	// MaxPeerCount 定义每个认可对等方将尝试的合格对等方的最大数量
	// 为自己的隐式集合传播私有数据。默认值为1。
	MaxPeerCount int
}

// GlobalConfig 从viper获取一组配置，构建并返回配置结构。
func GlobalConfig() *PrivdataConfig {
	c := &PrivdataConfig{}
	c.loadPrivDataConfig()
	return c
}

// loadPrivDataConfig 加载私有数据配置。
func (c *PrivdataConfig) loadPrivDataConfig() {
	// 加载 reconcileSleepInterval 配置
	c.ReconcileSleepInterval = viper.GetDuration("peer.gossip.pvtData.reconcileSleepInterval")
	if c.ReconcileSleepInterval == 0 {
		logger.Warning("配置 peer.gossip.pvtData.reconcileSleepInterval 未设置，默认为", reconcileSleepIntervalDefault)
		c.ReconcileSleepInterval = reconcileSleepIntervalDefault
	}

	// 加载 reconcileBatchSize 配置
	c.ReconcileBatchSize = viper.GetInt("peer.gossip.pvtData.reconcileBatchSize")
	if c.ReconcileBatchSize == 0 {
		logger.Warning("配置 peer.gossip.pvtData.reconcileBatchSize 未设置，默认为", reconcileBatchSizeDefault)
		c.ReconcileBatchSize = reconcileBatchSizeDefault
	}

	// 加载 reconciliationEnabled 配置
	c.ReconciliationEnabled = viper.GetBool("peer.gossip.pvtData.reconciliationEnabled")

	// 加载 implicitCollectionDisseminationPolicy 配置
	requiredPeerCount := viper.GetInt("peer.gossip.pvtData.implicitCollectionDisseminationPolicy.requiredPeerCount")

	maxPeerCount := implicitCollectionMaxPeerCountDefault
	if viper.Get("peer.gossip.pvtData.implicitCollectionDisseminationPolicy.maxPeerCount") != nil {
		// 允许将 maxPeerCount 覆盖为 0，这将有效地禁用节点上的传播
		maxPeerCount = viper.GetInt("peer.gossip.pvtData.implicitCollectionDisseminationPolicy.maxPeerCount")
	}

	// 检查 requiredPeerCount 和 maxPeerCount 的有效性
	if requiredPeerCount < 0 {
		panic(fmt.Sprintf("peer.gossip.pvtData.implicitCollectionDisseminationPolicy.requiredPeerCount (%d) 不能小于零",
			requiredPeerCount))
	}
	if maxPeerCount < requiredPeerCount {
		panic(fmt.Sprintf("peer.gossip.pvtData.implicitCollectionDisseminationPolicy.maxPeerCount (%d) 不能小于 requiredPeerCount (%d)",
			maxPeerCount, requiredPeerCount))
	}

	// 设置 ImplicitCollDisseminationPolicy 配置
	c.ImplicitCollDisseminationPolicy.RequiredPeerCount = requiredPeerCount
	c.ImplicitCollDisseminationPolicy.MaxPeerCount = maxPeerCount
}
