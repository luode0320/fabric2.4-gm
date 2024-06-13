/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package service

import (
	"time"

	"github.com/hyperledger/fabric/gossip/election"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/spf13/viper"
)

const (
	btlPullMarginDefault           = 10
	transientBlockRetentionDefault = 1000
)

// ServiceConfig 是gossip服务的配置结构
type ServiceConfig struct {
	// PeerTLSEnabled 对等TLS启用/禁用。
	PeerTLSEnabled bool
	// Endpoint 这将覆盖peer向其组织中的peer发布的节点。
	Endpoint              string
	NonBlockingCommitMode bool
	// UseLeaderElection 定义对等体何时初始化 “领导者” 选择的动态算法。
	UseLeaderElection bool
	// OrgLeader 静态地将peer定义为组织 “领导者”。
	OrgLeader bool
	// ElectionStartupGracePeriod 领导选举启动时peer等待稳定会员的最长时间 (单位: 秒)
	ElectionStartupGracePeriod time.Duration
	// ElectionMembershipSampleInterval gossip成员资格样本检查其稳定性的时间间隔 (单位: 秒)。
	ElectionMembershipSampleInterval time.Duration
	// ElectionLeaderAliveThreshold 从上次声明消息到peer决定执行leader选举的时间 (单位: 秒)
	ElectionLeaderAliveThreshold time.Duration
	// ElectionLeaderElectionDuration peer发送propose提案消息并声明自己为leader (发送声明消息) 之间的时间 (单位: 秒)
	ElectionLeaderElectionDuration time.Duration
	// PvtDataPullRetryThreshold 确定在没有私有数据的情况下提交该块之前，尝试从对等方提取与给定块相对应的私有数据的最大持续时间
	PvtDataPullRetryThreshold time.Duration
	// PvtDataPushAckTimeout 是在认可时间等待来自每个对等方的认可的最长时间。
	PvtDataPushAckTimeout time.Duration
	// BtlPullMargin 是要活动的块拉取余量，用作缓冲区以防止对等方尝试从对等方拉取即将在接下来的N个块中清除的私有数据。
	BtlPullMargin uint64
	// TransientstoreMaxBlockRetention 定义提交时当前分类帐的高度与驻留在保证不会被清除的临时存储中的私有数据之间的最大差异。
	TransientstoreMaxBlockRetention uint64
	// SkipPullingInvalidTransactionsDuringCommit 指示是否需要在提交期间跳过从其他对等方拉取无效事务的私有数据，并且仅通过reconciler拉取。
	SkipPullingInvalidTransactionsDuringCommit bool
}

func GlobalConfig() *ServiceConfig {
	c := &ServiceConfig{}
	c.loadGossipConfig()
	return c
}

func (c *ServiceConfig) loadGossipConfig() {
	// 开启tls
	c.PeerTLSEnabled = viper.GetBool("peer.tls.enabled")
	// 这将覆盖peer向其组织中的peer发布的节点。
	c.Endpoint = viper.GetString("peer.gossip.endpoint")
	// 非阻塞提交模式
	c.NonBlockingCommitMode = viper.GetBool("peer.gossip.nonBlockingCommitMode")
	// 相当于配置为从节点, 参与主节点的选举
	c.UseLeaderElection = viper.GetBool("peer.gossip.useLeaderElection")
	// 主节点, 主节点可以有多个, 主节点参与和orderer节点的通信, leader是与订购服务建立连接并使用交付协议从订购服务中提取分类帐块的对等方。
	c.OrgLeader = viper.GetBool("peer.gossip.orgLeader")
	// 领导选举启动时peer等待稳定会员的最长时间 (单位: 秒)
	c.ElectionStartupGracePeriod = util.GetDurationOrDefault("peer.gossip.election.startupGracePeriod", election.DefStartupGracePeriod)
	// gossip成员资格样本检查其稳定性的时间间隔 (单位: 秒)。
	c.ElectionMembershipSampleInterval = util.GetDurationOrDefault("peer.gossip.election.membershipSampleInterval", election.DefMembershipSampleInterval)
	// 上次声明消息到peer决定执行leader选举的时间 (单位: 秒)
	c.ElectionLeaderAliveThreshold = util.GetDurationOrDefault("peer.gossip.election.leaderAliveThreshold", election.DefLeaderAliveThreshold)
	// peer发送propose提案消息并声明自己为leader (发送声明消息) 之间的时间 (单位: 秒)
	c.ElectionLeaderElectionDuration = util.GetDurationOrDefault("peer.gossip.election.leaderElectionDuration", election.DefLeaderElectionDuration)
	// 是在认可时间等待每个对等体在私有数据推送时确认的最长时间。
	c.PvtDataPushAckTimeout = viper.GetDuration("peer.gossip.pvtData.pushAckTimeout")
	// 确定在没有私有数据的情况下提交该块之前，尝试从对等方提取与给定块相对应的私有数据的最大持续时间
	c.PvtDataPullRetryThreshold = viper.GetDuration("peer.gossip.pvtData.pullRetryThreshold")
	// 指示是否需要在提交期间跳过从其他对等方拉取无效事务的私有数据，并且仅通过reconciler拉取。
	c.SkipPullingInvalidTransactionsDuringCommit = viper.GetBool("peer.gossip.pvtData.skipPullingInvalidTransactionsDuringCommit")
	// 要活动的块拉取余量，用作缓冲区以防止对等方尝试从对等方拉取即将在接下来的N个块中清除的私有数据。
	c.BtlPullMargin = btlPullMarginDefault
	if viper.IsSet("peer.gossip.pvtData.btlPullMargin") {
		btlMarginVal := viper.GetInt("peer.gossip.pvtData.btlPullMargin")
		if btlMarginVal >= 0 {
			c.BtlPullMargin = uint64(btlMarginVal)
		}
	}

	// 定义提交时当前分类帐的高度与驻留在保证不会被清除的临时存储中的私有数据之间的最大差异。
	c.TransientstoreMaxBlockRetention = uint64(viper.GetInt("peer.gossip.pvtData.transientstoreMaxBlockRetention"))
	if c.TransientstoreMaxBlockRetention == 0 {
		logger.Warning("配置 peer.gossip.pvtData.transientstoreMaxBlockRetention 未设置，默认为", transientBlockRetentionDefault)
		c.TransientstoreMaxBlockRetention = transientBlockRetentionDefault
	}
}
