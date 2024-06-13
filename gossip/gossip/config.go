/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"net"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/election"
	"github.com/hyperledger/fabric/gossip/gossip/algo"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/spf13/viper"
)

// Config 是gossip组件的配置
type Config struct {
	// BindPort 是gossip绑定到的端口，仅用于测试。
	BindPort int
	// 特定gossip实例的Id。
	ID string
	// BootstrapPeers 是我们在启动时连接的同行。
	BootstrapPeers []string
	// PropagateIterations 消息被推送到远程对等方的次数。
	PropagateIterations int
	// PropagatePeerNum 是选择将消息推送到的对等体的数量。
	PropagatePeerNum int

	// MaxBlockCountToStore 是我们存储在内存中的最大块数。
	MaxBlockCountToStore int

	// MaxPropagationBurstSize 是在触发推送到远程对等方之前存储的最大消息数。
	MaxPropagationBurstSize int
	// MaxPropagationBurstLatency 连续推送消息之间的最大时间 (单位: 毫秒)
	MaxPropagationBurstLatency time.Duration

	// PullInterval 确定拉动阶段的频率 (单位: 秒) 必须大于 digestWaitTime + responseWaitTime
	PullInterval time.Duration
	// PullPeerNum 是要从中拉出的对等体的数量。
	PullPeerNum int

	// SkipBlockVerification controls either we skip verifying block message or not.
	SkipBlockVerification bool

	// PublishCertPeriod 是从启动证书包含在Alive消息中的时间。
	PublishCertPeriod time.Duration
	// PublishStateInfoInterval 确定将状态信息消息推送到对等体的频率。
	PublishStateInfoInterval time.Duration
	// RequestStateInfoInterval 确定从对等体拉取状态信息消息的频率。
	RequestStateInfoInterval time.Duration

	// TLSCerts is the TLS certificates of the peer.
	TLSCerts *common.TLSCertificates

	// InternalEndpoint 是我们向组织中的对等方发布的端点。
	InternalEndpoint string
	// ExternalEndpoint 对等方将此端点而不是selfEndpoint发布到外部组织。
	ExternalEndpoint string
	// TimeForMembershipTracker 成员资格跟踪器轮询的时间间隔
	TimeForMembershipTracker time.Duration

	// DigestWaitTime 在pull引擎处理传入摘要之前等待的时间。
	DigestWaitTime time.Duration
	// RequestWaitTime 是在pull引擎删除传入的nonce之前等待的时间。
	RequestWaitTime time.Duration
	// ResponseWaitTime 是在pull拉动引擎结束拉动之前等待的时间。
	ResponseWaitTime time.Duration

	// DialTimeout 拨号超时 (单位: 秒)
	DialTimeout time.Duration
	// ConnTimeout 连接超时 (单位: 秒)
	ConnTimeout time.Duration
	// RecvBuffSize 收到消息的缓冲区大小
	RecvBuffSize int
	// SendBuffSize 发送消息的缓冲区大小
	SendBuffSize int

	// MsgExpirationTimeout 指示领导消息过期超时。从上次声明消息到peer决定执行leader选举的时间 (单位: 秒)
	MsgExpirationTimeout time.Duration

	// AliveTimeInterval Alive检查间隔 (单位: 秒)
	AliveTimeInterval time.Duration
	// AliveExpirationTimeout is the alive expiration timeout.
	AliveExpirationTimeout time.Duration
	// AliveExpirationCheckInterval 是有效的过期检查间隔。
	AliveExpirationCheckInterval time.Duration
	// ReconnectInterval 重新连接间隔 (单位: 秒)
	ReconnectInterval time.Duration
	// MsgExpirationFactor 是有效消息TTL的过期因子
	MsgExpirationFactor int
	// MaxConnectionAttempts 尝试连接到对等体的最大次数
	MaxConnectionAttempts int
}

// GlobalConfig 从给定的端点，证书和引导对等体构建配置。
func GlobalConfig(endpoint string, certs *common.TLSCertificates, bootPeers ...string) (*Config, error) {
	c := &Config{}
	err := c.loadConfig(endpoint, certs, bootPeers...)
	return c, err
}

func (c *Config) loadConfig(endpoint string, certs *common.TLSCertificates, bootPeers ...string) error {
	_, p, err := net.SplitHostPort(endpoint)
	if err != nil {
		return err
	}
	port, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return err
	}
	// gossip绑定到的端口，仅用于测试。
	c.BindPort = int(port)
	// 是我们在启动时连接的同行。
	c.BootstrapPeers = bootPeers
	// 当前节点
	c.ID = endpoint
	// 是我们存储在内存中的最大块数。
	c.MaxBlockCountToStore = util.GetIntOrDefault("peer.gossip.maxBlockCountToStore", 10)
	// 连续推送消息之间的最大时间 (单位: 毫秒)
	c.MaxPropagationBurstLatency = util.GetDurationOrDefault("peer.gossip.maxPropagationBurstLatency", 10*time.Millisecond)
	// 在触发推送到远程对等方之前存储的最大消息数
	c.MaxPropagationBurstSize = util.GetIntOrDefault("peer.gossip.maxPropagationBurstSize", 10)
	// 消息被推送到远程对等方的次数
	c.PropagateIterations = util.GetIntOrDefault("peer.gossip.propagateIterations", 1)
	// 选择将消息推送到的对等体数
	c.PropagatePeerNum = util.GetIntOrDefault("peer.gossip.propagatePeerNum", 3)
	// 确定拉动阶段的频率 (单位: 秒) 必须大于 digestWaitTime + responseWaitTime
	c.PullInterval = util.GetDurationOrDefault("peer.gossip.pullInterval", 4*time.Second)
	// 要从中拉取的对等体的数量
	c.PullPeerNum = util.GetIntOrDefault("peer.gossip.pullPeerNum", 3)
	// 我们向组织中的对等方发布的端点。
	c.InternalEndpoint = endpoint
	// 对等方将此端点而不是selfEndpoint发布到外部组织。
	c.ExternalEndpoint = viper.GetString("peer.gossip.externalEndpoint")
	// 启动证书的时间包括在Alive消息中 (单位: 秒)
	c.PublishCertPeriod = util.GetDurationOrDefault("peer.gossip.publishCertPeriod", 10*time.Second)
	// 确定从对等体拉取状态信息消息的频率 (单位: 秒)
	c.RequestStateInfoInterval = util.GetDurationOrDefault("peer.gossip.requestStateInfoInterval", 4*time.Second)
	// 确定向对等体推送状态信息消息的频率 (单位: 秒)
	c.PublishStateInfoInterval = util.GetDurationOrDefault("peer.gossip.publishStateInfoInterval", 4*time.Second)
	// 我们是否应该跳过验证块消息 (当前未使用)
	c.SkipBlockVerification = viper.GetBool("peer.gossip.skipBlockVerification")
	// tls证书
	c.TLSCerts = certs
	// 成员资格跟踪器轮询的时间间隔
	c.TimeForMembershipTracker = util.GetDurationOrDefault("peer.gossip.membershipTrackerInterval", 5*time.Second)
	// 在pull引擎处理传入摘要之前等待的时间。
	c.DigestWaitTime = util.GetDurationOrDefault("peer.gossip.digestWaitTime", algo.DefDigestWaitTime)
	// 是在pull引擎删除传入的nonce之前等待的时间。
	c.RequestWaitTime = util.GetDurationOrDefault("peer.gossip.requestWaitTime", algo.DefRequestWaitTime)
	// 是在pull拉动引擎结束拉动之前等待的时间。
	c.ResponseWaitTime = util.GetDurationOrDefault("peer.gossip.responseWaitTime", algo.DefResponseWaitTime)
	// 拨号超时 (单位: 秒)
	c.DialTimeout = util.GetDurationOrDefault("peer.gossip.dialTimeout", comm.DefDialTimeout)
	// 连接超时 (单位: 秒)
	c.ConnTimeout = util.GetDurationOrDefault("peer.gossip.connTimeout", comm.DefConnTimeout)
	// 收到消息的缓冲区大小
	c.RecvBuffSize = util.GetIntOrDefault("peer.gossip.recvBuffSize", comm.DefRecvBuffSize)
	// 发送消息的缓冲区大小
	c.SendBuffSize = util.GetIntOrDefault("peer.gossip.sendBuffSize", comm.DefSendBuffSize)
	// 从上次声明消息到peer决定执行leader选举的时间 (单位: 秒)
	c.MsgExpirationTimeout = util.GetDurationOrDefault("peer.gossip.election.leaderAliveThreshold", election.DefLeaderAliveThreshold) * 10
	// Alive检查间隔 (单位: 秒)
	c.AliveTimeInterval = util.GetDurationOrDefault("peer.gossip.aliveTimeInterval", discovery.DefAliveTimeInterval)
	// 活动过期超时 (单位: 秒)
	c.AliveExpirationTimeout = util.GetDurationOrDefault("peer.gossip.aliveExpirationTimeout", 5*c.AliveTimeInterval)
	// 有效的过期检查间隔。
	c.AliveExpirationCheckInterval = c.AliveExpirationTimeout / 10
	// 重新连接间隔 (单位: 秒)
	c.ReconnectInterval = util.GetDurationOrDefault("peer.gossip.reconnectInterval", c.AliveExpirationTimeout)
	// 尝试连接到对等体的最大次数
	c.MaxConnectionAttempts = util.GetIntOrDefault("peer.gossip.maxConnectionAttempts", discovery.DefMaxConnectionAttempts)
	// 是有效消息TTL的过期因子
	c.MsgExpirationFactor = util.GetIntOrDefault("peer.gossip.msgExpirationFactor", discovery.DefMsgExpirationFactor)

	return nil
}
