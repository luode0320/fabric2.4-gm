/*
版权所有 IBM Corp. 保留所有权利。

许可证标识符：Apache-2.0
*/

package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/hyperledger/fabric-lib-go/healthz"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/inactive"
	"github.com/pkg/errors"
)

// 生成模拟代码的指令
//go:generate counterfeiter -o mock/health_checker.go -fake-name HealthChecker . healthChecker

// healthChecker 定义了健康检查器的合同
type healthChecker interface {
	RegisterChecker(component string, checker healthz.HealthChecker) error
}

// New 创建基于Kafka的共识器实例。由orderer的main.go调用。
func New(config localconfig.Kafka, mp metrics.Provider, healthChecker healthChecker, icr InactiveChainRegistry, mkChain func(string)) (consensus.Consenter, *Metrics) {
	if config.Verbose {
		// 如果配置为详细模式，则激活相应的日志级别
		flogging.ActivateSpec(flogging.Global.Spec() + ":orderer.consensus.kafka.sarama=debug")
	}

	// 初始化broker配置
	brokerConfig := newBrokerConfig(
		config.TLS,
		config.SASLPlain,
		config.Retry,
		config.Version,
		defaultPartition)

	// 初始化度量指标
	metrics := NewMetrics(mp, brokerConfig.MetricRegistry)

	// 返回consenterImpl实例及度量指标
	return &consenterImpl{
		mkChain:               mkChain,        // 创建链的回调函数
		inactiveChainRegistry: icr,            // 不活动链注册表
		brokerConfigVal:       brokerConfig,   // Kafka broker的配置
		tlsConfigVal:          config.TLS,     // TLS配置
		retryOptionsVal:       config.Retry,   // 重试选项
		kafkaVersionVal:       config.Version, // Kafka版本
		topicDetailVal: &sarama.TopicDetail{ // 主题详情
			NumPartitions:     1,
			ReplicationFactor: config.Topic.ReplicationFactor,
		},
		healthChecker: healthChecker, // 健康检查器
		metrics:       metrics,       // 度量指标
	}, metrics
}

// InactiveChainRegistry 接口用于注册不活跃的链
type InactiveChainRegistry interface {
	TrackChain(chainName string, genesisBlock *cb.Block, createChain func())
}

// commonConsenter 共识接口。
type commonConsenter interface {
	Metrics() *Metrics                // 指标
	brokerConfig() *sarama.Config     // broker配置
	retryOptions() localconfig.Retry  // retry 重试
	topicDetail() *sarama.TopicDetail // 主题详情
}

// consenterImpl 结构体设计用于实现 consensus.Consenter 接口，核心职责是管理与维护特定链的共识过程，特别是针对基于Kafka的共识机制。
// 此结构体整合了多个关键组件和配置，确保了与Kafka集群的有效交互，以及共识过程中的故障检测与恢复能力。
type consenterImpl struct {
	// mkChain 是一个函数指针，用于在需要时创建新的链实例。它接受一个字符串参数，通常为通道ID。
	mkChain func(string)

	// brokerConfigVal 包含了与Kafka代理通信所需的配置信息，比如 brokers 地址、版本兼容性设定等。
	brokerConfigVal *sarama.Config

	// tlsConfigVal 存储了TLS相关的配置，确保与Kafka Broker之间的通信安全。
	tlsConfigVal localconfig.TLS

	// retryOptionsVal 定义了在连接Kafka或其他操作失败时重试的策略，如重试次数、间隔等。
	retryOptionsVal localconfig.Retry

	// kafkaVersionVal 指定了目标Kafka集群的版本信息，用于客户端兼容性调整。
	kafkaVersionVal sarama.KafkaVersion

	// topicDetailVal 包含了关于Kafka主题的详细设置，如分区数、副本因子等，这对于共识日志主题尤为重要。
	topicDetailVal *sarama.TopicDetail

	// healthChecker 负责检查此共识组件的健康状态，确保系统稳定运行。
	healthChecker healthChecker

	// metrics 提供了一套度量指标，用于监控共识过程及Kafka交互的性能和稳定性。
	metrics *Metrics

	// inactiveChainRegistry 用于注册和管理不活跃的链，有助于资源管理和系统维护。
	inactiveChainRegistry InactiveChainRegistry
}

// HandleChain 根据给定的支持资源创建或返回一个共识.Chain实例。实现共识.Consenter接口。
func (consenter *consenterImpl) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	// 检查节点是否从Raft迁移而来
	if consenter.inactiveChainRegistry != nil {
		logger.Infof("该节点已从Kafka迁移到Raft，跳过Kafka链的激活")
		// 跟踪链，当应激活时调用回调函数
		consenter.inactiveChainRegistry.TrackChain(support.ChannelID(), support.Block(0), func() {
			consenter.mkChain(support.ChannelID())
		})
		// 返回一个表示链不被当前节点服务的inactive.Chain实例
		return &inactive.Chain{Err: errors.Errorf("通道 %s 不由我服务", support.ChannelID())}, nil
	}

	// 创建共识实例
	lastOffsetPersisted, lastOriginalOffsetProcessed, lastResubmittedConfigOffset := getOffsets(metadata.Value, support.ChannelID())
	ch, err := newChain(consenter, support, lastOffsetPersisted, lastOriginalOffsetProcessed, lastResubmittedConfigOffset)
	if err != nil {
		return nil, err
	}

	// 注册健康检查
	consenter.healthChecker.RegisterChecker(ch.channel.String(), ch)
	return ch, nil
}

// 指标
func (consenter *consenterImpl) Metrics() *Metrics {
	return consenter.metrics
}

// broker配置
func (consenter *consenterImpl) brokerConfig() *sarama.Config {
	return consenter.brokerConfigVal
}

// retry 重试
func (consenter *consenterImpl) retryOptions() localconfig.Retry {
	return consenter.retryOptionsVal
}

// 主题详情
func (consenter *consenterImpl) topicDetail() *sarama.TopicDetail {
	return consenter.topicDetailVal
}
