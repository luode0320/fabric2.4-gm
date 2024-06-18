/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// 用于捕获处理消息到区块过程中的各种情况的指标计数 -- 参见processMessagesToBlocks函数
const (
	// indexRecvError 接收消息时发生错误
	indexRecvError = iota

	// indexUnmarshalError 反序列化消息内容时出错
	indexUnmarshalError

	// indexRecvPass 消息接收成功
	indexRecvPass

	// indexProcessConnectPass 成功处理Connect类型消息
	indexProcessConnectPass

	// indexProcessTimeToCutError 在判断是否需要切割区块时发生错误
	indexProcessTimeToCutError

	// indexProcessTimeToCutPass 成功判断并处理切割区块的时机
	indexProcessTimeToCutPass

	// indexProcessRegularError 处理常规消息时出错
	indexProcessRegularError

	// indexProcessRegularPass 成功处理常规消息
	indexProcessRegularPass

	// indexSendTimeToCutError 在需要切割区块时发送信号出错
	indexSendTimeToCutError

	// indexSendTimeToCutPass 成功发送切割区块的信号
	indexSendTimeToCutPass

	// indexExitChanPass 通过退出通道成功传递信号
	indexExitChanPass
)

// 用于根据提供的共识器、支持资源以及偏移量信息创建一个新的链实例。
// 参数:
//   - consenter: 满足commonConsenter接口的共识器实例，提供了基础配置和度量指标。
//   - support: 提供了链操作支持，如账本访问、配置验证等功能。
//   - lastOffsetPersisted: 上次持久化消息的偏移量。
//   - lastOriginalOffsetProcessed: 最后处理的原始消息偏移量。
//   - lastResubmittedConfigOffset: 最后重新提交的配置消息偏移量。
//
// 返回:
//   - *chainImpl: 新创建的链实例。
//   - error: 如果创建过程中发生错误。
func newChain(
	consenter commonConsenter,
	support consensus.ConsenterSupport,
	lastOffsetPersisted int64,
	lastOriginalOffsetProcessed int64,
	lastResubmittedConfigOffset int64,
) (*chainImpl, error) {
	// 获取上次截断区块的高度 = 最新的区块高度 - 1
	lastCutBlockNumber := getLastCutBlockNumber(support.Height())
	// 记录日志，表明链启动并记录相关偏移量和最后截断的区块高度
	logger.Infof("[通道: %s] 启动链，最后持久化偏移量为 %d，最后记录的区块编号为 [%d]",
		support.ChannelID(), lastOffsetPersisted, lastCutBlockNumber)

	// 初始化信号量，用于控制是否完成重新处理飞行中的消息
	doneReprocessingMsgInFlight := make(chan struct{})
	// 1. 从未重新提交过任何配置消息（lastResubmittedConfigOffset == 0）
	// 2. 最近重新提交的配置消息已经被处理（lastResubmittedConfigOffset == lastOriginalOffsetProcessed）
	// 3. 在最新的重新提交配置消息之后，已经处理了一个或多个普通消息（lastResubmittedConfigOffset < lastOriginalOffsetProcessed）
	if lastResubmittedConfigOffset == 0 || lastResubmittedConfigOffset <= lastOriginalOffsetProcessed {
		// 已经跟上重新处理进度，关闭通道以允许广播继续
		close(doneReprocessingMsgInFlight)
	}

	// 更新共识器的度量指标，记录最后持久化偏移量
	consenter.Metrics().LastOffsetPersisted.With("channel", support.ChannelID()).Set(float64(lastOffsetPersisted))

	// 创建并返回chainImpl实例，初始化内部成员变量
	return &chainImpl{
		consenter:                   consenter,                                         // 共识器实例
		ConsenterSupport:            support,                                           // 支持资源
		channel:                     newChannel(support.ChannelID(), defaultPartition), // 创建通道实例
		lastOffsetPersisted:         lastOffsetPersisted,                               // 持久化偏移量
		lastOriginalOffsetProcessed: lastOriginalOffsetProcessed,                       // 处理的偏移量
		lastResubmittedConfigOffset: lastResubmittedConfigOffset,                       // 重新提交的配置偏移量
		lastCutBlockNumber:          lastCutBlockNumber,                                // 最后截断的区块号

		haltChan:                    make(chan struct{}),         // 用于停止链的通道
		startChan:                   make(chan struct{}),         // 用于启动链的通道
		doneReprocessingMsgInFlight: doneReprocessingMsgInFlight, // 重新处理消息完成信号
	}, nil
}

//go:generate counterfeiter -o mock/sync_producer.go --fake-name SyncProducer . syncProducer

// syncProducer 接口定义了一组方法，用于同步地向Kafka brokers发送消息。
type syncProducer interface {
	// SendMessage 发送单个生产者消息至Kafka。返回消息被分配的分区编号、在该分区的偏移量以及可能的错误。
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)

	// SendMessages 批量发送生产者消息至Kafka。接受一个消息切片作为参数，返回错误（如果有）。
	SendMessages(msgs []*sarama.ProducerMessage) error

	// Close 关闭同步生产者，释放相关资源，返回关闭操作时可能发生的错误。
	Close() error
}

// kafka共识结构体
type chainImpl struct {
	// kafka配置
	consenter commonConsenter

	// 继承自 consensus.ConsenterSupport 接口，提供了链支持功能，如账本访问和配置支持。
	consensus.ConsenterSupport

	// 表示当前链的kafka的相关信息, 包括主题和分区名称等。
	channel channel

	// 记录了kafka最后一次持久化消息的偏移量, 下一次将从 lastOffsetPersisted + 1 的偏移量开始消费。
	lastOffsetPersisted int64

	// 记录了最后一次处理的原始消息偏移量。
	lastOriginalOffsetProcessed int64

	// 记录了最近重新提交的配置消息的偏移量。
	lastResubmittedConfigOffset int64

	// 记录了最近截断的区块编号。
	lastCutBlockNumber uint64

	// 是同步消息生产者的实例，用于向Kafka发送消息。
	producer syncProducer

	// 是Sarama消费者实例，用于从Kafka主题消费消息。
	parentConsumer sarama.Consumer

	// 是针对特定分区的消费者，用于从Kafka的单一分区消费消息。
	channelConsumer sarama.PartitionConsumer

	// doneReprocessingMutex 用于在改变 doneReprocessingMsgInFlight 状态时进行同步。
	doneReprocessingMutex sync.Mutex

	// doneReprocessingMsgInFlight 用于通知是否有待处理的飞行中消息需要等待。
	doneReprocessingMsgInFlight chan struct{}

	// errorChan 当分区消费者发生错误时关闭，用以传播错误状态。
	errorChan chan struct{}

	// doneProcessingMessagesToBlocks 通知链已完成将消息处理成区块。
	doneProcessingMessagesToBlocks chan struct{}

	// 当启动过程中的可重试步骤完成时关闭，表示启动完成。
	startChan chan struct{}

	// 接收到停止请求时关闭，一旦关闭将触发消息处理循环的退出，且不会再打开。
	haltChan chan struct{}

	// 控制将待处理消息打包进区块的批处理超时时间。
	timer <-chan time.Time

	// 列出了该链的副本ID列表，与Kafka分区对应。
	replicaIDs []int32
}

// Errored 返回一个通道，当分区消费者发生错误时，此通道将被关闭。
// 此方法由Deliver()函数调用以检查错误状态。
func (chain *chainImpl) Errored() <-chan struct{} {
	// 检查启动阶段是否已完成
	select {
	case <-chain.startChan: // 如果启动已完成
		// 直接返回记录真实错误状态的通道
		return chain.errorChan
	default: // 如果启动还未完成
		// 创建并立即关闭一个“哑”错误通道，用以指示在启动过程中始终视为存在错误
		dummyError := make(chan struct{})
		close(dummyError)
		// 返回已关闭的通道，表示错误状态（尽管这可能仅是启动过程中的临时状态）
		return dummyError
	}
}

// Start 启动共识的主循环处理过程。
func (chain *chainImpl) Start() {
	// 启动一个新的goroutine来执行startThread函数，确保Start方法本身不会阻塞
	go startThread(chain)
}

// 作为启动共识处理流程的入口点，负责执行一系列初始化操作，包括但不限于创建Kafka主题、设置生产者与消费者、发送CONNECT消息、获取副本信息等
// 确保链能够开始正常工作并保持与Kafka主题的数据同步。
// 所有步骤完成后，会启动一个循环处理消息至区块的流程，以维护链的实时状态。
func startThread(chain *chainImpl) {
	var err error

	// 如果通道对应的Kafka主题尚不存在，则为其创建(如你所见, 仅仅时为了创建主题)。
	// 因为通道是动态创建的, 而通道就是kafka的主题, 所有不可能创建kafka连接的时候就默认一个主题
	// 而是根据创建的通道动态创建主题
	err = setupTopicForChannel(chain.consenter.retryOptions(), chain.haltChan, chain.SharedConfig().KafkaBrokers(), chain.consenter.brokerConfig(), chain.consenter.topicDetail(), chain.channel)
	if err != nil {
		// 当前仅记录日志，如果主题创建失败，则依赖于Kafka代理的自动创建主题设置
		logger.Infof("[通道: %s] 创建Kafka主题失败 = %s", chain.channel.topic(), err)
	}

	// 初始化生产者
	chain.producer, err = setupProducerForChannel(chain.consenter.retryOptions(), chain.haltChan, chain.SharedConfig().KafkaBrokers(), chain.consenter.brokerConfig(), chain.channel)
	if err != nil {
		logger.Panicf("[通道: %s] 无法设置生产者 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] 生产者设置成功", chain.ChannelID())

	// 使用生产者发送CONNECT消息, 通常用于健康检查或维持与Kafka服务器的活跃连接状态。
	if err = sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel); err != nil {
		logger.Panicf("[通道: %s] 无法发送CONNECT消息 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] CONNECT消息发送成功", chain.channel.topic())

	// 初始化父级消费者, (先创建一个消费者的连接, 但是不指定连接到哪个主题和分区, 后面动态创建)
	// 默认情况下每个消费者实例都是独立的，即每个实例都会创建一个唯一的消费者组（内部生成）。这意味着，如果不显式设置消费者组ID，每次创建的消费者实例都将独自消费所有分区的消息
	chain.parentConsumer, err = setupParentConsumerForChannel(chain.consenter.retryOptions(), chain.haltChan, chain.SharedConfig().KafkaBrokers(), chain.consenter.brokerConfig(), chain.channel)
	if err != nil {
		logger.Panicf("[通道: %s] 无法设置父级消费者 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] 父级消费者设置成功", chain.channel.topic())

	// 初始化通道分区消费者
	chain.channelConsumer, err = setupChannelConsumerForChannel(chain.consenter.retryOptions(), chain.haltChan, chain.parentConsumer, chain.channel, chain.lastOffsetPersisted+1)
	if err != nil {
		logger.Panicf("[通道: %s] 无法设置通道消费者 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] 通道消费者设置成功", chain.channel.topic())

	// 获取健康集群副本信息
	chain.replicaIDs, err = getHealthyClusterReplicaInfo(chain.consenter.retryOptions(), chain.haltChan, chain.SharedConfig().KafkaBrokers(), chain.consenter.brokerConfig(), chain.channel)
	if err != nil {
		logger.Panicf("[通道: %s] 获取副本ID失败 = %s", chain.channel.topic(), err)
	}

	// 初始化doneProcessingMessagesToBlocks(已完成将消息处理到块)通道
	chain.doneProcessingMessagesToBlocks = make(chan struct{})

	// 初始化errorChan通道，用于响应请求的错误传递
	chain.errorChan = make(chan struct{})

	// 关闭startChan, 表示启动完成, 允许Broadcast请求通过
	close(chain.startChan)

	logger.Infof("[通道: %s] 启动阶段完成成功", chain.channel.topic())

	// 开始处理消息至区块的循环，保持与通道的最新状态同步
	chain.processMessagesToBlocks()
}

// Halt 释放为该Chain分配的资源。实现了consensus.Chain接口。
func (chain *chainImpl) Halt() {
	select {
	case <-chain.startChan: // 等待直到Chain启动完成
		// Chain已完成启动，因此可以停止它
		select {
		case <-chain.haltChan: // 检查Chain是否已被请求停止
			// 此结构很有用，因为它允许Halt()被单个线程多次调用而不会引起恐慌。
			// 回忆从已关闭通道接收会立即返回（零值）。
			logger.Warningf("[channel: %s] 链的停止请求再次发生", chain.ChannelID())
		default:
			logger.Criticalf("[channel: %s] 请求停止链", chain.ChannelID())
			// 开始停止链的操作
			close(chain.haltChan)
			// 等待消息到区块的处理完成以进行关闭
			<-chain.doneProcessingMessagesToBlocks
			// 关闭Kafka生产者和消费者
			chain.closeKafkaObjects()
			logger.Debugf("[channel: %s] 已关闭haltChan", chain.ChannelID())
		}
	default:
		logger.Warningf("[channel: %s] 在停止链之前等待链完成启动", chain.ChannelID())
		<-chain.startChan // 继续等待Chain启动完成
		chain.Halt()      // 重新调用Halt以执行停止操作
	}
}

func (chain *chainImpl) WaitReady() error {
	select {
	case <-chain.startChan: // The Start phase has completed
		select {
		case <-chain.haltChan: // The chain has been halted, stop here
			return fmt.Errorf("consenter for this channel has been halted")
		case <-chain.doneReprocessing(): // Block waiting for all re-submitted messages to be reprocessed
			return nil
		}
	default: // Not ready yet
		return fmt.Errorf("backing Kafka cluster has not completed booting; try again later")
	}
}

func (chain *chainImpl) doneReprocessing() <-chan struct{} {
	chain.doneReprocessingMutex.Lock()
	defer chain.doneReprocessingMutex.Unlock()
	return chain.doneReprocessingMsgInFlight
}

func (chain *chainImpl) reprocessConfigComplete() {
	chain.doneReprocessingMutex.Lock()
	defer chain.doneReprocessingMutex.Unlock()
	close(chain.doneReprocessingMsgInFlight)
}

func (chain *chainImpl) reprocessConfigPending() {
	chain.doneReprocessingMutex.Lock()
	defer chain.doneReprocessingMutex.Unlock()
	chain.doneReprocessingMsgInFlight = make(chan struct{})
}

// Order 实现consensus.Chain接口。由gRPC广播调用。
func (chain *chainImpl) Order(env *cb.Envelope, configSeq uint64) error {
	return chain.order(env, configSeq, int64(0))
}

// 函数负责将给定的信封（Envelope）按照特定配置序列和原始偏移量进行排序处理。
// 具体步骤包括序列化信封、创建正常消息实例，并尝试将其加入到队列中。
// 如果序列化失败或无法成功入队，函数会返回相应的错误。
func (chain *chainImpl) order(env *cb.Envelope, configSeq uint64, originalOffset int64) error {
	// 尝试将信封序列化为字节切片，以便存储或传输
	marshaledEnv, err := protoutil.Marshal(env)
	if err != nil {
		// 序列化失败时，返回错误信息
		return errors.Errorf("无法入队，无法序列化信封: %s", err)
	}

	// 使用序列化后的信封、配置序列号和原始偏移量创建一个新的正常消息实例
	newMessage := newNormalMessage(marshaledEnv, configSeq, originalOffset)

	// 尝试将新消息加入到链的队列中
	if !chain.enqueue(newMessage) {
		// 若无法成功入队，则返回错误
		return errors.Errorf("无法入队")
	}

	// 若一切顺利，返回nil表示操作成功
	return nil
}

// Configure 实现consensus.Chain接口。由gRPC广播调用。
func (chain *chainImpl) Configure(config *cb.Envelope, configSeq uint64) error {
	// 调用内部的configure方法以处理配置更新，此处originalOffset初始化为0，因为它是由Broadcast直接调用，
	// 而非重放或恢复过程中，所以不需要特定的原始偏移量信息。
	return chain.configure(config, configSeq, int64(0))
}

// 负责配置处理，将配置更新消息封装并尝试入队。
// 参数:
//   - config: 配置更新的信封。
//   - configSeq: 配置序列号，标识配置更新的顺序。
//   - originalOffset: 原始消息偏移量，与消息在日志中的位置相关。
//
// 返回:
//   - error: 如果在序列化配置、创建配置消息或尝试入队时发生错误，则返回相应的错误信息。
func (chain *chainImpl) configure(config *cb.Envelope, configSeq uint64, originalOffset int64) error {
	// 将配置信封序列化为字节数组
	marshaledConfig, err := protoutil.Marshal(config)
	if err != nil {
		// 序列化失败时，返回错误信息
		return fmt.Errorf("无法入队，因为无法序列化配置信息，错误原因是 %s", err)
	}
	// 使用序列化后的配置信息创建一个新的配置消息
	configMsg := newConfigMessage(marshaledConfig, configSeq, originalOffset)
	// 尝试将配置消息加入到发送队列中
	if !chain.enqueue(configMsg) {
		// 若消息未能成功入队，返回错误信息
		return fmt.Errorf("无法入队")
	}
	// 成功入队，返回nil表示无错误
	return nil
}

// 接收一个*KafkaMessage类型的消息，并尝试将其加入到发送队列中。
// 如果消息成功入队，则返回true；否则，返回false。
func (chain *chainImpl) enqueue(kafkaMsg *ab.KafkaMessage) bool {
	// 记录调试日志，表明正在为指定通道入队Kafka消息
	logger.Debugf("[channel: %s] 正在入队Kafka...", chain.ChannelID())

	// 使用select语句检查当前链的状态
	select {
	case <-chain.startChan: // 当启动阶段已完成
		// 再次使用select嵌套判断链是否已被停止
		select {
		case <-chain.haltChan: // 如果链已停止，则记录警告日志并返回false
			logger.Warningf("[channel: %s] 该通道的共识器已被停止", chain.ChannelID())
			return false
		default: // 否则，继续执行消息发送逻辑
			// 将Kafka消息序列化为字节数组
			payload, err := protoutil.Marshal(kafkaMsg)
			if err != nil {
				// 序列化失败时记录错误日志，并返回false
				logger.Errorf("[channel: %s] 无法序列化Kafka消息，原因：%s", chain.ChannelID(), err)
				return false
			}
			// 使用newProducerMessage函数创建生产者消息实例
			message := newProducerMessage(chain.channel, payload)
			// 尝试通过生产者发送消息
			if _, _, err = chain.producer.SendMessage(message); err != nil {
				// 发送失败时记录错误日志，并返回false
				logger.Errorf("[channel: %s] 无法入队信封，原因：%s", chain.ChannelID(), err)
				return false
			}
			// 成功入队后记录调试日志
			logger.Debugf("[channel: %s] 信封已成功入队", chain.ChannelID())
			return true
		}
	default: // 如果链尚未启动，记录警告日志并返回false
		logger.Warningf("[channel: %s] 尚未启动，不会入队Kafka", chain.ChannelID())
		return false
	}
}

// HealthCheck 方法用于检查链的健康状况。
// 它通过向Kafka集群发送一个特殊的消息（CONNECT消息）来验证生产者是否能够成功通信。
func (chain *chainImpl) HealthCheck(ctx context.Context) error {
	var err error

	// 创建一个新的CONNECT消息的payload，并将其序列化
	payload := protoutil.MarshalOrPanic(newConnectMessage())

	// 使用当前链的通道信息和序列化的payload创建一个新的ProducerMessage
	message := newProducerMessage(chain.channel, payload)

	// 尝试通过生产者发送此消息到Kafka
	_, _, err = chain.producer.SendMessage(message)
	if err != nil {
		// 如果发送消息失败，记录警告日志
		logger.Warnf("[通道 %s] 无法发布CONNECT消息，错误 = %s", chain.channel.topic(), err)
		// 特别处理“没有足够副本”的错误，附加上副本ID的信息后重新抛出错误
		if err == sarama.ErrNotEnoughReplicas {
			errMsg := fmt.Sprintf("[副本ID: %d]", chain.replicaIDs)
			return errors.WithMessage(err, errMsg)
		}
	}
	// 如果没有错误，说明健康检查通过
	return nil
}

// 负责消耗指定通道的Kafka消费者中的消息，并将有序消息流转换为通道账本的区块。
// 返回:
//   - []uint64: 各类操作的计数统计。
//   - error: 在处理消息到区块过程中遇到的错误。
func (chain *chainImpl) processMessagesToBlocks() ([]uint64, error) {
	counts := make([]uint64, 11) // 用于指标和测试的计数器
	msg := new(ab.KafkaMessage)

	defer func() {
		// 标记不再处理消息至区块
		close(chain.doneProcessingMessagesToBlocks)
	}()

	defer func() {
		// 当调用Halt()时
		select {
		case <-chain.errorChan: // 如果已经关闭，不做处理
		default:
			close(chain.errorChan)
		}
	}()

	// 订阅消息的标识符
	subscription := fmt.Sprintf("向%s/%d添加了订阅", chain.channel.topic(), chain.channel.partition())
	var topicPartitionSubscriptionResumed <-chan string // 订阅恢复的通知通道
	var deliverSessionTimer *time.Timer                 // 会话超时定时器
	var deliverSessionTimedOut <-chan time.Time         // 会话超时事件通道

	for {
		select {
		case <-chain.haltChan:
			logger.Warningf("[通道: %s] 通道的共识器退出", chain.ChannelID())
			counts[indexExitChanPass]++
			return counts, nil

		// kafka消费错误
		case kafkaErr := <-chain.channelConsumer.Errors():
			logger.Errorf("[通道: %s] 消费过程中发生错误: %s", chain.ChannelID(), kafkaErr)
			counts[indexRecvError]++

			select {
			case <-chain.errorChan: // 如果已关闭，不做处理
			default:
				switch kafkaErr.Err {
				// 对于除ErrOffsetOutOfRange(Err偏移超出范围)之外的所有错误，Kafka消费者会自动重试
				case sarama.ErrOffsetOutOfRange:
					logger.Errorf("[通道: %s] 消费过程中发生不可恢复的错误: %s", chain.ChannelID(), kafkaErr)
					close(chain.errorChan)
				default:
					if topicPartitionSubscriptionResumed == nil {
						// 注册监听器
						topicPartitionSubscriptionResumed = saramaLogger.NewListener(subscription)
						// 启动会话超时计时器
						deliverSessionTimer = time.NewTimer(chain.consenter.retryOptions().NetworkTimeouts.ReadTimeout)
						deliverSessionTimedOut = deliverSessionTimer.C
					}
				}
			}

			select {
			case <-chain.errorChan: // 不再忽略错误
				logger.Warningf("[通道: %s] 关闭了errorChan", chain.ChannelID())
				// 使用给定的重试选项向通道发送一个CONNECT消息。通常用于健康检查或维持与Kafka服务器的活跃连接状态。
				go sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel)
			default: // 忽略错误
				logger.Warningf("[通道: %s] 若消费错误持续，传递会话将被丢弃。", chain.ChannelID())
			}

		// 订阅恢复的通知通道
		case <-topicPartitionSubscriptionResumed:
			// 停止监听订阅消息
			saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
			// 禁用订阅事件通道
			topicPartitionSubscriptionResumed = nil
			// 停止超时计时器
			if !deliverSessionTimer.Stop() {
				<-deliverSessionTimer.C
			}
			logger.Warningf("[通道: %s] 消费将继续。", chain.ChannelID())

		// 同上，处理会话超时
		case <-deliverSessionTimedOut:
			saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
			topicPartitionSubscriptionResumed = nil
			close(chain.errorChan)
			logger.Warningf("[通道: %s] 关闭了errorChan", chain.ChannelID())
			// 使用给定的重试选项向通道发送一个CONNECT消息。通常用于健康检查或维持与Kafka服务器的活跃连接状态。
			go sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel)

		case in, ok := <-chain.channelConsumer.Messages():
			if !ok {
				logger.Criticalf("[通道: %s] Kafka消费者已关闭。", chain.ChannelID())
				return counts, nil
			}
			// 检查是否错过订阅事件
			if topicPartitionSubscriptionResumed != nil {
				saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
				topicPartitionSubscriptionResumed = nil
				if !deliverSessionTimer.Stop() {
					<-deliverSessionTimer.C
				}
			}

			select {
			case <-chain.errorChan:
				chain.errorChan = make(chan struct{})
				logger.Infof("[通道: %s] 标记共识器再次可用", chain.ChannelID())
			default:
			}

			// 解码消息
			if err := proto.Unmarshal(in.Value, msg); err != nil {
				logger.Criticalf("[通道: %s] 无法解码消费的消息 = %s", chain.ChannelID(), err)
				counts[indexUnmarshalError]++
				continue
			} else {
				logger.Debugf("[通道: %s] 成功解码消费的消息，偏移量为%d。检查类型...", chain.ChannelID(), in.Offset)
				counts[indexRecvPass]++
			}

			// 根据消息类型处理
			switch msg.Type.(type) {
			// 成功处理Connect类型消息
			case *ab.KafkaMessage_Connect:
				_ = chain.processConnect(chain.ChannelID())
				counts[indexProcessConnectPass]++

			// 在判断是否需要切割区块时发生错误
			case *ab.KafkaMessage_TimeToCut:
				// 处理接收到的时间切分（TimeToCut）消息，根据消息指示切割区块。
				if err := chain.processTimeToCut(msg.GetTimeToCut(), in.Offset); err != nil {
					logger.Warningf("[通道: %s] %s", chain.ChannelID(), err)
					logger.Criticalf("[通道: %s] 通道的共识器退出", chain.ChannelID())
					counts[indexProcessTimeToCutError]++
					return counts, err
				}
				counts[indexProcessTimeToCutPass]++

			// 处理kafka普通消息
			case *ab.KafkaMessage_Regular:
				if err := chain.processRegular(msg.GetRegular(), in.Offset); err != nil {
					logger.Warningf("[通道: %s] 处理类型为REGULAR的传入消息时发生错误 = %s", chain.ChannelID(), err)
					counts[indexProcessRegularError]++
				} else {
					counts[indexProcessRegularPass]++
				}
			}

		// 发送时间切分消息
		case <-chain.timer:
			if err := sendTimeToCut(chain.producer, chain.channel, chain.lastCutBlockNumber+1, &chain.timer); err != nil {
				logger.Errorf("[通道: %s] 无法发布时间切分消息 = %s", chain.ChannelID(), err)
				counts[indexSendTimeToCutError]++
			} else {
				counts[indexSendTimeToCutPass]++
			}
		}
	}
}

// 关闭与Kafka相关的所有对象（消费者和生产者）并收集可能发生的错误。
func (chain *chainImpl) closeKafkaObjects() []error {
	var errs []error // 用于收集关闭操作中遇到的所有错误

	// 尝试关闭channelConsumer并记录错误
	err := chain.channelConsumer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] 无法干净地关闭channelConsumer = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] 已关闭channel消费者", chain.ChannelID())
	}

	// 尝试关闭parentConsumer并记录错误
	err = chain.parentConsumer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] 无法干净地关闭parentConsumer = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] 已关闭父级消费者", chain.ChannelID())
	}

	// 尝试关闭producer并记录错误
	err = chain.producer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] 无法干净地关闭producer = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] 已关闭生产者", chain.ChannelID())
	}

	// 返回所有收集到的错误
	return errs
}

// Helper functions

// 根据区块链高度计算上一个已切割区块的高度。
// 参数:
//   - blockchainHeight: 当前区块链的整体高度，即已包含所有区块的总数。
//
// 返回:
//   - uint64: 表示上一个被切割并添加到区块链上的区块的高度。
//
// 说明:
//   - 该函数简单地从当前区块链高度中减去1，因为最新的高度代表的是最新添加的区块，
//     而上一个被确认的区块则是高度减一的那个。
func getLastCutBlockNumber(blockchainHeight uint64) uint64 {
	return blockchainHeight - 1 // 上一个切割区块的高度等于总高度减一
}

// getOffsets 函数用于从给定的元数据值中提取不同的偏移量信息。
// 参数:
//   - metadataValue: 包含Kafka元数据的字节切片。
//   - chainID: 当前链的标识符字符串。
//
// 返回:
//
//   - persisted: 最后一个已持久化的消息偏移量。
//     在Hyperledger Fabric中，这意味着区块链账本中最后一条被确认并且已安全写入存储的消息对应的Kafka消息偏移量。
//     此偏移量对于系统恢复特别重要，因为它指示了从Kafka中应该开始读取的点，以确保账本数据的连续性和完整性。
//     如果Orderer节点重启或者有新节点加入，会使用此偏移量来确定从哪个位置开始读取消息，以确保不会遗漏或重复处理任何交易。
//
//   - processed: 用于在重新验证和重新排序消息时跟踪处理的最新偏移量。此值用于删除来自多个排序程序的重新提交的重复消息，以便我们不必再次重新处理它。
//     在普通的业务消息处理流程中，这是Orderer处理消息流中的最后一个消息的位置。
//     它帮助跟踪消息处理进度，确保系统了解哪些消息已经被成功处理。
//     这个值在故障恢复时也很关键，帮助系统知道从哪里恢复正常的业务消息处理流程。
//
//   - resubmitted: 最后一次重新提交的配置更新消息的偏移量。通过将其与 LastOriginalOffsetProcessed 进行比较，我们可以确定是否仍有已重新提交但尚未处理的 CONFIG 消息
//     在Fabric中，配置更新（比如通道配置更改）是非常重要的操作，通常需要确保其被网络中的所有Orderer节点正确应用。
//     如果配置更新消息因为某种原因没有被正确处理，可能需要重新提交。
//     这个属性帮助追踪最后一次成功处理的配置更新消息的位置，确保配置的一致性和正确性，尤其是在系统恢复或动态配置更新场景下。
func getOffsets(metadataValue []byte, chainID string) (persisted int64, processed int64, resubmitted int64) {
	if metadataValue != nil {
		// 首先从账本提示中提取与排序器相关的元数据
		kafkaMetadata := &ab.KafkaMetadata{}
		if err := proto.Unmarshal(metadataValue, kafkaMetadata); err != nil {
			// 如果无法反序列化解析排序器元数据，则认为账本可能已损坏，并记录错误信息后终止程序
			logger.Panicf("[channel: %s] 账本可能存在损坏："+
				"无法反序列化最近块中的排序器元数据", chainID)
		}
		return kafkaMetadata.LastOffsetPersisted,
			kafkaMetadata.LastOriginalOffsetProcessed,
			kafkaMetadata.LastResubmittedConfigOffset
	}

	// 若无有效元数据，则返回kafka最旧的一次偏移量
	return sarama.OffsetOldest - 1, int64(0), int64(0) // 默认值
}

// 用于创建一个新的CONNECT类型Kafka消息结构体。
// 这种类型的消息通常用于健康检查或维持与Kafka服务器的活跃连接状态。
func newConnectMessage() *ab.KafkaMessage {
	return &ab.KafkaMessage{
		// 指定消息类型为Connect
		Type: &ab.KafkaMessage_Connect{
			Connect: &ab.KafkaMessageConnect{
				// Connect消息的负载在此场景下为空
				Payload: nil,
			},
		},
	}
}

// 用于创建一个新的表示常规消息的 `ab.KafkaMessage` 实例。
// 这种消息类型包含指定的有效载荷、配置序列号、消息类别（普通）以及原始偏移量。
func newNormalMessage(payload []byte, configSeq uint64, originalOffset int64) *ab.KafkaMessage {
	return &ab.KafkaMessage{
		// 指定消息类型为常规消息
		Type: &ab.KafkaMessage_Regular{
			Regular: &ab.KafkaMessageRegular{
				// 设置消息的有效载荷
				Payload: payload,
				// 消息关联的配置序列号
				ConfigSeq: configSeq,
				// 消息类别为普通消息
				Class: ab.KafkaMessageRegular_NORMAL,
				// 消息在Kafka中的原始偏移量
				OriginalOffset: originalOffset,
			},
		},
	}
}

// 用于创建一个新的配置类型Kafka消息。
// 参数:
//   - config: 配置数据的字节切片形式。
//   - configSeq: 配置序列号，用于追踪配置更新的版本。
//   - originalOffset: 源消息的偏移量，用于引用或追踪消息在日志中的原始位置。
//
// 返回:
//   - *ab.KafkaMessage: 一个封装了配置信息的Kafka消息实例，类型标记为配置（CONFIG）。
func newConfigMessage(config []byte, configSeq uint64, originalOffset int64) *ab.KafkaMessage {
	return &ab.KafkaMessage{
		// 指定消息类型为常规消息，并进一步细化为配置类型
		Type: &ab.KafkaMessage_Regular{
			Regular: &ab.KafkaMessageRegular{
				// 配置消息的有效载荷
				Payload: config,

				// 配置序列号，标识此配置更新的唯一序号
				ConfigSeq: configSeq,

				// 消息类别明确标记为配置（CONFIG）
				Class: ab.KafkaMessageRegular_CONFIG,

				// 源消息在日志中的偏移量，保持消息的上下文关系
				OriginalOffset: originalOffset,
			},
		},
	}
}

// 创建一个新的KafkaMessage，用于指示时间切分事件。
// 参数:
//   - blockNumber: 即将进行时间切分的区块编号。
//
// 返回:
//   - *ab.KafkaMessage: 一个指向KafkaMessage结构体的指针，该消息类型设置为TimeToCut，
//     并包含了指定区块编号的详细时间切分信息。
func newTimeToCutMessage(blockNumber uint64) *ab.KafkaMessage {
	// 初始化并返回一个KafkaMessage实例，
	// 其中Type字段被设置为一个TimeToCut消息，
	// 包含了待切块的区块编号。
	return &ab.KafkaMessage{
		Type: &ab.KafkaMessage_TimeToCut{
			TimeToCut: &ab.KafkaMessageTimeToCut{
				BlockNumber: blockNumber, // 设置区块编号
			},
		},
	}
}

// 用于创建一个新的`sarama.ProducerMessage`实例，该实例封装了准备发送至Kafka的消息细节。
// 参数:
//   - channel: 指定消息要发送到的Kafka主题通道信息。
//   - pld: 需要发送的实际消息负载（字节切片形式）。
//
// 返回:
//   - *sarama.ProducerMessage: 一个填充了主题、键、值的生产者消息实例，准备用于发送。
func newProducerMessage(channel channel, pld []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		// 设置消息要发送到的Kafka主题名称
		Topic: channel.topic(),

		// 设置消息的键，这里将通道的分区号转换为字符串作为键。
		// 注意：这里使用了`strconv.Itoa`来将整数转为字符串，并且有注释提示未来可以考虑实现一个更直接的`IntEncoder`。
		Key: sarama.StringEncoder(strconv.Itoa(int(channel.partition()))),

		// 设置消息的值，直接使用提供的字节切片作为消息负载
		Value: sarama.ByteEncoder(pld),
	}
}

// 处理接收到的连接消息。
// 参数:
//   - channelName: 字符串，表示与消息关联的通道名称。
//
// 功能:
//   - 当前函数会记录一条调试日志，表明收到了一个连接消息，
//     并且说明对于这种类型的消息，当前实现选择忽略它。
//
// 返回:
//   - error: 函数总是返回nil，表示处理过程没有遇到错误。
func (chain *chainImpl) processConnect(channelName string) error {
	// 记录调试日志，说明收到连接消息并决定忽略
	logger.Debugf("[channel: %s] 收到连接消息 - 将其忽略", channelName)

	// 因为这里对连接消息不做具体处理，直接返回无错误
	return nil
}

// 处理常规类型的消息，这些消息通常包含事务数据。
// 参数:
//   - regularMessage: 指向常规Kafka消息的指针，包含交易信封。
//   - receivedOffset: 消息在Kafka主题中的原始偏移量。
//
// 功能:
//   - 根据传入的规则更新`lastOriginalOffsetProcessed`。
//   - 使用BlockCutter对消息进行排序，判断是否需要切割新的区块。
//   - 管理区块切割定时器，依据是否有待处理消息启动或停止定时器。
//   - 分别处理切割单个区块和两个区块的情况，确保偏移量的正确更新和存储。
//   - 调用CreateNextBlock和WriteBlock方法来创建新区块，并附加上相应的Kafka元数据。
func (chain *chainImpl) processRegular(regularMessage *ab.KafkaMessageRegular, receivedOffset int64) error {
	// 当提交普通消息时，我们也会用 'newOffset' 更新 'lastoriignaloffsetprocessed'。
	// 调用者有责任根据以下规则推断 “newoffset” 的正确值:
	// -如果关闭重新提交，则应始终为零
	// -如果消息在第一遍提交，这意味着它没有重新验证和重新排序，这个值
	// 应与当前的 'lestoriginaloffsetprocessed' 相同
	// -如果消息经过重新验证和重新排序，则此值应为该消息的 “originaloffset”
	// Kafka消息，因此 'lastoriiinaloffsetprocessed' 是高级的

	// 定义一个内部函数用于提交普通消息并处理偏移量更新
	commitNormalMsg := func(message *cb.Envelope, newOffset int64) {
		// 使用BlockCutter对消息进行排序，返回当前批次和是否还有剩余未处理消息
		batches, pending := chain.BlockCutter().Ordered(message)
		logger.Debugf("[通道: %s] 排序结果：批次内项目数量 = %d，待处理 = %v", chain.ChannelID(), len(batches), pending)

		// 根据定时器状态和待处理消息情况调整定时器
		switch {
		case chain.timer != nil && !pending:
			// 定时器正在运行且没有待处理消息，停止定时器
			chain.timer = nil
		case chain.timer == nil && pending:
			// 无运行定时器且有待处理消息，启动定时器(到达时间之后, 发送切割区块高度的消息到kafka, 然后kafka消费入块)
			chain.timer = time.After(chain.SharedConfig().BatchTimeout())
			logger.Debugf("[通道: %s] 开始了 %s 批次超时定时器", chain.ChannelID(), chain.SharedConfig().BatchTimeout().String())
		default:
			// 什么都不做的时候:
			// 1. 计时器已在运行，并且有消息挂起
			// 2. 未设置计时器，并且没有挂起的消息
		}

		// 如果没有切割新的区块，仅更新偏移量和处理定时器
		if len(batches) == 0 {
			chain.lastOriginalOffsetProcessed = newOffset
			return
		}

		// 计算LastOffsetPersisted的值，考虑新消息是否被包含在第一个批次中
		var offset int64
		if pending || len(batches) == 2 {
			offset = receivedOffset - 1
		} else {
			// 单独切割一个区块，可以安全更新偏移量
			chain.lastOriginalOffsetProcessed = newOffset
			offset = receivedOffset
		}

		// 切割并处理第一个区块
		block := chain.CreateNextBlock(batches[0])
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         offset,
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
			LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
		}
		chain.WriteBlock(block, metadata)
		chain.lastCutBlockNumber++
		logger.Debugf("[通道: %s] 批次已满，切割区块 [%d] - 最后持久化的偏移量现在为 %d", chain.ChannelID(), chain.lastCutBlockNumber, offset)

		// 如果存在第二个批次，继续处理并切割第二个区块
		if len(batches) == 2 {
			chain.lastOriginalOffsetProcessed = newOffset
			offset++

			block := chain.CreateNextBlock(batches[1])
			metadata = &ab.KafkaMetadata{
				LastOffsetPersisted:         offset,
				LastOriginalOffsetProcessed: newOffset,
				LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
			}
			chain.WriteBlock(block, metadata)
			chain.lastCutBlockNumber++
			logger.Debugf("[通道: %s] 批次已满，切割区块 [%d] - 最后持久化的偏移量现在为 %d", chain.ChannelID(), chain.lastCutBlockNumber, offset)
		}
	}

	// 当提交配置消息时，我们同时使用`newOffset`更新`lastOriginalOffsetProcessed`。
	// 调用者需根据以下规则推断出正确的`newOffset`值：
	// - 如果重提交功能关闭，则`newOffset`应始终为零。
	// - 如果消息在初次传递时被提交，即未经重新验证和重新排序，`newOffset`的值应与当前`lastOriginalOffsetProcessed`相同。
	// - 如果消息经过了重新验证和重新排序，`newOffset`应当是那个Kafka消息的`OriginalOffset`，这样可以推进`lastOriginalOffsetProcessed`的值。
	commitConfigMsg := func(message *cb.Envelope, newOffset int64) {
		// 记录日志，表明接收到配置消息
		logger.Debugf("[channel: %s] 收到配置消息", chain.ChannelID())
		// 切割挂起的消息到一个批次中（如果有）
		batch := chain.BlockCutter().Cut()

		if batch != nil {
			// 如果有挂起消息被切割，记录日志并创建新区块包含这些消息
			logger.Debugf("[channel: %s] 将待处理消息切割成区块", chain.ChannelID())
			block := chain.CreateNextBlock(batch)
			// 为刚创建的区块设置元数据并写入
			metadata := &ab.KafkaMetadata{
				LastOffsetPersisted:         receivedOffset - 1, // 注意：偏移量减一，因当前消息还未计入
				LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
				LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
			}
			chain.WriteBlock(block, metadata)
			// 更新最近切割的区块号
			chain.lastCutBlockNumber++
		}

		// 记录日志，准备为配置消息创建独立的区块
		logger.Debugf("[channel: %s] 为配置消息创建独立区块", chain.ChannelID())
		// 更新`lastOriginalOffsetProcessed`为新的偏移量
		chain.lastOriginalOffsetProcessed = newOffset
		// 创建仅包含当前配置消息的新区块
		block := chain.CreateNextBlock([]*cb.Envelope{message})
		// 为配置区块设置元数据并写入
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         receivedOffset, // 当前消息的偏移量已被持久化
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
			LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
		}
		chain.WriteConfigBlock(block, metadata)
		// 增加最近切割的区块号
		chain.lastCutBlockNumber++
		// 将定时器重置为nil，表示当前操作无需等待更多操作
		chain.timer = nil
	}

	// 获取当前通道配置序列号
	seq := chain.Sequence()

	// 反序列化常规消息的有效载荷为Envelope结构体
	env := &cb.Envelope{}
	if err := proto.Unmarshal(regularMessage.Payload, env); err != nil {
		// 此错误理论上不应发生，因为消息在入口处应已做过验证
		return fmt.Errorf("无法反序列化常规消息的有效载荷，原因：%s", err)
	}

	// 记录日志，指示正在处理特定类型的常规Kafka消息
	logger.Debugf("[通道: %s] 正在处理类型为%s的常规Kafka消息", chain.ChannelID(), regularMessage.Class.String())

	// 检查接收到的消息是否来自v1.1之前的排序服务节点，或者是否明确禁用了重提交功能
	// 在这些情况下，所有排序服务节点应如同v1.1之前的行为：再次验证但不重新排序
	// 这是因为早期版本的排序服务节点无法识别重新排序的消息，重提交可能导致消息被重复提交
	if regularMessage.Class == ab.KafkaMessageRegular_UNKNOWN || !chain.SharedConfig().Capabilities().Resubmission() {
		// 收到类型为UNKNOWN的消息或重提交功能关闭，表明网络中存在v1.0.x版本的排序节点
		logger.Warningf("[通道: %s] 此排序节点正以兼容模式运行", chain.ChannelID())

		// 解析信封中的通道头
		chdr, err := protoutil.ChannelHeader(env)
		if err != nil {
			return fmt.Errorf("因通道头反序列化错误而丢弃错误的配置消息，错误：%s", err)
		}

		// 根据通道头对消息进行分类
		class := chain.ClassifyMsg(chdr)
		switch class {
		case msgprocessor.ConfigMsg:
			// 处理解析和处理配置消息，如有错误则丢弃
			if _, _, err := chain.ProcessConfigMsg(env); err != nil {
				return fmt.Errorf("因错误 = %s 而丢弃错误的配置消息", err)
			}
			commitConfigMsg(env, chain.lastOriginalOffsetProcessed)

		case msgprocessor.NormalMsg:
			// 处理解析和处理普通消息，如有错误则丢弃
			if _, err := chain.ProcessNormalMsg(env); err != nil {
				return fmt.Errorf("因错误 = %s 而丢弃错误的普通消息", err)
			}
			commitNormalMsg(env, chain.lastOriginalOffsetProcessed)

		case msgprocessor.ConfigUpdateMsg:
			// 不应接收到ConfigUpdate类型的消息，直接返回错误
			return fmt.Errorf("未预期到类型为ConfigUpdate的消息")

		default:
			// 遇到不支持的消息分类时，触发恐慌
			logger.Panicf("[通道: %s] 不支持的消息分类：%v", chain.ChannelID(), class)
		}

		return nil
	}

	switch regularMessage.Class {
	case ab.KafkaMessageRegular_UNKNOWN:
		// 遇到未知类型的消息，这应该是处理过的，直接报错
		logger.Panicf("[通道: %s] 类型为UNKNOWN的Kafka消息应已被处理", chain.ChannelID())

	case ab.KafkaMessageRegular_NORMAL:
		// 处理常规消息，需要重新验证和排序
		if regularMessage.OriginalOffset != 0 {
			// 收到重提交的常规消息，记录原始偏移量
			logger.Debugf("[通道: %s] 收到原始偏移量为 %d 的重提交常规消息", chain.ChannelID(), regularMessage.OriginalOffset)

			// 如果已经处理过该偏移量的消息，则丢弃
			if regularMessage.OriginalOffset <= chain.lastOriginalOffsetProcessed {
				logger.Debugf(
					"[通道: %s] 原始偏移量(%d) <= 最后处理的偏移量(%d)，消息已被消费，丢弃",
					chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)
				return nil
			}

			// 第一次接收到该重提交消息
			logger.Debugf(
				"[通道: %s] 原始偏移量(%d) > 最后处理的偏移量(%d)，首次接收到重提交的常规消息",
				chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)
		}

		// 配置序列已前进，需要重新验证, 重新推送到kafka排序
		if regularMessage.ConfigSeq < seq {
			logger.Debugf("[通道: %s] 自常规消息验证以来配置序列已前进，重新验证", chain.ChannelID())
			configSeq, err := chain.ProcessNormalMsg(env)
			if err != nil {
				return fmt.Errorf("丢弃无效的常规消息，因为 %s", err)
			}

			logger.Debugf("[通道: %s] 常规消息仍有效，重新提交", chain.ChannelID())
			// 无论首次还是重排，都设置原始偏移量并重新排序
			if err := chain.order(env, configSeq, receivedOffset); err != nil {
				return fmt.Errorf("重新提交常规消息时出错，因为 %s", err)
			}
			return nil
		}

		// 此处的消息已被验证为有效
		// 更新lastOriginalOffsetProcessed，如果消息被重新验证和排序
		offset := regularMessage.OriginalOffset
		if offset == 0 {
			offset = chain.lastOriginalOffsetProcessed
		}
		commitNormalMsg(env, offset)

	case ab.KafkaMessageRegular_CONFIG:
		// 处理配置消息，同样需要重新验证和排序
		if regularMessage.OriginalOffset != 0 {
			// 收到重提交的配置消息，记录原始偏移量
			logger.Debugf("[通道: %s] 收到原始偏移量为 %d 的重提交配置消息", chain.ChannelID(), regularMessage.OriginalOffset)

			// 如果已处理过，则丢弃
			if regularMessage.OriginalOffset <= chain.lastOriginalOffsetProcessed {
				logger.Debugf(
					"[通道: %s] 原始偏移量(%d) <= 最后处理的偏移量(%d)，消息已被消费，丢弃",
					chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)
				return nil
			}

			// 首次接收到重提交的配置消息
			logger.Debugf(
				"[通道: %s] 原始偏移量(%d) > 最后处理的偏移量(%d)，首次接收到重提交的配置消息",
				chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)

			// 如果是最后一个重提交的配置消息且无需再次验证，则解除入口消息阻塞
			if regularMessage.OriginalOffset == chain.lastResubmittedConfigOffset &&
				regularMessage.ConfigSeq == seq {
				logger.Debugf("[通道: %s] 原始偏移量为 %d 的配置消息是最后的待重处理消息，且无需重新验证，现在解除入口消息阻塞",
					chain.ChannelID(), regularMessage.OriginalOffset)
				chain.reprocessConfigComplete()
			}

			// 为了网络一致性，更新lastResubmittedConfigOffset
			if chain.lastResubmittedConfigOffset < regularMessage.OriginalOffset {
				chain.lastResubmittedConfigOffset = regularMessage.OriginalOffset
			}
		}

		// 配置序列已前进，需要重新验证
		if regularMessage.ConfigSeq < seq {
			logger.Debugf("[通道: %s] 自配置消息验证以来配置序列已前进，重新验证", chain.ChannelID())
			configEnv, configSeq, err := chain.ProcessConfigMsg(env)
			if err != nil {
				return fmt.Errorf("拒绝配置消息，因为 %s", err)
			}

			// 重新排序配置消息
			if err := chain.configure(configEnv, configSeq, receivedOffset); err != nil {
				return fmt.Errorf("重新提交配置消息时出错，因为 %s", err)
			}
			logger.Debugf("[通道: %s] 重提交偏移量为 %d 的配置消息，阻止入口消息", chain.ChannelID(), receivedOffset)
			chain.lastResubmittedConfigOffset = receivedOffset // 记录最后重提交的消息偏移量
			chain.reprocessConfigPending()                     // 开始阻止入口消息
			return nil
		}

		// 此处的消息已被验证为有效
		// 更新lastOriginalOffsetProcessed，如果消息被重新验证和排序
		offset := regularMessage.OriginalOffset
		if offset == 0 {
			offset = chain.lastOriginalOffsetProcessed
		}
		commitConfigMsg(env, offset)

	default:
		// 不支持的常规Kafka消息类型
		return errors.Errorf("不支持的常规Kafka消息类型: %v", regularMessage.Class.String())
	}

	return nil
}

// 处理接收到的时间切分（TimeToCut）消息，根据消息指示切割区块。
func (chain *chainImpl) processTimeToCut(ttcMessage *ab.KafkaMessageTimeToCut, receivedOffset int64) error {
	ttcNumber := ttcMessage.GetBlockNumber()
	logger.Debugf("[通道: %s] 收到一个切块时间消息，对应区块[%d]", chain.ChannelID(), ttcNumber)
	if ttcNumber == chain.lastCutBlockNumber+1 {
		// 清除计时器，因为正确的时间切分指令已到达
		chain.timer = nil
		logger.Debugf("[通道: %s] 计时器已清空", chain.ChannelID())
		// 使用BlockCutter切割批次
		batch := chain.BlockCutter().Cut()
		if len(batch) == 0 {
			// 没有待处理请求时收到切块指令，可能表明存在错误
			return fmt.Errorf("收到了正确的时间切分消息（对应区块[%d]），但没有待处理的请求；这可能意味着存在错误", chain.lastCutBlockNumber+1)
		}
		// 创建新区块
		block := chain.CreateNextBlock(batch)
		// 准备Kafka元数据
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         receivedOffset,
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
		}
		// 写入新区块并更新元数据
		chain.WriteBlock(block, metadata)
		// 更新最后切割的区块号
		chain.lastCutBlockNumber++
		logger.Debugf("[通道: %s] 正确的时间切分已接收，已切割区块[%d]", chain.ChannelID(), chain.lastCutBlockNumber)
		return nil
	} else if ttcNumber > chain.lastCutBlockNumber+1 {
		// 收到未来时间切分消息，可能指示错误
		return fmt.Errorf("收到的时间切分消息过大（%d），超过允许或预期的范围（%d）；这可能意味着存在错误", ttcNumber, chain.lastCutBlockNumber+1)
	}

	// 忽略过时的时间切分消息
	logger.Debugf("[通道: %s] 忽略过时的时间切分消息，对应区块[%d]", chain.ChannelID(), ttcNumber)
	return nil
}

// WriteBlock 作为对 consenter 支持的 WriteBlock 方法的一个包装器，该方法负责编码元数据，
// 并更新度量指标。
// 参数:
//   - block: 指向要写入的区块链块的指针。
//   - metadata: 指向Kafka元数据的指针，包含诸如最后持久化偏移量等信息。
//
// 功能:
//   - 首先，使用 protoutil 库将元数据编码为字节流。
//   - 然后，调用底层 consenter 支持的 WriteBlock 方法，将区块及编码后的元数据写入最佳到区块链的结构中(此时并没有写入文件的操作)。
//   - 最后，更新与通道相关的指标，记录最后持久化的偏移量，用于监控和跟踪。
func (chain *chainImpl) WriteBlock(block *cb.Block, metadata *ab.KafkaMetadata) {
	chain.ConsenterSupport.WriteBlock(block, protoutil.MarshalOrPanic(metadata))
	chain.consenter.Metrics().LastOffsetPersisted.With("channel", chain.ChannelID()).Set(float64(metadata.LastOffsetPersisted))
}

// WriteConfigBlock 作为对 consenter 支持的 WriteConfigBlock 方法的一个封装，
// 负责编码元数据并更新相关度量指标。
// 参数:
//   - block: 指向待写入的配置区块的指针。
//   - metadata: 指向Kafka元数据的指针，携带如最后持久化偏移量等关键信息。
//
// 功能:
//   - 使用 protoutil 库将元数据序列化为字节数组。
//   - 调用底层 consenter 支持的接口来写入配置区块及序列化后的元数据。
//   - 更新度量指标，记录当前通道的最后持久化偏移量，这对于监控系统状态至关重要。
func (chain *chainImpl) WriteConfigBlock(block *cb.Block, metadata *ab.KafkaMetadata) {
	chain.ConsenterSupport.WriteConfigBlock(block, protoutil.MarshalOrPanic(metadata))
	chain.consenter.Metrics().LastOffsetPersisted.With("channel", chain.ChannelID()).Set(float64(metadata.LastOffsetPersisted))
}

// 使用给定的重试选项向通道发送一个CONNECT消息。通常用于健康检查或维持与Kafka服务器的活跃连接状态。
// 参数:
//   - retryOptions: 控制重试行为的本地配置。
//   - exitChan: 一个通道，用于通知此函数应停止重试并退出。
//   - producer: 已配置的Sarama同步生产者，用于发送消息。
//   - channel: 消息目标的通道信息。
//
// 返回:
//   - error: 如果在发送CONNECT消息过程中发生错误，则返回错误；否则返回nil。
func sendConnectMessage(retryOptions localconfig.Retry, exitChan chan struct{}, producer sarama.SyncProducer, channel channel) error {
	logger.Infof("[channel: %s] 即将发送CONNECT消息...", channel.topic())

	// 创建一个新的CONNECT消息的payload
	payload := protoutil.MarshalOrPanic(newConnectMessage())
	// 根据通道和payload构造一个新的生产者消息
	message := newProducerMessage(channel, payload)

	// 定义重试时的提示信息
	retryMsg := "尝试发送CONNECT消息..."
	// 初始化一个重试过程，用于发送CONNECT消息
	postConnect := newRetryProcess(retryOptions, exitChan, channel, retryMsg, func() error {
		select {
		// 监听退出通道，若收到信号则退出循环
		case <-exitChan:
			logger.Debugf("[channel: %s] 通道的共识器正在退出，中断重试", channel)
			return nil
		// 默认情况，尝试发送消息
		default:
			_, _, err := producer.SendMessage(message)
			return err // 返回发送消息的错误结果
		}
	})

	// 执行重试逻辑以发送CONNECT消息
	return postConnect.retry()
}

// 在定时器到期时发送一个时间切分（TimeToCut）消息到Kafka。
// 参数:
//   - producer: 已配置的Sarama同步生产者，用于发送消息。
//   - channel: 消息目标的通道信息。
//   - timeToCutBlockNumber: 指示应切割新区块的区块编号。
//   - timer: 表示当前时间切分消息触发定时器的通道，到期后置为nil。
//
// 返回:
//   - error: 如果发送消息过程中出现错误，则返回该错误；否则返回nil。
func sendTimeToCut(producer sarama.SyncProducer, channel channel, timeToCutBlockNumber uint64, timer *<-chan time.Time) error {
	logger.Debugf("[通道: %s] 切块时间区块[%d]的计时器已过期", channel.topic(), timeToCutBlockNumber)
	// 定时器执行完毕后将其设为nil
	*timer = nil
	// 构造时间切分消息的payload
	payload := protoutil.MarshalOrPanic(newTimeToCutMessage(timeToCutBlockNumber))
	// 根据通道和payload创建生产者消息
	message := newProducerMessage(channel, payload)
	// 发送时间切分消息到Kafka
	_, _, err := producer.SendMessage(message)
	return err // 返回发送操作的错误状态
}

// 使用给定的重试选项为特定通道设置分区消费者。
// 参数:
//   - retryOptions: 控制定时重试策略的配置。
//   - haltChan: 一个通道，当接收到信号时停止重试并退出函数。
//   - parentConsumer: 已经初始化的父级Kafka消费者实例。
//   - channel: 包含通道主题和分区信息的结构。
//   - startFrom: 从该偏移量开始消费消息。
//
// 返回:
//   - sarama.PartitionConsumer: 成功创建的分区消费者实例。
//   - error: 如果在创建消费者过程中发生错误，则返回错误；否则返回nil。
func setupChannelConsumerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, parentConsumer sarama.Consumer, channel channel, startFrom int64) (sarama.PartitionConsumer, error) {
	var err error
	var channelConsumer sarama.PartitionConsumer // 初始化分区消费者变量

	// 记录日志信息，指出即将为通道设置消费者，并指定起始偏移量
	logger.Infof("[通道: %s] 正在为此通道设置消费者 (起始偏移量: %d)...", channel.topic(), startFrom)

	// 定义在重试逻辑中使用的提示信息
	retryMsg := "连接到Kafka集群"

	// 创建一个新的重试处理过程，尝试从指定偏移量开始为通道的特定分区创建消费者
	setupChannelConsumer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		// 使用父级消费者尝试消费指定通道主题和分区的指定起始偏移量的消息
		channelConsumer, err = parentConsumer.ConsumePartition(channel.topic(), channel.partition(), startFrom)
		return err // 返回任何可能发生的错误
	})

	// 执行重试逻辑以设置通道消费者
	return channelConsumer, setupChannelConsumer.retry()
}

// 使用给定的重试选项为指定通道生产者设置父级消费者(先创建一个消费者的连接, 但是不指定连接到哪个主题和分区, 后面动态创建)。
// 默认情况下每个消费者实例都是独立的，即每个实例都会创建一个唯一的消费者组（内部生成）。这意味着，如果不显式设置消费者组ID，每次创建的消费者实例都将独自消费所有分区的消息
// 参数:
//   - retryOptions: 控制重试逻辑的本地配置。
//   - haltChan: 用于通知应停止重试的通道。
//   - brokers: Kafka代理的地址列表。
//   - brokerConfig: 与Kafka交互的Sarama配置。
//   - channel: 消费者将订阅的通道信息。
//
// 返回:
//   - sarama.Consumer: 成功连接到Kafka集群后创建的消费者实例。
//   - error: 如果在设置消费者过程中发生错误，则返回错误；否则返回nil。
func setupParentConsumerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) (sarama.Consumer, error) {
	var err error
	var parentConsumer sarama.Consumer // 初始化父级消费者变量

	// 记录日志，指示正在为通道设置父级消费者
	logger.Infof("[channel: %s] 正在为此通道设置父级消费者...", channel.topic())

	// 定义在重试时输出的提示信息
	retryMsg := "连接到Kafka集群"

	// 创建一个新的重试过程，尝试连接到Kafka并创建消费者
	setupParentConsumer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		// 尝试使用提供的配置和代理地址创建一个新的消费者实例
		parentConsumer, err = sarama.NewConsumer(brokers, brokerConfig)
		return err // 返回错误以供重试逻辑使用
	})

	// 执行重试逻辑以设置父级消费者
	return parentConsumer, setupParentConsumer.retry()
}

// 使用给定的重试选项为特定通道设置生产者（writer/producer）。
// 参数:
//   - retryOptions: 控制重试行为的本地配置。
//   - haltChan: 一个通道，用于通知此函数应停止重试并立即返回。
//   - brokers: Kafka代理的地址列表。
//   - brokerConfig: 与Kafka代理交互时使用的Sarama配置。
//   - channel: 要为之设置生产者的通道信息。
//
// 返回:
//   - sarama.SyncProducer: 成功连接到Kafka集群后创建的同步生产者。
//   - error: 如果在设置生产者过程中发生错误，则返回错误；否则返回nil。
func setupProducerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) (sarama.SyncProducer, error) {
	var err error
	var producer sarama.SyncProducer // 初始化生产者变量

	// 记录日志信息，指示正在为此通道设置生产者
	logger.Infof("[channel: %s] 正在为此通道设置生产者...", channel.topic())

	// 定义重试时的提示信息
	retryMsg := "连接到Kafka集群"

	// 初始化一个重试过程，尝试连接到Kafka并创建生产者
	setupProducer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		// 尝试使用提供的配置和代理地址创建一个新的同步生产者
		producer, err = sarama.NewSyncProducer(brokers, brokerConfig)
		return err // 返回错误以供重试逻辑使用
	})

	// 执行重试逻辑以尝试设置生产者
	return producer, setupProducer.retry()
}

// 如果频道对应的Kafka主题尚不存在，则为其创建(如你所见, 仅仅时为了创建主题)。
// 因为通道是动态创建的, 而通道就是主题, 所有不可能创建kafka连接的时候就默认一个主题
// 而是根据创建的通道动态创建主题
func setupTopicForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, topicDetail *sarama.TopicDetail, channel channel) error {
	// 要求Kafka版本至少为v0.10.1.0
	if !brokerConfig.Version.IsAtLeast(sarama.V0_10_1_0) {
		return nil
	}

	// 记录日志，表明开始为当前通道设置主题
	logger.Infof("[通道: %s] 正在为此通道设置主题...", channel.topic())

	// 定义重试逻辑的消息
	retryMsg := fmt.Sprintf("为通道 [%s] 创建Kafka主题 [%s]",
		channel.topic(), channel.String())

	// 初始化一个重试过程
	setupTopic := newRetryProcess(
		retryOptions,
		haltChan,
		channel,
		retryMsg,
		func() error {
			var err error
			clusterMembers := make(map[int32]*sarama.Broker) // 存储集群中的代理信息
			var controllerId int32                           // 控制器ID

			// 遍历所有代理地址，获取元数据
			for _, address := range brokers {
				broker := sarama.NewBroker(address)
				err = broker.Open(brokerConfig)

				if err != nil {
					continue
				}

				if ok, _ := broker.Connected(); !ok {
					continue
				}
				defer broker.Close()

				// 发起元数据请求，根据Kafka版本选择API版本
				var apiVersion int16
				if brokerConfig.Version.IsAtLeast(sarama.V0_11_0_0) {
					apiVersion = 4 // 禁止自动创建主题
				} else {
					apiVersion = 1
				}
				metadata, err := broker.GetMetadata(&sarama.MetadataRequest{
					Version:                apiVersion,
					Topics:                 []string{channel.topic()},
					AllowAutoTopicCreation: false,
				})
				if err != nil {
					continue
				}

				controllerId = metadata.ControllerID
				for _, broker := range metadata.Brokers {
					clusterMembers[broker.ID()] = broker
				}

				// 检查主题是否已存在或有错误
				for _, topic := range metadata.Topics {
					if topic.Name == channel.topic() {
						if topic.Err != sarama.ErrUnknownTopicOrPartition {
							return nil // 自动创建主题已启用，无需操作
						}
					}
				}
				break // 成功获取元数据后跳出循环
			}

			// 确认是否获取到了集群元数据
			if len(clusterMembers) == 0 {
				return fmt.Errorf("为创建主题 [%s] 失败，无法获取集群元数据", channel.topic())
			}

			// 连接到控制器并创建主题
			controller := clusterMembers[controllerId]
			err = controller.Open(brokerConfig)

			if err != nil {
				return err
			}

			var ok bool
			ok, err = controller.Connected()
			if !ok {
				return err
			}
			defer controller.Close()

			// 构建创建主题的请求
			req := &sarama.CreateTopicsRequest{
				Version: 0,
				TopicDetails: map[string]*sarama.TopicDetail{
					channel.topic(): topicDetail,
				},
				Timeout: 3 * time.Second,
			}
			resp, err := controller.CreateTopics(req)
			if err != nil {
				return err
			}

			// 检查响应中的错误
			if topicErr, ok := resp.TopicErrors[channel.topic()]; ok {
				if topicErr.Err == sarama.ErrNoError || topicErr.Err == sarama.ErrTopicAlreadyExists {
					return nil // 主题已存在或无错误，视为成功
				}
				if topicErr.Err == sarama.ErrInvalidTopic {
					// 主题无效，记录警告并停止通道
					logger.Warningf("[通道: %s] 创建主题失败，原因 = %s", channel.topic(), topicErr.Err.Error())
					go func() { haltChan <- struct{}{} }()
				}
				return fmt.Errorf("创建主题时出错: [%s]", topicErr.Err.Error())
			}

			return nil
		})

	return setupTopic.retry() // 执行重试逻辑
}

// 只有当集群处于健康状态时，才能准确获取副本ID信息。
// 否则，副本请求不会返回完整的初始副本集。当健康检查返回错误时，
// 需要此信息来提供上下文。
// 参数:
//   - retryOptions: 控制重试行为的本地配置。
//   - haltChan: 用于指示应停止重试的通道。
//   - brokers: Kafka代理的地址列表。
//   - brokerConfig: Sarama客户端配置。
//   - channel: 包含主题和分区信息的通道实例。
//
// 返回:
//   - []int32: 成功检索到的副本ID列表。
//   - error: 如果在获取副本信息过程中发生错误，则返回错误；否则返回nil。
func getHealthyClusterReplicaInfo(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) ([]int32, error) {
	var replicaIDs []int32 // 初始化副本ID列表

	// 定义重试时的日志提示信息
	retryMsg := "获取复制通道的Kafka代理列表"
	// 初始化一个新的重试过程来获取副本信息
	getReplicaInfo := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		// 使用配置创建Kafka客户端
		client, err := sarama.NewClient(brokers, brokerConfig)
		if err != nil {
			return err // 客户端创建失败，返回错误
		}
		defer client.Close() // 确保客户端在函数结束时关闭

		// 尝试获取指定主题和分区的副本列表
		replicaIDs, err = client.Replicas(channel.topic(), channel.partition())
		if err != nil {
			return err // 获取副本信息失败，返回错误
		}
		return nil // 成功获取副本信息
	})

	// 执行重试逻辑以获取副本信息
	return replicaIDs, getReplicaInfo.retry()
}
