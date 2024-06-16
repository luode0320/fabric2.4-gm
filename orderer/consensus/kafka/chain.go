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

// newChain 用于根据提供的共识器、支持资源以及偏移量信息创建一个新的链实例。
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

	// 记录了最后一次持久化消息的偏移量。
	lastOffsetPersisted int64

	// 记录了最后一次处理的原始消息偏移量。
	lastOriginalOffsetProcessed int64

	// 记录了最近重新提交的配置消息的偏移量。
	lastResubmittedConfigOffset int64

	// 记录了最近截断的区块编号。
	lastCutBlockNumber uint64

	// 是同步消息生产者的实例，用于向Kafka发送消息。
	producer syncProducer

	// parentConsumer 是Sarama消费者实例，用于从Kafka主题消费消息。
	parentConsumer sarama.Consumer

	// channelConsumer 是针对特定分区的消费者，用于从Kafka的单一分区消费消息。
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

	// timer 控制将待处理消息打包进区块的批处理超时时间。
	timer <-chan time.Time

	// replicaIDs 列出了该链的副本ID列表，与Kafka分区对应。
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

	// 使用生产者发送CONNECT消息
	if err = sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel); err != nil {
		logger.Panicf("[通道: %s] 无法发送CONNECT消息 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] CONNECT消息发送成功", chain.channel.topic())

	// 初始化父级消费者
	chain.parentConsumer, err = setupParentConsumerForChannel(chain.consenter.retryOptions(), chain.haltChan, chain.SharedConfig().KafkaBrokers(), chain.consenter.brokerConfig(), chain.channel)
	if err != nil {
		logger.Panicf("[通道: %s] 无法设置父级消费者 = %s", chain.channel.topic(), err)
	}
	logger.Infof("[通道: %s] 父级消费者设置成功", chain.channel.topic())

	// 初始化通道消费者
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

	// 初始化doneProcessingMessagesToBlocks通道
	chain.doneProcessingMessagesToBlocks = make(chan struct{})

	// 初始化errorChan通道，用于Deliver请求的错误传递
	chain.errorChan = make(chan struct{})

	// 关闭startChan，允许Broadcast请求通过
	close(chain.startChan)

	logger.Infof("[通道: %s] 启动阶段完成成功", chain.channel.topic())

	// 开始处理消息至区块的循环，保持与通道的最新状态同步
	chain.processMessagesToBlocks()
}

// Halt frees the resources which were allocated for this Chain. Implements the
// consensus.Chain interface.
func (chain *chainImpl) Halt() {
	select {
	case <-chain.startChan:
		// chain finished starting, so we can halt it
		select {
		case <-chain.haltChan:
			// This construct is useful because it allows Halt() to be called
			// multiple times (by a single thread) w/o panicking. Recall that a
			// receive from a closed channel returns (the zero value) immediately.
			logger.Warningf("[channel: %s] Halting of chain requested again", chain.ChannelID())
		default:
			logger.Criticalf("[channel: %s] Halting of chain requested", chain.ChannelID())
			// stat shutdown of chain
			close(chain.haltChan)
			// wait for processing of messages to blocks to finish shutting down
			<-chain.doneProcessingMessagesToBlocks
			// close the kafka producer and the consumer
			chain.closeKafkaObjects()
			logger.Debugf("[channel: %s] Closed the haltChan", chain.ChannelID())
		}
	default:
		logger.Warningf("[channel: %s] Waiting for chain to finish starting before halting", chain.ChannelID())
		<-chain.startChan
		chain.Halt()
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

// Implements the consensus.Chain interface. Called by Broadcast().
func (chain *chainImpl) Order(env *cb.Envelope, configSeq uint64) error {
	return chain.order(env, configSeq, int64(0))
}

func (chain *chainImpl) order(env *cb.Envelope, configSeq uint64, originalOffset int64) error {
	marshaledEnv, err := protoutil.Marshal(env)
	if err != nil {
		return errors.Errorf("cannot enqueue, unable to marshal envelope: %s", err)
	}
	if !chain.enqueue(newNormalMessage(marshaledEnv, configSeq, originalOffset)) {
		return errors.Errorf("cannot enqueue")
	}
	return nil
}

// 此方法由Broadcast函数调用
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

// processMessagesToBlocks drains the Kafka consumer for the given channel, and
// takes care of converting the stream of ordered messages into blocks for the
// channel's ledger.
func (chain *chainImpl) processMessagesToBlocks() ([]uint64, error) {
	counts := make([]uint64, 11) // For metrics and tests
	msg := new(ab.KafkaMessage)

	defer func() {
		// notify that we are not processing messages to blocks
		close(chain.doneProcessingMessagesToBlocks)
	}()

	defer func() { // When Halt() is called
		select {
		case <-chain.errorChan: // If already closed, don't do anything
		default:
			close(chain.errorChan)
		}
	}()

	subscription := fmt.Sprintf("added subscription to %s/%d", chain.channel.topic(), chain.channel.partition())
	var topicPartitionSubscriptionResumed <-chan string
	var deliverSessionTimer *time.Timer
	var deliverSessionTimedOut <-chan time.Time

	for {
		select {
		case <-chain.haltChan:
			logger.Warningf("[channel: %s] Consenter for channel exiting", chain.ChannelID())
			counts[indexExitChanPass]++
			return counts, nil
		case kafkaErr := <-chain.channelConsumer.Errors():
			logger.Errorf("[channel: %s] Error during consumption: %s", chain.ChannelID(), kafkaErr)
			counts[indexRecvError]++
			select {
			case <-chain.errorChan: // If already closed, don't do anything
			default:

				switch kafkaErr.Err {
				case sarama.ErrOffsetOutOfRange:
					// the kafka consumer will auto retry for all errors except for ErrOffsetOutOfRange
					logger.Errorf("[channel: %s] Unrecoverable error during consumption: %s", chain.ChannelID(), kafkaErr)
					close(chain.errorChan)
				default:
					if topicPartitionSubscriptionResumed == nil {
						// register listener
						topicPartitionSubscriptionResumed = saramaLogger.NewListener(subscription)
						// start session timout timer
						deliverSessionTimer = time.NewTimer(chain.consenter.retryOptions().NetworkTimeouts.ReadTimeout)
						deliverSessionTimedOut = deliverSessionTimer.C
					}
				}
			}
			select {
			case <-chain.errorChan: // we are not ignoring the error
				logger.Warningf("[channel: %s] Closed the errorChan", chain.ChannelID())
				// This covers the edge case where (1) a consumption error has
				// closed the errorChan and thus rendered the chain unavailable to
				// deliver clients, (2) we're already at the newest offset, and (3)
				// there are no new Broadcast requests coming in. In this case,
				// there is no trigger that can recreate the errorChan again and
				// mark the chain as available, so we have to force that trigger via
				// the emission of a CONNECT message. TODO Consider rate limiting
				go sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel)
			default: // we are ignoring the error
				logger.Warningf("[channel: %s] Deliver sessions will be dropped if consumption errors continue.", chain.ChannelID())
			}
		case <-topicPartitionSubscriptionResumed:
			// stop listening for subscription message
			saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
			// disable subscription event chan
			topicPartitionSubscriptionResumed = nil

			// stop timeout timer
			if !deliverSessionTimer.Stop() {
				<-deliverSessionTimer.C
			}
			logger.Warningf("[channel: %s] Consumption will resume.", chain.ChannelID())

		case <-deliverSessionTimedOut:
			// stop listening for subscription message
			saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
			// disable subscription event chan
			topicPartitionSubscriptionResumed = nil

			close(chain.errorChan)
			logger.Warningf("[channel: %s] Closed the errorChan", chain.ChannelID())

			// make chain available again via CONNECT message trigger
			go sendConnectMessage(chain.consenter.retryOptions(), chain.haltChan, chain.producer, chain.channel)

		case in, ok := <-chain.channelConsumer.Messages():
			if !ok {
				logger.Criticalf("[channel: %s] Kafka consumer closed.", chain.ChannelID())
				return counts, nil
			}

			// catch the possibility that we missed a topic subscription event before
			// we registered the event listener
			if topicPartitionSubscriptionResumed != nil {
				// stop listening for subscription message
				saramaLogger.RemoveListener(subscription, topicPartitionSubscriptionResumed)
				// disable subscription event chan
				topicPartitionSubscriptionResumed = nil
				// stop timeout timer
				if !deliverSessionTimer.Stop() {
					<-deliverSessionTimer.C
				}
			}

			select {
			case <-chain.errorChan: // If this channel was closed...
				chain.errorChan = make(chan struct{}) // ...make a new one.
				logger.Infof("[channel: %s] Marked consenter as available again", chain.ChannelID())
			default:
			}
			if err := proto.Unmarshal(in.Value, msg); err != nil {
				// This shouldn't happen, it should be filtered at ingress
				logger.Criticalf("[channel: %s] Unable to unmarshal consumed message = %s", chain.ChannelID(), err)
				counts[indexUnmarshalError]++
				continue
			} else {
				logger.Debugf("[channel: %s] Successfully unmarshalled consumed message, offset is %d. Inspecting type...", chain.ChannelID(), in.Offset)
				counts[indexRecvPass]++
			}
			switch msg.Type.(type) {
			case *ab.KafkaMessage_Connect:
				_ = chain.processConnect(chain.ChannelID())
				counts[indexProcessConnectPass]++
			case *ab.KafkaMessage_TimeToCut:
				if err := chain.processTimeToCut(msg.GetTimeToCut(), in.Offset); err != nil {
					logger.Warningf("[channel: %s] %s", chain.ChannelID(), err)
					logger.Criticalf("[channel: %s] Consenter for channel exiting", chain.ChannelID())
					counts[indexProcessTimeToCutError]++
					return counts, err // TODO Revisit whether we should indeed stop processing the chain at this point
				}
				counts[indexProcessTimeToCutPass]++
			case *ab.KafkaMessage_Regular:
				if err := chain.processRegular(msg.GetRegular(), in.Offset); err != nil {
					logger.Warningf("[channel: %s] Error when processing incoming message of type REGULAR = %s", chain.ChannelID(), err)
					counts[indexProcessRegularError]++
				} else {
					counts[indexProcessRegularPass]++
				}
			}
		case <-chain.timer:
			if err := sendTimeToCut(chain.producer, chain.channel, chain.lastCutBlockNumber+1, &chain.timer); err != nil {
				logger.Errorf("[channel: %s] cannot post time-to-cut message = %s", chain.ChannelID(), err)
				// Do not return though
				counts[indexSendTimeToCutError]++
			} else {
				counts[indexSendTimeToCutPass]++
			}
		}
	}
}

func (chain *chainImpl) closeKafkaObjects() []error {
	var errs []error

	err := chain.channelConsumer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] could not close channelConsumer cleanly = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] Closed the channel consumer", chain.ChannelID())
	}

	err = chain.parentConsumer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] could not close parentConsumer cleanly = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] Closed the parent consumer", chain.ChannelID())
	}

	err = chain.producer.Close()
	if err != nil {
		logger.Errorf("[channel: %s] could not close producer cleanly = %s", chain.ChannelID(), err)
		errs = append(errs, err)
	} else {
		logger.Debugf("[channel: %s] Closed the producer", chain.ChannelID())
	}

	return errs
}

// Helper functions

func getLastCutBlockNumber(blockchainHeight uint64) uint64 {
	return blockchainHeight - 1
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

func newNormalMessage(payload []byte, configSeq uint64, originalOffset int64) *ab.KafkaMessage {
	return &ab.KafkaMessage{
		Type: &ab.KafkaMessage_Regular{
			Regular: &ab.KafkaMessageRegular{
				Payload:        payload,
				ConfigSeq:      configSeq,
				Class:          ab.KafkaMessageRegular_NORMAL,
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

func newTimeToCutMessage(blockNumber uint64) *ab.KafkaMessage {
	return &ab.KafkaMessage{
		Type: &ab.KafkaMessage_TimeToCut{
			TimeToCut: &ab.KafkaMessageTimeToCut{
				BlockNumber: blockNumber,
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

func (chain *chainImpl) processConnect(channelName string) error {
	logger.Debugf("[channel: %s] It's a connect message - ignoring", channelName)
	return nil
}

func (chain *chainImpl) processRegular(regularMessage *ab.KafkaMessageRegular, receivedOffset int64) error {
	// When committing a normal message, we also update `lastOriginalOffsetProcessed` with `newOffset`.
	// It is caller's responsibility to deduce correct value of `newOffset` based on following rules:
	// - if Resubmission is switched off, it should always be zero
	// - if the message is committed on first pass, meaning it's not re-validated and re-ordered, this value
	//   should be the same as current `lastOriginalOffsetProcessed`
	// - if the message is re-validated and re-ordered, this value should be the `OriginalOffset` of that
	//   Kafka message, so that `lastOriginalOffsetProcessed` is advanced
	commitNormalMsg := func(message *cb.Envelope, newOffset int64) {
		batches, pending := chain.BlockCutter().Ordered(message)
		logger.Debugf("[channel: %s] Ordering results: items in batch = %d, pending = %v", chain.ChannelID(), len(batches), pending)

		switch {
		case chain.timer != nil && !pending:
			// Timer is already running but there are no messages pending, stop the timer
			chain.timer = nil
		case chain.timer == nil && pending:
			// Timer is not already running and there are messages pending, so start it
			chain.timer = time.After(chain.SharedConfig().BatchTimeout())
			logger.Debugf("[channel: %s] Just began %s batch timer", chain.ChannelID(), chain.SharedConfig().BatchTimeout().String())
		default:
			// Do nothing when:
			// 1. Timer is already running and there are messages pending
			// 2. Timer is not set and there are no messages pending
		}

		if len(batches) == 0 {
			// If no block is cut, we update the `lastOriginalOffsetProcessed`, start the timer if necessary and return
			chain.lastOriginalOffsetProcessed = newOffset
			return
		}

		offset := receivedOffset
		if pending || len(batches) == 2 {
			// If the newest envelope is not encapsulated into the first batch,
			// the `LastOffsetPersisted` should be `receivedOffset` - 1.
			offset--
		} else {
			// We are just cutting exactly one block, so it is safe to update
			// `lastOriginalOffsetProcessed` with `newOffset` here, and then
			// encapsulate it into this block. Otherwise, if we are cutting two
			// blocks, the first one should use current `lastOriginalOffsetProcessed`
			// and the second one should use `newOffset`, which is also used to
			// update `lastOriginalOffsetProcessed`
			chain.lastOriginalOffsetProcessed = newOffset
		}

		// Commit the first block
		block := chain.CreateNextBlock(batches[0])
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         offset,
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
			LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
		}
		chain.WriteBlock(block, metadata)
		chain.lastCutBlockNumber++
		logger.Debugf("[channel: %s] Batch filled, just cut block [%d] - last persisted offset is now %d", chain.ChannelID(), chain.lastCutBlockNumber, offset)

		// Commit the second block if exists
		if len(batches) == 2 {
			chain.lastOriginalOffsetProcessed = newOffset
			offset++

			block := chain.CreateNextBlock(batches[1])
			metadata := &ab.KafkaMetadata{
				LastOffsetPersisted:         offset,
				LastOriginalOffsetProcessed: newOffset,
				LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
			}
			chain.WriteBlock(block, metadata)
			chain.lastCutBlockNumber++
			logger.Debugf("[channel: %s] Batch filled, just cut block [%d] - last persisted offset is now %d", chain.ChannelID(), chain.lastCutBlockNumber, offset)
		}
	}

	// When committing a config message, we also update `lastOriginalOffsetProcessed` with `newOffset`.
	// It is caller's responsibility to deduce correct value of `newOffset` based on following rules:
	// - if Resubmission is switched off, it should always be zero
	// - if the message is committed on first pass, meaning it's not re-validated and re-ordered, this value
	//   should be the same as current `lastOriginalOffsetProcessed`
	// - if the message is re-validated and re-ordered, this value should be the `OriginalOffset` of that
	//   Kafka message, so that `lastOriginalOffsetProcessed` is advanced
	commitConfigMsg := func(message *cb.Envelope, newOffset int64) {
		logger.Debugf("[channel: %s] Received config message", chain.ChannelID())
		batch := chain.BlockCutter().Cut()

		if batch != nil {
			logger.Debugf("[channel: %s] Cut pending messages into block", chain.ChannelID())
			block := chain.CreateNextBlock(batch)
			metadata := &ab.KafkaMetadata{
				LastOffsetPersisted:         receivedOffset - 1,
				LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
				LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
			}
			chain.WriteBlock(block, metadata)
			chain.lastCutBlockNumber++
		}

		logger.Debugf("[channel: %s] Creating isolated block for config message", chain.ChannelID())
		chain.lastOriginalOffsetProcessed = newOffset
		block := chain.CreateNextBlock([]*cb.Envelope{message})
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         receivedOffset,
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
			LastResubmittedConfigOffset: chain.lastResubmittedConfigOffset,
		}
		chain.WriteConfigBlock(block, metadata)
		chain.lastCutBlockNumber++
		chain.timer = nil
	}

	seq := chain.Sequence()

	env := &cb.Envelope{}
	if err := proto.Unmarshal(regularMessage.Payload, env); err != nil {
		// This shouldn't happen, it should be filtered at ingress
		return fmt.Errorf("failed to unmarshal payload of regular message because = %s", err)
	}

	logger.Debugf("[channel: %s] Processing regular Kafka message of type %s", chain.ChannelID(), regularMessage.Class.String())

	// If we receive a message from a pre-v1.1 orderer, or resubmission is explicitly disabled, every orderer
	// should operate as the pre-v1.1 ones: validate again and not attempt to reorder. That is because the
	// pre-v1.1 orderers cannot identify re-ordered messages and resubmissions could lead to committing
	// the same message twice.
	//
	// The implicit assumption here is that the resubmission capability flag is set only when there are no more
	// pre-v1.1 orderers on the network. Otherwise it is unset, and this is what we call a compatibility mode.
	if regularMessage.Class == ab.KafkaMessageRegular_UNKNOWN || !chain.SharedConfig().Capabilities().Resubmission() {
		// Received regular message of type UNKNOWN or resubmission if off, indicating an OSN network with v1.0.x orderer
		logger.Warningf("[channel: %s] This orderer is running in compatibility mode", chain.ChannelID())

		chdr, err := protoutil.ChannelHeader(env)
		if err != nil {
			return fmt.Errorf("discarding bad config message because of channel header unmarshalling error = %s", err)
		}

		class := chain.ClassifyMsg(chdr)
		switch class {
		case msgprocessor.ConfigMsg:
			if _, _, err := chain.ProcessConfigMsg(env); err != nil {
				return fmt.Errorf("discarding bad config message because = %s", err)
			}

			commitConfigMsg(env, chain.lastOriginalOffsetProcessed)

		case msgprocessor.NormalMsg:
			if _, err := chain.ProcessNormalMsg(env); err != nil {
				return fmt.Errorf("discarding bad normal message because = %s", err)
			}

			commitNormalMsg(env, chain.lastOriginalOffsetProcessed)

		case msgprocessor.ConfigUpdateMsg:
			return fmt.Errorf("not expecting message of type ConfigUpdate")

		default:
			logger.Panicf("[channel: %s] Unsupported message classification: %v", chain.ChannelID(), class)
		}

		return nil
	}

	switch regularMessage.Class {
	case ab.KafkaMessageRegular_UNKNOWN:
		logger.Panicf("[channel: %s] Kafka message of type UNKNOWN should have been processed already", chain.ChannelID())

	case ab.KafkaMessageRegular_NORMAL:
		// This is a message that is re-validated and re-ordered
		if regularMessage.OriginalOffset != 0 {
			logger.Debugf("[channel: %s] Received re-submitted normal message with original offset %d", chain.ChannelID(), regularMessage.OriginalOffset)

			// But we've reprocessed it already
			if regularMessage.OriginalOffset <= chain.lastOriginalOffsetProcessed {
				logger.Debugf(
					"[channel: %s] OriginalOffset(%d) <= LastOriginalOffsetProcessed(%d), message has been consumed already, discard",
					chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)
				return nil
			}

			logger.Debugf(
				"[channel: %s] OriginalOffset(%d) > LastOriginalOffsetProcessed(%d), "+
					"this is the first time we receive this re-submitted normal message",
				chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)

			// In case we haven't reprocessed the message, there's no need to differentiate it from those
			// messages that will be processed for the first time.
		}

		// The config sequence has advanced
		if regularMessage.ConfigSeq < seq {
			logger.Debugf("[channel: %s] Config sequence has advanced since this normal message got validated, re-validating", chain.ChannelID())
			configSeq, err := chain.ProcessNormalMsg(env)
			if err != nil {
				return fmt.Errorf("discarding bad normal message because = %s", err)
			}

			logger.Debugf("[channel: %s] Normal message is still valid, re-submit", chain.ChannelID())

			// For both messages that are ordered for the first time or re-ordered, we set original offset
			// to current received offset and re-order it.
			if err := chain.order(env, configSeq, receivedOffset); err != nil {
				return fmt.Errorf("error re-submitting normal message because = %s", err)
			}

			return nil
		}

		// Any messages coming in here may or may not have been re-validated
		// and re-ordered, BUT they are definitely valid here

		// advance lastOriginalOffsetProcessed if message is re-validated and re-ordered
		offset := regularMessage.OriginalOffset
		if offset == 0 {
			offset = chain.lastOriginalOffsetProcessed
		}

		commitNormalMsg(env, offset)

	case ab.KafkaMessageRegular_CONFIG:
		// This is a message that is re-validated and re-ordered
		if regularMessage.OriginalOffset != 0 {
			logger.Debugf("[channel: %s] Received re-submitted config message with original offset %d", chain.ChannelID(), regularMessage.OriginalOffset)

			// But we've reprocessed it already
			if regularMessage.OriginalOffset <= chain.lastOriginalOffsetProcessed {
				logger.Debugf(
					"[channel: %s] OriginalOffset(%d) <= LastOriginalOffsetProcessed(%d), message has been consumed already, discard",
					chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)
				return nil
			}

			logger.Debugf(
				"[channel: %s] OriginalOffset(%d) > LastOriginalOffsetProcessed(%d), "+
					"this is the first time we receive this re-submitted config message",
				chain.ChannelID(), regularMessage.OriginalOffset, chain.lastOriginalOffsetProcessed)

			if regularMessage.OriginalOffset == chain.lastResubmittedConfigOffset && // This is very last resubmitted config message
				regularMessage.ConfigSeq == seq { // AND we don't need to resubmit it again
				logger.Debugf("[channel: %s] Config message with original offset %d is the last in-flight resubmitted message"+
					"and it does not require revalidation, unblock ingress messages now", chain.ChannelID(), regularMessage.OriginalOffset)
				chain.reprocessConfigComplete() // Therefore, we could finally unblock broadcast
			}

			// Somebody resubmitted message at offset X, whereas we didn't. This is due to non-determinism where
			// that message was considered invalid by us during re-validation, however somebody else deemed it to
			// be valid, and resubmitted it. We need to advance lastResubmittedConfigOffset in this case in order
			// to enforce consistency across the network.
			if chain.lastResubmittedConfigOffset < regularMessage.OriginalOffset {
				chain.lastResubmittedConfigOffset = regularMessage.OriginalOffset
			}
		}

		// The config sequence has advanced
		if regularMessage.ConfigSeq < seq {
			logger.Debugf("[channel: %s] Config sequence has advanced since this config message got validated, re-validating", chain.ChannelID())
			configEnv, configSeq, err := chain.ProcessConfigMsg(env)
			if err != nil {
				return fmt.Errorf("rejecting config message because = %s", err)
			}

			// For both messages that are ordered for the first time or re-ordered, we set original offset
			// to current received offset and re-order it.
			if err := chain.configure(configEnv, configSeq, receivedOffset); err != nil {
				return fmt.Errorf("error re-submitting config message because = %s", err)
			}

			logger.Debugf("[channel: %s] Resubmitted config message with offset %d, block ingress messages", chain.ChannelID(), receivedOffset)
			chain.lastResubmittedConfigOffset = receivedOffset // Keep track of last resubmitted message offset
			chain.reprocessConfigPending()                     // Begin blocking ingress messages

			return nil
		}

		// Any messages coming in here may or may not have been re-validated
		// and re-ordered, BUT they are definitely valid here

		// advance lastOriginalOffsetProcessed if message is re-validated and re-ordered
		offset := regularMessage.OriginalOffset
		if offset == 0 {
			offset = chain.lastOriginalOffsetProcessed
		}

		commitConfigMsg(env, offset)

	default:
		return errors.Errorf("unsupported regular kafka message type: %v", regularMessage.Class.String())
	}

	return nil
}

func (chain *chainImpl) processTimeToCut(ttcMessage *ab.KafkaMessageTimeToCut, receivedOffset int64) error {
	ttcNumber := ttcMessage.GetBlockNumber()
	logger.Debugf("[channel: %s] It's a time-to-cut message for block [%d]", chain.ChannelID(), ttcNumber)
	if ttcNumber == chain.lastCutBlockNumber+1 {
		chain.timer = nil
		logger.Debugf("[channel: %s] Nil'd the timer", chain.ChannelID())
		batch := chain.BlockCutter().Cut()
		if len(batch) == 0 {
			return fmt.Errorf("got right time-to-cut message (for block [%d]),"+
				" no pending requests though; this might indicate a bug", chain.lastCutBlockNumber+1)
		}
		block := chain.CreateNextBlock(batch)
		metadata := &ab.KafkaMetadata{
			LastOffsetPersisted:         receivedOffset,
			LastOriginalOffsetProcessed: chain.lastOriginalOffsetProcessed,
		}
		chain.WriteBlock(block, metadata)
		chain.lastCutBlockNumber++
		logger.Debugf("[channel: %s] Proper time-to-cut received, just cut block [%d]", chain.ChannelID(), chain.lastCutBlockNumber)
		return nil
	} else if ttcNumber > chain.lastCutBlockNumber+1 {
		return fmt.Errorf("got larger time-to-cut message (%d) than allowed/expected (%d)"+
			" - this might indicate a bug", ttcNumber, chain.lastCutBlockNumber+1)
	}
	logger.Debugf("[channel: %s] Ignoring stale time-to-cut-message for block [%d]", chain.ChannelID(), ttcNumber)
	return nil
}

// WriteBlock acts as a wrapper around the consenter support WriteBlock, encoding the metadata,
// and updating the metrics.
func (chain *chainImpl) WriteBlock(block *cb.Block, metadata *ab.KafkaMetadata) {
	chain.ConsenterSupport.WriteBlock(block, protoutil.MarshalOrPanic(metadata))
	chain.consenter.Metrics().LastOffsetPersisted.With("channel", chain.ChannelID()).Set(float64(metadata.LastOffsetPersisted))
}

// WriteConfigBlock acts as a wrapper around the consenter support WriteConfigBlock, encoding the metadata,
// and updating the metrics.
func (chain *chainImpl) WriteConfigBlock(block *cb.Block, metadata *ab.KafkaMetadata) {
	chain.ConsenterSupport.WriteConfigBlock(block, protoutil.MarshalOrPanic(metadata))
	chain.consenter.Metrics().LastOffsetPersisted.With("channel", chain.ChannelID()).Set(float64(metadata.LastOffsetPersisted))
}

// Post a CONNECT message to the channel using the given retry options. This
// prevents the panicking that would occur if we were to set up a consumer and
// seek on a partition that hadn't been written to yet.
func sendConnectMessage(retryOptions localconfig.Retry, exitChan chan struct{}, producer sarama.SyncProducer, channel channel) error {
	logger.Infof("[channel: %s] About to post the CONNECT message...", channel.topic())

	payload := protoutil.MarshalOrPanic(newConnectMessage())
	message := newProducerMessage(channel, payload)

	retryMsg := "Attempting to post the CONNECT message..."
	postConnect := newRetryProcess(retryOptions, exitChan, channel, retryMsg, func() error {
		select {
		case <-exitChan:
			logger.Debugf("[channel: %s] Consenter for channel exiting, aborting retry", channel)
			return nil
		default:
			_, _, err := producer.SendMessage(message)
			return err
		}
	})

	return postConnect.retry()
}

func sendTimeToCut(producer sarama.SyncProducer, channel channel, timeToCutBlockNumber uint64, timer *<-chan time.Time) error {
	logger.Debugf("[channel: %s] Time-to-cut block [%d] timer expired", channel.topic(), timeToCutBlockNumber)
	*timer = nil
	payload := protoutil.MarshalOrPanic(newTimeToCutMessage(timeToCutBlockNumber))
	message := newProducerMessage(channel, payload)
	_, _, err := producer.SendMessage(message)
	return err
}

// Sets up the partition consumer for a channel using the given retry options.
func setupChannelConsumerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, parentConsumer sarama.Consumer, channel channel, startFrom int64) (sarama.PartitionConsumer, error) {
	var err error
	var channelConsumer sarama.PartitionConsumer

	logger.Infof("[channel: %s] Setting up the channel consumer for this channel (start offset: %d)...", channel.topic(), startFrom)

	retryMsg := "Connecting to the Kafka cluster"
	setupChannelConsumer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		channelConsumer, err = parentConsumer.ConsumePartition(channel.topic(), channel.partition(), startFrom)
		return err
	})

	return channelConsumer, setupChannelConsumer.retry()
}

// Sets up the parent consumer for a channel using the given retry options.
func setupParentConsumerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) (sarama.Consumer, error) {
	var err error
	var parentConsumer sarama.Consumer

	logger.Infof("[channel: %s] Setting up the parent consumer for this channel...", channel.topic())

	retryMsg := "Connecting to the Kafka cluster"
	setupParentConsumer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		parentConsumer, err = sarama.NewConsumer(brokers, brokerConfig)
		return err
	})

	return parentConsumer, setupParentConsumer.retry()
}

// Sets up the writer/producer for a channel using the given retry options.
func setupProducerForChannel(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) (sarama.SyncProducer, error) {
	var err error
	var producer sarama.SyncProducer

	logger.Infof("[channel: %s] Setting up the producer for this channel...", channel.topic())

	retryMsg := "Connecting to the Kafka cluster"
	setupProducer := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		producer, err = sarama.NewSyncProducer(brokers, brokerConfig)
		return err
	})

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

// Replica ID information can accurately be retrieved only when the cluster
// is healthy. Otherwise, the replica request does not return the full set
// of initial replicas. This information is needed to provide context when
// a health check returns an error.
func getHealthyClusterReplicaInfo(retryOptions localconfig.Retry, haltChan chan struct{}, brokers []string, brokerConfig *sarama.Config, channel channel) ([]int32, error) {
	var replicaIDs []int32

	retryMsg := "Getting list of Kafka brokers replicating the channel"
	getReplicaInfo := newRetryProcess(retryOptions, haltChan, channel, retryMsg, func() error {
		client, err := sarama.NewClient(brokers, brokerConfig)
		if err != nil {
			return err
		}
		defer client.Close()

		replicaIDs, err = client.Replicas(channel.topic(), channel.partition())
		if err != nil {
			return err
		}
		return nil
	})

	return replicaIDs, getReplicaInfo.retry()
}
