/*
版权所有IBM Corp.保留所有权利。

SPDX-License-Identifier: Apache-2.0
*/

package solo // 定义了Solo共识类型的包

import (
	"fmt"
	"time" // 引入时间相关的包

	cb "github.com/hyperledger/fabric-protos-go/common"         // 引入Fabric的通用协议缓冲区定义
	flogging "github.com/hyperledger/fabric/common/flogging"    // 引入Fabric的日志记录模块
	consensus "github.com/hyperledger/fabric/orderer/consensus" // 引入共识接口定义
)

var logger = flogging.MustGetLogger("orderer.consensus.solo") // 初始化日志记录器，用于记录Solo共识相关日志

// consenter 结构体代表Solo共识的核心逻辑，当前为空，因为Solo共识逻辑主要在chain结构体中实现。
type consenter struct{}

// chain 结构体包含Solo共识链的主要状态和行为，包括与共识支持接口的交互、消息通道和退出信号通道。
type chain struct {
	support  consensus.ConsenterSupport // 提供对共识支持接口的引用
	sendChan chan *message              // 发送消息到主循环处理的通道
	exitChan chan struct{}              // 用于通知链停止的通道
}

// message 结构体用来封装待处理的消息，可以是普通交易消息或配置更新消息。
type message struct {
	configSeq uint64       // 配置序列号，用于跟踪配置更新
	normalMsg *cb.Envelope // 普通交易消息的Envelope
	configMsg *cb.Envelope // 配置更新消息的Envelope
}

// New 创建一个新的Solo共识实例。
func New() consensus.Consenter {
	return &consenter{} // 实例化空的consenter对象
}

// HandleChain 处理新链的初始化，警告用户Solo共识仅适用于测试。
func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	logger.Warningf("使用Solo排序服务已过时，仅限测试环境，并可能在未来移除。")
	return newChain(support), nil // 创建并返回新的chain实例
}

// newChain 创建一个新的chain实例，初始化其内部状态。
func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support:  support,
		sendChan: make(chan *message), // 初始化消息发送通道
		exitChan: make(chan struct{}), // 初始化退出通道
	}
}

// Start 启动链的主循环处理过程。
func (ch *chain) Start() {
	go ch.main() // 在新goroutine中运行链的主循环
}

// Halt 安全地停止链的运行。
func (ch *chain) Halt() {
	select {
	case <-ch.exitChan: // 如果已经退出，则什么也不做
	default: // 否则，关闭退出通道以触发停止
		close(ch.exitChan)
	}
}

// WaitReady 立即返回，因为Solo共识不需要额外的准备阶段。
func (ch *chain) WaitReady() error {
	return nil
}

// Order 接受普通交易消息并将其放入处理队列。
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		normalMsg: env,
	}: // 尝试发送消息
		return nil
	case <-ch.exitChan: // 如果链正在退出，则返回错误
		return fmt.Errorf("退出中")
	}
}

// Configure 接受配置更新消息并将其放入处理队列。
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}: // 尝试发送配置消息
		return nil
	case <-ch.exitChan: // 如果链正在退出，则返回错误
		return fmt.Errorf("退出中")
	}
}

// Errored 返回一个通道，当链遇到错误或退出时关闭。
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan // 直接返回退出通道
}

// main 是链的主循环函数，处理消息、执行批次切割、生成区块并写入账本。
func (ch *chain) main() {
	var timer <-chan time.Time // 定时器通道，用于控制批次超时
	var err error              // 用于存储处理消息时发生的错误

	for {
		seq := ch.support.Sequence() // 获取当前的配置序列号。详细见附录Sequence
		err = nil                    // 重置错误

		select {

		// 处理接收到的消息
		case msg := <-ch.sendChan:
			// 根据消息类型分别处理。详细见附录configMsg
			if msg.configMsg == nil {
				// 普通消息处理逻辑, 如果消息的序号小于通道的需要, 需要检测有效性
				if msg.configSeq < seq {
					// 检查消息的配置序列号是否过期。详细见附录ProcessNormalMsg
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					if err != nil {
						logger.Warningf("丢弃无效的普通消息: %s", err)
						continue
					}
				}
				// 对消息进行批次切割，并写入区块
				batches, pending := ch.support.BlockCutter().Ordered(msg.normalMsg)
				for _, batch := range batches {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}
				// 根据是否有待处理消息管理定时器
				if timer != nil && !pending {
					timer = nil // 无待处理消息，关闭定时器
				} else if timer == nil && pending {
					timer = time.After(ch.support.SharedConfig().BatchTimeout()) // 启动定时器
					logger.Debugf("开始 %s 批次超时计时", ch.support.SharedConfig().BatchTimeout().String())
				}
			} else {
				// 配置更新消息处理逻辑
				if msg.configSeq < seq {
					// 检查配置消息的序列号是否过期
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("丢弃无效的配置消息: %s", err)
						continue
					}
				}
				// 切割当前批次并处理配置更新
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}
				// 创建并写入配置区块
				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil // 处理完配置消息后重置定时器
			}

		// 定时器触发逻辑
		case <-timer:
			timer = nil // 清除定时器
			batch := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Warningf("批次超时，但无待处理请求，可能表明存在错误")
				continue
			}
			logger.Debugf("批次超时，创建新区块")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, nil)

		// 收到退出信号时，结束循环
		case <-ch.exitChan:
			logger.Debugf("退出主循环")
			return
		}
	}
}
