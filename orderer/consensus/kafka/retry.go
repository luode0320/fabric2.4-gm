/*
版权所有 IBM Corp. 保留所有权利。

许可证标识符：Apache-2.0
*/

package kafka

import (
	"fmt"
	"time"

	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
)

// retryProcess 结构体封装了重试逻辑，用于在遇到错误时尝试重新执行某个函数。
type retryProcess struct {
	shortPollingInterval, shortTimeout time.Duration // 短期轮询间隔与总超时时间
	longPollingInterval, longTimeout   time.Duration // 长期轮询间隔与总超时时间
	exit                               chan struct{} // 退出信号通道
	channel                            channel       // 当前通道信息
	msg                                string        // 重试过程中的日志信息
	fn                                 func() error  // 尝试执行的函数
}

// newRetryProcess 创建一个新的重试过程实例。
func newRetryProcess(retryOptions localconfig.Retry, exit chan struct{}, channel channel, msg string, fn func() error) *retryProcess {
	return &retryProcess{
		shortPollingInterval: retryOptions.ShortInterval,
		shortTimeout:         retryOptions.ShortTotal,
		longPollingInterval:  retryOptions.LongInterval,
		longTimeout:          retryOptions.LongTotal,
		exit:                 exit,
		channel:              channel,
		msg:                  msg,
		fn:                   fn,
	}
}

// retry 执行重试逻辑，首先尝试快速重试，若失败则转为慢速重试。
func (rp *retryProcess) retry() error {
	if err := rp.try(rp.shortPollingInterval, rp.shortTimeout); err != nil {
		logger.Debugf("[channel: %s] 切换到长重试间隔", rp.channel.topic())
		return rp.try(rp.longPollingInterval, rp.longTimeout)
	}
	return nil
}

// try 实施一次重试尝试，根据给定的间隔和总超时时间。
func (rp *retryProcess) try(interval, total time.Duration) (err error) {
	// 确保轮询间隔有效，避免无效配置导致的恐慌
	if rp.shortPollingInterval == 0 {
		return fmt.Errorf("非法值")
	}

	// 首次尝试成功则直接返回
	logger.Debugf("[channel: %s] %s", rp.channel.topic(), rp.msg)
	if err = rp.fn(); err == nil {
		logger.Debugf("[channel: %s] 重试成功，跳出重试循环", rp.channel.topic())
		return
	}

	logger.Debugf("[channel: %s] 初始化尝试失败 = %s", rp.channel.topic(), err)

	// 启动定时器进行周期性重试
	tickInterval := time.NewTicker(interval)
	tickTotal := time.NewTicker(total)
	defer tickTotal.Stop()
	defer tickInterval.Stop()

	logger.Debugf("[channel: %s] 每隔 %s 重试，总共重试 %s", rp.channel.topic(), interval.String(), total.String())

	for {
		select {
		case <-rp.exit:
			logger.Warningf("[channel: %s] 接收到退出信号", rp.channel.topic())
			return fmt.Errorf("接收到退出指令")
		case <-tickTotal.C:
			// 总超时，结束重试
			return
		case <-tickInterval.C:
			logger.Debugf("[channel: %s] %s", rp.channel.topic(), rp.msg)
			if err = rp.fn(); err == nil {
				logger.Debugf("[channel: %s] 重试成功，跳出重试循环", rp.channel.topic())
				return
			}

			logger.Debugf("[channel: %s] 由于操作失败 = %s，需要继续重试", rp.channel.topic(), err)
		}
	}
}
