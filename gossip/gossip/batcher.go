/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type emitBatchCallback func([]interface{})

// batchingEmitter 用于gossip推送/转发阶段。
// 消息被添加到batchingEmitter中，定期分批转发T次然后丢弃。
// 如果batchingEmitter存储的消息数量达到一定容量，也会触发消息分发
type batchingEmitter interface {
	// Add 添加要批量处理的消息
	Add(interface{})

	// Stop 停止组件
	Stop()

	// Size 返回要发出的待处理消息的数量
	Size() int
}

// 接受以下参数：
// iterations: 每条消息被转发的次数
// burstSize：由于消息计数而触发转发的阈值
// Latency：每条消息在不被转发的情况下可以存储的最大延迟
// cb：为了进行转发而调用的回调
func newBatchingEmitter(iterations, burstSize int, latency time.Duration, cb emitBatchCallback) batchingEmitter {
	if iterations < 0 {
		panic(errors.Errorf("peer.gossip.propagateIterations配置, 消息被推送到远程对等方的次数为负"))
	}

	// 批量发送器的实现。
	p := &batchingEmitterImpl{
		cb:         cb,                         // 批量发送回调函数
		delay:      latency,                    // 每次迭代之间的延迟
		iterations: iterations,                 // 迭代次数
		burstSize:  burstSize,                  // 每次迭代的批量大小
		lock:       &sync.Mutex{},              // 互斥锁
		buff:       make([]*batchedMessage, 0), // 批量消息缓冲区
		stopFlag:   int32(0),                   // 停止标志
	}

	if iterations != 0 {
		// 周期性发射
		go p.periodicEmit()
	}

	return p
}

// 周期性发射
func (p *batchingEmitterImpl) periodicEmit() {
	for !p.toDie() {
		time.Sleep(p.delay)
		p.lock.Lock()
		// 执行批量发送操作。
		p.emit()
		p.lock.Unlock()
	}
}

// emit 执行批量发送操作。
func (p *batchingEmitterImpl) emit() {
	// 如果发送器已经停止，则直接返回
	if p.toDie() {
		return
	}

	// 如果批量消息缓冲区为空，则直接返回
	if len(p.buff) == 0 {
		return
	}

	// 创建一个长度为 len(p.buff) 的 msgs2beEmitted 切片，用于存储待发送的消息
	msgs2beEmitted := make([]interface{}, len(p.buff))

	// 遍历批量消息缓冲区，将每个批量消息的数据存储到 msgs2beEmitted 切片中
	for i, v := range p.buff {
		msgs2beEmitted[i] = v.data
	}

	p.cb(msgs2beEmitted) // 调用批量发送回调函数，将 msgs2beEmitted 作为参数传递给该函数，执行批量发送操作

	// 减少计数器(发送的次数)
	p.decrementCounters()
}

func (p *batchingEmitterImpl) decrementCounters() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		msg.iterationsLeft--
		if msg.iterationsLeft == 0 {
			p.buff = append(p.buff[:i], p.buff[i+1:]...)
			n--
			i--
		}
	}
}

func (p *batchingEmitterImpl) toDie() bool {
	return atomic.LoadInt32(&(p.stopFlag)) == int32(1)
}

// batchingEmitterImpl 是批量发送器的实现。
type batchingEmitterImpl struct {
	iterations int               // 迭代次数
	burstSize  int               // 每次迭代的批量大小
	delay      time.Duration     // 每次迭代之间的延迟
	cb         emitBatchCallback // 批量发送回调函数
	lock       *sync.Mutex       // 互斥锁
	buff       []*batchedMessage // 批量消息缓冲区
	stopFlag   int32             // 停止标志
}

type batchedMessage struct {
	data           interface{}
	iterationsLeft int
}

func (p *batchingEmitterImpl) Stop() {
	atomic.StoreInt32(&(p.stopFlag), int32(1))
}

func (p *batchingEmitterImpl) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.buff)
}

// Add 方法用于将消息添加到批量发射器的缓冲区中，当达到预设的爆发大小时，将批量发送消息。
func (p *batchingEmitterImpl) Add(message interface{}) {
	// 检查是否还有迭代次数，如果没有则直接返回，不执行任何操作
	if p.iterations == 0 {
		return
	}

	// 加锁以确保并发安全性
	p.lock.Lock()
	defer p.lock.Unlock()

	// 将消息封装为 batchedMessage 类型，其中包含原始数据和剩余的迭代次数
	p.buff = append(p.buff, &batchedMessage{
		data:           message,      // 消息数据
		iterationsLeft: p.iterations, // 剩余迭代次数，表示消息需要传播的次数
	})

	// 检查缓冲区中的消息数量是否达到了爆发大小
	if len(p.buff) >= p.burstSize {
		// 如果达到，则触发消息的批量发送
		p.emit()
	}
}
