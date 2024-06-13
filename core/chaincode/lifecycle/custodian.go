/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lifecycle

import (
	"sync"

	"github.com/hyperledger/fabric/core/container"
)

//go:generate counterfeiter -o mock/chaincode_launcher.go --fake-name ChaincodeLauncher . ChaincodeLauncher
type ChaincodeLauncher interface {
	Launch(ccid string) error
	Stop(ccid string) error
}

// ChaincodeCustodian 负责在链码可用时将其构建和启动排队，并在不再由活动链码定义引用时停止。
type ChaincodeCustodian struct {
	cond       *sync.Cond        // 条件变量，用于同步协程之间的操作
	mutex      sync.Mutex        // 互斥锁，用于保护共享资源的访问
	choreQueue []*chaincodeChore // 链码任务队列，存储待处理的链码任务
	halt       bool              // 标志位，表示是否停止链码处理
}

// chaincodeChore 表示工作单元，由工作协程执行。
// 它标识与工作相关联的链码。如果工作是启动链码，则 runnable 为 true（stoppable 为 false）。
// 如果工作是停止链码，则 stoppable 为 true（runnable 为 false）。
// 如果工作仅是构建链码，则 runnable 和 stoppable 均为 false。
type chaincodeChore struct {
	chaincodeID string // 链码的标识符
	runnable    bool   // 是否可运行（启动链码）
	stoppable   bool   // 是否可停止（停止链码）
}

// NewChaincodeCustodian 创建一个链码保管者的实例。
// 实例化者的责任是生成一个 goroutine 来处理 Work 方法以及相应的依赖关系。
//
// 返回值：
//   - *ChaincodeCustodian：链码保管者实例。
func NewChaincodeCustodian() *ChaincodeCustodian {
	cc := &ChaincodeCustodian{}
	cc.cond = sync.NewCond(&cc.mutex)
	return cc
}

func (cc *ChaincodeCustodian) NotifyInstalled(chaincodeID string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.choreQueue = append(cc.choreQueue, &chaincodeChore{
		chaincodeID: chaincodeID,
	})
	cc.cond.Signal()
}

func (cc *ChaincodeCustodian) NotifyInstalledAndRunnable(chaincodeID string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.choreQueue = append(cc.choreQueue, &chaincodeChore{
		chaincodeID: chaincodeID,
		runnable:    true,
	})
	cc.cond.Signal()
}

func (cc *ChaincodeCustodian) NotifyStoppable(chaincodeID string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.choreQueue = append(cc.choreQueue, &chaincodeChore{
		chaincodeID: chaincodeID,
		stoppable:   true,
	})
	cc.cond.Signal()
}

func (cc *ChaincodeCustodian) Close() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.halt = true
	cc.cond.Signal()
}

// Work 是 ChaincodeCustodian 的工作函数，用于执行链码的构建、启动和停止任务
// 方法接收者：
//   - cc：ChaincodeCustodian 结构体的指针，表示链码的管理者
//
// 输入参数：
//   - buildRegistry：container.BuildRegistry 结构体的指针，表示链码构建的注册表
//   - builder：ChaincodeBuilder 接口的实例，用于构建链码
//   - launcher：ChaincodeLauncher 接口的实例，用于启动和停止链码
func (cc *ChaincodeCustodian) Work(buildRegistry *container.BuildRegistry, builder ChaincodeBuilder, launcher ChaincodeLauncher) {
	for {
		cc.mutex.Lock()
		if len(cc.choreQueue) == 0 && !cc.halt {
			cc.cond.Wait()
		}
		if cc.halt {
			cc.mutex.Unlock()
			return
		}
		chore := cc.choreQueue[0]
		cc.choreQueue = cc.choreQueue[1:]
		cc.mutex.Unlock()

		if chore.runnable {
			// 启动链码
			if err := launcher.Launch(chore.chaincodeID); err != nil {
				logger.Warningf("无法启动链码 '%s': %s", chore.chaincodeID, err)
			}
			continue
		}

		if chore.stoppable {
			// 停止链码
			if err := launcher.Stop(chore.chaincodeID); err != nil {
				logger.Warningf("无法停止链码 '%s': %s", chore.chaincodeID, err)
			}
			continue
		}

		buildStatus, ok := buildRegistry.BuildStatus(chore.chaincodeID)
		if ok {
			logger.Debugf("正在跳过链码 %s 的生成，因为它已经在进行中", chore.chaincodeID)
			continue
		}
		// 构建链码
		err := builder.Build(chore.chaincodeID)
		if err != nil {
			logger.Warningf("无法生成链码 '%s': %s", chore.chaincodeID, err)
		}
		buildStatus.Notify(err)
	}
}
