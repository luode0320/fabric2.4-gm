/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
)

// HandlerRegistry 维护链码处理程序实例。
type HandlerRegistry struct {
	allowUnsolicitedRegistration bool                    // 是否允许未经请求的注册
	mutex                        sync.Mutex              // 互斥锁，用于保护 handlers 和 launching
	handlers                     map[string]*Handler     // 链码 cname 到关联处理程序的映射
	launching                    map[string]*LaunchState // 正在启动的链码到 LaunchState 的映射
}

type LaunchState struct {
	mutex    sync.Mutex
	notified bool
	done     chan struct{}
	err      error
}

func NewLaunchState() *LaunchState {
	return &LaunchState{
		done: make(chan struct{}),
	}
}

func (l *LaunchState) Done() <-chan struct{} {
	return l.done
}

func (l *LaunchState) Err() error {
	l.mutex.Lock()
	err := l.err
	l.mutex.Unlock()
	return err
}

func (l *LaunchState) Notify(err error) {
	l.mutex.Lock()
	if !l.notified {
		l.notified = true
		l.err = err
		close(l.done)
	}
	l.mutex.Unlock()
}

// NewHandlerRegistry 构造一个HandlerRegistry维护链码处理程序实例。
func NewHandlerRegistry(allowUnsolicitedRegistration bool) *HandlerRegistry {
	return &HandlerRegistry{
		handlers:                     map[string]*Handler{},        // 链码 cname 到关联处理程序的映射
		launching:                    map[string]*LaunchState{},    // 正在启动的链码到 LaunchState 的映射
		allowUnsolicitedRegistration: allowUnsolicitedRegistration, // 是否允许未经请求的注册
	}
}

// Launching 表示链码正在启动。返回的 LaunchState 提供了确定操作何时完成以及是否失败的机制。bool 值指示链码是否已经启动。
// 方法接收者：
//   - r：HandlerRegistry 结构体的指针，表示处理程序注册表
//
// 输入参数：
//   - ccid：string 类型，表示链码的 ChaincodeID
//
// 返回值：
//   - *LaunchState：表示一个 LaunchState 实例，提供了操作完成和失败状态的机制
//   - bool：表示链码是否已经启动
func (r *HandlerRegistry) Launching(ccid string) (*LaunchState, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// 如果链码已经在启动中或已经启动
	if launchState, ok := r.launching[ccid]; ok {
		return launchState, true
	}

	// 如果处理程序已经注册但未经过启动
	if _, ok := r.handlers[ccid]; ok {
		launchState := NewLaunchState()
		launchState.Notify(nil)
		return launchState, true
	}

	// 第一次尝试启动，需要启动运行时
	launchState := NewLaunchState()
	r.launching[ccid] = launchState
	return launchState, false
}

// Ready indicates that the chaincode registration has completed and the
// READY response has been sent to the chaincode.
func (r *HandlerRegistry) Ready(ccid string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	launchStatus := r.launching[ccid]
	if launchStatus != nil {
		launchStatus.Notify(nil)
	}
}

// Failed indicates that registration of a launched chaincode has failed.
func (r *HandlerRegistry) Failed(ccid string, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	launchStatus := r.launching[ccid]
	if launchStatus != nil {
		launchStatus.Notify(err)
	}
}

// Handler retrieves the handler for a chaincode instance.
func (r *HandlerRegistry) Handler(ccid string) *Handler {
	r.mutex.Lock()
	h := r.handlers[ccid]
	r.mutex.Unlock()
	return h
}

// Register adds a chaincode handler to the registry.
// An error will be returned if a handler is already registered for the
// chaincode. An error will also be returned if the chaincode has not already
// been "launched", and unsolicited registration is not allowed.
func (r *HandlerRegistry) Register(h *Handler) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.handlers[h.chaincodeID] != nil {
		chaincodeLogger.Debugf("duplicate registered handler(key:%s) return error", h.chaincodeID)
		return errors.Errorf("duplicate chaincodeID: %s", h.chaincodeID)
	}

	// This chaincode was not launched by the peer but is attempting
	// to register. Only allowed in development mode.
	if r.launching[h.chaincodeID] == nil && !r.allowUnsolicitedRegistration {
		return errors.Errorf("peer will not accept external chaincode connection %s (except in dev mode)", h.chaincodeID)
	}

	r.handlers[h.chaincodeID] = h

	chaincodeLogger.Debugf("registered handler complete for chaincode %s", h.chaincodeID)
	return nil
}

// Deregister clears references to state associated specified chaincode.
// As part of the cleanup, it closes the handler so it can cleanup any state.
// If the registry does not contain the provided handler, an error is returned.
func (r *HandlerRegistry) Deregister(ccid string) error {
	chaincodeLogger.Debugf("deregister handler: %s", ccid)

	r.mutex.Lock()
	handler := r.handlers[ccid]
	delete(r.handlers, ccid)
	delete(r.launching, ccid)
	r.mutex.Unlock()

	if handler == nil {
		return errors.Errorf("could not find handler: %s", ccid)
	}

	handler.Close()

	chaincodeLogger.Debugf("deregistered handler with key: %s", ccid)
	return nil
}

// TxQueryExecutorGetter 用于获取事务查询执行器。
type TxQueryExecutorGetter struct {
	HandlerRegistry *HandlerRegistry // 处理程序注册表
	CCID            string           // 链码 ID
}

func (g *TxQueryExecutorGetter) TxQueryExecutor(chainID, txID string) ledger.SimpleQueryExecutor {
	handler := g.HandlerRegistry.Handler(g.CCID)
	if handler == nil {
		return nil
	}
	txContext := handler.TXContexts.Get(chainID, txID)
	if txContext == nil {
		return nil
	}
	return txContext.TXSimulator
}
