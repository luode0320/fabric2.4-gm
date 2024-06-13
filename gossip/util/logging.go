/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"go.uber.org/zap/zapcore"
)

// Logger names for logger initialization.
const (
	ChannelLogger     = "gossip.channel"
	CommLogger        = "gossip.comm"
	DiscoveryLogger   = "gossip.discovery"
	ElectionLogger    = "gossip.election"
	GossipLogger      = "gossip.gossip"
	CommMockLogger    = "gossip.comm.mock"
	PullLogger        = "gossip.pull"
	ServiceLogger     = "gossip.service"
	StateLogger       = "gossip.state"
	PrivateDataLogger = "gossip.privdata"
)

var (
	loggers  = make(map[string]Logger)
	lock     = sync.Mutex{}
	testMode bool
)

// defaultTestSpec is the default logging level for gossip tests
var defaultTestSpec = "WARNING"

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})
	IsEnabledFor(l zapcore.Level) bool
	With(args ...interface{}) *flogging.FabricLogger
}

// GetLogger 根据给定的 gossip 日志记录器名称和 peerID 返回一个日志记录器。
//
// 输入参数：
//   - name：gossip 日志记录器的名称。
//   - peerID：peer 的标识符。
//
// 返回值：
//   - Logger：Logger 实例，用于记录日志。
func GetLogger(name string, peerID string) Logger {
	// 如果 peerID 不为空且处于测试模式下，则将名称添加上 peerID
	if peerID != "" && testMode {
		name = name + "#" + peerID
	}

	lock.Lock()
	defer lock.Unlock()

	// 检查是否已存在该名称的日志记录器
	if lgr, ok := loggers[name]; ok {
		return lgr
	}

	// 如果日志记录器不存在，则创建一个新的日志记录器
	lgr := flogging.MustGetLogger(name)
	loggers[name] = lgr
	return lgr
}

// SetupTestLogging sets the default log levels for gossip unit tests to defaultTestSpec
func SetupTestLogging() {
	SetupTestLoggingWithLevel(defaultTestSpec)
}

// SetupTestLoggingWithLevel sets the default log levels for gossip unit tests to level
func SetupTestLoggingWithLevel(level string) {
	testMode = true
	flogging.ActivateSpec(level)
}
