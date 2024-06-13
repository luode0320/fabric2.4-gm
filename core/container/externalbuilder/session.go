/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package externalbuilder

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/hyperledger/fabric/common/flogging"
)

type ExitFunc func(error)

type Session struct {
	mutex      sync.Mutex
	command    *exec.Cmd
	exited     chan struct{}
	exitErr    error
	waitStatus syscall.WaitStatus
	exitFuncs  []ExitFunc
}

// Start 方法用于启动提供的命令，并返回一个 Session 对象，该对象可用于等待命令完成或向进程发送信号。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - logger：*flogging.FabricLogger 类型，用于记录运行进程的标准错误输出。
//   - cmd：*exec.Cmd 类型，表示要启动的命令。
//   - exitFuncs：...ExitFunc 类型，表示在命令退出时要执行的函数。
//
// 返回值：
//   - *Session：表示命令的会话。
//   - error：如果启动命令时出错，则返回错误。
func Start(logger *flogging.FabricLogger, cmd *exec.Cmd, exitFuncs ...ExitFunc) (*Session, error) {
	// 使用命令的路径作为日志记录器的标签
	logger = logger.With("command", filepath.Base(cmd.Path))

	// 创建命令的标准错误输出管道
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	// 启动命令
	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// 创建并返回会话对象
	sess := &Session{
		command:   cmd,
		exitFuncs: exitFuncs,
		exited:    make(chan struct{}),
	}
	go sess.waitForExit(logger, stderr)

	return sess, nil
}

// waitForExit 方法用于等待命令退出并完成，并将标准错误输出复制到日志记录器。
// 方法接收者：s（Session类型的指针）
// 输入参数：
//   - logger：*flogging.FabricLogger 类型，用于记录命令的标准错误输出。
//   - stderr：io.Reader 类型，表示命令的标准错误输出。
func (s *Session) waitForExit(logger *flogging.FabricLogger, stderr io.Reader) {
	// 将标准错误输出复制到日志记录器，直到标准错误输出关闭
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		logger.Info(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		logger.Errorf("命令输出扫描失败: %s", err)
	}

	// 等待命令退出并完成
	err := s.command.Wait()

	// 更新状态并关闭退出通道
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.exitErr = err
	s.waitStatus = s.command.ProcessState.Sys().(syscall.WaitStatus)
	for _, exit := range s.exitFuncs {
		exit(s.exitErr)
	}
	close(s.exited)
}

// Wait 等待正在运行的命令终止并返回退出错误
// 从命令。如果命令已经退出，则退出err将为
// 立即返回。
func (s *Session) Wait() error {
	<-s.exited
	return s.exitErr
}

// Signal will send a signal to the running process.
func (s *Session) Signal(sig os.Signal) {
	s.command.Process.Signal(sig)
}
