/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package scc

import (
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/container/ccintf"
)

// SysCCVersion is a constant used for the version field of system chaincodes.
// Historically, the version of a system chaincode was implicitly tied to the exact
// build version of a peer, which does not work for collecting endorsements across
// sccs across peers.  Until there is a need for separate SCC versions, we use
// this constant here.
const SysCCVersion = "syscc"

// ChaincodeID returns the chaincode ID of a system chaincode of a given name.
func ChaincodeID(ccName string) string {
	return ccName + "." + SysCCVersion
}

// XXX should we change this name to actually match the package name? Leaving now for historical consistency
var sysccLogger = flogging.MustGetLogger("sccapi")

// BuiltinSCCs 是特殊的系统链码，与其他（插件）系统链码区分开来。
// 这些链码不需要在 '_lifecycle' 中进行初始化，并且可以在没有通道上下文的情况下调用。
// 预计 '_lifecycle' 最终将是唯一的内置 SCC。
// 注意，此字段只应在 _背书_ 方面使用，而不应在验证中使用，因为它可能会发生变化。
type BuiltinSCCs map[string]struct{}

func (bccs BuiltinSCCs) IsSysCC(name string) bool {
	_, ok := bccs[name]
	return ok
}

// ChaincodeStreamHandler 负责处理 ChaincodeStream 在对等节点和链码之间的通信
type ChaincodeStreamHandler interface {
	// HandleChaincodeStream 处理 ChaincodeStream 的通信
	HandleChaincodeStream(ccintf.ChaincodeStream) error
	// LaunchInProc 在进程中启动链码，并返回一个用于通知链码启动完成的通道
	LaunchInProc(packageID string) <-chan struct{}
}

// SelfDescribingSysCC 是自描述系统链码的接口
type SelfDescribingSysCC interface {
	// Name 返回系统链码的唯一名称
	Name() string

	// Chaincode 返回底层的链码
	Chaincode() shim.Chaincode
}

// DeploySysCC 是系统链码的钩子函数，用于将系统链码注册到 Fabric 中。
// 此调用直接将链码注册到链码处理程序，并绕过其他用户链码的构造。
// 输入参数：
//   - sysCC：SelfDescribingSysCC 接口的实例，表示自描述的系统链码
//   - chaincodeStreamHandler：ChaincodeStreamHandler 接口的实例，表示链码流处理程序
func DeploySysCC(sysCC SelfDescribingSysCC, chaincodeStreamHandler ChaincodeStreamHandler) {
	sysccLogger.Infof("部署系统链码 '%s'", sysCC.Name())

	// 获取系统链码的 ChaincodeID
	ccid := ChaincodeID(sysCC.Name())

	// 启动链码处理程序的内部进程，并返回一个完成信号通道, 在进程中启动链码，并返回一个用于通知链码启动完成的通道
	done := chaincodeStreamHandler.LaunchInProc(ccid)

	// 创建用于链码和对等节点之间通信的消息通道
	peerRcvCCSend := make(chan *pb.ChaincodeMessage)
	ccRcvPeerSend := make(chan *pb.ChaincodeMessage)

	// 启动链码支持流程
	go func() {
		// 创建链码支持流, 包装了接收和发送的通道
		stream := newInProcStream(peerRcvCCSend, ccRcvPeerSend)
		defer stream.CloseSend()

		sysccLogger.Debugf("启动链码-支持流  %s ", ccid)
		// 处理链码流
		err := chaincodeStreamHandler.HandleChaincodeStream(stream)
		sysccLogger.Criticalf("shim流以err结束: %v", err)
	}()

	// 启动链码进程
	go func(sysCC SelfDescribingSysCC) {
		// 创建链码流
		stream := newInProcStream(ccRcvPeerSend, peerRcvCCSend)
		defer stream.CloseSend()

		sysccLogger.Debugf("启动链码 %s", ccid)
		// 启动系统链码
		err := shim.StartInProc(ccid, stream, sysCC.Chaincode())
		sysccLogger.Criticalf("系统链码以err结尾: %v", err)
	}(sysCC)

	// 等待链码处理程序的内部进程完成
	<-done
}
