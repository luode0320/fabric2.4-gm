/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

//go:generate mockery -dir . -name StepClient -case underscore -output ./mocks/

// StepClient defines a client that sends and receives Step requests and responses.
type StepClient interface {
	Send(*orderer.StepRequest) error
	Recv() (*orderer.StepResponse, error)
	grpc.ClientStream
}

//go:generate mockery -dir . -name ClusterClient -case underscore -output ./mocks/

// ClusterClient creates streams that point to a remote cluster member.
type ClusterClient interface {
	Step(ctx context.Context, opts ...grpc.CallOption) (orderer.Cluster_StepClient, error)
}

// RPC 结构体定义了用于与远程集群节点执行远程过程调用（RPC）的组件和状态。
type RPC struct {
	consensusLock sync.Mutex             // 互斥锁，用于保护共识相关的操作，确保线程安全。
	submitLock    sync.Mutex             // 互斥锁，用于保护提交操作，避免并发提交导致的问题。
	Logger        *flogging.FabricLogger // 日志记录器，用于记录RPC模块的运行日志。

	Timeout time.Duration // 设置RPC调用的超时时间，单位通常为毫秒。
	Channel string        // 当前RPC调用所针对的通道名称。
	Comm    Communicator  // 通信器接口，用于实际执行网络通信，发送和接收RPC调用。

	lock          sync.RWMutex                         // 读写锁，用于保护StreamsByType字段，允许多个读取操作但只允许一个写入操作。
	StreamsByType map[OperationType]map[uint64]*Stream // 按照操作类型分类的流集合，用于管理与不同类型的RPC操作相关的流。
	// OperationType 是一个枚举类型，表示不同的RPC操作类型，如读取、写入等。
	// Stream 是代表RPC会话的结构体，包含与特定RPC调用相关的状态和数据。
}

// NewStreamsByType 函数创建并返回一个映射，该映射的键是 OperationType（操作类型），
// 值是另一个映射，这个内部映射的键是目的地节点的ID（uint64类型），值是与该目的地节点关联的 Stream 对象。
// 这个函数初始化了共识操作和提交操作两种类型下的目的地到 Stream 的映射，
// 为后续的通信提供了组织结构，便于管理和追踪不同操作类型下的通信流。
func NewStreamsByType() map[OperationType]map[uint64]*Stream {
	m := make(map[OperationType]map[uint64]*Stream)
	m[ConsensusOperation] = make(map[uint64]*Stream) // 初始化共识操作下的目的地到 Stream 的映射
	m[SubmitOperation] = make(map[uint64]*Stream)    // 初始化提交操作下的目的地到 Stream 的映射
	return m
}

// OperationType 枚举类型表示RPC可以执行的操作类型，
// 比如发送一个交易，或者是一个与共识相关的消息。
type OperationType int

const (
	// ConsensusOperation 表示共识相关的操作，如投票、心跳等共识协议中的消息交互。
	ConsensusOperation OperationType = iota

	// SubmitOperation 表示提交操作，通常是指向Orderer提交交易或配置更新等数据的RPC调用。
	SubmitOperation
)

// String 方法实现了fmt.Stringer接口，用于将OperationType转换为字符串表示形式。
// 根据OperationType的值，返回相应的字符串描述。
func (ot OperationType) String() string {
	if ot == SubmitOperation {
		return "transaction" // 如果是SubmitOperation，返回"transaction"字符串
	}

	return "consensus" // 否则，返回"consensus"字符串，假设为共识相关操作
}

// SendConsensus 将给定的 ConsensusRequest 消息传递给raft.Node实例。
// 方法接收者：s（RPC类型的指针）
// 输入参数：
//   - destination：目标节点的ID。
//   - msg：ConsensusRequest消息。
//
// 返回值：
//   - error：如果在发送消息时出错，则返回错误。
func (s *RPC) SendConsensus(destination uint64, msg *orderer.ConsensusRequest) error {
	if s.Logger.IsEnabledFor(zapcore.DebugLevel) {
		defer s.consensusSent(time.Now(), destination, msg)
	}

	// 获取或创建与目标节点的流,这里是发送请求的核心位置
	stream, err := s.getOrCreateStream(destination, ConsensusOperation)
	if err != nil {
		return err
	}

	// 创建StepRequest消息
	req := &orderer.StepRequest{
		Payload: &orderer.StepRequest_ConsensusRequest{
			ConsensusRequest: msg,
		},
	}

	// 加锁以确保同一时间只有一个goroutine发送消息
	s.consensusLock.Lock()
	defer s.consensusLock.Unlock()

	// 发送消息到流
	err = stream.Send(req)
	if err != nil {
		s.unMapStream(destination, ConsensusOperation, stream.ID)
	}

	return err
}

// SendSubmit 方法向指定目标节点发送 SubmitRequest。
func (s *RPC) SendSubmit(destination uint64, request *orderer.SubmitRequest, report func(error)) error {
	if s.Logger.IsEnabledFor(zapcore.DebugLevel) {
		defer s.submitSent(time.Now(), destination, request)
	}

	// 获取或创建与目标节点(领导者)的流,这里是发送请求的核心位置
	stream, err := s.getOrCreateStream(destination, SubmitOperation)
	if err != nil {
		return err
	}

	// 构建 StepRequest
	req := &orderer.StepRequest{
		Payload: &orderer.StepRequest_SubmitRequest{
			SubmitRequest: request,
		},
	}

	// 处理发送失败时的操作
	unmapOnFailure := func(err error) {
		if err != nil && err.Error() == io.EOF.Error() {
			s.Logger.Infof("因遇到过时流，取消映射到 %d 的事务流", destination)
			s.unMapStream(destination, SubmitOperation, stream.ID)
		}
		report(err)
	}

	// 加锁保证发送的原子性
	s.submitLock.Lock()
	defer s.submitLock.Unlock()

	// 发送请求并处理发送结果, 方法向远程集群成员发送给定请求，并在发送结果上调用 unmapOnFailure 函数。
	err = stream.SendWithReport(req, unmapOnFailure)
	if err != nil {
		// 取消映射指定类型和目标节点的流
		s.unMapStream(destination, SubmitOperation, stream.ID)
	}
	return err
}

func (s *RPC) submitSent(start time.Time, to uint64, msg *orderer.SubmitRequest) {
	s.Logger.Debugf("Sending msg of %d bytes to %d on channel %s took %v", submitMsgLength(msg), to, s.Channel, time.Since(start))
}

func (s *RPC) consensusSent(start time.Time, to uint64, msg *orderer.ConsensusRequest) {
	s.Logger.Debugf("Sending msg of %d bytes to %d on channel %s took %v", len(msg.Payload), to, s.Channel, time.Since(start))
}

// getOrCreateStream 获取给定目标节点的Submit流,这里是发送请求的核心位置。
// 方法接收者：s（RPC类型的指针）
// 输入参数：
//   - destination：目标节点的ID。
//   - operationType：操作类型。
//
// 返回值：
//   - *Stream：Submit流。
//   - error：如果在获取或创建流时出错，则返回错误。
func (s *RPC) getOrCreateStream(destination uint64, operationType OperationType) (*Stream, error) {
	// 检查是否已经存在与目标节点的流
	stream := s.getStream(destination, operationType)
	if stream != nil {
		return stream, nil
	}

	// 获取与目标节点的远程通信stub
	stub, err := s.Comm.Remote(s.Channel, destination)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 创建新的流,这里是发送请求的核心位置
	stream, err = stub.NewStream(s.Timeout)
	if err != nil {
		return nil, err
	}

	// 将流映射到目标节点和操作类型
	s.mapStream(destination, stream, operationType)

	return stream, nil
}

func (s *RPC) getStream(destination uint64, operationType OperationType) *Stream {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.StreamsByType[operationType][destination]
}

func (s *RPC) mapStream(destination uint64, stream *Stream, operationType OperationType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.StreamsByType[operationType][destination] = stream
	s.cleanCanceledStreams(operationType)
}

// 取消映射指定类型和目标节点的流
func (s *RPC) unMapStream(destination uint64, operationType OperationType, streamIDToUnmap uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 获取指定类型和目标节点的流
	stream, exists := s.StreamsByType[operationType][destination]
	if !exists {
		s.Logger.Debugf("未找到发送至 %d 的 %s 流，无需取消映射", destination, operationType)
		return
	}

	// 检查要取消映射的流是否匹配
	if stream.ID != streamIDToUnmap {
		s.Logger.Debugf("%s 流至 %d 的 ID 为 %d，与要取消映射的 ID %d 不匹配", operationType, destination, stream.ID, streamIDToUnmap)
		return
	}

	// 取消映射指定类型和目标节点的流
	delete(s.StreamsByType[operationType], destination)
}

func (s *RPC) cleanCanceledStreams(operationType OperationType) {
	for destination, stream := range s.StreamsByType[operationType] {
		if !stream.Canceled() {
			continue
		}
		s.Logger.Infof("Removing stream %d to %d for channel %s because it is canceled", stream.ID, destination, s.Channel)
		delete(s.StreamsByType[operationType], destination)
	}
}

func submitMsgLength(request *orderer.SubmitRequest) int {
	if request.Payload == nil {
		return 0
	}
	return len(request.Payload.Payload)
}
