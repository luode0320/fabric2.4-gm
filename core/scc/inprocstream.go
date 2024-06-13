/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package scc

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/hyperledger/fabric-protos-go/peer"
)

// SendPanicFailure
type SendPanicFailure string

func (e SendPanicFailure) Error() string {
	return fmt.Sprintf("send failure %s", string(e))
}

// PeerChaincodeStream 是 Peer 和链码实例之间的流接口
type inProcStream struct {
	recv      <-chan *pb.ChaincodeMessage // 接收通道，用于接收来自 Peer 的 ChaincodeMessage
	send      chan<- *pb.ChaincodeMessage // 发送通道，用于向 Peer 发送 ChaincodeMessage
	closeOnce sync.Once                   // 用于确保关闭操作只执行一次的同步对象
}

func newInProcStream(recv <-chan *pb.ChaincodeMessage, send chan<- *pb.ChaincodeMessage) *inProcStream {
	// 是 Peer 和链码实例之间的流接口
	return &inProcStream{recv: recv, send: send}
}

func (s *inProcStream) Send(msg *pb.ChaincodeMessage) (err error) {
	// send may happen on a closed channel when the system is
	// shutting down. Just catch the exception and return error
	defer func() {
		if r := recover(); r != nil {
			err = SendPanicFailure(fmt.Sprintf("%s", r))
			return
		}
	}()
	s.send <- msg
	return
}

func (s *inProcStream) Recv() (*pb.ChaincodeMessage, error) {
	msg, ok := <-s.recv
	if !ok {
		return nil, errors.New("channel is closed")
	}
	return msg, nil
}

func (s *inProcStream) CloseSend() error {
	s.closeOnce.Do(func() { close(s.send) })
	return nil
}
