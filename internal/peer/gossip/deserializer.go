/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	mspproto "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protoutil"
)

// DeserializersManager 是访问本地和通道反序列化器的支持接口
type DeserializersManager interface {

	// Deserialize 接收SerializedIdentity字节并反序列化为 mspproto.SerializedIdentity
	Deserialize(raw []byte) (*mspproto.SerializedIdentity, error)

	// GetLocalMSPIdentifier 返回本地MSP标识符
	GetLocalMSPIdentifier() string

	// GetLocalDeserializer 返回本地身份反序列化程序
	GetLocalDeserializer() msp.IdentityDeserializer

	// GetChannelDeserializers 返回通道反序列化器的映射
	GetChannelDeserializers() map[string]msp.IdentityDeserializer
}

// NewDeserializersManager 返回DeserializersManager的新实例
func NewDeserializersManager(localMSP msp.MSP) DeserializersManager {
	return &mspDeserializersManager{
		localMSP: localMSP,
	}
}

type mspDeserializersManager struct {
	localMSP msp.MSP
}

func (m *mspDeserializersManager) Deserialize(raw []byte) (*mspproto.SerializedIdentity, error) {
	return protoutil.UnmarshalSerializedIdentity(raw)
}

func (m *mspDeserializersManager) GetLocalMSPIdentifier() string {
	id, _ := m.localMSP.GetIdentifier()
	return id
}

func (m *mspDeserializersManager) GetLocalDeserializer() msp.IdentityDeserializer {
	return m.localMSP
}

func (m *mspDeserializersManager) GetChannelDeserializers() map[string]msp.IdentityDeserializer {
	return mgmt.GetDeserializers()
}
