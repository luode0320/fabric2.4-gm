/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/gossip/api"
)

var saLogger = flogging.MustGetLogger("peer.gossip.sa")

// mspSecurityAdvisor 使用对等方的msp实现SecurityAdvisor接口。
//
// 为了使系统安全，让msp保持最新是至关重要的。
// 频道的msp通过
// 由订购服务分发的配置事务。
//
// 此实现假定这些机制都已到位并正常工作。
type mspSecurityAdvisor struct {
	deserializer DeserializersManager
}

// NewSecurityAdvisor 创建实现MessageCryptoService的mspSecurityAdvisor的新实例
func NewSecurityAdvisor(deserializer DeserializersManager) api.SecurityAdvisor {
	return &mspSecurityAdvisor{deserializer: deserializer}
}

// OrgByPeerIdentity 返回给定对等身份的 OrgIdentityType 组织的身份。
// 如果发生任何错误，则返回nil。
// 此方法不验证peerIdentity。 这个验证应该在执行流程中适当地完成。
func (advisor *mspSecurityAdvisor) OrgByPeerIdentity(peerIdentity api.PeerIdentityType) api.OrgIdentityType {
	// 验证参数
	if len(peerIdentity) == 0 {
		saLogger.Error("对等身份无效。它必须与nil不同.")

		return nil
	}

	// 请注意，peerIdentity被假定为身份的序列化。
	// 所以，第一步是身份反序列化

	// TODO: 此方法应返回由两个字段组成的结构:
	// 1. 标识所属MSP的MSPidentifier之一，
	// 2. 然后是这个身份所拥有的组织单位的列表。
	// 对于gossip使用，这是我们现在需要的第一部分，
	// 即返回身份的MSP标识符 (identity.GetMSPIdentifier())

	// 首先检查本地MSP。
	identity, err := advisor.deserializer.GetLocalDeserializer().DeserializeIdentity([]byte(peerIdentity))
	if err == nil {
		return []byte(identity.GetMSPIdentifier())
	}

	// 对照检查managers, GetChannelDeserializers: 获取通道反序列化器
	for chainID, mspManager := range advisor.deserializer.GetChannelDeserializers() {
		// 反序列化标识
		identity, err := mspManager.DeserializeIdentity([]byte(peerIdentity))
		if err != nil {
			saLogger.Debugf("失败的反序列化标识 [% x] on [%s]: [%s]", peerIdentity, chainID, err)
			continue
		}

		return []byte(identity.GetMSPIdentifier())
	}

	saLogger.Warningf("peer身份证书 [%s] 不能被分离msp id. 因为没有找到通道反序列化器 msp.IdentityDeserializer.", string(peerIdentity))

	return nil
}
