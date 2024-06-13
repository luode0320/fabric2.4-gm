/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

type resources interface {
	// OrdererConfig returns the config.Orderer for the channel
	// and whether the Orderer config exists
	OrdererConfig() (channelconfig.Orderer, bool)
}

// NewExpirationRejectRule returns a rule that rejects messages signed by identities
// who's identities have expired, given the capability is active
func NewExpirationRejectRule(filterSupport resources) Rule {
	return &expirationRejectRule{filterSupport: filterSupport}
}

type expirationRejectRule struct {
	filterSupport resources
}

// Apply 检查创建信封的标识是否已过期
func (exp *expirationRejectRule) Apply(message *common.Envelope) error {
	ordererConf, ok := exp.filterSupport.OrdererConfig()
	if !ok {
		logger.Panic("编程错误: 未找到 orderer 配置")
	}
	if !ordererConf.Capabilities().ExpirationCheck() {
		return nil
	}
	signedData, err := protoutil.EnvelopeAsSignedData(message)
	if err != nil {
		return errors.Errorf("无法将消息转换为签名数据: %s", err)
	}
	expirationTime := crypto.ExpiresAt(signedData[0].Identity)
	// 身份尚未过期
	if expirationTime.IsZero() || time.Now().Before(expirationTime) {
		return nil
	}
	return errors.New("广播客户端身份标识已过期")
}
