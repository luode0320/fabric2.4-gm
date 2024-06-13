/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"github.com/hyperledger/fabric/common/policies"
)

// LogSanityChecks 日志健全性检查, 打印策略日志
func LogSanityChecks(res Resources) {
	pm := res.PolicyManager()
	for _, policyName := range []string{policies.ChannelReaders, policies.ChannelWriters} {
		_, ok := pm.GetPolicy(policyName)
		if !ok {
			logger.Warningf("当前配置没有策略 '%s', 这可能会导致生产系统出现问题", policyName)
		} else {
			logger.Debugf("正如预期的那样，当前配置具有策略 '%s'", policyName)
		}
	}
	if _, ok := pm.Manager([]string{policies.ApplicationPrefix}); ok {
		// 如果定义了应用程序组件，则检查默认应用程序策略
		for _, policyName := range []string{
			policies.ChannelApplicationReaders, // 是通道的应用程序读取器策略的标签
			policies.ChannelApplicationWriters, // 是通道的应用程序编写器策略的标签
			policies.ChannelApplicationAdmins,  // 是通道的应用程序管理策略的标签
		} {
			_, ok := pm.GetPolicy(policyName)
			if !ok {
				logger.Warningf("当前配置没有策略 '%s',这可能会导致生产系统出现问题", policyName)
			} else {
				logger.Debugf("正如预期的那样，当前配置具有策略 '%s'", policyName)
			}
		}
	}
	if _, ok := pm.Manager([]string{policies.OrdererPrefix}); ok {
		for _, policyName := range []string{
			policies.BlockValidation,       // 是应验证通道的块签名的策略的标签
			policies.ChannelOrdererAdmins,  // 是通道的orderer admin策略的标签
			policies.ChannelOrdererWriters, // 是通道的orderer writers策略的标签
			policies.ChannelOrdererReaders, // 是通道的orderer readers的策略
		} {
			_, ok := pm.GetPolicy(policyName)
			if !ok {
				logger.Warningf("当前配置没有策略 '%s', 这可能会导致生产系统出现问题", policyName)
			} else {
				logger.Debugf("正如预期的那样，当前配置具有策略 '%s'", policyName)
			}
		}
	}
}
