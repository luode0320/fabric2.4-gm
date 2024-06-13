/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package aclmgmt

import "github.com/hyperledger/fabric/core/policy"

// implementation of aclMgmt. CheckACL calls in fabric result in the following flow
//
//	if resourceProvider[resourceName]
//	   return resourceProvider[resourceName].CheckACL(...)
//	else
//	   return defaultProvider[resourceName].CheckACL(...)
//
// with rescfgProvider encapsulating resourceProvider and defaultProvider
type aclMgmtImpl struct {
	// resource provider gets resource information from config
	rescfgProvider ACLProvider
}

// CheckACL 检查通道上资源的ACL（访问控制列表），使用idinfo进行测试。
// 方法接收者：am（aclMgmtImpl类型的指针）
// 输入参数：
//   - resName：资源名称。
//   - channelID：通道ID。
//   - idinfo：身份信息，可以是SignedProposal等对象，从中提取id以便与策略进行测试。
//
// 返回值：
//   - error：如果ACL检查失败，则返回错误；否则返回nil。
func (am *aclMgmtImpl) CheckACL(resName string, channelID string, idinfo interface{}) error {
	// 使用基于资源的配置提供者（默认为1.0提供者）来检查ACL
	return am.rescfgProvider.CheckACL(resName, channelID, idinfo)
}

// CheckACLNoChannel checks the ACL for the resource for the local MSP
// using the idinfo. idinfo is an object such as SignedProposal
// from which an id can be extracted for testing against a policy.
func (am *aclMgmtImpl) CheckACLNoChannel(resName string, idinfo interface{}) error {
	// use the resource based config provider (which will in turn default to 1.0 provider)
	return am.rescfgProvider.CheckACLNoChannel(resName, idinfo)
}

// NewACLProvider 由两个提供程序组成，一个是提供的提供程序，另一个是默认提供程序（使用 ChannelReaders 和 ChannelWriters 进行 1.0 ACL 管理）。
// 如果提供的提供程序为 nil，则创建一个基于资源的 ACL 提供程序。
// 参数：
//   - rg: ResourceGetter 接口，表示资源获取器。
//   - policyChecker: policy.PolicyChecker 接口，表示策略检查器。
//
// 返回值：
//   - ACLProvider: 表示 ACL 提供程序。
func NewACLProvider(rg ResourceGetter, policyChecker policy.PolicyChecker) ACLProvider {
	return &aclMgmtImpl{
		// newDefaultACLProvider:  _lifecycle、LSCC、QSCC、CSCC、 non-scc 的策略, 分表属于Admins、Members角色, 还是各自的读写权限配置
		rescfgProvider: newResourceProvider(rg, newDefaultACLProvider(policyChecker)),
	}
}
