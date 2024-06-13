/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package aclmgmt

import (
	"github.com/hyperledger/fabric/common/flogging"
)

var aclLogger = flogging.MustGetLogger("aclmgmt")

type ACLProvider interface {
	// CheckACL 使用idinfo检查通道资源的ACL。
	// idinfo是一个对象，如SignedProposal，从中可以提取id以根据策略进行测试
	CheckACL(resName string, channelID string, idinfo interface{}) error

	// CheckACLNoChannel 使用idinfo检查本地MSP的资源的ACL。idinfo是一个对象，如SignedProposal，可以从中提取id以根据策略进行测试。
	CheckACLNoChannel(resName string, idinfo interface{}) error
}
