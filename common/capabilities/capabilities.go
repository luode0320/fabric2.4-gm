/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package capabilities

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("common.capabilities")

// provider is the 'plugin' parameter for registry.
type provider interface {
	// HasCapability should report whether the binary supports this capability.
	HasCapability(capability string) bool

	// Type is used to make error messages more legible.
	Type() string
}

// registry is a common structure intended to be used to support specific aspects of capabilities
// such as orderer, application, and channel.
type registry struct {
	provider     provider
	capabilities map[string]*cb.Capability
}

func newRegistry(p provider, capabilities map[string]*cb.Capability) *registry {
	return &registry{
		provider:     p,
		capabilities: capabilities,
	}
}

// Supported 检查此二进制文件是否支持所有所需的能力。
// 方法接收者：r（registry类型的指针）
// 返回值：
//   - error：如果有所需的能力不受支持，则返回错误；否则返回nil。
func (r *registry) Supported() error {
	for capabilityName := range r.capabilities {
		// 检查提供者是否具有所需的能力
		if r.provider.HasCapability(capabilityName) {
			logger.Debugf("%s capability %s 支持并启用", r.provider.Type(), capabilityName)
			continue
		}

		// 返回错误，指示所需的能力不受支持
		return errors.Errorf("%s capability %s 是必需的启用的, 目前 capability 配置不支持", r.provider.Type(), capabilityName)
	}
	return nil
}
