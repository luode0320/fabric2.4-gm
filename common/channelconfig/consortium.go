/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/pkg/errors"
)

const (
	// ChannelCreationPolicyKey is the key used in the consortium config to denote the policy
	// to be used in evaluating whether a channel creation request is authorized
	ChannelCreationPolicyKey = "ChannelCreationPolicy"
)

// ConsortiumProtos holds the config protos for the consortium config
type ConsortiumProtos struct {
	ChannelCreationPolicy *cb.Policy
}

// ConsortiumConfig holds the consortium's configuration information
type ConsortiumConfig struct {
	protos *ConsortiumProtos
	orgs   map[string]Org
}

// NewConsortiumConfig 创建一个新的 ConsortiumConfig 实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - consortiumGroup：*cb.ConfigGroup，表示联盟配置组
//   - mspConfig：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 返回值：
//   - *ConsortiumConfig：表示 ConsortiumConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewConsortiumConfig(consortiumGroup *cb.ConfigGroup, mspConfig *MSPConfigHandler) (*ConsortiumConfig, error) {
	// 创建 ConsortiumConfig 实例，并初始化 protos 和 orgs
	cc := &ConsortiumConfig{
		protos: &ConsortiumProtos{},
		orgs:   make(map[string]Org),
	}

	// 从联盟配置组中反序列化值到 ConsortiumProtos 实例
	if err := DeserializeProtoValuesFromGroup(consortiumGroup, cc.protos); err != nil {
		return nil, errors.Wrap(err, "无法反序列化值")
	}

	// 遍历联盟配置组中的每个组织，并为每个组织创建一个 OrganizationConfig 实例，并将其添加到 orgs 中
	for orgName, orgGroup := range consortiumGroup.Groups {
		var err error
		if cc.orgs[orgName], err = NewOrganizationConfig(orgName, orgGroup, mspConfig); err != nil {
			return nil, err
		}
	}

	return cc, nil
}

// Organizations returns the set of organizations in the consortium
func (cc *ConsortiumConfig) Organizations() map[string]Org {
	return cc.orgs
}

// CreationPolicy returns the policy structure used to validate
// the channel creation
func (cc *ConsortiumConfig) ChannelCreationPolicy() *cb.Policy {
	return cc.protos.ChannelCreationPolicy
}
