/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
)

const (
	// ConsortiumsGroupKey 是consortiums config的组名
	ConsortiumsGroupKey = "Consortiums"
)

// ConsortiumsConfig 保存联盟配置信息
type ConsortiumsConfig struct {
	consortiums map[string]Consortium
}

// NewConsortiumsConfig 创建一个新的 ConsortiumsConfig 联盟实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - consortiumsGroup：*cb.ConfigGroup，表示联盟配置组
//   - mspConfig：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 返回值：
//   - *ConsortiumsConfig：表示 ConsortiumsConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewConsortiumsConfig(consortiumsGroup *cb.ConfigGroup, mspConfig *MSPConfigHandler) (*ConsortiumsConfig, error) {
	// 创建 ConsortiumsConfig 联盟实例，并初始化 consortiums
	cc := &ConsortiumsConfig{
		consortiums: make(map[string]Consortium),
	}

	// 遍历联盟配置组中的每个联盟，并为每个联盟创建一个 ConsortiumConfig 实例，并将其添加到 consortiums 中
	for consortiumName, consortiumGroup := range consortiumsGroup.Groups {
		var err error
		if cc.consortiums[consortiumName], err = NewConsortiumConfig(consortiumGroup, mspConfig); err != nil {
			return nil, err
		}
	}
	return cc, nil
}

// Consortiums returns a map of the current consortiums
func (cc *ConsortiumsConfig) Consortiums() map[string]Consortium {
	return cc.consortiums
}
