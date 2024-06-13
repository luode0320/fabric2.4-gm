/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"

	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
)

const (
	// AnchorPeersKey is the key name for the AnchorPeers ConfigValue
	AnchorPeersKey = "AnchorPeers"
)

// ApplicationOrgProtos 从配置反序列化
type ApplicationOrgProtos struct {
	AnchorPeers *pb.AnchorPeers
}

// ApplicationOrgConfig 定义应用程序组织的配置
type ApplicationOrgConfig struct {
	*OrganizationConfig                       // 组织配置
	protos              *ApplicationOrgProtos // 应用程序组织的协议定义
	name                string                // 组织名称
}

// NewApplicationOrgConfig 创建一个新的应用程序组织配置。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - id：string，表示应用程序组织的标识符
//   - orgGroup：*cb.ConfigGroup，表示组织配置组
//   - mspConfig：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 返回值：
//   - *ApplicationOrgConfig：表示 ApplicationOrgConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewApplicationOrgConfig(id string, orgGroup *cb.ConfigGroup, mspConfig *MSPConfigHandler) (*ApplicationOrgConfig, error) {
	// 检查应用程序组织配置是否包含子组
	if len(orgGroup.Groups) > 0 {
		return nil, fmt.Errorf("应用程序组织配置不包含子组织")
	}

	// 创建 ApplicationOrgProtos 和 OrganizationProtos 实例，并从组中反序列化值
	protos := &ApplicationOrgProtos{}
	orgProtos := &OrganizationProtos{}

	// 从配置组中反序列化所有值的值。
	if err := DeserializeProtoValuesFromGroup(orgGroup, protos, orgProtos); err != nil {
		return nil, errors.Wrap(err, "无法反序列化值")
	}

	// 创建 ApplicationOrgConfig 实例，并初始化名称、protos 和 mspConfigHandler
	aoc := &ApplicationOrgConfig{
		name:   id,     // 组织名称
		protos: protos, // 应用程序组织的协议定义
		OrganizationConfig: &OrganizationConfig{ // 组织配置
			name:             id,        // 组织名称
			protos:           orgProtos, // 组织的协议定义
			mspConfigHandler: mspConfig, // MSP 配置处理器
		},
	}

	// 验证应用程序组织配置。
	if err := aoc.Validate(); err != nil {
		return nil, err
	}

	return aoc, nil
}

// AnchorPeers returns the list of anchor peers of this Organization
func (aog *ApplicationOrgConfig) AnchorPeers() []*pb.AnchorPeer {
	return aog.protos.AnchorPeers.AnchorPeers
}

// Validate 验证应用程序组织配置。
// 方法接收者：
//   - aoc：*ApplicationOrgConfig，表示 ApplicationOrgConfig 的指针
//
// 输入参数：
//   - 无
//
// 返回值：
//   - error：表示验证过程中可能出现的错误
func (aoc *ApplicationOrgConfig) Validate() error {
	logger.Debugf("锚节点 %v 来自组织 %s", aoc.protos.AnchorPeers, aoc.name)
	return aoc.OrganizationConfig.Validate()
}
