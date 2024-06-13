/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/capabilities"
	"github.com/pkg/errors"
)

const (
	// ApplicationGroupKey 是应用程序配置的组名
	ApplicationGroupKey = "Application"

	// ACLsKey is the name of the ACLs config
	ACLsKey = "ACLs"
)

// ApplicationProtos is used as the source of the ApplicationConfig
type ApplicationProtos struct {
	ACLs         *pb.ACLs
	Capabilities *cb.Capabilities
}

// ApplicationConfig implements the Application interface
type ApplicationConfig struct {
	applicationOrgs map[string]ApplicationOrg
	protos          *ApplicationProtos
}

// NewApplicationConfig 从应用程序配置组创建配置。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - appGroup：*cb.ConfigGroup，表示应用程序配置组
//   - mspConfig：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 返回值：
//   - *ApplicationConfig：表示 ApplicationConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewApplicationConfig(appGroup *cb.ConfigGroup, mspConfig *MSPConfigHandler) (*ApplicationConfig, error) {
	// 创建 ApplicationConfig 实例，并初始化应用程序组织映射和 ApplicationProtos
	ac := &ApplicationConfig{
		applicationOrgs: make(map[string]ApplicationOrg),
		protos:          &ApplicationProtos{},
	}

	// 从应用程序配置组中反序列化值到 ApplicationProtos 实例
	if err := DeserializeProtoValuesFromGroup(appGroup, ac.protos); err != nil {
		return nil, errors.Wrap(err, "应用程序无法反序列化值")
	}

	// 如果应用程序配置不支持 ACLs，则验证是否指定了 ACLs
	if !ac.Capabilities().ACLs() {
		if _, ok := appGroup.Values[ACLsKey]; ok {
			return nil, errors.New("应用程序如果没有所需的功能，则不能指定acl")
		}
	}

	var err error
	for orgName, orgGroup := range appGroup.Groups {
		// 创建 ApplicationOrgConfig 实例，并将其添加到应用程序组织映射中
		ac.applicationOrgs[orgName], err = NewApplicationOrgConfig(orgName, orgGroup, mspConfig)
		if err != nil {
			return nil, err
		}
	}

	return ac, nil
}

// Organizations returns a map of org ID to ApplicationOrg
func (ac *ApplicationConfig) Organizations() map[string]ApplicationOrg {
	return ac.applicationOrgs
}

// Capabilities returns a map of capability name to Capability
func (ac *ApplicationConfig) Capabilities() ApplicationCapabilities {
	return capabilities.NewApplicationProvider(ac.protos.Capabilities.Capabilities)
}

// APIPolicyMapper returns a PolicyMapper that maps API names to policies
func (ac *ApplicationConfig) APIPolicyMapper() PolicyMapper {
	pm := newAPIsProvider(ac.protos.ACLs.Acls)

	return pm
}
