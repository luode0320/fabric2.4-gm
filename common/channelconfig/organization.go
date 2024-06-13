/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"

	cb "github.com/hyperledger/fabric-protos-go/common"
	mspprotos "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
)

const (
	// MSPKey 是orderer组中MSP定义的键
	MSPKey = "MSP"
)

// OrganizationProtos 用于反序列化组织配置
type OrganizationProtos struct {
	MSP *mspprotos.MSPConfig
}

// OrganizationConfig 存储组织的配置
type OrganizationConfig struct {
	protos           *OrganizationProtos // 组织的协议定义
	mspConfigHandler *MSPConfigHandler   // MSP 配置处理器
	msp              msp.MSP             // MSP（成员服务提供者）
	mspID            string              // MSP 标识
	name             string              // 组织名称
}

// NewOrganizationConfig 创建一个新的组织配置。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - name：string，表示组织的名称
//   - orgGroup：*cb.ConfigGroup，表示组织配置组
//   - mspConfigHandler：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 返回值：
//   - *OrganizationConfig：表示 OrganizationConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewOrganizationConfig(name string, orgGroup *cb.ConfigGroup, mspConfigHandler *MSPConfigHandler) (*OrganizationConfig, error) {
	if len(orgGroup.Groups) > 0 {
		return nil, fmt.Errorf("组织不支持子组织")
	}

	// 存储组织的配置
	oc := &OrganizationConfig{
		protos:           &OrganizationProtos{}, // 组织的协议定义
		name:             name,                  // 组织名称
		mspConfigHandler: mspConfigHandler,      // MSP 配置处理器
	}

	if err := DeserializeProtoValuesFromGroup(orgGroup, oc.protos); err != nil {
		return nil, errors.Wrap(err, "无法反序列化值")
	}

	if err := oc.Validate(); err != nil {
		return nil, err
	}

	return oc, nil
}

// Name returns the name this org is referred to in config
func (oc *OrganizationConfig) Name() string {
	return oc.name
}

// MSPID returns the MSP ID associated with this org
func (oc *OrganizationConfig) MSPID() string {
	return oc.mspID
}

// MSP returns the actual MSP implementation for this org.
func (oc *OrganizationConfig) MSP() msp.MSP {
	return oc.msp
}

// Validate 返回配置是否有效
func (oc *OrganizationConfig) Validate() error {
	return oc.validateMSP()
}

// validateMSP 验证组织的MSP（成员服务提供者）配置。
// 方法接收者：oc（OrganizationConfig类型的指针）
// 输入参数：无
// 返回值：
//   - error：如果验证过程中出现错误，则返回错误；否则返回nil。
func (oc *OrganizationConfig) validateMSP() error {
	var err error

	logger.Debugf("为组织设置MSP %s", oc.name)

	// 提议MSP配置
	oc.msp, err = oc.mspConfigHandler.ProposeMSP(oc.protos.MSP)
	if err != nil {
		return err
	}

	// 获取MSP的标识符
	oc.mspID, _ = oc.msp.GetIdentifier()

	// 检查MSP的标识符是否为空
	if oc.mspID == "" {
		return fmt.Errorf("组织 %s 的 MSPID 为空", oc.name)
	}

	return nil
}
