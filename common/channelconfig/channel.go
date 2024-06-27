/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"
	"math"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/capabilities"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
)

// Channel config keys
const (
	// ConsortiumKey is the key for the cb.ConfigValue for the Consortium message
	ConsortiumKey = "Consortium"

	// HashingAlgorithmKey 是hashingalorithm消息的cb.ConfigItem类型键名称
	HashingAlgorithmKey = "HashingAlgorithm"

	// BlockDataHashingStructureKey 是BlockDataHashingStructure消息的cb.ConfigItem类型键名
	BlockDataHashingStructureKey = "BlockDataHashingStructure"

	// OrdererAddressesKey is the cb.ConfigItem type key name for the OrdererAddresses message
	OrdererAddressesKey = "OrdererAddresses"

	// ChannelGroupKey is the name of the channel group
	ChannelGroupKey = "Channel"

	// CapabilitiesKey 是指功能的键的名称，它出现在通道中，
	// 应用程序和orderer级别，此常量用于所有三个级别。
	CapabilitiesKey = "Capabilities"
)

// ChannelValues gives read only access to the channel configuration
type ChannelValues interface {
	// HashingAlgorithm returns the default algorithm to be used when hashing
	// such as computing block hashes, and CreationPolicy digests
	HashingAlgorithm() func(input []byte) []byte

	// BlockDataHashingStructureWidth returns the width to use when constructing the
	// Merkle tree to compute the BlockData hash
	BlockDataHashingStructureWidth() uint32

	// OrdererAddresses returns the list of valid orderer addresses to connect to to invoke Broadcast/Deliver
	OrdererAddresses() []string
}

// ChannelProtos 是提议配置被解组成的地方
type ChannelProtos struct {
	HashingAlgorithm          *cb.HashingAlgorithm          // 哈希算法
	BlockDataHashingStructure *cb.BlockDataHashingStructure // 区块数据哈希结构
	OrdererAddresses          *cb.OrdererAddresses          // 排序节点地址
	Consortium                *cb.Consortium                // 联盟
	Capabilities              *cb.Capabilities              // 功能
}

// ChannelConfig 存储通道配置
type ChannelConfig struct {
	protos            *ChannelProtos            // 通道的协议定义
	hashingAlgorithm  func(input []byte) []byte // 哈希算法函数
	mspManager        msp.MSPManager            // MSP 管理器
	appConfig         *ApplicationConfig        // 应用程序配置
	ordererConfig     *OrdererConfig            // 排序节点配置
	consortiumsConfig *ConsortiumsConfig        // 联盟配置
}

// NewChannelConfig 创建一个新的 ChannelConfig。
// 输入参数：
//   - channelGroup：*cb.ConfigGroup，表示通道组
//   - bccsp：bccsp.BCCSP，表示 BCCSP 实例
//
// 返回值：
//   - *ChannelConfig：表示 ChannelConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewChannelConfig(channelGroup *cb.ConfigGroup, bccsp bccsp.BCCSP) (*ChannelConfig, error) {
	// 创建 ChannelConfig 实例
	cc := &ChannelConfig{
		protos: &ChannelProtos{}, // 通道的协议定义
	}

	// 从通道组中反序列化值到 ChannelProtos 实例
	if err := DeserializeProtoValuesFromGroup(channelGroup, cc.protos); err != nil {
		return nil, errors.Wrap(err, "无法反序列化值通道组 channelGroup")
	}

	// 获取通道的能力
	channelCapabilities := cc.Capabilities()

	// 验证通道配置的有效性
	if err := cc.Validate(channelCapabilities); err != nil {
		return nil, err
	}

	// 创建 MSPConfigHandler 实例
	mspConfigHandler := NewMSPConfigHandler(channelCapabilities.MSPVersion(), bccsp)

	var err error
	for groupName, group := range channelGroup.Groups {
		switch groupName {
		case ApplicationGroupKey:
			// 创建 ApplicationConfig 实例
			cc.appConfig, err = NewApplicationConfig(group, mspConfigHandler)
		case OrdererGroupKey:
			// 创建 OrdererConfig 实例
			cc.ordererConfig, err = NewOrdererConfig(group, mspConfigHandler, channelCapabilities)
		case ConsortiumsGroupKey:
			// 创建 ConsortiumsConfig 联盟实例
			cc.consortiumsConfig, err = NewConsortiumsConfig(group, mspConfigHandler)
		default:
			return nil, fmt.Errorf("不允许的通道组: %s", group)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "无法创建通道 %s 子组织配置", groupName)
		}
	}

	// 创建 MSPManager 实例
	if cc.mspManager, err = mspConfigHandler.CreateMSPManager(); err != nil {
		return nil, err
	}

	return cc, nil
}

// MSPManager returns the MSP manager for this config
func (cc *ChannelConfig) MSPManager() msp.MSPManager {
	return cc.mspManager
}

// OrdererConfig 返回与此通道关联的orderer配置
func (cc *ChannelConfig) OrdererConfig() *OrdererConfig {
	return cc.ordererConfig
}

// ApplicationConfig returns the application config associated with this channel
func (cc *ChannelConfig) ApplicationConfig() *ApplicationConfig {
	return cc.appConfig
}

// ConsortiumsConfig returns the consortium config associated with this channel if it exists
func (cc *ChannelConfig) ConsortiumsConfig() *ConsortiumsConfig {
	return cc.consortiumsConfig
}

// HashingAlgorithm returns a function pointer to the chain hashing algorithm
func (cc *ChannelConfig) HashingAlgorithm() func(input []byte) []byte {
	return cc.hashingAlgorithm
}

// BlockDataHashingStructureWidth returns the width to use when forming the block data hashing structure
func (cc *ChannelConfig) BlockDataHashingStructureWidth() uint32 {
	return cc.protos.BlockDataHashingStructure.Width
}

// OrdererAddresses returns the list of valid orderer addresses to connect to to invoke Broadcast/Deliver
func (cc *ChannelConfig) OrdererAddresses() []string {
	return cc.protos.OrdererAddresses.Addresses
}

// ConsortiumName returns the name of the consortium this channel was created under
func (cc *ChannelConfig) ConsortiumName() string {
	return cc.protos.Consortium.Name
}

// Capabilities 返回有关此通道的可用功能的信息
func (cc *ChannelConfig) Capabilities() ChannelCapabilities {
	_ = cc.protos
	_ = cc.protos.Capabilities
	_ = cc.protos.Capabilities.Capabilities
	return capabilities.NewChannelProvider(cc.protos.Capabilities.Capabilities)
}

// Validate 检查生成的配置 protos，并确保值的正确性。
// 方法接收者：
//   - cc：*ChannelConfig，表示 ChannelConfig 的指针
//
// 输入参数：
//   - channelCapabilities：ChannelCapabilities，表示通道的能力
//
// 返回值：
//   - error：表示验证过程中可能出现的错误
func (cc *ChannelConfig) Validate(channelCapabilities ChannelCapabilities) error {
	// 定义验证器函数的切片
	validatorFuncs := []func() error{
		cc.validateHashingAlgorithm,          // 验证哈希算法。
		cc.validateBlockDataHashingStructure, // 验证区块数据哈希结构。
	}

	// 遍历验证器函数切片，并依次调用每个验证器函数
	for _, validator := range validatorFuncs {
		if err := validator(); err != nil {
			return err
		}
	}

	// 如果通道不支持组织特定的 orderer 地址，则验证 orderer 地址
	if !channelCapabilities.OrgSpecificOrdererEndpoints() {
		return cc.validateOrdererAddresses()
	}

	return nil
}

// validateHashingAlgorithm 验证哈希算法。
// 方法接收者：
//   - cc：*ChannelConfig，表示 ChannelConfig 的指针
//
// 输入参数：
//   - 无
//
// 返回值：
//   - error：表示验证过程中可能出现的错误
func (cc *ChannelConfig) validateHashingAlgorithm() error {
	// 根据哈希算法的名称选择相应的哈希算法函数
	switch cc.protos.HashingAlgorithm.Name {
	case bccsp.SHA256:
		cc.hashingAlgorithm = util.ComputeSHA256
	case bccsp.SHA3_256:
		cc.hashingAlgorithm = util.ComputeSHA3256
	case bccsp.GMSM3:
		cc.hashingAlgorithm = util.ComputeGMSM3
	default:
		return fmt.Errorf("未知的哈希算法类型: %s", cc.protos.HashingAlgorithm.Name)
	}

	return nil
}

// validateBlockDataHashingStructure 验证区块数据哈希结构。
// 方法接收者：
//   - cc：*ChannelConfig，表示 ChannelConfig 的指针
//
// 输入参数：
//   - 无
//
// 返回值：
//   - error：表示验证过程中可能出现的错误
func (cc *ChannelConfig) validateBlockDataHashingStructure() error {
	// 检查区块数据哈希结构的宽度是否为 math.MaxUint32
	if cc.protos.BlockDataHashingStructure.Width != math.MaxUint32 {
		return fmt.Errorf("此版本中的BlockDataHashStructure仅支持MaxUint32 = 4294967295宽度")
	}
	return nil
}

func (cc *ChannelConfig) validateOrdererAddresses() error {
	if len(cc.protos.OrdererAddresses.Addresses) == 0 {
		return fmt.Errorf("Must set some OrdererAddresses")
	}
	return nil
}
