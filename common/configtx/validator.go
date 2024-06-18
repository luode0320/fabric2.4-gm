/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"regexp"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("common.configtx")

// Constraints for valid channel and config IDs
var (
	ChannelAllowedChars = "[a-z][a-z0-9.-]*"
	configAllowedChars  = "[a-zA-Z0-9.-]+"
	MaxLength           = 249
	illegalNames        = map[string]struct{}{
		".":  {},
		"..": {},
	}
)

// ValidatorImpl 实现了 Validator 接口
type ValidatorImpl struct {
	channelID   string                // 通道ID
	sequence    uint64                // 序列号
	configMap   map[string]comparable // 配置映射
	configProto *cb.Config            // 配置协议
	namespace   string                // 命名空间
	pm          policies.Manager      // 策略管理器
}

// validateConfigID makes sure that the config element names (ie map key of
// ConfigGroup) comply with the following restrictions
//  1. Contain only ASCII alphanumerics, dots '.', dashes '-'
//  2. Are shorter than 250 characters.
//  3. Are not the strings "." or "..".
func validateConfigID(configID string) error {
	re, _ := regexp.Compile(configAllowedChars)
	// Length
	if len(configID) <= 0 {
		return errors.New("config ID illegal, cannot be empty")
	}
	if len(configID) > MaxLength {
		return errors.Errorf("config ID illegal, cannot be longer than %d", MaxLength)
	}
	// Illegal name
	if _, ok := illegalNames[configID]; ok {
		return errors.Errorf("name '%s' for config ID is not allowed", configID)
	}
	// Illegal characters
	matched := re.FindString(configID)
	if len(matched) != len(configID) {
		return errors.Errorf("config ID '%s' contains illegal characters", configID)
	}

	return nil
}

// ValidateChannelID makes sure that proposed channel IDs comply with the
// following restrictions:
//  1. Contain only lower case ASCII alphanumerics, dots '.', and dashes '-'
//  2. Are shorter than 250 characters.
//  3. Start with a letter
//
// This is the intersection of the Kafka restrictions and CouchDB restrictions
// with the following exception: '.' is converted to '_' in the CouchDB naming
// This is to accommodate existing channel names with '.', especially in the
// behave tests which rely on the dot notation for their sluggification.
//
// note: this function is a copy of the same in core/tx/endorser/parser.go
func ValidateChannelID(channelID string) error {
	re, _ := regexp.Compile(ChannelAllowedChars)
	// Length
	if len(channelID) <= 0 {
		return errors.Errorf("channel ID illegal, cannot be empty")
	}
	if len(channelID) > MaxLength {
		return errors.Errorf("channel ID illegal, cannot be longer than %d", MaxLength)
	}

	// Illegal characters
	matched := re.FindString(channelID)
	if len(matched) != len(channelID) {
		return errors.Errorf("'%s' contains illegal characters", channelID)
	}

	return nil
}

// NewValidatorImpl 构造一个 Validator 接口的新实现。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - channelID：string，表示通道的ID
//   - config：*cb.Config，表示配置信息
//   - namespace：string，表示命名空间
//   - pm：policies.Manager，表示策略管理器
//
// 返回值：
//   - *ValidatorImpl：表示 ValidatorImpl 的指针
//   - error：表示创建过程中可能出现的错误
func NewValidatorImpl(channelID string, config *cb.Config, namespace string, pm policies.Manager) (*ValidatorImpl, error) {
	// 检查 config 参数是否为 nil
	if config == nil {
		return nil, errors.Errorf("config参数为nil")
	}

	// 检查 channel group 是否为 nil
	if config.ChannelGroup == nil {
		return nil, errors.Errorf("通道组为nil")
	}

	// 验证 channelID 是否有效
	if err := ValidateChannelID(channelID); err != nil {
		return nil, errors.Errorf("channelID 通道ID错误: %s", err)
	}

	// 将 config.ChannelGroup 转换为 mapConfig，并返回 configMap
	configMap, err := mapConfig(config.ChannelGroup, namespace)
	if err != nil {
		return nil, errors.Errorf("将配置 config.ChannelGroup 转换为映射 mapConfig 时出错: %s", err)
	}

	// 创建 ValidatorImpl 实例，并初始化 namespace、pm、sequence、configMap、channelID 和 configProto
	return &ValidatorImpl{
		namespace:   namespace,       // 命名空间
		pm:          pm,              // 策略管理器
		sequence:    config.Sequence, // 序列号
		configMap:   configMap,       // 配置映射
		channelID:   channelID,       // 通道ID
		configProto: config,          // 配置协议
	}, nil
}

// ProposeConfigUpdate 接收类型为CONFIG_UPDATE的信封，并生成一个
// ConfigEnvelope结构体，该结构体将作为CONFIG类型消息的信封有效载荷数据使用
func (vi *ValidatorImpl) ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error) {
	return vi.proposeConfigUpdate(configtx)
}

// proposeConfigUpdate 是 ValidatorImpl 类型的一个内部方法，用于处理配置更新提议。
// 它接收一个包含配置更新信息的信封(*cb.Envelope)作为输入。
func (vi *ValidatorImpl) proposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error) {
	// 尝试将接收到的信封转换为配置更新结构体
	configUpdateEnv, err := protoutil.EnvelopeToConfigUpdate(configtx)
	if err != nil {
		// 转换失败时返回错误
		return nil, errors.Errorf("转换信封为配置更新时出错: %s", err)
	}

	// 验证并授权配置更新
	configMap, err := vi.authorizeUpdate(configUpdateEnv)
	if err != nil {
		// 授权失败时返回错误
		return nil, errors.Errorf("授权配置更新时出错: %s", err)
	}

	// 将配置映射转换回通道组结构体
	channelGroup, err := configMapToConfig(configMap, vi.namespace)
	if err != nil {
		// 转换失败时返回错误
		return nil, errors.Errorf("无法将配置映射还原为通道组: %s", err)
	}

	// 创建并返回新的配置信封，其中包含了更新后的序列号和通道组信息，
	// 同时记录本次更新的原始信封
	return &cb.ConfigEnvelope{
		Config: &cb.Config{
			Sequence:     vi.sequence + 1, // 序列号递增
			ChannelGroup: channelGroup,
		},
		LastUpdate: configtx,
	}, nil
}

// Validate simulates applying a ConfigEnvelope to become the new config
func (vi *ValidatorImpl) Validate(configEnv *cb.ConfigEnvelope) error {
	if configEnv == nil {
		return errors.Errorf("config envelope is nil")
	}

	if configEnv.Config == nil {
		return errors.Errorf("config envelope has nil config")
	}

	if configEnv.Config.Sequence != vi.sequence+1 {
		return errors.Errorf("config currently at sequence %d, cannot validate config at sequence %d", vi.sequence, configEnv.Config.Sequence)
	}

	configUpdateEnv, err := protoutil.EnvelopeToConfigUpdate(configEnv.LastUpdate)
	if err != nil {
		return err
	}

	configMap, err := vi.authorizeUpdate(configUpdateEnv)
	if err != nil {
		return err
	}

	channelGroup, err := configMapToConfig(configMap, vi.namespace)
	if err != nil {
		return errors.Errorf("could not turn configMap back to channelGroup: %s", err)
	}

	// reflect.Equal will not work here, because it considers nil and empty maps as different
	if !proto.Equal(channelGroup, configEnv.Config.ChannelGroup) {
		return errors.Errorf("ConfigEnvelope LastUpdate did not produce the supplied config result")
	}

	return nil
}

// ChannelID retrieves the channel ID associated with this manager
func (vi *ValidatorImpl) ChannelID() string {
	return vi.channelID
}

// Sequence 返回配置的序列号
func (vi *ValidatorImpl) Sequence() uint64 {
	return vi.sequence
}

// ConfigProto returns the config proto which initialized this Validator
func (vi *ValidatorImpl) ConfigProto() *cb.Config {
	return vi.configProto
}
