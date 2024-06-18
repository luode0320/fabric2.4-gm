/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"strings"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

func (vi *ValidatorImpl) verifyReadSet(readSet map[string]comparable) error {
	for key, value := range readSet {
		existing, ok := vi.configMap[key]
		if !ok {
			return errors.Errorf("existing config does not contain element for %s but was in the read set", key)
		}

		if existing.version() != value.version() {
			return errors.Errorf("proposed update requires that key %s be at version %d, but it is currently at version %d", key, value.version(), existing.version())
		}
	}
	return nil
}

// computeDeltaSet 计算两个配置映射（readSet 和 writeSet）之间的差异集。
// 差异集包含writeSet中相对于readSet有所修改或新增的配置项。
func computeDeltaSet(readSet, writeSet map[string]comparable) map[string]comparable {
	// 初始化结果映射用于存放差异集
	result := make(map[string]comparable)

	// 遍历writeSet中的所有配置项
	for key, newValue := range writeSet {
		// 尝试从readSet中找到对应键的值
		readVal, exists := readSet[key]

		// 如果键存在于readSet中且版本相同，则跳过，表示这部分配置未变更
		if exists && readVal.version() == newValue.version() {
			continue
		}

		// 如果键不存在于readSet或版本不同，则认为该配置项被修改或新增
		// 将其加入到差异集中
		result[key] = newValue
	}

	// 返回计算得到的差异集
	return result
}
func validateModPolicy(modPolicy string) error {
	if modPolicy == "" {
		return errors.Errorf("mod_policy not set")
	}

	trimmed := modPolicy
	if modPolicy[0] == '/' {
		trimmed = modPolicy[1:]
	}

	for i, pathElement := range strings.Split(trimmed, pathSeparator) {
		err := validateConfigID(pathElement)
		if err != nil {
			return errors.Wrapf(err, "path element at %d is invalid", i)
		}
	}
	return nil
}

// verifyDeltaSet 方法用于验证给定的 deltaSet 是否与签名数据集 signedData 相匹配。
// 方法接收者：vi *ValidatorImpl，表示 ValidatorImpl 结构体的指针。
// 输入参数：
//   - deltaSet map[string]comparable，表示要验证的 deltaSet。
//   - signedData []*protoutil.SignedData，表示签名数据集的指针数组。
//
// 返回值：
//   - error，表示验证过程中的错误，如果验证通过则返回nil。
func (vi *ValidatorImpl) verifyDeltaSet(deltaSet map[string]comparable, signedData []*protoutil.SignedData) error {
	if len(deltaSet) == 0 {
		return errors.Errorf("增量集 deltaSet 为空-更新将不起作用")
	}

	for key, value := range deltaSet {
		logger.Debugf("正在处理对策略的更改: %s", key)
		if err := validateModPolicy(value.modPolicy()); err != nil {
			return errors.Wrapf(err, "无效的策略模式 mod_policy 对于元素: %s", key)
		}

		existing, ok := vi.configMap[key]
		if !ok {
			if value.version() != 0 {
				return errors.Errorf("试图将策略 [%s] 设置为版本 %d, 但是不存在", key, value.version())
			}

			continue
		}
		if value.version() != existing.version()+1 {
			return errors.Errorf("尝试将策略 [%s] 设置为版本 %d, 但版本为 %d", key, value.version(), existing.version())
		}

		policy, ok := vi.policyForItem(existing)
		if !ok {
			return errors.Errorf("意外缺少策略 %s (项目 %s)", existing.modPolicy(), key)
		}

		// 方法用于评估给定的签名数据集是否满足策略要求
		if err := policy.EvaluateSignedData(signedData); err != nil {
			logger.Warnw("不满足更新通道配置的策略", "key", key, "policy", policy, "signingIdenties", protoutil.LogMessageForSerializedIdentities(signedData))
			return errors.Wrapf(err, "不满足 %s 的策略", key)
		}
	}
	return nil
}

// 验证写入集（writeSet）中的所有键是否都存在于完整提议配置（fullProposedConfig）中。
// writeSet: 待验证的写入集，包含一组键值对，表示期望在配置中更新或设置的条目。
// fullProposedConfig: 完整的提议配置映射，用于对比验证写入集的合法性。
// 错误返回：
//   - 如果写入集中的任何一个键在完整提议配置中找不到，则返回一个相应的错误，指出键名未出现于提议配置中。
//   - 如果所有键都存在于提议配置中，则返回nil，表示验证通过。
func verifyFullProposedConfig(writeSet, fullProposedConfig map[string]comparable) error {
	// 遍历写入集中的所有键
	for key := range writeSet {
		// 检查当前键是否在完整提议配置中存在
		if _, ok := fullProposedConfig[key]; !ok {
			// 如果不存在，则返回错误
			return errors.Errorf("writeset包含的key %s 未出现在建议的配置中", key)
		}
	}
	// 所有键都验证通过，返回nil
	return nil
}

// 验证所有修改的配置项是否满足相应的修改策略条件（通过签名集确认）。
// 返回一个包含已修改配置项的映射。
func (vi *ValidatorImpl) authorizeUpdate(configUpdateEnv *cb.ConfigUpdateEnvelope) (map[string]comparable, error) {
	if configUpdateEnv == nil {
		return nil, errors.Errorf("无法处理空值")
	}

	// 解码ConfigUpdateEnvelope中的ConfigUpdate
	configUpdate, err := UnmarshalConfigUpdate(configUpdateEnv.ConfigUpdate)
	if err != nil {
		return nil, err
	}

	// 检查ConfigUpdate中的通道ID是否与验证器的通道ID匹配
	if configUpdate.ChannelId != vi.channelID {
		return nil, errors.Errorf("ConfigUpdate 针对通道 %s, 但当前验证环境为通道 %s", configUpdate.ChannelId, vi.channelID)
	}

	// 将ConfigUpdate的ReadSet转换为映射结构
	readSet, err := mapConfig(configUpdate.ReadSet, vi.namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "转换 ReadSet 至映射时出错")
	}
	// 验证ReadSet
	err = vi.verifyReadSet(readSet)
	if err != nil {
		return nil, errors.Wrapf(err, "验证 ReadSet 失败")
	}

	// 同样处理WriteSet
	writeSet, err := mapConfig(configUpdate.WriteSet, vi.namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "转换 WriteSet 至映射时出错")
	}

	// 计算ReadSet与WriteSet的差异集deltaSet
	deltaSet := computeDeltaSet(readSet, writeSet)

	// 将ConfigUpdateEnvelope转换为SignedData结构，用于后续验证
	signedData, err := protoutil.ConfigUpdateEnvelopeAsSignedData(configUpdateEnv)
	if err != nil {
		return nil, err
	}

	// 验证差异集deltaSet是否满足签名策略
	if err = vi.verifyDeltaSet(deltaSet, signedData); err != nil {
		return nil, errors.Wrapf(err, "验证差异集时出错")
	}

	// 根据差异集计算完整的新配置fullProposedConfig
	fullProposedConfig := vi.computeUpdateResult(deltaSet)

	// 验证生成的完整配置是否有效
	if err := verifyFullProposedConfig(writeSet, fullProposedConfig); err != nil {
		return nil, errors.Wrapf(err, "验证完整配置失败")
	}

	// 成功返回完整的拟更新配置映射
	return fullProposedConfig, nil
}

func (vi *ValidatorImpl) policyForItem(item comparable) (policies.Policy, bool) {
	manager := vi.pm

	modPolicy := item.modPolicy()
	logger.Debugf("Getting policy for item %s with mod_policy %s", item.key, modPolicy)

	// If the mod_policy path is relative, get the right manager for the context
	// If the item has a zero length path, it is the root group, use the base policy manager
	// if the mod_policy path is absolute (starts with /) also use the base policy manager
	if len(modPolicy) > 0 && modPolicy[0] != policies.PathSeparator[0] && len(item.path) != 0 {
		var ok bool

		manager, ok = manager.Manager(item.path[1:])
		if !ok {
			logger.Debugf("Could not find manager at path: %v", item.path[1:])
			return nil, ok
		}

		// In the case of the group type, its key is part of its path for the purposes of finding the policy manager
		if item.ConfigGroup != nil {
			manager, ok = manager.Manager([]string{item.key})
		}
		if !ok {
			logger.Debugf("Could not find group at subpath: %v", item.key)
			return nil, ok
		}
	}

	return manager.GetPolicy(item.modPolicy())
}

// 接受由更新生成的配置映射，并基于旧配置生成一个合并后的新配置映射。
func (vi *ValidatorImpl) computeUpdateResult(updatedConfig map[string]comparable) map[string]comparable {
	// 初始化一个新的配置映射，用于存放合并后的结果
	newConfigMap := make(map[string]comparable)

	// 首先，将原有配置映射中的所有项复制到新配置映射中
	for key, value := range vi.configMap {
		newConfigMap[key] = value
	}

	// 然后，遍历更新后的配置映射，对于其中的每一项，不论是否已存在于新映射中，
	// 都直接覆盖或新增到新配置映射中，实现配置的更新或追加
	for key, value := range updatedConfig {
		newConfigMap[key] = value
	}

	// 返回合并后的新配置映射
	return newConfigMap
}
