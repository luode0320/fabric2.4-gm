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

func computeDeltaSet(readSet, writeSet map[string]comparable) map[string]comparable {
	result := make(map[string]comparable)
	for key, value := range writeSet {
		readVal, ok := readSet[key]

		if ok && readVal.version() == value.version() {
			continue
		}

		// 如果readset中的键是不同的版本，我们将其包含
		// 对更新的完整性进行错误检查是针对配置进行的
		result[key] = value
	}
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

func verifyFullProposedConfig(writeSet, fullProposedConfig map[string]comparable) error {
	for key := range writeSet {
		if _, ok := fullProposedConfig[key]; !ok {
			return errors.Errorf("writeset contained key %s which did not appear in proposed config", key)
		}
	}
	return nil
}

// authorizeUpdate validates that all modified config has the corresponding modification policies satisfied by the signature set
// it returns a map of the modified config
func (vi *ValidatorImpl) authorizeUpdate(configUpdateEnv *cb.ConfigUpdateEnvelope) (map[string]comparable, error) {
	if configUpdateEnv == nil {
		return nil, errors.Errorf("无法处理nil")
	}

	configUpdate, err := UnmarshalConfigUpdate(configUpdateEnv.ConfigUpdate)
	if err != nil {
		return nil, err
	}

	if configUpdate.ChannelId != vi.channelID {
		return nil, errors.Errorf("ConfigUpdate 用于通道 %s, 但信封用于通道 %s", configUpdate.ChannelId, vi.channelID)
	}

	readSet, err := mapConfig(configUpdate.ReadSet, vi.namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "映射 ReadSet 时出错")
	}
	err = vi.verifyReadSet(readSet)
	if err != nil {
		return nil, errors.Wrapf(err, "验证 ReadSet 时出错")
	}

	writeSet, err := mapConfig(configUpdate.WriteSet, vi.namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "错误映射 WriteSet")
	}

	deltaSet := computeDeltaSet(readSet, writeSet)
	signedData, err := protoutil.ConfigUpdateEnvelopeAsSignedData(configUpdateEnv)
	if err != nil {
		return nil, err
	}

	if err = vi.verifyDeltaSet(deltaSet, signedData); err != nil {
		return nil, errors.Wrapf(err, "验证 DeltaSet 时出错")
	}

	fullProposedConfig := vi.computeUpdateResult(deltaSet)
	if err := verifyFullProposedConfig(writeSet, fullProposedConfig); err != nil {
		return nil, errors.Wrapf(err, "full config did not verify")
	}

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

// computeUpdateResult takes a configMap generated by an update and produces a new configMap overlaying it onto the old config
func (vi *ValidatorImpl) computeUpdateResult(updatedConfig map[string]comparable) map[string]comparable {
	newConfigMap := make(map[string]comparable)
	for key, value := range vi.configMap {
		newConfigMap[key] = value
	}

	for key, value := range updatedConfig {
		newConfigMap[key] = value
	}
	return newConfigMap
}
