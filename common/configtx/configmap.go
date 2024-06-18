/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"strings"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	groupPrefix  = "[Group]  "
	valuePrefix  = "[Value]  "
	policyPrefix = "[Policy] "

	pathSeparator = "/"

	// Hacky fix constants, used in recurseConfigMap
	hackyFixOrdererCapabilities = "[Value]  /Channel/Orderer/Capabilities"
	hackyFixNewModPolicy        = "Admins"
)

// mapConfig 函数设计用于本文件外部调用，
// 它接收一个 ConfigGroup，并生成一个从全限定路径（fqPath）到可比较元素（comparable）的映射，
// 如果遇到无效的键，则返回错误。
func mapConfig(channelGroup *cb.ConfigGroup, rootGroupKey string) (map[string]comparable, error) {
	// 初始化结果映射
	result := make(map[string]comparable)
	// 当传入的channelGroup非空时，递归处理ConfigGroup以填充映射
	if channelGroup != nil {
		err := recurseConfig(result, []string{rootGroupKey}, channelGroup)
		if err != nil {
			// 如果递归过程中出现错误，返回该错误
			return nil, err
		}
	}
	// 成功处理后，返回映射结果和nil错误
	return result, nil
}

// addToMap is used only internally by mapConfig
func addToMap(cg comparable, result map[string]comparable) error {
	var fqPath string

	switch {
	case cg.ConfigGroup != nil:
		fqPath = groupPrefix
	case cg.ConfigValue != nil:
		fqPath = valuePrefix
	case cg.ConfigPolicy != nil:
		fqPath = policyPrefix
	}

	if err := validateConfigID(cg.key); err != nil {
		return errors.WithMessagef(err, "illegal characters in key: %s", fqPath)
	}

	if len(cg.path) == 0 {
		fqPath += pathSeparator + cg.key
	} else {
		fqPath += pathSeparator + strings.Join(cg.path, pathSeparator) + pathSeparator + cg.key
	}

	logger.Debugf("Adding to config map: %s", fqPath)

	result[fqPath] = cg

	return nil
}

// recurseConfig 函数仅供 mapConfig 内部使用，用于递归遍历配置组（ConfigGroup），
// 将每个配置项（包括子组、值、策略）转换为可比较对象并添加到结果映射中。
func recurseConfig(result map[string]comparable, path []string, group *cb.ConfigGroup) error {
	// 尝试将当前配置组添加到结果映射中，key为路径中的最后一项
	if err := addToMap(comparable{key: path[len(path)-1], path: path[:len(path)-1], ConfigGroup: group}, result); err != nil {
		return err
	}

	// 遍历子配置组，构建下一层路径，并递归处理每个子组
	for key, subgroup := range group.Groups {
		nextPath := make([]string, len(path)+1)
		copy(nextPath, path)
		nextPath[len(nextPath)-1] = key
		if err := recurseConfig(result, nextPath, subgroup); err != nil {
			return err
		}
	}

	// 处理并添加配置值到结果映射
	for key, value := range group.Values {
		if err := addToMap(comparable{key: key, path: path, ConfigValue: value}, result); err != nil {
			return err
		}
	}

	// 处理并添加配置策略到结果映射
	for key, policy := range group.Policies {
		if err := addToMap(comparable{key: key, path: path, ConfigPolicy: policy}, result); err != nil {
			return err
		}
	}

	// 所有配置项处理完毕，无错误发生则返回nil
	return nil
}

// 用于将外部调用提供的configMap转换回*cb.ConfigGroup结构。
// configMap: 包含配置信息的映射。
// rootGroupKey: 根配置组的键。
func configMapToConfig(configMap map[string]comparable, rootGroupKey string) (*cb.ConfigGroup, error) {
	// 构建根路径
	rootPath := pathSeparator + rootGroupKey
	// 调用递归函数转换映射
	return recurseConfigMap(rootPath, configMap)
}

// 是configMapToConfig函数的内部辅助函数，用于递归地将configMap转换为ConfigGroup结构。
// 注意：此函数不再直接修改configMap中cb.Config*类型的条目。
func recurseConfigMap(path string, configMap map[string]comparable) (*cb.ConfigGroup, error) {
	// 构建组路径
	groupPath := groupPrefix + path
	// 从映射中查找对应的组
	group, ok := configMap[groupPath]
	if !ok {
		return nil, errors.Errorf("路径 %s 下缺少组", groupPath)
	}

	// 确保找到了ConfigGroup
	if group.ConfigGroup == nil {
		return nil, errors.Errorf("在组路径 %s 下未找到ConfigGroup", groupPath)
	}

	// 创建新的ConfigGroup并合并现有数据
	newConfigGroup := protoutil.NewConfigGroup()
	proto.Merge(newConfigGroup, group.ConfigGroup)

	// 递归处理子组、值和策略，并添加到新ConfigGroup中
	for key := range group.Groups {
		updatedGroup, err := recurseConfigMap(path+pathSeparator+key, configMap)
		if err != nil {
			return nil, err
		}
		newConfigGroup.Groups[key] = updatedGroup
	}

	for key := range group.Values {
		valuePath := valuePrefix + path + pathSeparator + key
		value, ok := configMap[valuePath]
		if !ok {
			return nil, errors.Errorf("路径 %s 下缺少值", valuePath)
		}
		if value.ConfigValue == nil {
			return nil, errors.Errorf("在值路径 %s 下未找到ConfigValue", valuePath)
		}
		newConfigGroup.Values[key] = proto.Clone(value.ConfigValue).(*cb.ConfigValue)
	}

	for key := range group.Policies {
		policyPath := policyPrefix + path + pathSeparator + key
		policy, ok := configMap[policyPath]
		if !ok {
			return nil, errors.Errorf("路径 %s 下缺少策略", policyPath)
		}
		if policy.ConfigPolicy == nil {
			return nil, errors.Errorf("在策略路径 %s 下未找到ConfigPolicy", policyPath)
		}
		newConfigGroup.Policies[key] = proto.Clone(policy.ConfigPolicy).(*cb.ConfigPolicy)
		logger.Debugf("为键 %s 设置策略为 %+v", key, group.Policies[key])
	}

	// 特殊处理逻辑，针对早期版本通道配置中可能存在的mod_policy未设置的问题进行修复
	if _, ok := configMap[hackyFixOrdererCapabilities]; ok {
		// 如果发现mod_policy为空，则按照特定策略进行修正
		if newConfigGroup.ModPolicy == "" {
			logger.Debugf("对组 %s 的空mod_policy执行升级修复", groupPath)
			newConfigGroup.ModPolicy = hackyFixNewModPolicy
		}

		for key, value := range newConfigGroup.Values {
			if value.ModPolicy == "" {
				logger.Debugf("对值 %s 的空mod_policy执行升级修复", valuePrefix+path+pathSeparator+key)
				value.ModPolicy = hackyFixNewModPolicy
			}
		}

		for key, policy := range newConfigGroup.Policies {
			if policy.ModPolicy == "" {
				logger.Debugf("对策略 %s 的空mod_policy执行升级修复", policyPrefix+path+pathSeparator+key)
				policy.ModPolicy = hackyFixNewModPolicy
			}
		}
	}

	// 返回构建好的ConfigGroup
	return newConfigGroup, nil
}
