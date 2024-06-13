/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package update

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

func computePoliciesMapUpdate(original, updated map[string]*cb.ConfigPolicy) (readSet, writeSet, sameSet map[string]*cb.ConfigPolicy, updatedMembers bool) {
	readSet = make(map[string]*cb.ConfigPolicy)
	writeSet = make(map[string]*cb.ConfigPolicy)

	// 所有修改的配置进入读/写集，但万一映射成员更改，
	// 我们保留与添加到读/写集相同的配置
	sameSet = make(map[string]*cb.ConfigPolicy)

	for policyName, originalPolicy := range original {
		updatedPolicy, ok := updated[policyName]
		if !ok {
			updatedMembers = true
			continue
		}

		if originalPolicy.ModPolicy == updatedPolicy.ModPolicy && proto.Equal(originalPolicy.Policy, updatedPolicy.Policy) {
			sameSet[policyName] = &cb.ConfigPolicy{
				Version: originalPolicy.Version,
			}
			continue
		}

		writeSet[policyName] = &cb.ConfigPolicy{
			Version:   originalPolicy.Version + 1,
			ModPolicy: updatedPolicy.ModPolicy,
			Policy:    updatedPolicy.Policy,
		}
	}

	for policyName, updatedPolicy := range updated {
		if _, ok := original[policyName]; ok {
			// 如果updatedPolicy在原始策略集中，则它已被处理
			continue
		}
		updatedMembers = true
		writeSet[policyName] = &cb.ConfigPolicy{
			Version:   0,
			ModPolicy: updatedPolicy.ModPolicy,
			Policy:    updatedPolicy.Policy,
		}
	}

	return
}

func computeValuesMapUpdate(original, updated map[string]*cb.ConfigValue) (readSet, writeSet, sameSet map[string]*cb.ConfigValue, updatedMembers bool) {
	readSet = make(map[string]*cb.ConfigValue)
	writeSet = make(map[string]*cb.ConfigValue)

	// 所有修改的配置进入读/写集，但万一映射成员更改，
	// 我们保留与添加到读/写集相同的配置
	sameSet = make(map[string]*cb.ConfigValue)

	for valueName, originalValue := range original {
		updatedValue, ok := updated[valueName]
		if !ok {
			updatedMembers = true
			continue
		}

		if originalValue.ModPolicy == updatedValue.ModPolicy && bytes.Equal(originalValue.Value, updatedValue.Value) {
			sameSet[valueName] = &cb.ConfigValue{
				Version: originalValue.Version,
			}
			continue
		}

		writeSet[valueName] = &cb.ConfigValue{
			Version:   originalValue.Version + 1,
			ModPolicy: updatedValue.ModPolicy,
			Value:     updatedValue.Value,
		}
	}

	for valueName, updatedValue := range updated {
		if _, ok := original[valueName]; ok {
			// 如果updatedValue在原始值集中，则它已被处理
			continue
		}
		updatedMembers = true
		writeSet[valueName] = &cb.ConfigValue{
			Version:   0,
			ModPolicy: updatedValue.ModPolicy,
			Value:     updatedValue.Value,
		}
	}

	return
}

func computeGroupsMapUpdate(original, updated map[string]*cb.ConfigGroup) (readSet, writeSet, sameSet map[string]*cb.ConfigGroup, updatedMembers bool) {
	readSet = make(map[string]*cb.ConfigGroup)
	writeSet = make(map[string]*cb.ConfigGroup)

	// 所有修改的配置进入读/写集，但万一映射成员更改，
	// 我们保留与添加到读/写集相同的配置
	sameSet = make(map[string]*cb.ConfigGroup)

	for groupName, originalGroup := range original {
		updatedGroup, ok := updated[groupName]
		if !ok {
			updatedMembers = true
			continue
		}

		groupReadSet, groupWriteSet, groupUpdated := computeGroupUpdate(originalGroup, updatedGroup)
		if !groupUpdated {
			sameSet[groupName] = groupReadSet
			continue
		}

		readSet[groupName] = groupReadSet
		writeSet[groupName] = groupWriteSet

	}

	for groupName, updatedGroup := range updated {
		if _, ok := original[groupName]; ok {
			// 如果updatedGroup位于原始组集中，则它已被处理
			continue
		}
		updatedMembers = true
		_, groupWriteSet, _ := computeGroupUpdate(protoutil.NewConfigGroup(), updatedGroup)
		writeSet[groupName] = &cb.ConfigGroup{
			Version:   0,
			ModPolicy: updatedGroup.ModPolicy,
			Policies:  groupWriteSet.Policies,
			Values:    groupWriteSet.Values,
			Groups:    groupWriteSet.Groups,
		}
	}

	return
}

func computeGroupUpdate(original, updated *cb.ConfigGroup) (readSet, writeSet *cb.ConfigGroup, updatedGroup bool) {
	// 计算策略映射更新
	readSetPolicies, writeSetPolicies, sameSetPolicies, policiesMembersUpdated := computePoliciesMapUpdate(original.Policies, updated.Policies)
	// 计算值映射更新
	readSetValues, writeSetValues, sameSetValues, valuesMembersUpdated := computeValuesMapUpdate(original.Values, updated.Values)
	// 计算组映射更新
	readSetGroups, writeSetGroups, sameSetGroups, groupsMembersUpdated := computeGroupsMapUpdate(original.Groups, updated.Groups)

	// 如果更新的组与更新的组 “相等” (没有成员或mod策略更改)
	if !(policiesMembersUpdated || valuesMembersUpdated || groupsMembersUpdated || original.ModPolicy != updated.ModPolicy) {

		// 如果在任何策略/值/组映射中没有修改的条目
		if len(readSetPolicies) == 0 &&
			len(writeSetPolicies) == 0 &&
			len(readSetValues) == 0 &&
			len(writeSetValues) == 0 &&
			len(readSetGroups) == 0 &&
			len(writeSetGroups) == 0 {
			return &cb.ConfigGroup{
					Version: original.Version,
				}, &cb.ConfigGroup{
					Version: original.Version,
				}, false
		}

		return &cb.ConfigGroup{
				Version:  original.Version,
				Policies: readSetPolicies,
				Values:   readSetValues,
				Groups:   readSetGroups,
			}, &cb.ConfigGroup{
				Version:  original.Version,
				Policies: writeSetPolicies,
				Values:   writeSetValues,
				Groups:   writeSetGroups,
			}, true
	}

	for k, samePolicy := range sameSetPolicies {
		readSetPolicies[k] = samePolicy
		writeSetPolicies[k] = samePolicy
	}

	for k, sameValue := range sameSetValues {
		readSetValues[k] = sameValue
		writeSetValues[k] = sameValue
	}

	for k, sameGroup := range sameSetGroups {
		readSetGroups[k] = sameGroup
		writeSetGroups[k] = sameGroup
	}

	return &cb.ConfigGroup{
			Version:  original.Version,
			Policies: readSetPolicies,
			Values:   readSetValues,
			Groups:   readSetGroups,
		}, &cb.ConfigGroup{
			Version:   original.Version + 1,
			Policies:  writeSetPolicies,
			Values:    writeSetValues,
			Groups:    writeSetGroups,
			ModPolicy: updated.ModPolicy,
		}, true
}

// Compute 函数用于计算给定原始配置和更新配置之间的配置更新。
// 输入参数：
//   - original：原始配置。
//   - updated：更新后的配置。
//
// 返回值：
//   - *cb.ConfigUpdate：计算得到的配置更新。
//   - error：如果计算配置更新过程中出现错误，则返回相应的错误信息。
func Compute(original, updated *cb.Config) (*cb.ConfigUpdate, error) {
	if original.ChannelGroup == nil {
		return nil, fmt.Errorf("未包含用于原始配置的通道组")
	}

	if updated.ChannelGroup == nil {
		return nil, fmt.Errorf("未包含用于更新配置的通道组")
	}

	// 计算通道组的更新
	readSet, writeSet, groupUpdated := computeGroupUpdate(original.ChannelGroup, updated.ChannelGroup)
	if !groupUpdated {
		return nil, fmt.Errorf("原始配置和更新配置之间未检测到差异")
	}

	// 创建配置更新对象
	return &cb.ConfigUpdate{
		ReadSet:  readSet,
		WriteSet: writeSet,
	}, nil
}
