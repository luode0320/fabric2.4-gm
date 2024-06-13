/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policies

import (
	"strings"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/pkg/errors"
)

// ImplicitMetaFromString 从字符串中解析并创建一个 ImplicitMetaPolicy。
// 输入参数：
//   - input：string，表示输入的字符串
//
// 返回值：
//   - *cb.ImplicitMetaPolicy：表示创建的 ImplicitMetaPolicy 实例
//   - error：表示解析过程中可能发生的错误
func ImplicitMetaFromString(input string) (*cb.ImplicitMetaPolicy, error) {
	// 将输入字符串按空格分割为两个部分
	args := strings.Split(input, " ")
	if len(args) != 2 {
		return nil, errors.Errorf("预期空格分隔的两个令牌, 但得到了 %d 个", len(args))
	}

	// 创建一个新的 ImplicitMetaPolicy 实例，并设置 SubPolicy 字段
	res := &cb.ImplicitMetaPolicy{
		SubPolicy: args[1],
	}

	// 根据第一个部分的值设置 Rule 字段
	switch args[0] {
	case cb.ImplicitMetaPolicy_ANY.String():
		res.Rule = cb.ImplicitMetaPolicy_ANY
	case cb.ImplicitMetaPolicy_ALL.String():
		res.Rule = cb.ImplicitMetaPolicy_ALL
	case cb.ImplicitMetaPolicy_MAJORITY.String():
		res.Rule = cb.ImplicitMetaPolicy_MAJORITY
	default:
		return nil, errors.Errorf("未知的通道 Policies 规则类型 '%s', 只支持 ALL、ANY、MAJORITY", args[0])
	}

	return res, nil
}
