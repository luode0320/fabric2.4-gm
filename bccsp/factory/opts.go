/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package factory

// GetDefaultOpts 为Opts提供默认实现
// 每次返回一个新实例
func GetDefaultOpts() *FactoryOpts {
	return &FactoryOpts{
		Default: "GM",
		SW: &SwOpts{
			Hash:     "GMSM3",
			Security: 256,
		},
	}
}

// FactoryName 返回提供程序的名称
func (o *FactoryOpts) FactoryName() string {
	return o.Default
}
