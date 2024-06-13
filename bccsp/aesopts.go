/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bccsp

import "io"

// AES128KeyGenOpts 包含128安全级别的AES密钥生成选项
type AES128KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *AES128KeyGenOpts) Algorithm() string {
	return AES128
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *AES128KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// AES192KeyGenOpts 包含192安全级别的AES密钥生成选项
type AES192KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *AES192KeyGenOpts) Algorithm() string {
	return AES192
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *AES192KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// AES256KeyGenOpts 包含256安全级别的AES密钥生成选项
type AES256KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *AES256KeyGenOpts) Algorithm() string {
	return AES256
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *AES256KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// AESCBCPKCS7ModeOpts 包含CBC模式下的AES加密选项
// 用PKCS7填充。
// 注意IV和PRNG都可以是nil。在这种情况下，BCCSP实现
// 应该使用加密安全PRNG对IV进行采样。
// 还要注意，IV或PRNG可以不同于nil。
type AESCBCPKCS7ModeOpts struct {
	// IV是底层密码使用的初始化向量。
	// IV的长度必须与块的块大小相同。
	// 它只在不同于nil时使用。
	IV []byte
	// PRNG是要由基础密码使用的PRNG的实例。
	// 它只在不同于nil时使用。
	PRNG io.Reader
}
