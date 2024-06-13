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

// ECDSAP256KeyGenOpts 包含使用曲线P-256生成ECDSA密钥的选项。
type ECDSAP256KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *ECDSAP256KeyGenOpts) Algorithm() string {
	return ECDSAP256
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAP256KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// ECDSAP384KeyGenOpts 包含使用曲线P-384生成ECDSA密钥的选项。
type ECDSAP384KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *ECDSAP384KeyGenOpts) Algorithm() string {
	return ECDSAP384
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAP384KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}
