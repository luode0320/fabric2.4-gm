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

// KeyStore 表示加密密钥的存储系统。
// 它允许存储和检索bccsp.Key对象。
// 密钥库可以是只读的，在这种情况下，StoreKey将返回一个错误。
type KeyStore interface {

	// ReadOnly 如果此密钥库是只读的，则返回true，否则返回false。
	// 如果ReadOnly为true，则StoreKey将失败。
	ReadOnly() bool

	// GetKey 返回一个key对象，其SKI是传递的对象。
	GetKey(ski []byte) (k Key, err error)

	// StoreKey 将密钥k存储在此密钥库中。
	// 如果此密钥库是只读的，则该方法将失败。
	StoreKey(k Key) (err error)
}
