/*
Copyright IBM Corp. 2017 All Rights Reserved.

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

package sw

import (
	"hash"

	"github.com/hyperledger/fabric/bccsp"
)

type hasher struct {
	hash func() hash.Hash
}

// Hash 接受一个消息（msg），并返回对消息进行哈希后的结果(参数opts无用)
func (c *hasher) Hash(msg []byte, opts bccsp.HashOpts) ([]byte, error) {
	// 获取一个哈希实例（hash.Hash）
	h := c.hash()
	// 将消息写入哈希实例
	h.Write(msg)
	// 调用Sum(nil)方法获取哈希结果
	return h.Sum(nil), nil
}

// GetHash 返回一个哈希实例(参数opts无用)
func (c *hasher) GetHash(opts bccsp.HashOpts) (hash.Hash, error) {
	return c.hash(), nil
}
