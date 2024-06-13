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
package sw

import (
	"crypto/sha256"
	"errors"
	"github.com/hyperledger/fabric/bccsp"
)

type aesPrivateKey struct {
	privKey    []byte
	exportable bool
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *aesPrivateKey) Bytes() (raw []byte, err error) {
	if k.exportable {
		return k.privKey, nil
	}

	return nil, errors.New("不支持转字节.")
}

// SKI 返回此密钥的主题密钥标识符。
func (k *aesPrivateKey) SKI() (ski []byte) {
	hash := sha256.New()
	hash.Write([]byte{0x01})
	hash.Write(k.privKey)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥是不对称的，则为false
func (k *aesPrivateKey) Symmetric() bool {
	return true
}

// Private 如果此密钥是私钥，则返回true，
// 否则为false。
func (k *aesPrivateKey) Private() bool {
	return true
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *aesPrivateKey) PublicKey() (bccsp.Key, error) {
	return nil, errors.New("无法对对称密钥调用此方法.")
}

// Key 返回公钥/私钥
func (k *aesPrivateKey) Key() interface{} {
	return k.privKey
}
