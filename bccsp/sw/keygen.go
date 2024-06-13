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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"

	"github.com/hyperledger/fabric/bccsp"
)

// ecdsaKeyGenerator ECDSA非对称密钥结构
type ecdsaKeyGenerator struct {
	// curve: 曲线。椭圆曲线参数
	curve elliptic.Curve
}

// KeyGen ECDSA非对称密钥生成
func (kg *ecdsaKeyGenerator) KeyGen(opts bccsp.KeyGenOpts) (bccsp.Key, error) {
	// 直接使用Go标准库的 crypto/ecdsa 加密包生成
	// 调用ecdsa.GenerateKey函数来生成一个ECDSA私钥。私钥包含了公钥的一部分信息，可以通过私钥的计算来生成完整的公钥
	// kg.curve是一个椭圆曲线参数，用于指定生成密钥对所使用的曲线。
	// rand.Reader是一个随机数生成器，用于生成私钥的随机数。
	privKey, err := ecdsa.GenerateKey(kg.curve, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("生成的ECDSA密钥失败 [%v]: [%s]", kg.curve, err)
	}
	// 包装到 ecdsaPrivateKey 中, ecdsaPrivateKey 是 bccsp.Key 接口的实现, 主要是提供了获取私钥/公钥的方法。
	return &ecdsaPrivateKey{privKey}, nil
}

// aesKeyGenerator AES对称密钥结构
type aesKeyGenerator struct {
	// 密钥长度
	length int
}

// KeyGen AES对称密钥生成
func (kg *aesKeyGenerator) KeyGen(opts bccsp.KeyGenOpts) (bccsp.Key, error) {
	// 生成一个指定长度的随机字节序列, 作为对称密钥
	lowLevelKey, err := GetRandomBytes(int(kg.length))
	if err != nil {
		return nil, fmt.Errorf("生成AES失败 %d 钥匙 [%s]", kg.length, err)
	}
	// 包装到 aesPrivateKey 中, ecdsaPrivateKey 是 bccsp.Key 接口的实现, 主要是提供了获取私钥/公钥的方法。
	return &aesPrivateKey{lowLevelKey, false}, nil
}
