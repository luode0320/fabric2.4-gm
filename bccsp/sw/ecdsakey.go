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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/bccsp"
)

type ecdsaPrivateKey struct {
	privKey *ecdsa.PrivateKey
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *ecdsaPrivateKey) Bytes() ([]byte, error) {
	return nil, errors.New("不支持转字节.")
}

// SKI 返回此密钥的主题密钥标识符。
func (k *ecdsaPrivateKey) SKI() []byte {
	if k.privKey == nil {
		return nil
	}

	// Marshall公钥
	raw := elliptic.Marshal(k.privKey.Curve, k.privKey.PublicKey.X, k.privKey.PublicKey.Y)

	// 散列它
	hash := sha256.New()
	hash.Write(raw)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥是不对称的，则为false
func (k *ecdsaPrivateKey) Symmetric() bool {
	return false
}

// Private 如果此密钥是私钥，则返回true，
// 否则为false。
func (k *ecdsaPrivateKey) Private() bool {
	return true
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *ecdsaPrivateKey) PublicKey() (bccsp.Key, error) {
	return &ecdsaPublicKey{&k.privKey.PublicKey}, nil
}

// Key 返回公钥/私钥
func (k *ecdsaPrivateKey) Key() interface{} {
	return k.privKey
}

type ecdsaPublicKey struct {
	pubKey *ecdsa.PublicKey
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *ecdsaPublicKey) Bytes() (raw []byte, err error) {
	raw, err = x509.MarshalPKIXPublicKey(k.pubKey)
	if err != nil {
		return nil, fmt.Errorf("转ecdsa公钥字节失败 [%s]", err)
	}
	return
}

// SKI 返回此密钥的主题密钥标识符。
func (k *ecdsaPublicKey) SKI() []byte {
	if k.pubKey == nil {
		return nil
	}

	// Marshall公钥
	raw := elliptic.Marshal(k.pubKey.Curve, k.pubKey.X, k.pubKey.Y)

	// 散列它
	hash := sha256.New()
	hash.Write(raw)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥是不对称的，则为false
func (k *ecdsaPublicKey) Symmetric() bool {
	return false
}

// Private 如果此密钥是私钥，则返回true，
// 否则为false。
func (k *ecdsaPublicKey) Private() bool {
	return false
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *ecdsaPublicKey) PublicKey() (bccsp.Key, error) {
	return k, nil
}

// Key 返回公钥/私钥
func (k *ecdsaPublicKey) Key() interface{} {
	return k.pubKey
}

// PackingecdsaKey 包装ecdsa key
func PackingecdsaKey(key interface{}) (bccsp.Key, error) {
	switch k := key.(type) {
	case *ecdsa.PrivateKey:
		return &ecdsaPrivateKey{privKey: k}, nil
	case *ecdsa.PublicKey:
		return &ecdsaPublicKey{pubKey: k}, nil
	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PrivateKey、*ecdsa.PublicKey")
	}
}
