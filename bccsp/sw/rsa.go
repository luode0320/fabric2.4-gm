/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sw

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/bccsp"
)

// 一个 rsaPrivateKey 包装RSA公共的标准库实现
// 具有满足bccsp.key接口的功能的键。
//
// 注意: Fabric不支持RSA签名或验证。此代码只是
// 允许msp在其证书链中包含RSA CAs。
type rsaPrivateKey struct {
	privKey *rsa.PrivateKey
}

func (k *rsaPrivateKey) Symmetric() bool { return false }
func (k *rsaPrivateKey) Private() bool   { return true }

func (k *rsaPrivateKey) PublicKey() (bccsp.Key, error) {
	return &rsaPublicKey{&k.privKey.PublicKey}, nil
}

// Bytes 将此键转换为其序列化表示形式。
func (k *rsaPrivateKey) Bytes() (raw []byte, err error) {
	if k.privKey == nil {
		return nil, errors.New("私钥是nil.")
	}
	raw, err = x509.MarshalPKIXPublicKey(k.privKey)
	if err != nil {
		return nil, fmt.Errorf("解析私钥失败 [%s]", err)
	}
	return
}

// SKI 返回此密钥的主题密钥标识符。
func (k *rsaPrivateKey) SKI() []byte {
	if k.privKey == nil {
		return nil
	}
	raw := x509.MarshalPKCS1PrivateKey(k.privKey)
	hash := sha256.Sum256(raw)
	return hash[:]
}

// Key 返回公钥/私钥
func (k *rsaPrivateKey) Key() interface{} {
	return k.privKey
}

// 一个 rsaPublicKey 包装RSA公共的标准库实现
// 具有满足bccsp.key接口的功能的键。
//
// 注意: Fabric不支持RSA签名或验证。此代码只是
// 允许msp在其证书链中包含RSA CAs。
type rsaPublicKey struct {
	pubKey *rsa.PublicKey
}

func (k *rsaPublicKey) Symmetric() bool               { return false }
func (k *rsaPublicKey) Private() bool                 { return false }
func (k *rsaPublicKey) PublicKey() (bccsp.Key, error) { return k, nil }

// Bytes 将此键转换为其序列化表示形式。
func (k *rsaPublicKey) Bytes() (raw []byte, err error) {
	if k.pubKey == nil {
		return nil, errors.New("公键是nil.")
	}
	raw, err = x509.MarshalPKIXPublicKey(k.pubKey)
	if err != nil {
		return nil, fmt.Errorf("解析公钥失败 [%s]", err)
	}
	return
}

// SKI 返回此密钥的主题密钥标识符。
func (k *rsaPublicKey) SKI() []byte {
	if k.pubKey == nil {
		return nil
	}

	// Marshal the public key and hash it
	raw := x509.MarshalPKCS1PublicKey(k.pubKey)
	hash := sha256.Sum256(raw)
	return hash[:]
}

// Key 返回公钥/私钥
func (k *rsaPublicKey) Key() interface{} {
	return k.pubKey
}

// PackingrsaKey 包装rsa key
func PackingrsaKey(key interface{}) (bccsp.Key, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return &rsaPrivateKey{privKey: k}, nil
	case *rsa.PublicKey:
		return &rsaPublicKey{pubKey: k}, nil
	default:
		return nil, errors.New("密钥类型无效。它必须是*rsa.PrivateKey、*rsa.PublicKey")
	}
}
