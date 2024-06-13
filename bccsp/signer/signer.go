/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package signer

import (
	"crypto"
	"io"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/pkg/errors"
)

// bccspCryptoSigner 是crypto.Signer的基于BCCSP的实现
type bccspCryptoSigner struct {
	csp bccsp.BCCSP // csp实例, 提供密码标准和算法的实现。
	key bccsp.Key   // 非对称密钥
	pk  interface{} // 公钥( crypto.Signer 类型)
}

// New 对外提供的加密对象, 返回一个新的基于BCCSP的crypto.Signer对象
func New(csp bccsp.BCCSP, key bccsp.Key) (crypto.Signer, error) {
	// 验证参数
	if csp == nil {
		return nil, errors.New("bccsp实例不能为nil.")
	}
	if key == nil {
		return nil, errors.New("密钥key不能为nil.")
	}
	if key.Symmetric() {
		return nil, errors.New("密钥必须是非对称密钥.")
	}

	// Marshall将bccsp公钥作为crypto.PublicKey
	pub, err := key.PublicKey()
	if err != nil {
		return nil, errors.Wrap(err, "获取公钥失败")
	}
	//
	//raw, err := pub.Bytes()
	//if err != nil {
	//	return nil, errors.Wrap(err, "获取公钥失败")
	//}
	//// 解析为公钥对象
	//pk, err := x509.ParsePKIXPublicKey(raw)
	//if err != nil {
	//	return nil, errors.Wrap(err, "将公钥der解码为到公钥实例失败")
	//}

	// 签名对象
	return &bccspCryptoSigner{csp, key, pub.Key()}, nil
}

// Public 对外提供获取公钥, 返回对应于不透明的公钥，
// 私钥。
func (s *bccspCryptoSigner) Public() crypto.PublicKey {
	return s.pk
}

// Sign 使用私钥对给定的摘要进行签名。配合msp.identities.go Verify方法验证
// 方法接收者：bccspCryptoSigner
// 输入参数：
//   - rand：随机数生成器(参数无用)
//   - digest：要签名的摘要(默认摘要未被hash)
//   - opts：签名选项(参数opts无用)
//
// 返回值：
//   - []byte：签名结果。
//   - error：如果签名过程中出现错误，则返回错误；否则返回 nil。
func (s *bccspCryptoSigner) Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	// 使用密码服务提供者（CSP）的 Sign 方法对摘要进行签名
	return s.csp.Sign(s.key, digest, nil)
}
