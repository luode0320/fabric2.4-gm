/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sw

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"

	"github.com/hyperledger/fabric/bccsp"
)

type aes256ImportKeyOptsKeyImporter struct{}

// KeyImport 对称密钥导入
func (*aes256ImportKeyOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	aesRaw, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("无效的原材料。预期的字节数组.")
	}

	if aesRaw == nil {
		return nil, errors.New("无效的原材料。它不能为零.")
	}

	if len(aesRaw) != 32 {
		return nil, fmt.Errorf("密钥长度无效 [%d]. 必须为32字节", len(aesRaw))
	}

	return &aesPrivateKey{aesRaw, false}, nil
}

type hmacImportKeyOptsKeyImporter struct{}

// KeyImport hmac密钥导入
func (*hmacImportKeyOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	aesRaw, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("无效的原材料。预期的字节数组.")
	}

	if len(aesRaw) == 0 {
		return nil, errors.New("无效的原材料。它不能为零.")
	}

	return &aesPrivateKey{aesRaw, false}, nil
}

type ecdsaPKIXPublicKeyImportOptsKeyImporter struct{}

// KeyImport ecdsa公钥导入
func (*ecdsaPKIXPublicKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	der, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("无效的原材料。预期的字节数组.")
	}

	if len(der) == 0 {
		return nil, errors.New("raw无效。它不能为零.")
	}

	lowLevelKey, err := derToPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("将PKIX转换为ECDSA公钥失败 [%s]", err)
	}

	ecdsaPK, ok := lowLevelKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("转换为ECDSA公钥失败。无效的原材料.")
	}

	return &ecdsaPublicKey{ecdsaPK}, nil
}

type ecdsaPrivateKeyImportOptsKeyImporter struct{}

// KeyImport ecdsa私钥导入
func (*ecdsaPrivateKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	der, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("[ECDSADERPrivateKeyImportOpts] 无效的原材料。预期的字节数组.")
	}

	if len(der) == 0 {
		return nil, errors.New("[ECDSADERPrivateKeyImportOpts] raw无效。它不能为零.")
	}

	lowLevelKey, err := derToPrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("将PKIX转换为ECDSA公钥失败 [%s]", err)
	}

	ecdsaSK, ok := lowLevelKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("转换到ECDSA私钥失败。无效原材料.")
	}

	return &ecdsaPrivateKey{ecdsaSK}, nil
}

type ecdsaGoPublicKeyImportOptsKeyImporter struct{}

// KeyImport ecdsa Go公共密钥导入选择密钥导入器
func (*ecdsaGoPublicKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	// 将需要转换的公钥 raw内容强转为 ecdsa.PublicKey 实例
	lowLevelKey, ok := raw.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("无效的原材料。预期 *ecdsa.PublicKey.")
	}
	// 返回公钥
	return &ecdsaPublicKey{lowLevelKey}, nil
}

type x509PublicKeyImportOptsKeyImporter struct {
	bccsp *CSP
}

// KeyImport x 509公钥导入选择密钥导入器
func (ki *x509PublicKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (bccsp.Key, error) {
	// 将需要转换的x509 raw内容强转为 x509.Certificate 实例
	x509Cert, ok := raw.(*x509.Certificate)
	if !ok {
		return nil, errors.New("无效的原材料。预期 * x509.Certificate.")
	}

	// 获取公钥
	pk := x509Cert.PublicKey
	// 强转公钥类型
	switch pk := pk.(type) {
	case *ecdsa.PublicKey:
		// 根据 reflect.Type 从CSP实例中获取ECDSAGO的导入器实例
		// 并且调用其KeyImport方法
		return ki.bccsp.KeyImporters[reflect.TypeOf(&bccsp.ECDSAGoPublicKeyImportOpts{})].KeyImport(
			pk,
			&bccsp.ECDSAGoPublicKeyImportOpts{Temporary: opts.Ephemeral()})
	case *rsa.PublicKey:
		// 此路径仅用于支持使用RSA证书的环境
		// 颁发ECDSA证书的机构。
		return &rsaPublicKey{pubKey: pk}, nil
	default:
		return nil, errors.New("无法识别证书的公钥类型。支持的PublicKey: [ECDSA, RSA]")
	}
}
