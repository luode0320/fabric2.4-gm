package gm

import (
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"

	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/utils"
)

// 实现内部的 KeyImporter 接口
type gmsm4ImportKeyOptsKeyImporter struct{}

// KeyImport 密钥导入器, 导入一个sm4的对称密钥
func (*gmsm4ImportKeyOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	sm4Raw, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("无效的原材料。应为字节数组。")
	}

	if sm4Raw == nil {
		return nil, errors.New("无效的原材料。它不能是零。")
	}
	// 赋值密钥, 生成sm4的对称密钥
	return &gmsm4PrivateKey{utils.Clone(sm4Raw), false}, nil
}

type gmsm2PrivateKeyImportOptsKeyImporter struct{}

// KeyImport 密钥导入器, 导入一个sm2的私钥。raw 属于 der/asn1 编码
func (*gmsm2PrivateKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	der, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("[GMSM2PrivateKeyImportOpts] 无效的原材料。预期为字节数组。")
	}

	if len(der) == 0 {
		return nil, errors.New("[GMSM2PrivateKeyImportOpts] raw无效。它不能为空。")
	}
	// byte[]转换为私钥
	lowLevelKey, err := utils.DERToPrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("转换密钥失败 [%s]", err)
	}

	gmsm2SK, ok := lowLevelKey.(*sm2.PrivateKey)
	if !ok {
		return nil, errors.New("转换私钥失败。无效私钥")
	}
	// 生成一个sm2的密钥
	return &gmsm2PrivateKey{gmsm2SK}, nil
}

type gmsm2PublicKeyImportOptsKeyImporter struct{}

// KeyImport 密钥导入器, 导入一个sm2的公钥
func (*gmsm2PublicKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	der, ok := raw.([]byte)
	if !ok {
		return nil, errors.New("[GMSM2PublicKeyImportOpts] 无效的原材料。应为字节数组。")
	}

	if len(der) == 0 {
		return nil, errors.New("[GMSM2PublicKeyImportOpts] raw无效。它不能是零。")
	}
	// byte转公钥
	lowLevelKey, err := utils.DERToGMPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("转换公钥失败 [%s]", err)
	}

	gmsm2SK, ok := lowLevelKey.(*sm2.PublicKey)
	if !ok {
		return nil, errors.New("转换为公钥失败。无效的公钥。")
	}
	// 生成一个sm2的公钥
	return &gmsm2PublicKey{gmsm2SK}, nil
}

type x509PublicKeyImportOptsKeyImporter struct {
	bccsp *CSP
}

// KeyImport 密钥导入器, 导入一个x509的公钥
func (ki *x509PublicKeyImportOptsKeyImporter) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	// 转换为 x509GM 证书
	sm2Cert, ok := raw.(*x509GM.Certificate)
	if !ok {
		cert, ok := raw.(*x509.Certificate)
		if !ok {
			return nil, errors.New("无效的原材料。预期 x509GM.证书格式")
		}
		// X509 证书格式转换为 x509GM SM2 证书格式
		sm2Cert = ParseX509Certificate2Sm2(cert)
	}

	// 获取证书中的公钥
	var sm2PublickKey *sm2.PublicKey
	// 所以适配一下这个ecdsa实例
	switch pk := sm2Cert.PublicKey.(type) {
	case ecdsa.PublicKey:
		sm2PublickKey = &sm2.PublicKey{
			Curve: pk.Curve,
			X:     pk.X,
			Y:     pk.Y,
		}
	case *ecdsa.PublicKey:
		sm2PublickKey = &sm2.PublicKey{
			Curve: pk.Curve,
			X:     pk.X,
			Y:     pk.Y,
		}
	case sm2.PublicKey:
		sm2PublickKey = &sm2.PublicKey{
			Curve: pk.Curve,
			X:     pk.X,
			Y:     pk.Y,
		}
	case *sm2.PublicKey:
		sm2PublickKey = &sm2.PublicKey{
			Curve: pk.Curve,
			X:     pk.X,
			Y:     pk.Y,
		}
	default:
		return nil, errors.New("无法识别证书的公钥类型。支持的键: [GMSM2]")
	}

	// 解析为sm2公钥
	der, err := x509GM.MarshalSm2PublicKey(sm2PublickKey)
	if err != nil {
		return nil, errors.New("解析为sm2公钥错误")
	}
	// 调用 GMSM2PublicKeyImportOpts sm2公钥导入器, 用 X509 的公钥生成一个 sm2 公钥
	return ki.bccsp.KeyImporters[reflect.TypeOf(&bccsp.GMSM2PublicKeyImportOpts{})].KeyImport(
		der,
		&bccsp.GMSM2PublicKeyImportOpts{Temporary: opts.Ephemeral()})
}
