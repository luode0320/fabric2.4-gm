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

package utils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
)

// pkcs8Info pkcs8一种标准的私钥编码格式。它定义了一种通用的方式来存储和传输私钥信息，无论私钥使用的是哪种具体的加密算法
// PKCS8格式的私钥通常包含了私钥本身以及与私钥相关的元数据，如算法标识符、版本号等。这种格式的私钥可以被用于不同的加密算法，如RSA、DSA、ECDSA等。
type pkcs8Info struct {
	Version             int                     // 表示PKCS8版本号的整数
	PrivateKeyAlgorithm []asn1.ObjectIdentifier // 表示私钥算法的ASN.1对象标识符（Object Identifier）的切片。
	PrivateKey          []byte                  // 表示私钥的字节数组。
}

// ecPrivateKey ECDSA的私钥信息
type ecPrivateKey struct {
	Version       int                   // 表示私钥版本号的整数。
	PrivateKey    []byte                // 表示私钥的字节数组。
	NamedCurveOID asn1.ObjectIdentifier `asn1:"optional,explicit,tag:0"` // 表示椭圆曲线的ASN.1对象标识符（Object Identifier），它是一个可选字段。
	PublicKey     asn1.BitString        `asn1:"optional,explicit,tag:1"` // 表示公钥的ASN.1位字符串（Bit String），它也是一个可选字段。
}

// 分别表示四种不同的椭圆曲线, 与ecPrivateKey.NamedCurveOID一样
var (
	oidNamedCurveP224 = asn1.ObjectIdentifier{1, 3, 132, 0, 33}
	oidNamedCurveP256 = asn1.ObjectIdentifier{1, 2, 840, 10045, 3, 1, 7}
	oidNamedCurveP384 = asn1.ObjectIdentifier{1, 3, 132, 0, 34}
	oidNamedCurveP521 = asn1.ObjectIdentifier{1, 3, 132, 0, 35}
)

// 标识ECDSA公钥的类型，以便在编码和解码过程中正确地识别和处理ECDSA公钥的数据
var oidPublicKeyECDSA = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}

// oidFromNamedCurve 获取不同的椭圆曲线
func oidFromNamedCurve(curve elliptic.Curve) (asn1.ObjectIdentifier, bool) {
	switch curve {
	case elliptic.P224():
		return oidNamedCurveP224, true
	case elliptic.P256():
		return oidNamedCurveP256, true
	case elliptic.P384():
		return oidNamedCurveP384, true
	case elliptic.P521():
		return oidNamedCurveP521, true
	}
	return nil, false
}

// PrivateKeyToDER 用于将私钥转换为DER编码的字节数组
// DER是一种二进制编码格式，它使用固定的规则将数据编码为字节序列
// DER通常用于在网络通信和存储中传输和保存数据，如数字证书、私钥等
func PrivateKeyToDER(privateKey interface{}) ([]byte, error) {
	if privateKey == nil {
		return nil, errors.New("私钥。它一定不同于nil")
	}
	switch k := privateKey.(type) {
	case *ecdsa.PrivateKey:
		return x509.MarshalECPrivateKey(k)
	case *sm2.PrivateKey:
		return x509GM.MarshalSm2UnecryptedPrivateKey(k)
	default:
		return nil, errors.New("密钥类型无效。它必须是*sm2.PrivateKey、*ecdsa.PrivateKey、*rsa.PrivateKey")
	}
}

// PrivateKeyToPEM 私钥转pem
// PEM则是一种基于文本的编码格式，它使用Base64编码将数据转换为可打印的ASCII字符序列，并添加了起始标记和结束标记
// PEM则常用于将DER编码的数据转换为文本格式，以便在配置文件、邮件等文本环境中使用
func PrivateKeyToPEM(privateKey interface{}, pwd []byte) ([]byte, error) {
	// 验证输入
	if len(pwd) != 0 {
		return PrivateKeyToEncryptedPEM(privateKey, pwd)
	}
	if privateKey == nil {
		return nil, errors.New("无效密钥, 它必须不等于 nil")
	}

	switch k := privateKey.(type) {
	case *sm2.PrivateKey:
		if k == nil {
			return nil, errors.New("sm2私钥无效。它必须不等于 nil.")
		}
		return x509GM.WritePrivateKeyToPem(k, nil)
	case *ecdsa.PrivateKey:
		if k == nil {
			return nil, errors.New("ecdsa私钥无效, 它必须不等于 nil.")
		}

		// 获取曲线的oid
		oidNamedCurve, ok := oidFromNamedCurve(k.Curve)
		if !ok {
			return nil, errors.New("未知椭圆曲线")
		}

		// 将私钥的字节序列进行填充，以满足ASN.1编码的要求
		// based on https://golang.org/src/crypto/x509/sec1.go
		privateKeyBytes := k.D.Bytes()
		paddedPrivateKey := make([]byte, (k.Curve.Params().N.BitLen()+7)/8)
		copy(paddedPrivateKey[len(paddedPrivateKey)-len(privateKeyBytes):], privateKeyBytes)
		// 使用ASN.1编码将EC私钥封装为ASN.1结构, 省略NamedCurveOID的兼容性，因为它是可选的
		asn1Bytes, err := asn1.Marshal(ecPrivateKey{
			Version:    1,
			PrivateKey: paddedPrivateKey,
			PublicKey:  asn1.BitString{Bytes: elliptic.Marshal(k.Curve, k.X, k.Y)},
		})

		if err != nil {
			return nil, fmt.Errorf("将EC密钥封送至asn1 [%s]", err)
		}
		// 将其转换为PKCS#8格式的字节序列
		var pkcs8Key pkcs8Info
		pkcs8Key.Version = 0
		pkcs8Key.PrivateKeyAlgorithm = make([]asn1.ObjectIdentifier, 2)
		pkcs8Key.PrivateKeyAlgorithm[0] = oidPublicKeyECDSA
		pkcs8Key.PrivateKeyAlgorithm[1] = oidNamedCurve
		pkcs8Key.PrivateKey = asn1Bytes

		pkcs8Bytes, err := asn1.Marshal(pkcs8Key)
		if err != nil {
			return nil, fmt.Errorf("将EC密钥封送至asn1 [%s]", err)
		}
		// 将PKCS#8格式的私钥转换为PEM格式，并返回PEM编码的字节序列
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: pkcs8Bytes,
			},
		), nil
	case *rsa.PrivateKey:
		if k == nil {
			return nil, errors.New("无效的rsa私钥。它必须不同于nil.")
		}
		// 返回经过PKCS#1编码的私钥的字节数组(PKCS#1编码可以看作是ASN.1数据的一种特定的DER编码形式)
		// PKCS#1编码是一种用于RSA密钥和签名的编码标准, 组合了asn1和der
		raw := x509.MarshalPKCS1PrivateKey(k)
		// 转pem
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "RSA PRIVATE KEY",
				Bytes: raw,
			},
		), nil
	default:
		return nil, errors.New("密钥类型无效, 它必须是 *sm2.PrivateKey、*ecdsa.PrivateKey、*rsa.PrivateKey")
	}
}

// PrivateKeyToEncryptedPEM ecdsa私钥转pem, 并加密pem
// PEM则是一种基于文本的编码格式，它使用Base64编码将数据转换为可打印的ASCII字符序列，并添加了起始标记和结束标记
// PEM则常用于将DER编码的数据转换为文本格式，以便在配置文件、邮件等文本环境中使用
func PrivateKeyToEncryptedPEM(privateKey interface{}, pwd []byte) ([]byte, error) {
	if privateKey == nil {
		return nil, errors.New("无效的私钥。它一定不同于nil.")
	}

	switch k := privateKey.(type) {
	case *ecdsa.PrivateKey:
		if k == nil {
			return nil, errors.New("ecdsa私钥无效。它必须不同于nil.")
		}
		// 接受一个ECDSA私钥key作为参数，并返回经过编码的私钥的字节数组
		// 函数内部首先调用oidFromNamedCurve函数来获取给定椭圆曲线的OID（Object Identifier）。
		// 然后，调用marshalECPrivateKeyWithOID函数，将私钥和椭圆曲线的OID作为参数进行编码。
		// 最后，将编码后的私钥表示为字节数组，并返回。
		raw, err := x509.MarshalECPrivateKey(k)

		if err != nil {
			return nil, err
		}
		// 加密的pem
		block, err := x509.EncryptPEMBlock(
			rand.Reader,
			"PRIVATE KEY",
			raw,
			pwd,
			x509.PEMCipherAES256)

		if err != nil {
			return nil, err
		}

		return pem.EncodeToMemory(block), nil
	case *sm2.PrivateKey:
		if k == nil {
			return nil, errors.New("sm2私钥无效。它必须不同于nil.")
		}
		// 加密的pem
		return x509GM.WritePrivateKeyToPem(k, pwd)
	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PrivateKey、*sm2.PrivateKey")
	}
}

// DERToPrivateKey der编码转私钥
// // DER是一种二进制编码格式，它使用固定的规则将数据编码为字节序列
// // DER通常用于在网络通信和存储中传输和保存数据，如数字证书、私钥等
func DERToPrivateKey(der []byte) (key interface{}, err error) {
	// 尝试数据解析为PKCS1格式的私钥对象, PKCS#1编码是一种用于RSA密钥和签名的编码标准, 组合了asn1和der
	if key, err = x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	// 尝试数据解析为PKCS8格式的私钥对象, 多种算法(rsa、ecdsa)
	if key, err = x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey:
			return
		default:
			return nil, errors.New("在PKCS #8包装中发现未知的私钥类型")
		}
	}
	// 尝试数据解析为ECDSA格式的私钥对象
	if key, err = x509.ParseECPrivateKey(der); err == nil {
		return
	}
	// 尝试数据解析为GMSM2未加密私钥对象, 内部调用了ParseSm2PrivateKey
	if key, err = x509GM.ParsePKCS8UnecryptedPrivateKey(der); err == nil {
		return
	}
	// 尝试数据解析为GMSM2格式的私钥对象
	if key, err = x509GM.ParseSm2PrivateKey(der); err == nil {
		return
	}

	return nil, errors.New("密钥类型无效。支持的密钥类型RSA、ECDSA、GMSM2")
}

// PEMtoPrivateKey pem转私钥, 内部使用 DERToPrivateKey 转私钥
func PEMtoPrivateKey(raw []byte, pwd []byte) (interface{}, error) {
	if len(raw) == 0 {
		return nil, errors.New("PEM无效。它必须不同于nil.")
	}
	// pem格式解密为der二进制格式
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码PEM失败。块必须不同于nil. [% x]", raw)
	}

	// TODO: 从标头派生密钥的类型
	// 是否加密
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) == 0 {
			return nil, errors.New("加密密钥。需要密码")
		}

		decrypted, err := x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("PEM解密失败 [%s]", err)
		}
		// 解析参数到私钥
		key, err := DERToPrivateKey(decrypted)
		if err != nil {
			return nil, err
		}
		return key, err
	}
	// der编码转ecdsa私钥
	cert, err := DERToPrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, err
}

// PEMtoAES pem转aes对称密钥
func PEMtoAES(raw []byte, pwd []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("PEM无效。它必须不同于nil.")
	}
	// 找到下一个PEM格式的块 (证书，私钥等)。它返回该块和输入的其余部分
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码PEM失败。块必须不同于nil. [% x]", raw)
	}
	// 是否加密的PEM块
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) == 0 {
			return nil, errors.New("加密密钥。密码必须是不同的fomnil")
		}

		decrypted, err := x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("PEM解密失败. [%s]", err)
		}
		return decrypted, nil
	}

	return block.Bytes, nil
}

// AEStoPEM aes转pem
func AEStoPEM(raw []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "AES PRIVATE KEY", Bytes: raw})
}

// AEStoEncryptedPEM aes转pem, 并加密
func AEStoEncryptedPEM(raw []byte, pwd []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("aes密钥无效。它必须不同于nil")
	}
	if len(pwd) == 0 {
		return AEStoPEM(raw), nil
	}
	// 加密
	block, err := x509.EncryptPEMBlock(
		rand.Reader,
		"AES PRIVATE KEY",
		raw,
		pwd,
		x509.PEMCipherAES256)

	if err != nil {
		return nil, err
	}
	// 编码到内存
	return pem.EncodeToMemory(block), nil
}

// PublicKeyToPEM ecdsa公钥转pem
func PublicKeyToPEM(publicKey interface{}, pwd []byte) ([]byte, error) {
	if len(pwd) != 0 {
		return PublicKeyToEncryptedPEM(publicKey, pwd)
	}

	if publicKey == nil {
		return nil, errors.New("公钥无效。它必须不同于nil.")
	}

	switch k := publicKey.(type) {
	case *ecdsa.PublicKey:
		if k == nil {
			return nil, errors.New("无效的ecdsa公钥。它一定不同于nil.")
		}
		PubASN1, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}

		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "PUBLIC KEY",
				Bytes: PubASN1,
			},
		), nil
	case *rsa.PublicKey:
		if k == nil {
			return nil, errors.New("rsa公钥无效。它必须不同于nil.")
		}
		PubASN1, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}

		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "RSA PUBLIC KEY",
				Bytes: PubASN1,
			},
		), nil
	case *sm2.PublicKey:
		if k == nil {
			return nil, errors.New("sm2公钥无效。它必须不同于nil.")
		}

		return x509GM.WritePublicKeyToPem(k)
	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PublicKey、*rsa.PublicKey、*sm2.PublicKey")
	}
}

// PublicKeyToEncryptedPEM ecdsa公钥转pem, 并加密
func PublicKeyToEncryptedPEM(publicKey interface{}, pwd []byte) ([]byte, error) {
	if publicKey == nil {
		return nil, errors.New("公钥无效。它必须不同于nil.")
	}
	if len(pwd) == 0 {
		return nil, errors.New("密码无效。它必须不同于nil.")
	}

	switch k := publicKey.(type) {
	case *ecdsa.PublicKey:
		if k == nil {
			return nil, errors.New("无效的ecdsa公钥。它一定不同于nil.")
		}
		raw, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}

		block, err := x509.EncryptPEMBlock(
			rand.Reader,
			"PUBLIC KEY",
			raw,
			pwd,
			x509.PEMCipherAES256)

		if err != nil {
			return nil, err
		}

		return pem.EncodeToMemory(block), nil
	case *sm2.PublicKey:
		if k == nil {
			return nil, errors.New("无效的ecdsa公钥。它一定不同于nil.")
		}

		return x509GM.WritePublicKeyToPem(k)
	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PublicKey、*sm2.PublicKey")
	}
}

// PublicKeyToDER ecdsa公钥转der
func PublicKeyToDER(publicKey interface{}) ([]byte, error) {
	if publicKey == nil {
		return nil, errors.New("公钥无效。它必须不同于nil.")
	}

	switch k := publicKey.(type) {
	case *ecdsa.PublicKey:
		if k == nil {
			return nil, errors.New("无效的ecdsa公钥。它一定不同于nil.")
		}
		PubASN1, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}

		return PubASN1, nil

	case *rsa.PublicKey:
		if k == nil {
			return nil, errors.New("rsa公钥无效。它必须不同于nil.")
		}
		PubASN1, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}

		return PubASN1, nil

	case *sm2.PublicKey:
		if k == nil {
			return nil, errors.New("GMSM2公钥无效。它必须不同于nil.")
		}
		der, err := x509GM.MarshalSm2PublicKey(k)
		if err != nil {
			return nil, err
		}
		return der, nil

	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PublicKey、*rsa.PublicKey、*sm2.PublicKey")
	}
}

// PEMtoPublicKey pem转公钥
func PEMtoPublicKey(raw []byte, pwd []byte) (interface{}, error) {
	if len(raw) == 0 {
		return nil, errors.New("PEM无效。它必须不同于nil.")
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码失败。块必须不同于nil. [% x]", raw)
	}

	// TODO: derive from header the type of the key
	// 是否加密
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) == 0 {
			return nil, errors.New("加密密钥。密码必须不同于nil")
		}
		// 加密并返回der二进制编码
		decrypted, err := x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("PEM解密失败. [%s]", err)
		}
		// der二进制转asn1格式字节公钥
		key, err := DERToPublicKey(decrypted)
		if err != nil {
			return nil, err
		}
		return key, err
	}
	// der二进制转asn1格式字节公钥
	cert, err := DERToPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, err
}

// DERToPublicKey der二进制转asn1格式字节公钥
func DERToPublicKey(raw []byte) (pub interface{}, err error) {
	if len(raw) == 0 {
		return nil, errors.New("DER无效。它必须不同于nil.")
	}
	key, err := x509.ParsePKIXPublicKey(raw)
	return key, err
}

// DERToGMPublicKey byte转国密sm2公钥
func DERToGMPublicKey(raw []byte) (pub interface{}, err error) {
	if len(raw) == 0 {
		return nil, errors.New("禁用DER。它必须与nil不同。")
	}
	// 尝试解析为GMSM2公钥对象
	if key, err := x509GM.ParseSm2PublicKey(raw); err == nil {
		return key, nil
	}
	// 尝试解析为PKIX格式的公钥对象(基于ECDSA（椭圆曲线数字签名算法）的公钥对象)
	key, err := x509GM.ParsePKIXPublicKey(raw)
	if err != nil {
		return nil, err
	}

	ecdsaPublicKey, ok := key.(*ecdsa.PublicKey)
	if ok {
		// 检查公钥的曲线是否为GMSM2曲线（P256Sm2）
		if ecdsaPublicKey.Curve == sm2.P256Sm2() {
			// 公钥对象转换为GMSM2公钥对象
			key = &sm2.PublicKey{Curve: sm2.P256Sm2(), X: ecdsaPublicKey.X, Y: ecdsaPublicKey.Y}
		}
	}
	return key, nil
}
