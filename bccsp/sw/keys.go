/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sw

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
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

// privateKeyToDER 用于将ECDSA私钥转换为DER编码的字节数组
// DER是一种二进制编码格式，它使用固定的规则将数据编码为字节序列
// DER通常用于在网络通信和存储中传输和保存数据，如数字证书、私钥等
func privateKeyToDER(privateKey *ecdsa.PrivateKey) ([]byte, error) {
	if privateKey == nil {
		return nil, errors.New("无效的ecdsa私钥。它一定不同于nil")
	}

	return x509.MarshalECPrivateKey(privateKey)
}

// privateKeyToPEM ecdsa私钥转pem
// PEM则是一种基于文本的编码格式，它使用Base64编码将数据转换为可打印的ASCII字符序列，并添加了起始标记和结束标记
// PEM则常用于将DER编码的数据转换为文本格式，以便在配置文件、邮件等文本环境中使用
func privateKeyToPEM(privateKey interface{}, pwd []byte) ([]byte, error) {
	// 验证输入
	if len(pwd) != 0 {
		// 使用加密的方式
		return privateKeyToEncryptedPEM(privateKey, pwd)
	}
	if privateKey == nil {
		return nil, errors.New("无效密钥。它必须不同于nil")
	}

	// 获取原型类型
	switch k := privateKey.(type) {
	case *ecdsa.PrivateKey:
		if k == nil {
			return nil, errors.New("ecdsa私钥无效。它必须不同于nil")
		}

		// 获取曲线的oid
		oidNamedCurve, ok := oidFromNamedCurve(k.Curve)
		if !ok {
			return nil, errors.New("未知椭圆曲线")
		}

		// 将私钥的字节序列进行填充，以满足ASN.1编码的要求
		// 基于 https://golang.org/src/crypto/x509/sec1.go
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
			return nil, fmt.Errorf("将EC密钥封送至asn1时出错: [%s]", err)
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
			return nil, fmt.Errorf("将EC密钥封送至asn1时出错: [%s]", err)
		}

		// 将PKCS#8格式的私钥转换为PEM格式，并返回PEM编码的字节序列
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: pkcs8Bytes,
			},
		), nil

	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PrivateKey")
	}
}

// privateKeyToPEM ecdsa私钥转pem, 并加密pem
// PEM则是一种基于文本的编码格式，它使用Base64编码将数据转换为可打印的ASCII字符序列，并添加了起始标记和结束标记
// PEM则常用于将DER编码的数据转换为文本格式，以便在配置文件、邮件等文本环境中使用
func privateKeyToEncryptedPEM(privateKey interface{}, pwd []byte) ([]byte, error) {
	if privateKey == nil {
		return nil, errors.New("私钥无效。它必须不同于nil")
	}

	switch k := privateKey.(type) {
	case *ecdsa.PrivateKey:
		if k == nil {
			return nil, errors.New("ecdsa私钥无效。它必须不同于nil")
		}
		raw, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, err
		}

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

	default:
		return nil, errors.New("密钥类型无效。它必须是 *ecdsa.PrivateKey")
	}
}

// derToPrivateKey der编码转ecdsa私钥
// DER是一种二进制编码格式，它使用固定的规则将数据编码为字节序列
// DER通常用于在网络通信和存储中传输和保存数据，如数字证书、私钥等
func derToPrivateKey(der []byte) (key interface{}, err error) {
	// 解析问rsa私钥, PKCS#1编码是一种用于RSA密钥和签名的编码标准, 组合了asn1和der
	if key, err = x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	// 解析为asn1字节
	if key, err = x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key.(type) {
		case *ecdsa.PrivateKey:
			return
		default:
			return nil, errors.New("在中发现未知的私钥类型 PKCS#8 包装")
		}
	}
	// 解析为ecdsa私钥
	if key, err = x509.ParseECPrivateKey(der); err == nil {
		return
	}

	return nil, errors.New("密钥类型无效。DER必须包含一个ecdsa.PrivateKey")
}

// pemToPrivateKey pem转ecdsa私钥
func pemToPrivateKey(raw []byte, pwd []byte) (interface{}, error) {
	// 找到下一个PEM格式的块 (证书，私钥等)。它返回该块和输入的其余部分
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码PEM失败。块必须不同于nil [% x]", raw)
	}

	// TODO: derive from header the type of the key
	// 是否加密的PEM块
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) == 0 {
			return nil, errors.New("加密密钥。需要密码")
		}

		decrypted, err := x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("PEM解密失败: [%s]", err)
		}

		key, err := derToPrivateKey(decrypted)
		if err != nil {
			return nil, err
		}
		return key, err
	}
	// der编码转ecdsa私钥
	cert, err := derToPrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, err
}

// pemToAES pem转aes对称密钥
func pemToAES(raw []byte, pwd []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("PEM无效。它必须不同于nil")
	}
	// 找到下一个PEM格式的块 (证书，私钥等)。它返回该块和输入的其余部分
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码PEM失败。块必须不同于 nil [% x]", raw)
	}
	// 是否加密的PEM块
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) == 0 {
			return nil, errors.New("加密密钥。密码必须是不同的fomnil")
		}

		decrypted, err := x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("PEM解密失败: [%s]", err)
		}
		return decrypted, nil
	}

	return block.Bytes, nil
}

// aesToPEM aes转pem
func aesToPEM(raw []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "AES PRIVATE KEY", Bytes: raw})
}

// aesToEncryptedPEM aes转pem, 并加密
func aesToEncryptedPEM(raw []byte, pwd []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("aes密钥无效。它必须不同于nil")
	}
	if len(pwd) == 0 {
		return aesToPEM(raw), nil
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

// publicKeyToPEM ecdsa公钥转pem
func publicKeyToPEM(publicKey interface{}, pwd []byte) ([]byte, error) {
	if len(pwd) != 0 {
		return publicKeyToEncryptedPEM(publicKey, pwd)
	}

	if publicKey == nil {
		return nil, errors.New("公钥无效。它必须不同于nil")
	}

	switch k := publicKey.(type) {
	case *ecdsa.PublicKey:
		if k == nil {
			return nil, errors.New("ecdsa公钥无效。它必须不同于nil")
		}
		// 解析为asn1字节
		PubASN1, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}
		// 转为pem内存
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  "PUBLIC KEY",
				Bytes: PubASN1,
			},
		), nil

	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PublicKey")
	}
}

// publicKeyToEncryptedPEM ecdsa公钥转pem, 并加密
func publicKeyToEncryptedPEM(publicKey interface{}, pwd []byte) ([]byte, error) {
	switch k := publicKey.(type) {
	case *ecdsa.PublicKey:
		if k == nil {
			return nil, errors.New("ecdsa公钥无效。它必须不同于nil")
		}
		raw, err := x509.MarshalPKIXPublicKey(k)
		if err != nil {
			return nil, err
		}
		// 转为pem, 并加密
		block, err := x509.EncryptPEMBlock(
			rand.Reader,
			"PUBLIC KEY",
			raw,
			pwd,
			x509.PEMCipherAES256)
		if err != nil {
			return nil, err
		}
		// 转内存
		return pem.EncodeToMemory(block), nil
	default:
		return nil, errors.New("密钥类型无效。它必须是*ecdsa.PublicKey")
	}
}

// pemToPublicKey pem转公钥
func pemToPublicKey(raw []byte, pwd []byte) (interface{}, error) {
	if len(raw) == 0 {
		return nil, errors.New("PEM无效。它必须不同于nil")
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("解码失败。块必须不同于nil [% x]", raw)
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
			return nil, fmt.Errorf("PEM解密失败: [%s]", err)
		}
		// der二进制转asn1格式字节公钥
		key, err := derToPublicKey(decrypted)
		if err != nil {
			return nil, err
		}
		return key, err
	}
	// der二进制转asn1格式字节公钥
	cert, err := derToPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, err
}

// derToPublicKey der二进制转asn1格式字节公钥
func derToPublicKey(raw []byte) (pub interface{}, err error) {
	if len(raw) == 0 {
		return nil, errors.New("DER无效。它必须不同于nil")
	}

	key, err := x509.ParsePKIXPublicKey(raw)

	return key, err
}
