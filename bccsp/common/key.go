package common

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/pem"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"github.com/hyperledger/fabric/bccsp/sw"
	"github.com/hyperledger/fabric/bccsp/utils"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	SW        = "SW"
	GM        = "GM"
	PRIV_SK   = "priv_sk"
	SUFFIX_SK = "_sk"
)

// NewPrivateKey 函数用于生成新的私钥。
// 返回值：
//   - *bccsp.Key：生成的私钥。
//   - error：如果生成私钥失败，则返回错误。
//
// @Author: 罗德
// @Date: 2023/10/24
func NewPrivateKey() (*bccsp.Key, error) {
	// 获取默认的CSP实例
	csp := factory.GetDefault()
	// 生成临时密钥，不保存到默认位置，通过keystorePath指定位置保存
	privateKey, err := csp.KeyGen(AsymmetricKeyTemp())
	if err != nil {
		return nil, errors.WithMessage(err, "生成密钥失败")
	}
	return &privateKey, nil
}

// GeneratePrivateKey 函数用于生成私钥并保存到指定路径中的文件中，并返回生成的私钥对象。
// 输入参数：
// - keystorePath: 保存私钥文件的路径
// - fileName: 私钥文件的名称
// 返回值：
// - *bccsp.Key: 生成的私钥对象
// - error: 如果生成私钥或保存私钥文件过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/23
func GeneratePrivateKey(keystorePath string, fileName string) (*bccsp.Key, error) {
	// 创建一个新的密钥
	key, err := NewPrivateKey()
	if err != nil {
		return nil, err
	}

	// 将私钥转换为PEM格式
	privateKeyToPEM, err := utils.PrivateKeyToPEM((*key).Key(), nil)

	// 保存私钥到 keyFile
	keyFile := filepath.Join(keystorePath, fileName)
	err = ioutil.WriteFile(keyFile, privateKeyToPEM, 0o600)
	if err != nil {
		return nil, errors.WithMessagef(err, "无法将私钥保存到文件 %s", keyFile)
	}

	return key, err
}

// GetPrivateKey 函数用于从指定路径的密钥文件夹中获取私钥。
// 输入参数：
//   - keystorePath：密钥文件夹的路径。
//   - suffix：密钥文件的后缀。
//
// 返回值：
//   - bccsp.Key：获取到的私钥对象。
//   - error：如果获取私钥过程中出现错误，则返回相应的错误信息。
//
// @Author: 罗德
// @Date: 2023/10/24
func GetPrivateKey(keystorePath string, suffix string) (bccsp.Key, error) {
	var priv bccsp.Key

	// 遍历文件夹
	walkFunc := func(path string, info os.FileInfo, pathErr error) error {
		// 如果文件后缀不是suffix，则跳过
		if !strings.HasSuffix(path, suffix) {
			return nil
		}

		rawKey, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		// 将PEM编码转换为DER/ASN.1原始编码
		decode, _ := pem.Decode(rawKey)

		// 导入密钥，并临时存储到内存中
		csp := factory.GetDefault()
		priv, err = csp.KeyImport(decode.Bytes, AsymmetricImportPrivateKeyTemp())
		if err != nil {
			return errors.WithMessage(err, path)
		}

		return nil
	}

	// 执行文件夹遍历
	err := filepath.Walk(keystorePath, walkFunc)
	if err != nil {
		return nil, errors.New("遍历密钥文件夹异常")
	}

	return priv, nil
}

// PackingKey 包装key
//
// @Author: 罗德
// @Date: 2023/10/24
func PackingKey(packingKey interface{}) (bccsp.Key, string, error) {
	switch pkey := packingKey.(type) {
	case *rsa.PrivateKey:
		key, err := sw.PackingrsaKey(pkey)
		if err != nil {
			return nil, SW, errors.New("包装rsa.PrivateKey异常")
		}
		return key, SW, nil
	case *rsa.PublicKey:
		key, err := sw.PackingrsaKey(pkey)
		if err != nil {
			return nil, SW, errors.New("包装rsa.PublicKey异常")
		}
		return key, SW, nil
	case *ecdsa.PrivateKey:
		key, err := sw.PackingecdsaKey(pkey)
		if err != nil {
			return nil, SW, errors.New("包装ecdsa.PrivateKey异常")
		}
		return key, SW, nil
	case *ecdsa.PublicKey:
		key, err := sw.PackingecdsaKey(pkey)
		if err != nil {
			return nil, SW, errors.New("包装ecdsa.PublicKey异常")
		}
		return key, SW, nil
	case *sm2.PrivateKey:
		key, err := gm.Packingsm2Key(pkey)
		if err != nil {
			return nil, GM, errors.New("包装sm2.PrivateKey异常")
		}
		return key, GM, nil
	case *sm2.PublicKey:
		key, err := gm.Packingsm2Key(pkey)
		if err != nil {
			return nil, GM, errors.New("包装sm2.PublicKey异常")
		}
		return key, GM, nil
	default:
		return nil, SW, errors.New("x509: 仅支持RSA、ECDSA和SM2密钥")
	}
}

// GetPrivateKeyFromCert 通过ski找到cert的公钥中的私钥
//
// @Author: 罗德
// @Date: 2023/11/20
func GetPrivateKeyFromCert(cert []byte, cs bccsp.BCCSP) (bccsp.Key, error) {
	// 以正确的格式获取公钥
	certPubK, err := GetPublicKeyFromCert(cert, cs)
	if err != nil {
		return nil, errors.WithMessage(err, "无法获取导入证书的公钥")
	}

	if certPubK == nil || certPubK.SKI() == nil {
		return nil, errors.New("获取密钥 SKI 失败")
	}

	// 获得给定 SKI 的钥匙
	key, err := cs.GetKey(certPubK.SKI())
	if err != nil {
		return nil, errors.WithMessage(err, "找不到 SKI 的匹配密钥")
	}

	if key != nil && !key.Private() {
		return nil, errors.Errorf("找到的密钥, SKI: %s", certPubK.SKI())
	}

	return key, nil
}

// GetPublicKeyFromCert 将从证书返回公钥
//
// @Author: 罗德
// @Date: 2023/11/20
func GetPublicKeyFromCert(cert []byte, cs bccsp.BCCSP) (bccsp.Key, error) {
	dcert, _ := pem.Decode(cert)
	certificate, err := ParseCertificate(dcert.Bytes)
	if err != nil {
		return nil, errors.Errorf("无法从解码的字节解析cert: %s", err)
	}

	// 以正确的格式获取公钥
	key, err := cs.KeyImport(certificate, X509ImportOpt())
	if err != nil {
		return nil, errors.WithMessage(err, "无法获取导入证书的公钥")
	}

	return key, nil
}

// GetImportPrivateKey 从pem字节片获取私有 BCCSP 密钥
// 输入参数：
//   - keystorePath：密钥文件夹的路径。
//   - suffix：密钥文件的后缀。
//
// 返回值：
//   - bccsp.Key：获取到的私钥对象。
//   - error：如果获取私钥过程中出现错误，则返回相应的错误信息。
func GetImportPrivateKey(keyBuff []byte, temporary bool) (bccsp.Key, error) {
	// 将PEM编码转换为DER/ASN.1原始编码
	decode, _ := pem.Decode(keyBuff)

	// 导入私钥, 创建bccsp.Key
	if temporary {
		keyImport, err := factory.GetDefault().KeyImport(decode.Bytes, AsymmetricImportPrivateKeyTemp())
		if err != nil {
			return nil, err
		}
		return keyImport, nil
	}

	keyImport, err := factory.GetDefault().KeyImport(decode.Bytes, AsymmetricImportPrivateKey())
	if err != nil {
		return nil, err
	}
	return keyImport, nil
}
