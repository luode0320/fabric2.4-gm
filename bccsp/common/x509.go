package common

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"github.com/pkg/errors"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// X509Template 函数用于生成一个基本的证书模板，包含了要创建的证书的各种属性和扩展。
// 返回值：
//   - x509.Certificate: 生成的证书模板
//
// @Author: 罗德
// @Date: 2023/10/24
func X509Template() x509.Certificate {
	// 生成一个随机的128位整数作为证书的序列号
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)

	// 有效期为10年
	expiry := 3650 * 24 * time.Hour
	// 舍入分钟和回溯5分钟
	notBefore := time.Now().Round(time.Minute).Add(-5 * time.Minute).UTC()

	// 要使用的基本模板
	x509 := x509.Certificate{
		// 证书的序列号，与之前生成的随机数相同
		SerialNumber: serialNumber,
		// 证书的生效日期
		NotBefore: notBefore,
		// 证书的过期日期，即生效日期加上有效期
		NotAfter: notBefore.Add(expiry).UTC(),
		// 基本约束是否有效，这里设置为true表示该证书是一个CA证书
		BasicConstraintsValid: true,
	}
	return x509
}

// CreateCertificate 函数用于创建签名证书，根据给定的模板、父证书、公钥和私钥。
// 返回的 []byte 字节数组并不是标准的x509证书(但是可以写入pem文件), 需要调用 ParseCertificate 生成标准的证书实例
// 输入参数：
//   - template: 证书模板，包含了要创建的证书的各种属性和扩展
//   - parent: 父证书，用于签发新证书的证书
//   - pub: 公钥，用于新证书的加密和签名
//   - priv: 私钥，用于新证书的签名
//
// 返回值：
//   - []byte: 创建的证书的der/asn1编码的字节数组
//   - error: 如果创建证书过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/24
func CreateCertificate(template, parent *x509.Certificate, pub bccsp.Key, priv crypto.Signer) ([]byte, error) {
	switch pub := pub.Key().(type) {
	case *rsa.PublicKey:
		// 使用RSA公钥创建证书
		return x509.CreateCertificate(rand.Reader, template, parent, pub, priv)
	case *ecdsa.PublicKey:
		// 使用ECDSA公钥创建证书
		return x509.CreateCertificate(rand.Reader, template, parent, pub, priv)
	case *sm2.PublicKey:
		// 不对待签名数据加密
		template := gm.ParseX509Certificate2Sm2(template)
		template.SignatureAlgorithm = x509GM.SM2WithSM3

		parent := gm.ParseX509Certificate2Sm2(parent)
		template.SignatureAlgorithm = x509GM.SM2WithSM3

		return x509GM.CreateCertificate(template, parent, pub, priv)
	default:
		return nil, errors.New("x509: 仅支持RSA、ECDSA和SM2密钥")
	}
}

// GetCertificate 函数用于从指定路径中获取证书文件，并返回解析后的x509.Certificate实例。
// 输入参数：
//   - certPath: 证书文件所在的路径
//
// 返回值：
//   - *x509.Certificate: 解析后的证书实例
//   - error: 如果解析证书过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/24
func GetCertificate(certPath string) (*x509.Certificate, error) {
	var cert *x509.Certificate
	var err error

	// 定义一个walkFunc函数，用于遍历指定路径下的文件
	walkFunc := func(path string, info os.FileInfo, err error) error {
		// 判断文件是否以.pem结尾，表示为证书文件
		if strings.HasSuffix(path, ".pem") {
			// 读取证书文件内容
			rawCert, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			// 解码为DER编码
			block, _ := pem.Decode(rawCert)
			if block == nil || block.Type != "CERTIFICATE" {
				return errors.Errorf("%s: 错误的PEM编码", path)
			}
			// 解析为x509.Certificate实例
			cert, err = ParseCertificate(block.Bytes)
			if err != nil {
				return errors.Errorf("%s: 错误的编码", path)
			}
		}
		return nil
	}

	// 遍历指定路径下的文件，并调用walkFunc函数处理每个文件
	err = filepath.Walk(certPath, walkFunc)
	if err != nil {
		return nil, err
	}

	return cert, err
}

// ParseCertificate 函数用于解析给定的证书字节数组，并返回解析后的x509.Certificate实例。
// 输入参数：
//   - raw: 证书的字节数组(字节属于der/asn1编码)
//
// 返回值：
//   - *x509.Certificate: 解析后的证书实例
//   - error: 如果解析证书过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/24
func ParseCertificate(raw []byte) (*x509.Certificate, error) {
	// 转换为 x509 证书
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		// 转换为 x509GM 证书
		parseCertificate, err := x509GM.ParseCertificate(raw)
		if err != nil {
			return nil, errors.New("解析失败: 仅支持 x509、x509GM证书格式")
		}
		// x509GM 证书转换 x509 证书
		certificate = gm.ParseSm2Certificate2X509(parseCertificate)
	}

	return certificate, nil
}

// MarshalPKIXPublicKey 返回给定ASN1 DER证书的公钥
//
// @Author: 罗德
// @Date: 2023/11/14
func MarshalPKIXPublicKey(pub interface{}) ([]byte, error) {
	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		key, err := x509GM.MarshalPKIXPublicKey(pub)
		return key, err
	case factory.SoftwareBasedFactoryName:
		key, err := x509.MarshalPKIXPublicKey(pub)
		return key, err
	}
	return nil, errors.New("")
}

// AddGMPEMCerts2X509CertPool 给定一个国密算法生成的pem证书字节数组， 解析并将其加入到x509.CertPool 实例中
//
// @Author: zhaoruobo
// @Date: 2023/11/8
func AddGMPEMCerts2X509CertPool(pemCerts []byte, cp *x509.CertPool) (*x509.CertPool, bool) {
	ok := false

	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		certBytes := block.Bytes
		cert, err := ParseCertificate(certBytes)
		if err != nil {
			continue
		}

		cp.AddCert(cert)
		ok = true
	}

	return cp, ok
}
