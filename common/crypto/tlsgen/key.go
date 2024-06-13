/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tlsgen

import (
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/utils"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
)

// newPrivKey 创建一个新的密钥 和 密钥的der/asn1编码格式字节
// 不建议使用, 推荐使用 privateKey, err := common.NewPrivateKey()
func newPrivKey() (*bccsp.Key, []byte, error) {
	csp := factory.GetDefault()
	// 临时密钥, 不保存到默认位置, 通过 keystorePath 指定位置保存
	privateKey, err := csp.KeyGen(common.AsymmetricKeyTemp())
	if err != nil {
		return nil, nil, errors.WithMessage(err, "生成密钥失败")
	}
	// 密钥转der/asn1格式
	privateKeyToDER, err := utils.PrivateKeyToDER(privateKey.Key())
	if err != nil {
		return nil, nil, err
	}
	return &privateKey, privateKeyToDER, nil
}

// newCertTemplate 创建一个x509的证书模板
func newCertTemplate() (x509.Certificate, error) {
	// 生成一个随机的128位整数作为证书的序列号
	sn, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return x509.Certificate{}, err
	}
	return x509.Certificate{
		// 证书的主题，包含了一些标识信息，这里只设置了序列号
		Subject: pkix.Name{SerialNumber: sn.String()},
		// 证书的生效日期，设置为当前时间的前24小时
		NotBefore: time.Now().Add(time.Hour * (-24)),
		// 证书的过期日期，设置为当前时间的后24小时
		NotAfter: time.Now().Add(time.Hour * 24),
		// 证书的密钥用途, 用于加密和数字签名
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		// 证书的序列号，与之前生成的随机数相同
		SerialNumber: sn,
	}, nil
}

// newCertKeyPair 创建一个新的tls证书
func newCertKeyPair(isCA bool, isServer bool, certSigner crypto.Signer, parent *x509.Certificate, hosts ...string) (*CertKeyPair, error) {
	// 创建一个新的密钥
	privateKey, err := common.NewPrivateKey()
	if err != nil {
		return nil, err
	}
	// 创建一个x509的证书模板
	template, err := newCertTemplate()
	if err != nil {
		return nil, err
	}

	// 设置过期时间 10年
	tenYearsFromNow := time.Now().Add(time.Hour * 24 * 365 * 10)
	// 如果证书是一个证书颁发机构（CA）的证书
	if isCA {
		// 证书的过期日期，设置为十年后
		template.NotAfter = tenYearsFromNow
		// 该证书是一个CA证书
		template.IsCA = true
		// 设置密钥用途, 用于签发其他证书和撤销列表
		template.KeyUsage |= x509.KeyUsageCertSign | x509.KeyUsageCRLSign
		// 设置扩展密钥用途, 用于客户端和服务器身份验证
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		}
		// 基本约束是否有效
		template.BasicConstraintsValid = true
	} else {
		// 生成的证书不是一个CA证书, 设置扩展密钥用途, 只能用于客户端身份验证
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	// 证书是用于服务器身份验证(tls证书)
	if isServer {
		// 证书的过期日期，设置为十年后
		template.NotAfter = tenYearsFromNow
		// 将ExtKeyUsageServerAuth添加到扩展密钥用途中，表示该证书可以用于服务器身份验证
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		// 将每个主机名解析为IP地址或DNS名称，并将其添加到相应的字段中（IPAddresses或DNSNames）
		for _, host := range hosts {
			if ip := net.ParseIP(host); ip != nil {
				template.IPAddresses = append(template.IPAddresses, ip)
			} else {
				template.DNSNames = append(template.DNSNames, host)
			}
		}
	}

	template.SubjectKeyId = (*privateKey).SKI()
	// 如果没有父证书，则是自签名证书
	if parent == nil || certSigner == nil {
		parent = &template
		certSigner = ((*privateKey).Key()).(crypto.Signer)
	}

	// 创建x509
	pub, err := (*privateKey).PublicKey()
	if err != nil {
		return nil, err
	}

	// 生成x509证书
	rawBytes, err := common.CreateCertificate(&template, parent, pub, certSigner)
	if err != nil {
		return nil, err
	}

	// 编码pem, 并返回它的字节
	cert := encodePEM("CERTIFICATE", rawBytes)
	// 重新解码
	block, _ := pem.Decode(cert)
	if block == nil {
		return nil, errors.Errorf("%s: 错误的PEM编码", cert)
	}

	// 解析证书
	tlscert, err := common.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	// 私钥解析为der/asn1格式的字节数组
	der, err := utils.PrivateKeyToDER((*privateKey).Key())
	if err != nil {
		return nil, err
	}
	// der私钥编码pem, 并返回它的字节
	privKey := encodePEM("EC PRIVATE KEY", der)
	return &CertKeyPair{
		Key:     privKey,
		Cert:    cert,
		Signer:  certSigner,
		TLSCert: tlscert,
	}, nil
}

// encodePEM 编码pem, 并返回它的字节
func encodePEM(keyType string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: keyType, Bytes: data})
}
