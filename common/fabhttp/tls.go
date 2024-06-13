/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fabhttp

import (
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	"github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/gm"
	"github.com/pkg/errors"
	"io/ioutil"
)

type TLS struct {
	Enabled            bool
	CertFile           string
	KeyFile            string
	ClientCertRequired bool
	ClientCACertFiles  []string
}

// Config 方法用于获取TLS配置。
//
// 返回值：
//   - *tls.Config: 表示TLS配置的*tls.Config实例
//   - error: 如果获取配置过程中发生错误，则返回非nil的错误对象；否则返回nil。
func (t TLS) Config() (*gmtls.Config, error) {
	var tlsConfig *gmtls.Config

	if t.Enabled {
		cert, err := gmtls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		for _, caPath := range t.ClientCACertFiles {
			// 读取客户端CA证书文件
			caPem, err := ioutil.ReadFile(caPath)
			if err != nil {
				return nil, err
			}
			// 将客户端CA证书添加到证书池中
			caCertPool.AppendCertsFromPEM(caPem)
		}
		tlsConfig = &gmtls.Config{
			GMSupport:                new(gmtls.GMSupport),
			Certificates:             []gmtls.Certificate{cert, cert}, // 设置服务器证书和私钥
			MinVersion:               gmtls.VersionGMSSL,
			MaxVersion:               gmtls.VersionTLS12,
			PreferServerCipherSuites: true,
			CipherSuites:             []uint16{gmtls.GMTLS_SM2_WITH_SM4_SM3, gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3}, // 设置TLS密码套件
			ClientCAs:                caCertPool,                                                                 // 设置客户端CA证书池
			GetCertificate:           t.GetCertificate,
		}
		if t.ClientCertRequired {
			tlsConfig.ClientAuth = gmtls.RequireAndVerifyClientCert // 设置要求客户端提供证书并进行验证
		} else {
			tlsConfig.ClientAuth = gmtls.VerifyClientCertIfGiven // 设置仅在客户端提供证书时进行验证
		}
	}

	return tlsConfig, nil
}

// 重写tls.Config 中的GetCertificate 方法
// 此方法中，通过获取客户端的握手信息，对其中的国密TLS证书进行解析并返回
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func (t TLS) GetCertificate(_ *gmtls.ClientHelloInfo) (*gmtls.Certificate, error) {
	certPem, err := ioutil.ReadFile(t.CertFile)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("read tls cert error : %s\n", err.Error()))
	}
	keyPem, err := ioutil.ReadFile(t.KeyFile)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("read tls key error : %s\n", err.Error()))
	}

	tlsCert, err := common.X509KeyPair(certPem, keyPem)

	var certificate *x509.Certificate
	if tlsCert.Leaf != nil {
		certificate = gm.ParseX509Certificate2Sm2(tlsCert.Leaf)
	}

	gmCert := gmtls.Certificate{
		Certificate:                 tlsCert.Certificate,
		PrivateKey:                  tlsCert.PrivateKey,
		SignedCertificateTimestamps: tlsCert.SignedCertificateTimestamps,
		Leaf:                        certificate,
	}
	return &gmCert, nil
}
