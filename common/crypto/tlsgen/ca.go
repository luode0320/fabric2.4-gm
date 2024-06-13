/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tlsgen

import (
	"crypto"
	"crypto/x509"
)

// CertKeyPair 表示TLS证书和相应的密钥，
// 两个PEM编码
type CertKeyPair struct {
	// Cert 是PEM编码的证书
	Cert []byte
	// Key 是证书对应的密钥，PEM编码
	Key []byte
	// 私钥
	crypto.Signer
	TLSCert *x509.Certificate
}

// CA 定义可以生成的证书颁发机构
// it签署的证书
type CA interface {
	// CertBytes 以PEM编码返回CA的证书
	CertBytes() []byte

	// NewIntermediateCA 新中间CA
	NewIntermediateCA() (CA, error)

	// NewClientCertKeyPair 返回证书和私钥对, 证书由CA签名，用于TLS客户端认证
	NewClientCertKeyPair() (*CertKeyPair, error)

	// NewServerCertKeyPair 返回证书和私钥对，并配置 host
	NewServerCertKeyPair(hosts ...string) (*CertKeyPair, error)

	// Signer 返回使用CA私钥签名的crypto.Signer。
	Signer() crypto.Signer
}

type ca struct {
	caCert *CertKeyPair
}

func NewCA() (CA, error) {
	c := &ca{}
	var err error
	// 新证书密钥对
	c.caCert, err = newCertKeyPair(true, false, nil, nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// NewIntermediateCA 生成tls中间证书
func (c *ca) NewIntermediateCA() (CA, error) {
	intermediateCA := &ca{}
	var err error
	// 新证书密钥对
	intermediateCA.caCert, err = newCertKeyPair(true, false, c.caCert.Signer, c.caCert.TLSCert)
	if err != nil {
		return nil, err
	}
	return intermediateCA, nil
}

// CertBytes 以PEM编码返回CA的证书
func (c *ca) CertBytes() []byte {
	return c.caCert.Cert
}

// NewClientCertKeyPair 返回证书和私钥对和nil，
// 或nil，失败时出错
// 证书由CA签名并用作客户端TLS证书
func (c *ca) NewClientCertKeyPair() (*CertKeyPair, error) {
	// 新证书密钥对
	return newCertKeyPair(false, false, c.caCert.Signer, c.caCert.TLSCert)
}

// NewServerCertKeyPair 创建一个服务器证书密钥对
// 方法接收者：
//   - c：ca 结构体的指针，表示证书颁发机构
//
// 输入参数：
//   - hosts：主机名列表，用于生成证书的 Subject Alternative Name (SAN) 字段
//
// 返回值：
//   - *CertKeyPair：CertKeyPair 结构体的指针，表示证书密钥对
//   - error：如果生成证书密钥对时发生错误，则返回相应的错误信息
func (c *ca) NewServerCertKeyPair(hosts ...string) (*CertKeyPair, error) {
	// 使用 caCert 的签名者和 TLS 证书生成新的证书密钥对
	keypair, err := newCertKeyPair(false, true, c.caCert.Signer, c.caCert.TLSCert, hosts...)
	if err != nil {
		return nil, err
	}
	return keypair, nil
}

// Signer 返回使用CA私钥签名的crypto.Signer。
func (c *ca) Signer() crypto.Signer {
	return c.caCert.Signer
}
