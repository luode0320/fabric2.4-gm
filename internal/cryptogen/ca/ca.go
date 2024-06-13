/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package ca

import (
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/common"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type CA struct {
	Name               string
	Country            string
	Province           string
	Locality           string
	OrganizationalUnit string
	StreetAddress      string
	PostalCode         string
	Signer             crypto.Signer
	SignCert           *x509.Certificate
}

// NewCA 创建CA的实例并将签名密钥对保存在
// baseDir/name
func NewCA(
	baseDir,
	org,
	name,
	country,
	province,
	locality,
	orgUnit,
	streetAddress,
	postalCode string,
) (*CA, error) {
	var ca *CA

	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}

	priv, err := common.GeneratePrivateKey(baseDir, common.PRIV_SK)
	if err != nil {
		return nil, err
	}

	template := x509Template()
	// this is a CA
	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageDigitalSignature |
		x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign |
		x509.KeyUsageCRLSign
	template.ExtKeyUsage = []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	}

	// 设置主题的组织
	subject := subjectTemplateAdditional(country, province, locality, orgUnit, streetAddress, postalCode)
	subject.Organization = []string{org}
	subject.CommonName = name

	template.Subject = subject
	template.SubjectKeyId = (*priv).SKI()

	// 获取公钥
	publicKey, err := (*priv).PublicKey()
	if err != nil {
		return nil, err
	}

	x509Cert, err := createCertificate(
		baseDir,
		name,
		&template,
		&template,
		&publicKey,
		(*priv).Key(),
	)
	if err != nil {
		return nil, err
	}
	ca = &CA{
		Name:               name,
		Signer:             ((*priv).Key()).(crypto.Signer),
		SignCert:           x509Cert,
		Country:            country,
		Province:           province,
		Locality:           locality,
		OrganizationalUnit: orgUnit,
		StreetAddress:      streetAddress,
		PostalCode:         postalCode,
	}

	return ca, err
}

// SignCertificate 基于一个父ca证书, 新生成一个X509证书
func (ca *CA) SignCertificate(
	baseDir,
	name string,
	orgUnits,
	alternateNames []string,
	pub *bccsp.Key,
	ku x509.KeyUsage,
	eku []x509.ExtKeyUsage,
) (*x509.Certificate, error) {
	template := x509Template()
	template.KeyUsage = ku
	template.ExtKeyUsage = eku

	// 设置主题的组织
	subject := subjectTemplateAdditional(
		ca.Country,
		ca.Province,
		ca.Locality,
		ca.OrganizationalUnit,
		ca.StreetAddress,
		ca.PostalCode,
	)
	subject.CommonName = name
	// 组织单位
	subject.OrganizationalUnit = append(subject.OrganizationalUnit, orgUnits...)

	template.Subject = subject
	for _, san := range alternateNames {
		// 首先尝试解析为IP地址
		ip := net.ParseIP(san)
		if ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, san)
		}
	}

	// 基于 ca.Signer密钥 生成签名的X509证书
	cert, err := createCertificate(
		baseDir,
		name,
		&template,
		ca.SignCert,
		pub,
		ca.Signer,
	)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// createCertificate 生成X509证书
func createCertificate(
	baseDir,
	name string,
	template,
	parent *x509.Certificate,
	pub *bccsp.Key,
	priv interface{},
) (*x509.Certificate, error) {
	// 创建x509公共证书
	certBytes, err := common.CreateCertificate(template, parent, (*pub), priv.(crypto.Signer))
	if err != nil {
		return nil, err
	}

	// 创建证书空文件
	fileName := filepath.Join(baseDir, name+"-cert.pem")
	certFile, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	// pem对证书进行编码, 并写入 certFile 证书
	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	certFile.Close()
	if err != nil {
		return nil, err
	}
	// 解析x509证书为x509实例
	x509Cert, err := common.ParseCertificate(certBytes)
	if err != nil {
		return nil, err
	}
	return x509Cert, nil
}

// X509主题的默认模板
func subjectTemplate() pkix.Name {
	return pkix.Name{
		Country:  []string{"US"},
		Locality: []string{"San Francisco"},
		Province: []string{"California"},
	}
}

// X509主题的附加内容
func subjectTemplateAdditional(
	country,
	province,
	locality,
	orgUnit,
	streetAddress,
	postalCode string,
) pkix.Name {
	name := subjectTemplate()
	if len(country) >= 1 {
		name.Country = []string{country}
	}
	if len(province) >= 1 {
		name.Province = []string{province}
	}

	if len(locality) >= 1 {
		name.Locality = []string{locality}
	}
	if len(orgUnit) >= 1 {
		name.OrganizationalUnit = []string{orgUnit}
	}
	if len(streetAddress) >= 1 {
		name.StreetAddress = []string{streetAddress}
	}
	if len(postalCode) >= 1 {
		name.PostalCode = []string{postalCode}
	}
	return name
}

// X509证书的默认模板
func x509Template() x509.Certificate {
	// g生成一个随机的128位整数作为证书的序列号
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

// LoadCertificate 从证书路径中的文件加载证书
// 不建议使用它, 推荐使用 common.GetCertificate(caDir)
func LoadCertificate(certPath string) (*x509.Certificate, error) {
	var cert *x509.Certificate
	var err error

	walkFunc := func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".pem") {
			rawCert, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			block, _ := pem.Decode(rawCert)
			if block == nil || block.Type != "CERTIFICATE" {
				return errors.Errorf("%s: wrong PEM encoding", path)
			}
			cert, err = x509.ParseCertificate(block.Bytes)
			if err != nil {
				return errors.Errorf("%s: wrong DER encoding", path)
			}
		}
		return nil
	}

	err = filepath.Walk(certPath, walkFunc)
	if err != nil {
		return nil, err
	}

	return cert, err
}
