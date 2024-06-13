/*
Copyright IBM Corp. 2017 All Rights Reserved.

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

package msp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/hyperledger/fabric/bccsp/utils"
	"github.com/pkg/errors"
)

type validity struct {
	NotBefore, NotAfter time.Time
}

type publicKeyInfo struct {
	Raw       asn1.RawContent
	Algorithm pkix.AlgorithmIdentifier
	PublicKey asn1.BitString
}

type certificate struct {
	Raw                asn1.RawContent
	TBSCertificate     tbsCertificate
	SignatureAlgorithm pkix.AlgorithmIdentifier
	SignatureValue     asn1.BitString
}

type tbsCertificate struct {
	Raw                asn1.RawContent
	Version            int `asn1:"optional,explicit,default:0,tag:0"`
	SerialNumber       *big.Int
	SignatureAlgorithm pkix.AlgorithmIdentifier
	Issuer             asn1.RawValue
	Validity           validity
	Subject            asn1.RawValue
	PublicKey          publicKeyInfo
	UniqueId           asn1.BitString   `asn1:"optional,tag:1"`
	SubjectUniqueId    asn1.BitString   `asn1:"optional,tag:2"`
	Extensions         []pkix.Extension `asn1:"optional,explicit,tag:3"`
}

func isECDSASignedCert(cert *x509.Certificate) bool {
	return cert.SignatureAlgorithm == x509.ECDSAWithSHA1 ||
		cert.SignatureAlgorithm == x509.ECDSAWithSHA256 ||
		cert.SignatureAlgorithm == x509.ECDSAWithSHA384 ||
		cert.SignatureAlgorithm == x509.ECDSAWithSHA512
}

// sanitizeECDSASignedCert 函数检查签名证书的签名是否为低 S 值。
// 这将根据 parentCert 的公钥进行检查。
// 如果签名不是低 S 值，则生成一个新的证书，该证书与原证书相同，但签名为低 S 值。
//
// 参数：
//   - cert *x509.Certificate：要检查的证书。
//   - parentCert *x509.Certificate：父证书，用于验证签名。
//
// 返回值：
//   - *x509.Certificate：如果签名为低 S 值，则返回原证书；否则返回新生成的证书。
//   - error：如果发生错误，则返回错误信息。
func sanitizeECDSASignedCert(cert *x509.Certificate, parentCert *x509.Certificate) (*x509.Certificate, error) {
	if cert == nil {
		return nil, errors.New("证书不能为nil")
	}
	if parentCert == nil {
		return nil, errors.New("父证书不能为nil")
	}

	expectedSig, err := utils.SignatureToLowS(parentCert.PublicKey.(*ecdsa.PublicKey), cert.Signature)
	if err != nil {
		return nil, err
	}

	// 如果签名与证书的签名相等，则无需进行任何操作
	if bytes.Equal(cert.Signature, expectedSig) {
		return cert, nil
	}
	// 否则，创建一个新的证书，其签名为低 S 值

	// 1. 将 cert.Raw 解组为 certificate 的实例，
	//    该实例是表示 x509 证书编码的较低级别接口
	var newCert certificate
	newCert, err = certFromX509Cert(cert)
	if err != nil {
		return nil, err
	}

	// 2. 更改签名
	newCert.SignatureValue = asn1.BitString{Bytes: expectedSig, BitLength: len(expectedSig) * 8}

	// 3. 再次编组 newCert，Raw 必须为 nil
	newCert.Raw = nil
	newRaw, err := asn1.Marshal(newCert)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling of the certificate failed")
	}

	// 4. 解析 newRaw，获取 x509 证书
	return x509.ParseCertificate(newRaw)
}

// certFromX509Cert 函数从给定的 x509.Certificate 中获取证书。
// 参数：
//   - cert *x509.Certificate：要获取的证书。
//
// 返回值：
//   - certificate：获取的证书。
//   - error：如果获取过程中出现错误，则返回相应的错误信息。
func certFromX509Cert(cert *x509.Certificate) (certificate, error) {
	var newCert certificate
	_, err := asn1.Unmarshal(cert.Raw, &newCert)
	if err != nil {
		return certificate{}, errors.Wrap(err, "证书解组失败")
	}
	return newCert, nil
}

// String 方法返回证书的 PEM 表示形式。
// 返回值：
//   - string：证书的 PEM 编码字符串。
func (c certificate) String() string {
	b, err := asn1.Marshal(c)
	if err != nil {
		return fmt.Sprintf("序列化证书失败：%v", err)
	}
	block := &pem.Block{
		Bytes: b,
		Type:  "CERTIFICATE",
	}
	b = pem.EncodeToMemory(block)
	return string(b)
}

// CertToPEM 函数将给定的 x509.Certificate 转换为 PEM 编码的字符串。
// 参数：
//   - certificate *x509.Certificate：要转换的证书。
//
// 返回值：
//   - string：PEM 编码的证书字符串。
func CertToPEM(certificate *x509.Certificate) string {
	cert, err := certFromX509Cert(certificate)
	if err != nil {
		mspIdentityLogger.Warning("将证书转换为 ASN.1 格式失败", err)
		return ""
	}
	return cert.String()
}
