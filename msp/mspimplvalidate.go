/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"bytes"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"math/big"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

func (msp *bccspmsp) validateIdentity(id *identity) error {
	id.validationMutex.Lock()
	defer id.validationMutex.Unlock()

	// 如果已验证，则返回缓存的验证值
	if id.validated {
		return id.validationErr
	}

	id.validated = true

	// 返回此MSP中传入的bccsp身份的证书链
	validationChain, err := msp.getCertificationChainForBCCSPIdentity(id)
	if err != nil {
		id.validationErr = errors.WithMessage(err, "无法获取认证链")
		mspLogger.Warnf("无法验证身份标识: %s (证书 subject=%s issuer=%s serialnumber=%d)", id.validationErr, id.cert.Subject, id.cert.Issuer, id.cert.SerialNumber)
		return id.validationErr
	}

	// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
	err = msp.validateIdentityAgainstChain(id, validationChain)
	if err != nil {
		id.validationErr = errors.WithMessage(err, "无法根据证书链验证身份证书是否被撤销")
		mspLogger.Warnf("无法验证身份标识: %s (证书 subject=%s issuer=%s serialnumber=%d)", id.validationErr, id.cert.Subject, id.cert.Issuer, id.cert.SerialNumber)
		return id.validationErr
	}

	// 验证 identity 身份的ou角色是否是配置的ou角色
	err = msp.internalValidateIdentityOusFunc(id)
	if err != nil {
		id.validationErr = errors.WithMessage(err, "无法验证身份的OUs")
		mspLogger.Warnf("无法验证身份标识: %s (证书 subject=%s issuer=%s serialnumber=%d)", id.validationErr, id.cert.Subject, id.cert.Issuer, id.cert.SerialNumber)
		return id.validationErr
	}

	return nil
}

// validateCAIdentity 方法用于验证 CA（证书颁发机构）的身份。
//
// 参数：
//   - id：要验证的身份。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息；如果验证成功，则返回 nil。
func (msp *bccspmsp) validateCAIdentity(id *identity) error {
	if !id.cert.IsCA {
		return errors.New("只能验证属于CA机构的身份")
	}

	// 方法根据给定的验证选项验证证书，并返回唯一的验证链
	validationChain, err := msp.getUniqueValidationChain(id.cert, msp.getValidityOptsForCert(id.cert))
	if err != nil {
		return errors.WithMessage(err, "无法获取认证链")
	}
	if len(validationChain) == 1 {
		// validationChain[0] 是根 CA 证书
		return nil
	}

	// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
	return msp.validateIdentityAgainstChain(id, validationChain)
}

// validateTLSCAIdentity 函数验证 TLS CA 身份的有效性。
// 输入参数：
//   - cert: x509.Certificate 对象，要验证的证书
//   - opts: x509.VerifyOptions 对象，验证选项
//
// 返回值：
//   - error: 如果验证过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) validateTLSCAIdentity(cert *x509.Certificate) error {
	if !cert.IsCA {
		return errors.New("只有 CA 身份可以进行验证")
	}
	// 获取证书链
	validationChain, err := msp.getUniqueValidationChain(cert, msp.getValidityTlsOptsForCert(cert))
	if err != nil {
		return errors.WithMessage(err, "无法获取证书链")
	}
	if len(validationChain) == 1 {
		// validationChain[0] 是根 CA 证书
		return nil
	}
	// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
	return msp.validateCertAgainstChain(cert, validationChain)
}

// validateIdentityAgainstChain 方法用于验证身份与验证链的一致性。
// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
// 参数：
//   - id：要验证的身份。
//   - validationChain：验证链，包含一系列证书。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息；如果验证成功，则返回 nil。
func (msp *bccspmsp) validateIdentityAgainstChain(id *identity, validationChain []*x509.Certificate) error {
	// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
	return msp.validateCertAgainstChain(id.cert, validationChain)
}

// todo luode 验证是否被撤销。在这里我们知道身份是有效的; 现在我们必须检查它是否已被撤销
func (msp *bccspmsp) validateCertAgainstChain(cert *x509.Certificate, validationChain []*x509.Certificate) error {
	// 返回所提供证书的主题密钥标识符
	// SKI 是一个由证书颁发机构（CA）生成的标识符，用于唯一标识证书中的公钥。它通常是通过对公钥进行哈希计算得到的。SKI 的目的是提供一种快速查找和匹配证书公钥的方法，而不必直接比较公钥本身。
	SKI, err := getSubjectKeyIdentifierFromCert(validationChain[1])
	if err != nil {
		return errors.WithMessage(err, "无法获取签名者证书的主题公钥标识符")
	}

	// 检查我们拥有的crl撤销证书中的ski公钥标识符, 有没有相同的, 如果有则说明此证书已经被撤销
	for _, crl := range msp.CRL {
		aki, err := getAuthorityKeyIdentifierFromCrl(crl)
		if err != nil {
			return errors.WithMessage(err, "无法获取crl的证书颁发机构（CA）的公钥标识符")
		}

		// 检查签署我们的证书的滑雪是否与任何crl的AKI匹配
		if bytes.Equal(aki, SKI) {
			// 我们有一个CRL，检查序列号是否被撤销
			for _, rc := range crl.TBSCertList.RevokedCertificates {
				if rc.SerialNumber.Cmp(cert.SerialNumber) == 0 {
					// 我们找到了一个CRL，其AKI与签署正在验证的证书的CA (根或中间) 的SKI匹配。
					// 作为预防措施，我们验证该CA也是此CRL的签名者。
					err = validationChain[1].CheckCRLSignature(crl)
					if err != nil {
						// 签署正在验证的证书的CA证书没有签署候选CRL-skip
						mspLogger.Warningf("标识的CRL上的签名无效, 错误 %+v", err)
						continue
					}

					// CRL还包括一个撤销时间，以便CA可以说 “从这个时间开始撤销此证书”；
					// 但是在这里，我们只是假设撤销从MSP配置提交和使用时立即应用，所以我们不会使用该字段
					return errors.New("证书已被吊销")
				}
			}
		}
	}

	return nil
}

// validateIdentityOUsV1 验证给定身份的组织单位（OU）是否与此 MSP 所识别的 OU 兼容，即交集不为空。
//
// 输入参数：
//   - id：要验证的身份。
//
// 返回值：
//   - error：如果身份的 OU 与 MSP 不兼容，则返回相应的错误；否则返回 nil。
func (msp *bccspmsp) validateIdentityOUsV1(id *identity) error {
	// 检查身份的 OU 是否与此 MSP 所识别的 OU 兼容，即交集不为空
	if len(msp.ouIdentifiers) > 0 {
		found := false
		// 遍历返回此实例的组织单位（OU）
		for _, OU := range id.GetOrganizationalUnits() {
			certificationIDs, exists := msp.ouIdentifiers[OU.OrganizationalUnitIdentifier]
			if exists {
				// 如果识别了，则验证给定身份的 OU 是否与其兼容
				for _, certificationID := range certificationIDs {
					if bytes.Equal(certificationID, OU.CertifiersIdentifier) {
						found = true
						break
					}
				}
			}
		}

		if !found {
			if len(id.GetOrganizationalUnits()) == 0 {
				return errors.New("身份证书不包含组织单位 (OU)")
			}
			return errors.Errorf("没有身份的组织单位 %s 在 MSP %s", OUIDs(id.GetOrganizationalUnits()), msp.name)
		}
	}

	return nil
}

func (msp *bccspmsp) validateIdentityOUsV11(id *identity) error {
	// Run the same checks as per V1
	err := msp.validateIdentityOUsV1(id)
	if err != nil {
		return err
	}

	// Perform V1_1 additional checks:
	//
	// -- Check for OU enforcement
	if !msp.ouEnforcement {
		// No enforcement required
		return nil
	}

	// Make sure that the identity has only one of the special OUs
	// used to tell apart clients or peers.
	counter := 0
	for _, OU := range id.GetOrganizationalUnits() {
		// Is OU.OrganizationalUnitIdentifier one of the special OUs?
		var nodeOU *OUIdentifier
		switch OU.OrganizationalUnitIdentifier {
		case msp.clientOU.OrganizationalUnitIdentifier:
			nodeOU = msp.clientOU
		case msp.peerOU.OrganizationalUnitIdentifier:
			nodeOU = msp.peerOU
		default:
			continue
		}

		// Yes. Then, enforce the certifiers identifier is this is specified.
		// It is not specified, it means that any certification path is fine.
		if len(nodeOU.CertifiersIdentifier) != 0 && !bytes.Equal(nodeOU.CertifiersIdentifier, OU.CertifiersIdentifier) {
			return errors.Errorf("certifiersIdentifier does not match: %v, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
		}
		counter++
		if counter > 1 {
			break
		}
	}

	// the identity should have exactly one OU role, return an error if the counter is not 1.
	if counter == 0 {
		return errors.Errorf("the identity does not have an OU that resolves to client or peer. OUs: %s, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
	}
	if counter > 1 {
		return errors.Errorf("the identity must be a client or a peer identity to be valid, not a combination of them. OUs: %s, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
	}

	return nil
}

// validateIdentityOUsV142 验证 identity 身份的ou角色
func (msp *bccspmsp) validateIdentityOUsV142(id *identity) error {
	// 根据V1运行相同的检查, 验证给定身份的组织单位（OU）是否与此 MSP 所识别的 OU 兼容，即交集不为空。
	err := msp.validateIdentityOUsV1(id)
	if err != nil {
		return err
	}

	// -- 检查或强制
	if !msp.ouEnforcement {
		// 无需强制执行
		return nil
	}

	// 确保该身份只有一个用于区分客户端、对等体和管理员的特殊OUs。
	counter := 0
	validOUs := make(map[string]*OUIdentifier)
	if msp.clientOU != nil {
		validOUs[msp.clientOU.OrganizationalUnitIdentifier] = msp.clientOU
	}
	if msp.peerOU != nil {
		validOUs[msp.peerOU.OrganizationalUnitIdentifier] = msp.peerOU
	}
	if msp.adminOU != nil {
		validOUs[msp.adminOU.OrganizationalUnitIdentifier] = msp.adminOU
	}
	if msp.ordererOU != nil {
		validOUs[msp.ordererOU.OrganizationalUnitIdentifier] = msp.ordererOU
	}

	// 遍历此实例的组织单位（OU）
	for _, OU := range id.GetOrganizationalUnits() {
		// OU.OrganizationalUnitIdentifier是配置的OU之一吗？
		nodeOU := validOUs[OU.OrganizationalUnitIdentifier]
		if nodeOU == nil {
			continue
		}

		// 是。然后，强制执行此中指定的认证者标识符。
		// 如果未指定，则表示任何证书路径都可以。
		if len(nodeOU.CertifiersIdentifier) != 0 && !bytes.Equal(nodeOU.CertifiersIdentifier, OU.CertifiersIdentifier) {
			mspLogger.Errorf("节点匹配到的 ou 组织单位 %s: 证书hash: [%x]", nodeOU.OrganizationalUnitIdentifier, string(nodeOU.CertifiersIdentifier))
			mspLogger.Errorf("节点接收到的 ou 组织单位 %s: 证书hash: [%x]", OU.OrganizationalUnitIdentifier, string(OU.CertifiersIdentifier))
			return errors.Errorf("config.yaml 中 NodeOUs 配置 Certificate 不匹配: %s, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
		}
		counter++
		if counter > 1 {
			break
		}
	}

	// 身份应该正好有一个OU角色，如果计数器不是1，则返回错误。
	if counter == 0 {
		return errors.Errorf("身份没有解析为client、peer、orderer或admin角色的OU. OUs: %s, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
	}
	if counter > 1 {
		return errors.Errorf("身份必须是client、peer、orderer或admin OU角色之一才能有效, 不能拥有多个角色OU. OUs: %s, MSP: [%s]", OUIDs(id.GetOrganizationalUnits()), msp.name)
	}

	return nil
}

// getValidityOptsForCert 方法为证书获取验证选项(根证书、直接证书、dns、证书扩展权限、过期)。
// 参数：
//   - cert *x509.Certificate：要获取验证选项的证书。
//
// 返回值：
//   - x509.VerifyOptions：证书的验证选项。
func (msp *bccspmsp) getValidityOptsForCert(cert *x509.Certificate) interface{} {
	// 首先复制选项以覆盖 CurrentTime 字段，
	// 以使证书在过期测试中通过，
	// 与实际本地当前时间无关。
	// 这是 FAB-3678 的临时解决方法
	// 提取证书公钥
	certPubK, _ := factory.GetDefault().KeyImport(cert, &bccsp.X509PublicKeyImportOpts{Temporary: true})

	_, typePub, _ := common.PackingKey(certPubK.Key())
	if typePub == common.SW {
		// 标准x509
		tempOpts := x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
		// 一组受信任的根证书
		tempOpts.Roots = msp.opts.Roots
		tempOpts.DNSName = msp.opts.DNSName
		// 中间证书是一个可选的证书池，它们不是信任锚，但可用于形成从叶证书到根证书的链。
		tempOpts.Intermediates = msp.opts.Intermediates
		// 定哪些扩展密钥使用值是可接受的
		tempOpts.KeyUsages = msp.opts.KeyUsages
		// 用于检查链中所有证书的有效性
		tempOpts.CurrentTime = cert.NotBefore.Add(time.Second)

		return tempOpts
	} else if typePub == common.GM {
		// x509国密
		tempOptsGM := x509GM.VerifyOptions{Roots: x509GM.NewCertPool(), Intermediates: x509GM.NewCertPool()}

		tempOptsGM.DNSName = msp.opts.DNSName
		// 一组受信任的根证书
		for _, root := range msp.rootCerts {
			certificate2Sm2 := gm.ParseX509Certificate2Sm2(root.(*identity).cert)
			tempOptsGM.Roots.AddCert(certificate2Sm2)
		}
		// 中间证书是一个可选的证书池，它们不是信任锚，但可用于形成从叶证书到根证书的链。
		for _, intermediateCert := range msp.intermediateCerts {
			certificate2Sm2 := gm.ParseX509Certificate2Sm2(intermediateCert.(*identity).cert)
			tempOptsGM.Roots.AddCert(certificate2Sm2)
		}
		// 定哪些扩展密钥使用值是可接受的
		for _, val := range msp.opts.KeyUsages {
			tempOptsGM.KeyUsages = append(tempOptsGM.KeyUsages, x509GM.ExtKeyUsage(val))
		}
		// 用于检查链中所有证书的有效性
		tempOptsGM.CurrentTime = cert.NotBefore.Add(time.Second)

		return tempOptsGM
	}

	return nil
}

// getValidityTlsOptsForCert 方法为tls证书获取验证选项(根证书、直接证书、dns、证书扩展权限、过期)。
// 参数：
//   - cert *x509.Certificate：要获取验证选项的证书。
//
// 返回值：
//   - x509.VerifyOptions：证书的验证选项。
func (msp *bccspmsp) getValidityTlsOptsForCert(cert *x509.Certificate) interface{} {
	// 首先复制选项以覆盖 CurrentTime 字段，
	// 以使证书在过期测试中通过，
	// 与实际本地当前时间无关。
	// 这是 FAB-3678 的临时解决方法
	// 提取证书公钥
	certPubK, _ := factory.GetDefault().KeyImport(cert, &bccsp.X509PublicKeyImportOpts{Temporary: true})

	_, typePub, _ := common.PackingKey(certPubK.Key())
	if typePub == common.SW {
		// 标准x509
		tempOpts := x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
		tempOpts.DNSName = msp.opts.DNSName
		// 一组受信任的根证书
		for _, root := range msp.tlsRootCerts {
			// 用于从 PEM 格式的字节中获取证书
			certTls, err := msp.getCertFromPem(root)
			if err != nil {
				return err
			}
			tempOpts.Roots.AddCert(certTls)
		}
		// 中间证书是一个可选的证书池，它们不是信任锚，但可用于形成从叶证书到根证书的链。
		for _, intermediateCert := range msp.tlsIntermediateCerts {
			// 用于从 PEM 格式的字节中获取证书
			intermediateCertTls, err := msp.getCertFromPem(intermediateCert)
			if err != nil {
				return err
			}
			tempOpts.Roots.AddCert(intermediateCertTls)
		}
		// 用于检查链中所有证书的有效性
		tempOpts.CurrentTime = cert.NotBefore.Add(time.Second)

		return tempOpts
	} else if typePub == common.GM {
		// x509国密
		tempOptsGM := x509GM.VerifyOptions{Roots: x509GM.NewCertPool(), Intermediates: x509GM.NewCertPool()}
		tempOptsGM.DNSName = msp.opts.DNSName
		// 一组受信任的根证书
		for _, root := range msp.tlsRootCerts {
			// 用于从 PEM 格式的字节中获取证书
			certTls, err := msp.getCertFromPem(root)
			if err != nil {
				return err
			}
			certificate2Sm2 := gm.ParseX509Certificate2Sm2(certTls)
			tempOptsGM.Roots.AddCert(certificate2Sm2)
		}
		// 中间证书是一个可选的证书池，它们不是信任锚，但可用于形成从叶证书到根证书的链。
		for _, intermediateCert := range msp.tlsIntermediateCerts {
			// 用于从 PEM 格式的字节中获取证书
			intermediateCertTls, err := msp.getCertFromPem(intermediateCert)
			if err != nil {
				return err
			}
			certificate2Sm2 := gm.ParseX509Certificate2Sm2(intermediateCertTls)
			tempOptsGM.Roots.AddCert(certificate2Sm2)
		}
		// 用于检查链中所有证书的有效性
		tempOptsGM.CurrentTime = cert.NotBefore.Add(time.Second)

		return tempOptsGM
	}

	return nil
}

/*
   This is the definition of the ASN.1 marshalling of AuthorityKeyIdentifier
   from https://www.ietf.org/rfc/rfc5280.txt

   AuthorityKeyIdentifier ::= SEQUENCE {
      keyIdentifier             [0] KeyIdentifier           OPTIONAL,
      authorityCertIssuer       [1] GeneralNames            OPTIONAL,
      authorityCertSerialNumber [2] CertificateSerialNumber OPTIONAL  }

   KeyIdentifier ::= OCTET STRING

   CertificateSerialNumber  ::=  INTEGER

*/

type authorityKeyIdentifier struct {
	KeyIdentifier             []byte  `asn1:"optional,tag:0"`
	AuthorityCertIssuer       []byte  `asn1:"optional,tag:1"`
	AuthorityCertSerialNumber big.Int `asn1:"optional,tag:2"`
}

// getAuthorityKeyIdentifierFromCrl 用于从给定的证书撤销列表（CRL）中获取 Authority Key Identifier（AKI）。AKI 是一个用于唯一标识证书颁发机构（CA）的标识符。
// AKI 用于识别颁发机构（CA），以便在验证证书时确定其信任链。它通常是通过对颁发机构的公钥进行哈希计算得到的。
// 输入参数：
//   - crl: 要获取颁发机构密钥标识符的CRL
//
// 返回值：
//   - []byte: 颁发机构密钥标识符的字节数组
//   - error: 如果解析颁发机构密钥标识符过程中出现错误，则返回相应的错误信息
func getAuthorityKeyIdentifierFromCrl(crl *pkix.CertificateList) ([]byte, error) {
	// 定义一个结构体 aki，用于存储获取到的 Authority Key Identifier。
	aki := authorityKeyIdentifier{}
	// 遍历 CRL 的所有扩展字段（Extensions）
	for _, ext := range crl.TBSCertList.Extensions {
		// 每个扩展字段，检查其标识符（Id）是否与 Authority Key Identifier 的标识符（2.5.29.35）相匹配。
		// 这个标识符是 ASN.1 对象标识符（Object Identifier），用于唯一标识 Authority Key Identifier 扩展字段
		if reflect.DeepEqual(ext.Id, asn1.ObjectIdentifier{2, 5, 29, 35}) {
			// 如果找到了匹配的扩展字段，使用 asn1.Unmarshal 函数将扩展字段的值（Value）解码为 aki 结构体，并将解码后的值存储在 aki 变量中
			_, err := asn1.Unmarshal(ext.Value, &aki)
			if err != nil {
				return nil, errors.Wrap(err, "解析证书颁发机构（CA）的公钥标识符失败")
			}

			return aki.KeyIdentifier, nil
		}
	}

	return nil, errors.New("authorityKeyIdentifier 证书颁发机构（CA）的公钥标识符中找不到")
}

// getSubjectKeyIdentifierFromCert 用于从给定的 x509 证书中获取 Subject Key Identifier（SKI）。SKI 是一个用于唯一标识证书公钥的标识符
// SKI 是一个由证书颁发机构（CA）生成的标识符，用于唯一标识证书中的公钥。它通常是通过对公钥进行哈希计算得到的。SKI 的目的是提供一种快速查找和匹配证书公钥的方法，而不必直接比较公钥本身。
//
// 参数：
//   - cert：要获取主题密钥标识符的证书。
//
// 返回值：
//   - []byte：主题密钥标识符的字节切片。
//   - error：如果发生错误，则返回错误信息；如果成功，则返回 nil。
func getSubjectKeyIdentifierFromCert(cert *x509.Certificate) ([]byte, error) {
	// 定义一个变量 SKI，用于存储获取到的 Subject Key Identifier
	var SKI []byte
	// 遍历证书的所有扩展字段
	for _, ext := range cert.Extensions {
		// 每个扩展字段，检查其标识符（Id）是否与 Subject Key Identifier 的标识符（2.5.29.14）相匹配。
		// 这个标识符是 ASN.1 对象标识符（Object Identifier），用于唯一标识 Subject Key Identifier 扩展字段。
		if reflect.DeepEqual(ext.Id, asn1.ObjectIdentifier{2, 5, 29, 14}) {
			// 如果找到了匹配的扩展字段，使用 asn1.Unmarshal 函数将扩展字段的值（Value）解码为字节切片，并将解码后的值存储在 SKI 变量中
			_, err := asn1.Unmarshal(ext.Value, &SKI)
			if err != nil {
				return nil, errors.Wrap(err, "Subject Key Identifier 证书公钥标识符编码错误")
			}

			return SKI, nil
		}
	}

	return nil, errors.New("subject Key Identifier 证书公钥标识符找不到")
}
