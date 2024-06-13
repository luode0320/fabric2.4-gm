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
	"encoding/base64"
	"fmt"
	"github.com/hyperledger/fabric/bccsp/common"
	"time"

	"github.com/golang/protobuf/proto"
	m "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp/utils"
	errors "github.com/pkg/errors"
)

// getCertifiersIdentifier 函数获取证书的认证者标识符(hash值)。
// 输入参数：
//   - certRaw: 证书的字节切片
//
// 返回值：
//   - []byte: 证书的认证者标识符的字节切片
//   - error: 如果获取过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getCertifiersIdentifier(certRaw []byte) ([]byte, error) {
	// 1. 从 PEM 格式的字节中获取 x509 证书
	cert, err := msp.getCertFromPem(certRaw)
	if err != nil {
		return nil, fmt.Errorf("获取证书失败 [%v]: [%s]", certRaw, err)
	}

	// 2. 对证书进行清理以确保进行逐字节比较, 返回具有 Low-S 低位的签名
	cert, err = msp.sanitizeCert(cert)
	if err != nil {
		return nil, fmt.Errorf("sanitizeCert 失败 %s", err)
	}

	found := false
	root := false
	// 在根证书中搜索
	for _, v := range msp.rootCerts {
		if v.(*identity).cert.Equal(cert) {
			found = true
			root = true
			break
		}
	}
	if !found {
		// 在根中间证书中搜索
		for _, v := range msp.intermediateCerts {
			if v.(*identity).cert.Equal(cert) {
				found = true
				break
			}
		}
	}
	if !found {
		// 证书无效，拒绝配置
		return nil, fmt.Errorf(" \n 添加 OU 失败. \n 证书 [%s] \n 不在根证书或中间证书中: base64:[%s]", string(certRaw), base64.StdEncoding.EncodeToString(certRaw))
	}

	// 3. 获取证书的认证路径
	var certifiersIdentifier []byte
	var chain []*x509.Certificate
	if root {
		chain = []*x509.Certificate{cert}
	} else {
		chain, err = msp.getValidationChain(cert, true)
		if err != nil {
			return nil, fmt.Errorf("计算证书的验证链失败 [%s]. [%s]", string(certRaw), err)
		}
	}

	// 4. 计算认证路径的哈希值
	certifiersIdentifier, err = msp.getCertificationChainIdentifierFromChain(chain)
	if err != nil {
		return nil, fmt.Errorf("计算证书的认证者标识符失败 [%v]. [%s]", certRaw, err)
	}

	return certifiersIdentifier, nil
}

// setupCrypto 函数用于设置哈希算法加密配置。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) setupCrypto(conf *m.FabricMSPConfig) error {
	// 赋值哈希算法
	msp.cryptoConfig = conf.CryptoConfig
	if msp.cryptoConfig == nil {
		// 如果 CryptoConfig 为 nil，则使用默认值hash算法
		msp.cryptoConfig = &m.FabricCryptoConfig{
			SignatureHashFamily:            common.SignatureHashFamily(),
			IdentityIdentifierHashFunction: common.SignatureHashFamily(),
		}
		mspLogger.Debugf("CryptoConfig 为 nil. 使用默认值hash算法 %s.", common.SignatureHashFamily())
	}
	if msp.cryptoConfig.SignatureHashFamily == "" {
		// 如果 SignatureHashFamily 为 ""，则使用默认值
		msp.cryptoConfig.SignatureHashFamily = common.SignatureHashFamily()
		mspLogger.Debugf("CryptoConfig.SignatureHashFamily 为 nil. 使用默认值hash算法 %s.", common.SignatureHashFamily())
	}
	if msp.cryptoConfig.IdentityIdentifierHashFunction == "" {
		// 如果 IdentityIdentifierHashFunction 为 ""，则使用默认值
		msp.cryptoConfig.IdentityIdentifierHashFunction = common.SignatureHashFamily()
		mspLogger.Debugf("CryptoConfig.IdentityIdentifierHashFunction 为 nil. 使用默认值hash算法 %s.", common.SignatureHashFamily())
	}

	return nil
}

// setupCAs 函数用于设置 CA 证书。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) setupCAs(conf *m.FabricMSPConfig) error {
	// 创建并填充 CA 证书集合 - 我们期望至少有一个 CA 证书
	if len(conf.RootCerts) == 0 {
		return errors.New("至少需要一个 CA 根证书")
	}

	// 预先创建带有根证书和中间证书的验证选项。
	// 这是为了使证书的清理工作正常进行。
	// 需要注意的是，根 CA 和中间 CA 证书也会进行清理。
	// 在它们的清理完成后，将使用清理后的证书重新创建 opts。
	msp.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	// 遍历所有根证书byte字节数组, 解析为x509证书实例, 并加入msp.opts.Roots
	for _, v := range conf.RootCerts {
		// []byte 转 x509
		cert, err := msp.getCertFromPem(v)
		if err != nil {
			return err
		}
		msp.opts.Roots.AddCert(cert)
	}
	// 遍历所有中间证书byte字节数组, 解析为x509证书实例, 并加入msp.opts.Intermediates
	for _, v := range conf.IntermediateCerts {
		// []byte 转 x509
		cert, err := msp.getCertFromPem(v)
		if err != nil {
			return err
		}
		msp.opts.Intermediates.AddCert(cert)
	}

	// 加载根 CA 和中间 CA 身份
	// 需要注意的是，当创建身份时，其证书会进行清理
	msp.rootCerts = make([]Identity, len(conf.RootCerts))
	// 遍历所有根证书byte字节数组, 解析为一个身份实例, 并加入msp.rootCerts
	for i, trustedCert := range conf.RootCerts {
		// 用于从pem中获取身份信息、公钥
		id, _, err := msp.getIdentityFromConf(trustedCert)
		if err != nil {
			return err
		}

		// 将证书的hash值作为 id, mspid 作为 name,  赋值给 rootCerts
		msp.rootCerts[i] = id
	}

	// 创建并填充中间证书集合（如果存在）
	msp.intermediateCerts = make([]Identity, len(conf.IntermediateCerts))
	// 遍历所有中间证书byte字节数组, 解析为一个身份实例, 并加入msp.intermediateCerts
	for i, trustedCert := range conf.IntermediateCerts {
		// 用于从pem中获取身份信息、公钥
		id, _, err := msp.getIdentityFromConf(trustedCert)
		if err != nil {
			return err
		}

		// 将证书的hash值作为 id, mspid 作为 name,  赋值给 rootCerts
		msp.intermediateCerts[i] = id
	}

	// 根 CA 和中间 CA 证书已经进行了清理，可以重新导入到 Roots 和 Intermediates
	msp.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	for _, id := range msp.rootCerts {
		msp.opts.Roots.AddCert(id.(*identity).cert)
	}
	for _, id := range msp.intermediateCerts {
		msp.opts.Intermediates.AddCert(id.(*identity).cert)
	}

	return nil
}

// setupAdmins 函数设置管理员。
// 输入参数：
//   - conf: m.FabricMSPConfig 对象，MSP配置
//
// 返回值：
//   - error: 如果设置过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupAdmins(conf *m.FabricMSPConfig) error {
	// 设置管理员的回调函数
	return msp.internalSetupAdmin(conf)
}

// setupAdminsPreV142 函数设置管理员（适用于Fabric v1.4.2之前的版本）。
// 输入参数：
//   - conf: m.FabricMSPConfig 对象，MSP配置
//
// 返回值：
//   - error: 如果设置过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupAdminsPreV142(conf *m.FabricMSPConfig) error {
	// 制作并填写一组管理证书 (如果存在)
	msp.admins = make([]Identity, len(conf.Admins))
	for i, admCert := range conf.Admins {
		// 从配置中获取身份信息(证书、公钥、msp实例)
		id, _, err := msp.getIdentityFromConf(admCert)
		if err != nil {
			return err
		}

		msp.admins[i] = id
	}

	return nil
}

// setupAdminsV142 函数设置管理员（适用于Fabric v1.4.2及更高版本）。
// 输入参数：
//   - conf: m.FabricMSPConfig 对象，MSP配置
//
// 返回值：
//   - error: 如果设置过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupAdminsV142(conf *m.FabricMSPConfig) error {
	// 制作并填写一组管理证书 (如果存在)
	if err := msp.setupAdminsPreV142(conf); err != nil {
		return err
	}

	if len(msp.admins) == 0 && (!msp.ouEnforcement || msp.adminOU == nil) {
		return errors.New("如果未配置admin ou节点分类，则必须声明administrators")
	}

	return nil
}

// isECDSASignatureAlgorithm 函数用于判断给定的算法标识符是否为 ECDSA 签名算法。
//
// 参数：
//   - algid asn1.ObjectIdentifier：算法标识符。
//
// 返回值：
//   - bool：如果是 ECDSA 签名算法，则返回 true；否则返回 false。
func isECDSASignatureAlgorithm(algid asn1.ObjectIdentifier) bool {
	// 这是 Go 1.14 支持的用于 CRL 签名的 ECDSA 算法集合
	ecdsaSignaureAlgorithms := []asn1.ObjectIdentifier{
		{1, 2, 840, 10045, 4, 1},    // oidSignatureECDSAWithSHA1
		{1, 2, 840, 10045, 4, 3, 2}, // oidSignatureECDSAWithSHA256
		{1, 2, 840, 10045, 4, 3, 3}, // oidSignatureECDSAWithSHA384
		{1, 2, 840, 10045, 4, 3, 4}, // oidSignatureECDSAWithSHA512
	}

	// 遍历 ECDSA 签名算法集合，判断给定的算法标识符是否相等
	for _, id := range ecdsaSignaureAlgorithms {
		if id.Equal(algid) {
			return true
		}
	}

	return false
}

// setupCRLs 方法用于设置 CRL（证书吊销列表）。
//
// 参数：
//   - conf *m.FabricMSPConfig：FabricMSPConfig 实例。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息；如果成功，则返回 nil。
func (msp *bccspmsp) setupCRLs(conf *m.FabricMSPConfig) error {
	// 设置 CRL（如果存在）
	msp.CRL = make([]*pkix.CertificateList, len(conf.RevocationList))
	for i, crlbytes := range conf.RevocationList {
		// 解析 CRL
		crl, err := x509.ParseCRL(crlbytes)
		if err != nil {
			return errors.Wrap(err, "无法解析吊销证书集合 RevocationList")
		}

		// 是否属于 ECDSA 签名, (Go 1.14版本的算法)
		if isECDSASignatureAlgorithm(crl.SignatureAlgorithm.Algorithm) {
			r, s, err := utils.UnmarshalECDSASignature(crl.SignatureValue.RightAlign())
			if err != nil {
				return err
			}
			sig, err := utils.MarshalECDSASignature(r, s)
			if err != nil {
				return err
			}
			crl.SignatureValue = asn1.BitString{Bytes: sig, BitLength: 8 * len(sig)}
		}

		// TODO: 预验证 CRL 上的签名，并创建一个映射，将 CA 证书与相应的 CRL 关联起来，
		//       以便在验证时可以根据要验证的证书链提前查找 CRL

		msp.CRL[i] = crl
	}

	return nil
}

// finalizeSetupCAs 方法用于完成设置 CA（证书颁发机构）的最后步骤。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息；如果成功，则返回 nil。
func (msp *bccspmsp) finalizeSetupCAs() error {
	// 确保我们的 CA 正确形成并且有效
	for _, id := range append(append([]Identity{}, msp.rootCerts...), msp.intermediateCerts...) {
		if !id.(*identity).cert.IsCA {
			return errors.Errorf("CA证书没有CA属性, (SN: %x)", id.(*identity).cert.SerialNumber)
		}

		// 返回所提供证书的主题密钥标识符
		if _, err := getSubjectKeyIdentifierFromCert(id.(*identity).cert); err != nil {
			return errors.WithMessagef(err, "主题密钥标识符扩展的CA证书问题, (SN: %x)", id.(*identity).cert.SerialNumber)
		}

		// 用于验证 CA（证书颁发机构）的身份
		if err := msp.validateCAIdentity(id.(*identity)); err != nil {
			return errors.WithMessagef(err, "CA证书无效, (SN: %s)", id.(*identity).cert.SerialNumber)
		}
	}

	// 填充 certificationTreeInternalNodesMap 以标记认证树的内部节点映射
	msp.certificationTreeInternalNodesMap = make(map[string]bool)
	for _, id := range append([]Identity{}, msp.intermediateCerts...) {
		// 根据给定的验证选项验证证书，并返回唯一的验证链
		chain, err := msp.getUniqueValidationChain(id.(*identity).cert, msp.getValidityOptsForCert(id.(*identity).cert))
		if err != nil {
			return errors.WithMessagef(err, "获取验证链失败, (SN: %s)", id.(*identity).cert.SerialNumber)
		}

		// 注意，chain[0] 是 id.(*identity).id，因此不计为父节点
		for i := 1; i < len(chain); i++ {
			msp.certificationTreeInternalNodesMap[string(chain[i].Raw)] = true
		}
	}

	return nil
}

func (msp *bccspmsp) setupNodeOUs(config *m.FabricMSPConfig) error {
	if config.FabricNodeOus != nil {

		msp.ouEnforcement = config.FabricNodeOus.Enable

		if config.FabricNodeOus.ClientOuIdentifier == nil || len(config.FabricNodeOus.ClientOuIdentifier.OrganizationalUnitIdentifier) == 0 {
			return errors.New("Failed setting up NodeOUs. ClientOU must be different from nil.")
		}

		if config.FabricNodeOus.PeerOuIdentifier == nil || len(config.FabricNodeOus.PeerOuIdentifier.OrganizationalUnitIdentifier) == 0 {
			return errors.New("Failed setting up NodeOUs. PeerOU must be different from nil.")
		}

		// ClientOU
		msp.clientOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.ClientOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.ClientOuIdentifier.Certificate) != 0 {
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.ClientOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.clientOU.CertifiersIdentifier = certifiersIdentifier
		}

		// PeerOU
		msp.peerOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.PeerOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.PeerOuIdentifier.Certificate) != 0 {
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.PeerOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.peerOU.CertifiersIdentifier = certifiersIdentifier
		}

	} else {
		msp.ouEnforcement = false
	}

	return nil
}

func (msp *bccspmsp) setupNodeOUsV142(config *m.FabricMSPConfig) error {
	if config.FabricNodeOus == nil {
		msp.ouEnforcement = false
		return nil
	}

	msp.ouEnforcement = config.FabricNodeOus.Enable

	counter := 0
	// ClientOU
	if config.FabricNodeOus.ClientOuIdentifier != nil {
		// 此身份ClientOU的name名称
		msp.clientOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.ClientOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.ClientOuIdentifier.Certificate) != 0 {
			// 函数获取证书的认证者标识符(hash值)
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.ClientOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.clientOU.CertifiersIdentifier = certifiersIdentifier
		}
		counter++
	} else {
		msp.clientOU = nil
	}

	// PeerOU
	if config.FabricNodeOus.PeerOuIdentifier != nil {
		// 此身份PeerOU的name名称
		msp.peerOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.PeerOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.PeerOuIdentifier.Certificate) != 0 {
			// 函数获取证书的认证者标识符(hash值)
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.PeerOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.peerOU.CertifiersIdentifier = certifiersIdentifier
		}
		counter++
	} else {
		msp.peerOU = nil
	}

	// AdminOU
	if config.FabricNodeOus.AdminOuIdentifier != nil {
		// 此身份AdminOU的name名称
		msp.adminOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.AdminOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.AdminOuIdentifier.Certificate) != 0 {
			// 函数获取证书的认证者标识符(hash值)
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.AdminOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.adminOU.CertifiersIdentifier = certifiersIdentifier
		}
		counter++
	} else {
		msp.adminOU = nil
	}

	// OrdererOU
	if config.FabricNodeOus.OrdererOuIdentifier != nil {
		// 此身份OrdererOU的name名称
		msp.ordererOU = &OUIdentifier{OrganizationalUnitIdentifier: config.FabricNodeOus.OrdererOuIdentifier.OrganizationalUnitIdentifier}
		if len(config.FabricNodeOus.OrdererOuIdentifier.Certificate) != 0 {
			// 函数获取证书的认证者标识符(hash值)
			certifiersIdentifier, err := msp.getCertifiersIdentifier(config.FabricNodeOus.OrdererOuIdentifier.Certificate)
			if err != nil {
				return err
			}
			msp.ordererOU.CertifiersIdentifier = certifiersIdentifier
		}
		counter++
	} else {
		msp.ordererOU = nil
	}

	if counter == 0 {
		// 禁用 NodeOU
		msp.ouEnforcement = false
	}

	return nil
}

// setupSigningIdentity 函数用于设置签名身份，根据给定的配置。
// 如果配置中存在签名身份，则将其设置为当前MSP的签名者。
// 输入参数：
//   - conf: FabricMSPConfig 配置对象
//
// 返回值：
//   - error: 如果设置签名身份过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupSigningIdentity(conf *m.FabricMSPConfig) error {
	if conf.SigningIdentity != nil {
		// 从Conf获取签名身份
		sid, err := msp.getSigningIdentityFromConf(conf.SigningIdentity)
		if err != nil {
			return err
		}
		// 返回签名过期时间, 检查是否过期
		expirationTime := sid.ExpiresAt()
		now := time.Now()
		if expirationTime.After(now) {
			duration := now.Sub(expirationTime).Round(24 * time.Hour) // 获取时间持续时间的整数部分，以天为单位
			days := int(duration.Hours() / 24)                        // 获取天数
			mspLogger.Infof("签名身份证书过期时间为: %d天", -days)
		} else if expirationTime.IsZero() {
			mspLogger.Infof("签名身份证书没有已知的过期时间")
		} else {
			duration := now.Sub(expirationTime).Round(24 * time.Hour) // 获取时间持续时间的整数部分，以天为单位
			days := int(duration.Hours() / 24)                        // 获取天数
			return errors.Errorf("签名身份证书过期时间为: %d天", -days)
		}

		msp.signer = sid
	}

	return nil
}

// setupOUs 函数设置组织单位标识符（OU）。
// 输入参数：
//   - conf: m.FabricMSPConfig 对象，MSP配置
//
// 返回值：
//   - error: 如果设置过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupOUs(conf *m.FabricMSPConfig) error {
	msp.ouIdentifiers = make(map[string][][]byte)
	for _, ou := range conf.OrganizationalUnitIdentifiers {
		// 函数获取证书的认证者标识符(hash值)
		certifiersIdentifier, err := msp.getCertifiersIdentifier(ou.Certificate)
		if err != nil {
			return errors.WithMessagef(err, "获取证书失败 [%v]", ou)
		}

		// 检查是否有重复项
		found := false
		for _, id := range msp.ouIdentifiers[ou.OrganizationalUnitIdentifier] {
			if bytes.Equal(id, certifiersIdentifier) {
				mspLogger.Warningf("在 OU 标识符中找到重复项 [%s, %v]", ou.OrganizationalUnitIdentifier, id)
				found = true
				break
			}
		}

		if !found {
			// 没有找到重复项，添加到 OU 标识符中
			msp.ouIdentifiers[ou.OrganizationalUnitIdentifier] = append(
				msp.ouIdentifiers[ou.OrganizationalUnitIdentifier],
				certifiersIdentifier,
			)
		}
	}

	return nil
}

// setupTLSCAs 设置 TLS 证书。
// 输入参数：
//   - conf: FabricMSPConfig 配置对象
//
// 返回值：
//   - error: 如果验证过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) setupTLSCAs(conf *m.FabricMSPConfig) error {
	// 加载TLS根和中间CA标识
	msp.tlsRootCerts = make([][]byte, len(conf.TlsRootCerts))
	rootCerts := make([]*x509.Certificate, len(conf.TlsRootCerts))
	for i, trustedCert := range conf.TlsRootCerts {
		// 用于从 PEM 格式的字节中获取证书
		cert, err := msp.getCertFromPem(trustedCert)
		if err != nil {
			return err
		}

		rootCerts[i] = cert
		msp.tlsRootCerts[i] = trustedCert
	}

	// 制作并填写一套中间证书 (如果有)
	msp.tlsIntermediateCerts = make([][]byte, len(conf.TlsIntermediateCerts))
	intermediateCerts := make([]*x509.Certificate, len(conf.TlsIntermediateCerts))
	for i, trustedCert := range conf.TlsIntermediateCerts {
		// 用于从 PEM 格式的字节中获取证书
		cert, err := msp.getCertFromPem(trustedCert)
		if err != nil {
			return err
		}

		intermediateCerts[i] = cert
		msp.tlsIntermediateCerts[i] = trustedCert
	}

	// 确保我们的CAs正确形成并且有效
	for _, cert := range append(append([]*x509.Certificate{}, rootCerts...), intermediateCerts...) {
		if cert == nil {
			continue
		}

		if !cert.IsCA {
			return errors.Errorf("CA证书没有CA属性, (SN: %x)", cert.SerialNumber)
		}
		if _, err := getSubjectKeyIdentifierFromCert(cert); err != nil {
			return errors.WithMessagef(err, "主题密钥标识符扩展的CA证书问题, (SN: %x)", cert.SerialNumber)
		}

		if err := msp.validateTLSCAIdentity(cert); err != nil {
			return errors.WithMessagef(err, "CA证书无效, (SN: %s)", cert.SerialNumber)
		}
	}

	return nil
}

func (msp *bccspmsp) setupV1(conf1 *m.FabricMSPConfig) error {
	err := msp.preSetupV1(conf1)
	if err != nil {
		return err
	}

	err = msp.postSetupV1(conf1)
	if err != nil {
		return err
	}

	return nil
}

func (msp *bccspmsp) preSetupV1(conf *m.FabricMSPConfig) error {
	// setup crypto config
	if err := msp.setupCrypto(conf); err != nil {
		return err
	}

	// Setup CAs
	if err := msp.setupCAs(conf); err != nil {
		return err
	}

	// Setup Admins
	if err := msp.setupAdmins(conf); err != nil {
		return err
	}

	// Setup CRLs
	if err := msp.setupCRLs(conf); err != nil {
		return err
	}

	// Finalize setup of the CAs
	if err := msp.finalizeSetupCAs(); err != nil {
		return err
	}

	// setup the signer (if present)
	if err := msp.setupSigningIdentity(conf); err != nil {
		return err
	}

	// setup TLS CAs
	if err := msp.setupTLSCAs(conf); err != nil {
		return err
	}

	// setup the OUs
	if err := msp.setupOUs(conf); err != nil {
		return err
	}

	return nil
}

// preSetupV142 函数用于在设置 v1.4.2 版本的 MSP 之前进行预设置。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) preSetupV142(conf *m.FabricMSPConfig) error {
	// 设置hash算法加密配置
	if err := msp.setupCrypto(conf); err != nil {
		return err
	}

	// 设置证书颁发机构（CAs）
	if err := msp.setupCAs(conf); err != nil {
		return err
	}

	// 设置证书吊销列表（CRLs）
	if err := msp.setupCRLs(conf); err != nil {
		return err
	}

	// 完成证书颁发机构（CAs）的设置
	if err := msp.finalizeSetupCAs(); err != nil {
		return err
	}

	// 设置签名者（如果存在）, 判断是否过期
	if err := msp.setupSigningIdentity(conf); err != nil {
		return err
	}

	// 设置 TLS 证书颁发机构（CAs）
	if err := msp.setupTLSCAs(conf); err != nil {
		return err
	}

	// 设置组织单元（OUs）
	if err := msp.setupOUs(conf); err != nil {
		return err
	}

	// 设置节点组织单元（NodeOUs）
	if err := msp.setupNodeOUsV142(conf); err != nil {
		return err
	}

	// 设置管理员
	if err := msp.setupAdmins(conf); err != nil {
		return err
	}

	return nil
}

// postSetupV1 函数用于在设置 v1 版本的 MSP 之后进行后置设置。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) postSetupV1(conf *m.FabricMSPConfig) error {
	// 确保管理员也是有效的成员
	// 这样，当我们验证管理员的 MSP 主体时
	// 我们可以简单地检查证书是否完全匹配
	for i, admin := range msp.admins {
		err := admin.Validate()
		if err != nil {
			return errors.WithMessagef(err, "管理员 %d 无效", i)
		}
	}

	return nil
}

func (msp *bccspmsp) setupV11(conf *m.FabricMSPConfig) error {
	err := msp.preSetupV1(conf)
	if err != nil {
		return err
	}

	// setup NodeOUs
	if err := msp.setupNodeOUs(conf); err != nil {
		return err
	}

	err = msp.postSetupV11(conf)
	if err != nil {
		return err
	}

	return nil
}

// setupV142 函数用于设置 MSP 的 v1.4.2 版本。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) setupV142(conf *m.FabricMSPConfig) error {
	// 函数用于在设置 v1.4.2 版本的 MSP 之前进行预设置。
	// 设置加密、证书、吊销证书、签名者、tls证书、NodeOUs节点组织单元、触发 internalSetupAdmin 设置管理员函数回调
	err := msp.preSetupV142(conf)
	if err != nil {
		return err
	}

	// 函数用于在设置 v1.4.2 (或者更高的)版本的 MSP 之后进行后置设置
	err = msp.postSetupV142(conf)
	if err != nil {
		return err
	}

	return nil
}

func (msp *bccspmsp) postSetupV11(conf *m.FabricMSPConfig) error {
	// Check for OU enforcement
	if !msp.ouEnforcement {
		// No enforcement required. Call post setup as per V1
		return msp.postSetupV1(conf)
	}

	// Check that admins are clients
	principalBytes, err := proto.Marshal(&m.MSPRole{Role: m.MSPRole_CLIENT, MspIdentifier: msp.name})
	if err != nil {
		return errors.Wrapf(err, "failed creating MSPRole_CLIENT")
	}
	principal := &m.MSPPrincipal{
		PrincipalClassification: m.MSPPrincipal_ROLE,
		Principal:               principalBytes,
	}
	for i, admin := range msp.admins {
		err = admin.SatisfiesPrincipal(principal)
		if err != nil {
			return errors.WithMessagef(err, "admin %d is invalid", i)
		}
	}

	return nil
}

// postSetupV142 函数用于在设置 v1.4.2 版本的 MSP 之后进行后置设置。
// 参数：
//   - conf *m.FabricMSPConfig：MSP 的配置信息。
//
// 返回值：
//   - error：如果设置过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) postSetupV142(conf *m.FabricMSPConfig) error {
	// 检查 OU 强制执行
	if !msp.ouEnforcement {
		// 不需要强制执行 OU。按照 V1 的方式进行后置设置
		return msp.postSetupV1(conf)
	}

	// 验证管理员实例是否是客户端或管理员, 任意一个判断成功即可
	for i, admin := range msp.admins {
		err1 := msp.hasOURole(admin, m.MSPRole_CLIENT)
		err2 := msp.hasOURole(admin, m.MSPRole_ADMIN)
		if err1 != nil && err2 != nil {
			return errors.Errorf("第%d管理员证书 无效 [%s,%s]", i, err1, err2)
		}
	}

	return nil
}
