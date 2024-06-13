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
	"encoding/hex"
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/verify"
	"strings"

	"github.com/golang/protobuf/proto"
	m "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/signer"
	"github.com/hyperledger/fabric/bccsp/sw"
	"github.com/hyperledger/fabric/bccsp/utils"
	"github.com/pkg/errors"
)

// mspSetupFuncType 是setup函数的原型
type mspSetupFuncType func(config *m.FabricMSPConfig) error

// validateIdentityOUsFuncType 是验证身份的功能的原型
type validateIdentityOUsFuncType func(id *identity) error

// satisfiesPrincipalInternalFuncType 是检查主体是否满足的函数的原型
type satisfiesPrincipalInternalFuncType func(id Identity, principal *m.MSPPrincipal) error

// setupAdminInternalFuncType 是设置管理员的函数的原型
type setupAdminInternalFuncType func(conf *m.FabricMSPConfig) error

// bccspmsp 这是一个MSP的实例化
// 使用BCCSP作为其加密原语。
type bccspmsp struct {
	// version 指定此msp的版本
	version MSPVersion
	// 以下函数指针用于更改行为
	// 这个MSP取决于其版本。
	// internalSetupFunc是指向setup函数的指针
	internalSetupFunc mspSetupFuncType

	// internalValidateIdentityOusFunc 是指向用于验证身份的OUs的函数的指针
	internalValidateIdentityOusFunc validateIdentityOUsFuncType

	// internalSatisfiesPrincipalInternalFunc 是指向函数的指针，用于检查主体是否得到满足
	internalSatisfiesPrincipalInternalFunc satisfiesPrincipalInternalFuncType

	// internalSetupAdmin 是指向设置此msp管理员的函数的指针
	internalSetupAdmin setupAdminInternalFuncType

	// 我们信任的CA证书列表
	rootCerts []Identity

	// 我们信任的中间证书列表
	intermediateCerts []Identity

	// 我们信任的CA TLS证书列表
	tlsRootCerts [][]byte

	// 我们信任的中间TLS证书列表
	tlsIntermediateCerts [][]byte

	// certificationTreeInternalNodesMap，其键对应于原材料(DER表示) 转换为字符串的证书，其值是布尔值。
	// True表示证书是证书树的内部节点。
	// False表示证书对应于证书树的叶子。
	certificationTreeInternalNodesMap map[string]bool

	// 签名标识列表(证书、私钥、签名公钥)
	signer SigningIdentity

	// 管理员身份列表
	admins []Identity

	// 加密提供者
	bccsp bccsp.BCCSP

	// 此MSP的提供程序标识符, mspid
	name string

	// MSP成员的验证选项
	opts *x509.VerifyOptions

	// 证书吊销列表
	CRL []*pkix.CertificateList

	// OUs列表
	ouIdentifiers map[string][][]byte

	// cryptoConfig 包含哈希算法配置
	cryptoConfig *m.FabricCryptoConfig

	// NodeOUs 配置
	ouEnforcement bool
	// 这些是客户端，对等体，管理员和orderer的ouidentifier。
	// 它们用于区分这些实体
	clientOU, peerOU, adminOU, ordererOU *OUIdentifier
}

// newBccspMsp 函数返回一个由 BCCSP 加密服务提供者支持的 MSP 实例。
// 它处理 x.509 证书，并可以生成由证书和密钥对支持的身份和签名身份。
// 参数：
//   - version MSPVersion：MSP 的版本。
//   - defaultBCCSP bccsp.BCCSP：默认的 BCCSP 加密服务提供者。
//
// 返回值：
//   - MSP：创建的 MSP 实例。
//   - error：如果创建过程中出现错误，则返回相应的错误信息。
func newBccspMsp(version MSPVersion, defaultBCCSP bccsp.BCCSP) (MSP, error) {
	mspLogger.Debugf("正在创建 BCCSP-based MSP 实例")

	theMsp := &bccspmsp{}
	theMsp.version = version
	theMsp.bccsp = defaultBCCSP
	switch version {
	case MSPv1_0:
		theMsp.internalSetupFunc = theMsp.setupV1
		theMsp.internalValidateIdentityOusFunc = theMsp.validateIdentityOUsV1
		theMsp.internalSatisfiesPrincipalInternalFunc = theMsp.satisfiesPrincipalInternalPreV13
		theMsp.internalSetupAdmin = theMsp.setupAdminsPreV142
	case MSPv1_1:
		theMsp.internalSetupFunc = theMsp.setupV11
		theMsp.internalValidateIdentityOusFunc = theMsp.validateIdentityOUsV11
		theMsp.internalSatisfiesPrincipalInternalFunc = theMsp.satisfiesPrincipalInternalPreV13
		theMsp.internalSetupAdmin = theMsp.setupAdminsPreV142
	case MSPv1_3:
		theMsp.internalSetupFunc = theMsp.setupV11
		theMsp.internalValidateIdentityOusFunc = theMsp.validateIdentityOUsV11
		theMsp.internalSatisfiesPrincipalInternalFunc = theMsp.satisfiesPrincipalInternalV13
		theMsp.internalSetupAdmin = theMsp.setupAdminsPreV142
	case MSPv1_4_3:
		// 设置加密、证书、吊销证书、签名者、tls证书、NodeOUs节点组织单元、触发 internalSetupAdmin 设置管理员函数回调
		theMsp.internalSetupFunc = theMsp.setupV142
		theMsp.internalValidateIdentityOusFunc = theMsp.validateIdentityOUsV142
		theMsp.internalSatisfiesPrincipalInternalFunc = theMsp.satisfiesPrincipalInternalV142
		// 设置验证管理员证书的函数回调
		theMsp.internalSetupAdmin = theMsp.setupAdminsV142
	default:
		return nil, errors.Errorf("无效的MSP版本 [%v]", version)
	}

	return theMsp, nil
}

// NewBccspMspWithKeyStore allows to create a BCCSP-based MSP whose underlying
// crypto material is available through the passed keystore
func NewBccspMspWithKeyStore(version MSPVersion, keyStore bccsp.KeyStore, bccsp bccsp.BCCSP) (MSP, error) {
	thisMSP, err := newBccspMsp(version, bccsp)
	if err != nil {
		return nil, err
	}

	csp, err := sw.NewWithParams(
		factory.GetDefaultOpts().SW.Security,
		factory.GetDefaultOpts().SW.Hash,
		keyStore)
	if err != nil {
		return nil, err
	}
	thisMSP.(*bccspmsp).bccsp = csp

	return thisMSP, nil
}

// getCertFromPem 函数用于从 PEM 格式的字节中获取 x509 证书。
// 参数：
//   - idBytes []byte：PEM 格式的证书字节。
//
// 返回值：
//   - *x509.Certificate：解析后的 x509 证书。
//   - error：如果解析过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) getCertFromPem(idBytes []byte) (*x509.Certificate, error) {
	if idBytes == nil {
		return nil, errors.New("从Pem字节数组中获取证书错误: Pem字节数组为空")
	}

	// 解码 PEM 格式的字节
	pemCert, _ := pem.Decode(idBytes)
	if pemCert == nil {
		return nil, errors.Errorf("从Pem获取证书错误: 无法解码 PEM 字节 [%v]", idBytes)
	}

	// 获取证书
	var cert *x509.Certificate
	cert, err := common.ParseCertificate(pemCert.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "从Pem获取证书错误: 解析 x509 证书失败")
	}

	return cert, nil
}

// getIdentityFromConf 函数用于从配置中获取身份信息(证书、公钥、msp实例)。
// 参数：
//   - idBytes []byte：PEM 格式的证书字节。
//
// 返回值：
//   - Identity：身份信息。
//   - bccsp.Key：公钥。
//   - error：如果获取过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) getIdentityFromConf(idBytes []byte) (Identity, bccsp.Key, error) {
	// 获取x509证书
	cert, err := msp.getCertFromPem(idBytes)
	if err != nil {
		return nil, nil, err
	}

	// 获取正确格式的公钥
	certPubK, err := msp.bccsp.KeyImport(cert, &bccsp.X509PublicKeyImportOpts{Temporary: true})
	if err != nil {
		return nil, nil, err
	}
	// 根据证书、公钥创建身份 Identity 信息
	identityNew, err := newIdentity(cert, certPubK, msp)
	if err != nil {
		return nil, nil, err
	}

	return identityNew, certPubK, nil
}

// getSigningIdentityFromConf 函数根据给定的 SigningIdentityInfo 从配置中获取签名身份。
// 输入参数：
//   - sidInfo: SigningIdentityInfo 对象，包含签名身份的信息
//
// 返回值：
//   - SigningIdentity: 获取到的签名身份
//   - error: 如果获取签名身份过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getSigningIdentityFromConf(sidInfo *m.SigningIdentityInfo) (SigningIdentity, error) {
	if sidInfo == nil {
		return nil, errors.New("从配置中获取签名身份错误: 签名证书没有找到")
	}

	// 提取身份的公共部分
	idPub, pubKey, err := msp.getIdentityFromConf(sidInfo.PublicSigner)
	if err != nil {
		return nil, err
	}

	// 在 BCCSP 密钥存储目录中查找公钥匹配的私钥
	privKey, err := msp.bccsp.GetKey(pubKey.SKI())
	// 安全性较低: 如果BCCSP无法找到密钥，则尝试从KeyInfo导入私钥(如果有的话, 一般是没有的)
	if err != nil {
		mspLogger.Debugf("无法找到 SKI [%s]，尝试使用 KeyMaterial 字段: %+v\n", hex.EncodeToString(pubKey.SKI()), err)
		if sidInfo.PrivateSigner == nil || sidInfo.PrivateSigner.KeyMaterial == nil {
			return nil, errors.New("SigningIdentityInfo 中未找到 KeyMaterial")
		}

		pemKey, _ := pem.Decode(sidInfo.PrivateSigner.KeyMaterial)
		if pemKey == nil {
			return nil, errors.Errorf("%s: 错误的 PEM 编码", sidInfo.PrivateSigner.KeyIdentifier)
		}
		privKey, err = msp.bccsp.KeyImport(pemKey.Bytes, &bccsp.ECDSAPrivateKeyImportOpts{Temporary: true})
		if err != nil {
			return nil, errors.WithMessage(err, "从配置中获取签名身份错误: 导入 EC 私钥失败")
		}
	}

	peerSigner, err := signer.New(msp.bccsp, privKey)
	if err != nil {
		return nil, errors.WithMessage(err, "从配置中获取签名身份错误: bccspCryptoSigner 签名对象初始化失败")
	}

	return newSigningIdentity(idPub.(*identity).cert, idPub.(*identity).pk, peerSigner, msp)
}

// Setup 方法用于设置 MSP 实例的内部数据结构，根据给定的 MSPConfig 引用。
//
// 参数：
//   - conf1 *m.MSPConfig：MSP 配置的引用。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息；如果成功，则返回 nil。
func (msp *bccspmsp) Setup(conf1 *m.MSPConfig) error {
	// 检查 conf1 是否为 nil
	if conf1 == nil {
		return errors.New("安装错误: msp目录没有解析到任何证书")
	}

	// 将 conf1.Config 反序列化为 FabricMSPConfig 实例
	conf := &m.FabricMSPConfig{}
	err := proto.Unmarshal(conf1.Config, conf)
	if err != nil {
		return errors.Wrap(err, "反序列化为 FabricMSPConfig 实例错误")
	}

	// 设置 MSP 的名称, 就是 mspid
	msp.name = conf.Name
	mspLogger.Debugf("设置MSPID: %s", msp.name)

	// 调用 internalSetupFunc 方法进行设置
	return msp.internalSetupFunc(conf)
}

// GetVersion returns the version of this MSP
func (msp *bccspmsp) GetVersion() MSPVersion {
	return msp.version
}

// GetType 函数返回此MSP的类型。
//
// 返回值：
//   - ProviderType: ProviderType 枚举值，表示MSP的类型
func (msp *bccspmsp) GetType() ProviderType {
	return FABRIC
}

// GetIdentifier returns the MSP identifier for this instance
func (msp *bccspmsp) GetIdentifier() (string, error) {
	return msp.name, nil
}

// GetTLSRootCerts returns the root certificates for this MSP
func (msp *bccspmsp) GetTLSRootCerts() [][]byte {
	return msp.tlsRootCerts
}

// GetTLSIntermediateCerts returns the intermediate root certificates for this MSP
func (msp *bccspmsp) GetTLSIntermediateCerts() [][]byte {
	return msp.tlsIntermediateCerts
}

// GetDefaultSigningIdentity 函数返回此MSP的默认签名身份（如果有）。
//
// 返回值：
//   - SigningIdentity: 默认签名身份
//   - error: 如果此MSP没有有效的默认签名身份，则返回相应的错误信息
func (msp *bccspmsp) GetDefaultSigningIdentity() (SigningIdentity, error) {
	mspLogger.Debugf("获取默认签名身份")

	if msp.signer == nil {
		return nil, errors.New("此MSP没有有效的默认签名身份")
	}

	return msp.signer, nil
}

// Validate 尝试确定提供的身份是否根据此 MSP 的信任根有效；如果身份有效，则返回 nil；否则返回错误。
//
// 输入参数：
//   - id：要验证的身份。
//
// 返回值：
//   - error：如果身份有效，则返回 nil；否则返回非空的错误。
func (msp *bccspmsp) Validate(id Identity) error {
	mspLogger.Debugf("MSP %s 验证身份", msp.name)

	switch id := id.(type) {
	case *identity:
		// 如果身份是 identity 类型，则使用 MSP 的 validateIdentity 方法进行验证
		return msp.validateIdentity(id)
	default:
		return errors.New("Identity 身份类型无法识别")
	}
}

// hasOURole 函数检查身份是否属于与指定的MSPRole相关联的组织单位。
// 此函数不检查证书的标识符，需要在之前进行适当的验证。
//
// 输入参数：
//   - id: Identity 接口，身份信息
//   - mspRole: m.MSPRole_MSPRoleType 枚举类型，MSP角色类型
//
// 返回值：
//   - error: 如果身份不属于指定的组织单位，则返回相应的错误信息
func (msp *bccspmsp) hasOURole(id Identity, mspRole m.MSPRole_MSPRoleType) error {
	// 检查 NodeOUs
	if !msp.ouEnforcement {
		return errors.New("未激活 NodeOUs。无法区分身份。")
	}

	mspLogger.Debugf("MSP %s 检查身份是否为客户端", msp.name)

	switch id := id.(type) {
	// 如果身份是特定类型的，则可以根据MSP的信任根进行验证
	case *identity:
		return msp.hasOURoleInternal(id, mspRole)
	default:
		return errors.New("未识别的身份类型")
	}
}

// hasOURoleInternal 函数检查身份是否属于与指定的MSPRole相关联的组织单位（内部函数）。
//
// 输入参数：
//   - id: *identity 对象，身份信息
//   - mspRole: m.MSPRole_MSPRoleType 枚举类型，MSP角色类型
//
// 返回值：
//   - error: 如果身份不属于指定的组织单位，则返回相应的错误信息
func (msp *bccspmsp) hasOURoleInternal(id *identity, mspRole m.MSPRole_MSPRoleType) error {
	var nodeOU *OUIdentifier
	switch mspRole {
	case m.MSPRole_CLIENT:
		nodeOU = msp.clientOU
	case m.MSPRole_PEER:
		nodeOU = msp.peerOU
	case m.MSPRole_ADMIN:
		nodeOU = msp.adminOU
	case m.MSPRole_ORDERER:
		nodeOU = msp.ordererOU
	default:
		return errors.New("无效的MSPRoleType。必须是CLIENT、PEER、ADMIN或ORDERER")
	}

	if nodeOU == nil {
		return errors.Errorf("无法进行分类测试，未定义类型为[%s]的节点OU，MSP: [%s]", mspRole, msp.name)
	}
	// 函数返回此实例的组织单位（OU）
	for _, OU := range id.GetOrganizationalUnits() {
		if OU.OrganizationalUnitIdentifier == nodeOU.OrganizationalUnitIdentifier {
			return nil
		}
	}

	return errors.Errorf("身份不包含OU [%s]，MSP: [%s]", mspRole, msp.name)
}

// DeserializeIdentity 在给定SerializedIdentity结构的字节级表示的情况下返回标识
func (msp *bccspmsp) DeserializeIdentity(serializedID []byte) (Identity, error) {
	mspLogger.Debug("获得身份")

	// 我们首先反序列化为SerializedIdentity以获取MSP ID
	sId := &m.SerializedIdentity{}
	err := proto.Unmarshal(serializedID, sId)
	if err != nil {
		return nil, errors.Wrap(err, "无法反序列化SerializedIdentity")
	}

	if sId.Mspid != msp.name {
		return nil, errors.Errorf("身份的MSP ID与节点配置的MSP ID不一致, 预期MSP ID %s, 已收到MSP ID %s", msp.name, sId.Mspid)
	}

	return msp.deserializeIdentityInternal(sId.IdBytes)
}

// deserializeIdentityInternal 返回给定其证书字节的身份Identity
func (msp *bccspmsp) deserializeIdentityInternal(serializedIdentity []byte) (Identity, error) {
	// 此MSP将始终以这种方式反序列化certs
	bl, _ := pem.Decode(serializedIdentity)
	if bl == nil {
		return nil, errors.New("无法解码PEM结构")
	}
	cert, err := common.ParseCertificate(bl.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "解析x509证书 parseCertificate 失败")
	}

	// 现在我们有了证书; 确保其字段 (例如，issuter.OU 或 Subject.OU) 与此MSP具有的MSP id匹配; 否则可能是攻击
	// TODO!
	// 我们还不能这样做，因为还没有标准化的方法将MSP ID编码到证书的x.509正文中
	pub, err := msp.bccsp.KeyImport(cert, &bccsp.X509PublicKeyImportOpts{Temporary: true})
	if err != nil {
		return nil, errors.WithMessage(err, "无法解析导入证书的公钥")
	}

	return newIdentity(cert, pub, msp)
}

// SatisfiesPrincipal 如果标识与主体匹配，则返回nil，否则返回错误
func (msp *bccspmsp) SatisfiesPrincipal(id Identity, principal *m.MSPPrincipal) error {
	principals, err := collectPrincipals(principal, msp.GetVersion())
	if err != nil {
		return err
	}
	for _, principal := range principals {
		err = msp.internalSatisfiesPrincipalInternalFunc(id, principal)
		if err != nil {
			return err
		}
	}
	return nil
}

// collectPrincipals collects principals from combined principals into a single MSPPrincipal slice.
func collectPrincipals(principal *m.MSPPrincipal, mspVersion MSPVersion) ([]*m.MSPPrincipal, error) {
	switch principal.PrincipalClassification {
	case m.MSPPrincipal_COMBINED:
		// Combined principals are not supported in MSP v1.0 or v1.1
		if mspVersion <= MSPv1_1 {
			return nil, errors.Errorf("invalid principal type %d", int32(principal.PrincipalClassification))
		}
		// Principal is a combination of multiple principals.
		principals := &m.CombinedPrincipal{}
		err := proto.Unmarshal(principal.Principal, principals)
		if err != nil {
			return nil, errors.Wrap(err, "could not unmarshal CombinedPrincipal from principal")
		}
		// Return an error if there are no principals in the combined principal.
		if len(principals.Principals) == 0 {
			return nil, errors.New("No principals in CombinedPrincipal")
		}
		// Recursively call msp.collectPrincipals for all combined principals.
		// There is no limit for the levels of nesting for the combined principals.
		var principalsSlice []*m.MSPPrincipal
		for _, cp := range principals.Principals {
			internalSlice, err := collectPrincipals(cp, mspVersion)
			if err != nil {
				return nil, err
			}
			principalsSlice = append(principalsSlice, internalSlice...)
		}
		// All the combined principals have been collected into principalsSlice
		return principalsSlice, nil
	default:
		return []*m.MSPPrincipal{principal}, nil
	}
}

// satisfiesPrincipalInternalPreV13 takes as arguments the identity and the principal.
// The function returns an error if one occurred.
// The function implements the behavior of an MSP up to and including v1.1.
func (msp *bccspmsp) satisfiesPrincipalInternalPreV13(id Identity, principal *m.MSPPrincipal) error {
	switch principal.PrincipalClassification {
	// in this case, we have to check whether the
	// identity has a role in the msp - member or admin
	case m.MSPPrincipal_ROLE:
		// Principal contains the msp role
		mspRole := &m.MSPRole{}
		err := proto.Unmarshal(principal.Principal, mspRole)
		if err != nil {
			return errors.Wrap(err, "could not unmarshal MSPRole from principal")
		}

		// at first, we check whether the MSP
		// identifier is the same as that of the identity
		if mspRole.MspIdentifier != msp.name {
			return errors.Errorf("the identity is a member of a different MSP (expected %s, got %s)", mspRole.MspIdentifier, id.GetMSPIdentifier())
		}

		// now we validate the different msp roles
		switch mspRole.Role {
		case m.MSPRole_MEMBER:
			// 在成员的情况下，我们简单地检查
			// 此标识对于MSP是否有效
			mspLogger.Debugf("检查identity是否满足的成员角色 %s", msp.name)
			return msp.Validate(id)
		case m.MSPRole_ADMIN:
			mspLogger.Debugf("Checking if identity satisfies ADMIN role for %s", msp.name)
			// in the case of admin, we check that the
			// id is exactly one of our admins
			if msp.isInAdmins(id.(*identity)) {
				return nil
			}
			return errors.New("This identity is not an admin")
		case m.MSPRole_CLIENT:
			fallthrough
		case m.MSPRole_PEER:
			mspLogger.Debugf("Checking if identity satisfies role [%s] for %s", m.MSPRole_MSPRoleType_name[int32(mspRole.Role)], msp.name)
			if err := msp.Validate(id); err != nil {
				return errors.Wrapf(err, "The identity is not valid under this MSP [%s]", msp.name)
			}

			if err := msp.hasOURole(id, mspRole.Role); err != nil {
				return errors.Wrapf(err, "The identity is not a [%s] under this MSP [%s]", m.MSPRole_MSPRoleType_name[int32(mspRole.Role)], msp.name)
			}
			return nil
		default:
			return errors.Errorf("invalid MSP role type %d", int32(mspRole.Role))
		}
	case m.MSPPrincipal_IDENTITY:
		// in this case we have to deserialize the principal's identity
		// and compare it byte-by-byte with our cert
		principalId, err := msp.DeserializeIdentity(principal.Principal)
		if err != nil {
			return errors.WithMessage(err, "invalid identity principal, not a certificate")
		}

		if bytes.Equal(id.(*identity).cert.Raw, principalId.(*identity).cert.Raw) {
			return principalId.Validate()
		}

		return errors.New("The identities do not match")
	case m.MSPPrincipal_ORGANIZATION_UNIT:
		// Principal contains the OrganizationUnit
		OU := &m.OrganizationUnit{}
		err := proto.Unmarshal(principal.Principal, OU)
		if err != nil {
			return errors.Wrap(err, "could not unmarshal OrganizationUnit from principal")
		}

		// at first, we check whether the MSP
		// identifier is the same as that of the identity
		if OU.MspIdentifier != msp.name {
			return errors.Errorf("the identity is a member of a different MSP (expected %s, got %s)", OU.MspIdentifier, id.GetMSPIdentifier())
		}

		// we then check if the identity is valid with this MSP
		// and fail if it is not
		err = msp.Validate(id)
		if err != nil {
			return err
		}

		// now we check whether any of this identity's OUs match the requested one
		for _, ou := range id.GetOrganizationalUnits() {
			if ou.OrganizationalUnitIdentifier == OU.OrganizationalUnitIdentifier &&
				bytes.Equal(ou.CertifiersIdentifier, OU.CertifiersIdentifier) {
				return nil
			}
		}

		// if we are here, no match was found, return an error
		return errors.New("The identities do not match")
	default:
		return errors.Errorf("invalid principal type %d", int32(principal.PrincipalClassification))
	}
}

// satisfiesPrincipalInternalV13 takes as arguments the identity and the principal.
// The function returns an error if one occurred.
// The function implements the additional behavior expected of an MSP starting from v1.3.
// For pre-v1.3 functionality, the function calls the satisfiesPrincipalInternalPreV13.
func (msp *bccspmsp) satisfiesPrincipalInternalV13(id Identity, principal *m.MSPPrincipal) error {
	switch principal.PrincipalClassification {
	case m.MSPPrincipal_COMBINED:
		return errors.New("SatisfiesPrincipalInternal shall not be called with a CombinedPrincipal")
	case m.MSPPrincipal_ANONYMITY:
		anon := &m.MSPIdentityAnonymity{}
		err := proto.Unmarshal(principal.Principal, anon)
		if err != nil {
			return errors.Wrap(err, "could not unmarshal MSPIdentityAnonymity from principal")
		}
		switch anon.AnonymityType {
		case m.MSPIdentityAnonymity_ANONYMOUS:
			return errors.New("Principal is anonymous, but X.509 MSP does not support anonymous identities")
		case m.MSPIdentityAnonymity_NOMINAL:
			return nil
		default:
			return errors.Errorf("Unknown principal anonymity type: %d", anon.AnonymityType)
		}

	default:
		// 使用pre-v1.3函数检查其他主体类型
		return msp.satisfiesPrincipalInternalPreV13(id, principal)
	}
}

// satisfiesPrincipalInternalV142 takes as arguments the identity and the principal.
// The function returns an error if one occurred.
// The function implements the additional behavior expected of an MSP starting from v2.0.
// For v1.3 functionality, the function calls the satisfiesPrincipalInternalPreV13.
func (msp *bccspmsp) satisfiesPrincipalInternalV142(id Identity, principal *m.MSPPrincipal) error {
	_, okay := id.(*identity)
	if !okay {
		return errors.New("无效的身份标识类型，需要 *identity")
	}

	switch principal.PrincipalClassification {
	case m.MSPPrincipal_ROLE:
		if !msp.ouEnforcement {
			break
		}

		// Principal contains the msp role
		mspRole := &m.MSPRole{}
		err := proto.Unmarshal(principal.Principal, mspRole)
		if err != nil {
			return errors.Wrap(err, "无法从主体负责人中反序列化 MSPRole")
		}

		// 首先，我们检查MSP是否
		// identifier与identity相同
		if mspRole.MspIdentifier != msp.name {
			return errors.Errorf("mspid标识与节点mspid不同(预期 %s, 实际 %s)", mspRole.MspIdentifier, id.GetMSPIdentifier())
		}

		// 现在我们只验证管理员角色，其他角色留给v1.3函数
		switch mspRole.Role {
		case m.MSPRole_ADMIN:
			mspLogger.Debugf("正在检查是否已将身份标识显式命名为 %s 的管理员", msp.name)
			// 在admin的情况下，我们检查
			// id正是我们的节点管理员之一
			if msp.isInAdmins(id.(*identity)) {
				return nil
			}

			// 或者它携带管理员OU，在这种情况下检查身份是否有效。
			mspLogger.Debugf("正在检查身份标识是否带有 %s 的管理员ou 证书", msp.name)
			if err := msp.Validate(id); err != nil {
				return errors.Wrapf(err, "身份标识在本节点 MSP [%s] 下无效", msp.name)
			}

			if err := msp.hasOURole(id, m.MSPRole_ADMIN); err != nil {
				return errors.Wrapf(err, "该身份标识不是节点 MSP [%s] 下的管理员身份", msp.name)
			}

			return nil
		case m.MSPRole_ORDERER:
			mspLogger.Debugf("Checking if identity satisfies role [%s] for %s", m.MSPRole_MSPRoleType_name[int32(mspRole.Role)], msp.name)
			if err := msp.Validate(id); err != nil {
				return errors.Wrapf(err, "The identity is not valid under this MSP [%s]", msp.name)
			}

			if err := msp.hasOURole(id, mspRole.Role); err != nil {
				return errors.Wrapf(err, "The identity is not a [%s] under this MSP [%s]", m.MSPRole_MSPRoleType_name[int32(mspRole.Role)], msp.name)
			}
			return nil
		}
	}

	// 使用v1.3函数检查其他主体类型
	return msp.satisfiesPrincipalInternalV13(id, principal)
}

// isInAdmins 检查是否是管理员身份证书
func (msp *bccspmsp) isInAdmins(id *identity) bool {
	for _, admincert := range msp.admins {
		if bytes.Equal(id.cert.Raw, admincert.(*identity).cert.Raw) {
			// 我们不需要检查admin是否是有效的身份
			// 根据这个MSP，因为我们已经在设置时检查了这个
			// 如果有匹配，我们可以返回
			return true
		}
	}
	return false
}

// getCertificationChain 函数返回此MSP中传入身份的证书链。
//
// 输入参数：
//   - id: Identity 接口，身份信息
//
// 返回值：
//   - []*x509.Certificate: x509.Certificate 对象的切片，表示证书链
//   - error: 如果获取证书链的过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getCertificationChain(id Identity) ([]*x509.Certificate, error) {
	mspLogger.Debugf("MSP %s 获取证书链", msp.name)

	switch id := id.(type) {
	// 如果身份是特定类型的，则可以根据MSP的信任根进行验证, 返回此MSP中传入的bccsp身份的证书链
	case *identity:
		return msp.getCertificationChainForBCCSPIdentity(id)
	default:
		return nil, errors.New("未识别的身份类型")
	}
}

// getCertificationChainForBCCSPIdentity 函数返回此MSP中传入的bccsp身份的证书链。
//
// 输入参数：
//   - id: *identity 对象，bccsp身份信息
//
// 返回值：
//   - []*x509.Certificate: x509.Certificate 对象的切片，表示证书链
//   - error: 如果获取证书链的过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getCertificationChainForBCCSPIdentity(id *identity) ([]*x509.Certificate, error) {
	if id == nil {
		return nil, errors.New("无效的bccsp身份。不能为空。")
	}

	// 我们期望有一个有效的VerifyOptions实例
	if msp.opts == nil {
		return nil, errors.New("无效的msp实例")
	}

	// CA不能直接用作身份..
	if id.cert.IsCA {
		return nil, errors.New("具有基本约束：证书颁发机构为true的X509证书不能用作身份")
	}
	// 返回给定证书的验证链
	return msp.getValidationChain(id.cert, false)
}

// getUniqueValidationChain 方法根据给定的验证选项验证证书，并返回唯一的验证链。
// 参数：
//   - cert *x509.Certificate：要验证的证书。
//   - opts x509.VerifyOptions：验证选项。
//
// 返回值：
//   - []*x509.Certificate：唯一的验证链。
//   - error：如果验证过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) getUniqueValidationChain(cert *x509.Certificate, opts interface{}) ([]*x509.Certificate, error) {
	// 请求 Golang 根据设置时构建的选项来验证证书
	if msp.opts == nil {
		return nil, errors.New("提供的标识没有验证选项")
	}

	// 验证签名
	certificates, err := verify.VerifyByOpts(cert, opts)
	if err != nil {
		return nil, err
	}

	// todo luode 进行 Go 1.14 中的其他验证检查。
	err = verifyLegacyNameConstraints(certificates)
	if err != nil {
		return nil, errors.WithMessage(err, "提供的标识无效")
	}

	return certificates, nil
}

var (
	oidExtensionSubjectAltName  = asn1.ObjectIdentifier{2, 5, 29, 17}
	oidExtensionNameConstraints = asn1.ObjectIdentifier{2, 5, 29, 30}
)

// verifyLegacyNameConstraints 函数执行在 Go 1.14 中作为证书验证过程的一部分的名称约束验证规则。
//
// 如果签名证书包含名称约束，叶子证书不包含 SAN 扩展，并且叶子证书的通用名称看起来像是主机名，
// 验证将失败，并返回一个 x509.CertificateInvalidError 错误，原因是 x509.NameConstraintsWithoutSANs。
func verifyLegacyNameConstraints(chain []*x509.Certificate) error {
	if len(chain) < 2 {
		return nil
	}

	// 叶子证书包含 SAN 扩展是可以的。
	if oidInExtensions(oidExtensionSubjectAltName, chain[0].Extensions) {
		return nil
	}
	// 叶子证书没有主机名在 CN 中也是可以的。
	if !validHostname(chain[0].Subject.CommonName) {
		return nil
	}
	// 如果中间证书或根证书有名称约束，验证将在 Go 1.14 中失败。
	for _, c := range chain[1:] {
		if oidInExtensions(oidExtensionNameConstraints, c.Extensions) {
			return x509.CertificateInvalidError{Cert: chain[0], Reason: x509.NameConstraintsWithoutSANs}
		}
	}
	return nil
}

func oidInExtensions(oid asn1.ObjectIdentifier, exts []pkix.Extension) bool {
	for _, ext := range exts {
		if ext.Id.Equal(oid) {
			return true
		}
	}
	return false
}

// validHostname reports whether host is a valid hostname that can be matched or
// matched against according to RFC 6125 2.2, with some leniency to accommodate
// legacy values.
//
// This implementation is sourced from the standard library.
func validHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")

	if len(host) == 0 {
		return false
	}

	for i, part := range strings.Split(host, ".") {
		if part == "" {
			// Empty label.
			return false
		}
		if i == 0 && part == "*" {
			// Only allow full left-most wildcards, as those are the only ones
			// we match, and matching literal '*' characters is probably never
			// the expected behavior.
			continue
		}
		for j, c := range part {
			if 'a' <= c && c <= 'z' {
				continue
			}
			if '0' <= c && c <= '9' {
				continue
			}
			if 'A' <= c && c <= 'Z' {
				continue
			}
			if c == '-' && j != 0 {
				continue
			}
			if c == '_' || c == ':' {
				// Not valid characters in hostnames, but commonly
				// found in deployments outside the WebPKI.
				continue
			}
			return false
		}
	}

	return true
}

// getValidationChain 函数返回给定证书的验证链。
//
// 输入参数：
//   - cert: *x509.Certificate 对象，要验证的证书
//   - isIntermediateChain: bool 值，指示是否验证中间证书链
//
// 返回值：
//   - []*x509.Certificate: x509.Certificate 对象的切片，表示验证链
//   - error: 如果获取验证链的过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getValidationChain(cert *x509.Certificate, isIntermediateChain bool) ([]*x509.Certificate, error) {
	// getValidityOptsForCert 为证书获取验证选项(根证书、直接证书、dns、证书扩展权限、过期)
	// getUniqueValidationChain 方法根据给定的验证选项验证证书，并返回唯一的验证链
	validationChain, err := msp.getUniqueValidationChain(cert, msp.getValidityOptsForCert(cert))
	if err != nil {
		return nil, errors.WithMessage(err, "获取验证链失败")
	}

	// 我们期望链的长度至少为2
	if len(validationChain) < 2 {
		return nil, errors.Errorf("期望链的长度至少为2，实际长度为 %d", len(validationChain))
	}

	// 检查父证书是否是证书树的叶子节点
	// 如果验证中间证书链，则第一个证书将是父证书
	parentPosition := 1
	if isIntermediateChain {
		parentPosition = 0
	}
	if msp.certificationTreeInternalNodesMap[string(validationChain[parentPosition].Raw)] {
		return nil, errors.Errorf("无效的验证链。父证书应该是证书树的叶子节点 [%v]", cert.Raw)
	}
	return validationChain, nil
}

// getCertificationChainIdentifier 函数返回此MSP中传入身份的证书链标识符。
// 标识符是将证书链中的证书连接起来后计算的SHA256哈希值。
//
// 输入参数：
//   - id: Identity 接口，身份信息
//
// 返回值：
//   - []byte: 表示证书链标识符的字节切片
//   - error: 如果获取证书链标识符的过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getCertificationChainIdentifier(id Identity) ([]byte, error) {
	// 返回此MSP中传入身份的证书链
	chain, err := msp.getCertificationChain(id)
	if err != nil {
		return nil, errors.WithMessagef(err, "获取 [%v] 的证书链失败", id)
	}

	// chain[0] 是表示身份的证书。
	// 将其丢弃
	return msp.getCertificationChainIdentifierFromChain(chain[1:])
}

// getCertificationChainIdentifierFromChain 函数从证书链中获取认证链标识符(hash值)。
// 输入参数：
//   - chain: []*x509.Certificate 对象，证书链
//
// 返回值：
//   - []byte: 认证链标识符的字节切片
//   - error: 如果获取过程中出现错误，则返回相应的错误信息
func (msp *bccspmsp) getCertificationChainIdentifierFromChain(chain []*x509.Certificate) ([]byte, error) {
	// 对证书链进行哈希计算
	// 使用身份证书的哈希值作为 IdentityIdentifier 中的标识符
	hashOpt, err := bccsp.GetHashOpt(msp.cryptoConfig.IdentityIdentifierHashFunction)
	if err != nil {
		return nil, errors.WithMessage(err, "获取哈希函数选项失败")
	}

	hf, err := msp.bccsp.GetHash(hashOpt)
	if err != nil {
		return nil, errors.WithMessage(err, "在计算认证链标识符时获取哈希函数失败")
	}
	for i := 0; i < len(chain); i++ {
		hf.Write(chain[i].Raw)
	}

	// 计算哈希值
	return hf.Sum(nil), nil
}

// sanitizeCert 函数确保使用 ECDSA 签名的 x509 证书具有 Low-S 签名。
// 如果不是这种情况，则重新生成证书以具有 Low-S 签名。
// 参数：
//   - cert *x509.Certificate：要清理的证书。
//
// 返回值：
//   - *x509.Certificate：清理后的证书。
//   - error：如果清理过程中出现错误，则返回相应的错误信息。
func (msp *bccspmsp) sanitizeCert(cert *x509.Certificate) (*x509.Certificate, error) {
	if isECDSASignedCert(cert) {
		// 查找父证书以执行清理
		var parentCert *x509.Certificate
		// 根据给定的验证选项验证证书，并返回唯一的验证链
		chain, err := msp.getUniqueValidationChain(cert, msp.getValidityOptsForCert(cert))
		if err != nil {
			return nil, err
		}

		// 在这一点上，cert 可能是根 CA 证书或中间 CA 证书
		if cert.IsCA && len(chain) == 1 {
			// cert 是根 CA 证书
			parentCert = cert
		} else {
			// 去出一个中间证书
			parentCert = chain[1]
		}

		// 根据父证书的公钥验证证书, 清理签名为低 S 值
		cert, err = sanitizeECDSASignedCert(cert, parentCert)
		if err != nil {
			return nil, err
		}
	}
	return cert, nil
}

// IsWellFormed 检查给定的标识是否可以反序列化为其提供程序特定的形式。
// 在这个MSP实现中，格式良好意味着PEM有一个类型，要么是字符串 “证书”，要么是完全缺少的类型。
func (msp *bccspmsp) IsWellFormed(identity *m.SerializedIdentity) error {
	// 解码为pem
	bl, rest := pem.Decode(identity.IdBytes)
	if bl == nil {
		return errors.New("PEM解码 identity 身份错误")
	}
	if len(rest) > 0 {
		return errors.Errorf("身份 %s 用于 MSP %s 具有尾随字节", string(identity.IdBytes), identity.Mspid)
	}

	// 重要: 此方法看起来非常类似于getCertFromPem(idBytes [] 字节) (* x509.Certificate，错误)
	// 但是我们:
	// 1) 必须确保PEM块为类型证书或为空
	// 2) 不能用此方法替换getCertFromPem，否则我们将引入验证逻辑的变化将导致链叉。
	if bl.Type != "CERTIFICATE" && bl.Type != "" {
		return errors.Errorf("pem 类型 is %s, 应该是 'CERTIFICATE' 或 ''", bl.Type)
	}
	cert, err := common.ParseCertificate(bl.Bytes)
	if err != nil {
		return err
	}

	// ECDSA是否签署了证书, 国密使用PureEd25519算法, 不需要检查
	if !isECDSASignedCert(cert) {
		return nil
	}

	// 身份是否以规范形式签名
	return isIdentitySignedInCanonicalForm(cert.Signature, identity.Mspid, identity.IdBytes)
}

// 身份是否以规范形式签名
func isIdentitySignedInCanonicalForm(sig []byte, mspID string, pemEncodedIdentity []byte) error {
	r, s, err := utils.UnmarshalECDSASignature(sig)
	if err != nil {
		return err
	}

	expectedSig, err := utils.MarshalECDSASignature(r, s)
	if err != nil {
		return err
	}

	if !bytes.Equal(expectedSig, sig) {
		return errors.Errorf("identity %s 用于 MSP %s 具有非规范签名",
			string(pemEncodedIdentity), mspID)
	}

	return nil
}
