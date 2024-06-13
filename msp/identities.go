/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

var mspIdentityLogger = flogging.MustGetLogger("msp.identity")

type identity struct {
	// id 包含此实例的标识符（MSPID 和身份标识符）
	id *IdentityIdentifier

	// cert 包含签署此实例的公钥的 x.509 证书
	cert *x509.Certificate

	// pk 是此实例的公钥
	pk bccsp.Key

	// msp 是拥有此身份的 MSP 的引用
	msp *bccspmsp

	// validationMutex 用于同步 validated 和 validationErr 的内存操作
	validationMutex sync.Mutex

	// validated 在对此实例调用 validateIdentity 函数后为 true
	validated bool

	// validationErr 包含此实例的验证错误。如果 validated 为 true，则可以读取它
	validationErr error
}

// newIdentity 函数用于创建身份实例。
// 参数：
//   - cert *x509.Certificate：证书。
//   - pk bccsp.Key：公钥。
//   - msp *bccspmsp：MSP 实例。
//
// 返回值：
//   - Identity：身份实例。
//   - error：如果创建过程中出现错误，则返回相应的错误信息。
func newIdentity(cert *x509.Certificate, pk bccsp.Key, msp *bccspmsp) (Identity, error) {
	if mspIdentityLogger.IsEnabledFor(zapcore.DebugLevel) {
		// x509 转 pem
		mspIdentityLogger.Debugf("为证书 %s 创建身份实例", CertToPEM(cert))
	}

	// 先对证书进行清理
	cert, err := msp.sanitizeCert(cert)
	if err != nil {
		return nil, err
	}

	// 计算身份标识符
	// 获取配置的hash函数
	hashOpt, err := bccsp.GetHashOpt(msp.cryptoConfig.IdentityIdentifierHashFunction)
	if err != nil {
		return nil, errors.WithMessage(err, "获取哈希函数失败")
	}

	// 计算证书内容的hash
	digest, err := msp.bccsp.Hash(cert.Raw, hashOpt)
	if err != nil {
		return nil, errors.WithMessage(err, "将原始证书哈希计算为 IdentityIdentifier 的 id 失败")
	}

	// 使用身份证书的哈希值作为 IdentityIdentifier 中的 id
	id := &IdentityIdentifier{
		Mspid: msp.name,
		Id:    hex.EncodeToString(digest),
	}

	return &identity{id: id, cert: cert, pk: pk, msp: msp}, nil
}

// ExpiresAt returns the time at which the Identity expires.
func (id *identity) ExpiresAt() time.Time {
	return id.cert.NotAfter
}

// SatisfiesPrincipal returns nil if this instance matches the supplied principal or an error otherwise
func (id *identity) SatisfiesPrincipal(principal *msp.MSPPrincipal) error {
	return id.msp.SatisfiesPrincipal(id, principal)
}

// GetIdentifier returns the identifier (MSPID/IDID) for this instance
func (id *identity) GetIdentifier() *IdentityIdentifier {
	return id.id
}

// GetMSPIdentifier returns the MSP identifier for this instance
func (id *identity) GetMSPIdentifier() string {
	return id.id.Mspid
}

// Validate returns nil if this instance is a valid identity or an error otherwise
func (id *identity) Validate() error {
	return id.msp.Validate(id)
}

type OUIDs []*OUIdentifier

func (o OUIDs) String() string {
	var res []string
	for _, id := range o {
		res = append(res, fmt.Sprintf("%s(%X)", id.OrganizationalUnitIdentifier, id.CertifiersIdentifier[0:8]))
	}

	return fmt.Sprintf("%s", res)
}

// GetOrganizationalUnits 函数返回此实例的组织单位（OU）。
//
// 返回值：
//   - []*OUIdentifier: OUIdentifier 对象的切片，表示组织单位
func (id *identity) GetOrganizationalUnits() []*OUIdentifier {
	if id.cert == nil {
		return nil
	}
	// 函数返回此MSP中传入身份的证书链标识符。 标识符是将证书链中的证书连接起来后计算的SHA256哈希值。
	cid, err := id.msp.getCertificationChainIdentifier(id)
	if err != nil {
		mspIdentityLogger.Errorf("获取 [%v] 的证书链标识符失败：[%+v]", id, err)
		return nil
	}

	var res []*OUIdentifier
	for _, unit := range id.cert.Subject.OrganizationalUnit {
		res = append(res, &OUIdentifier{
			OrganizationalUnitIdentifier: unit,
			CertifiersIdentifier:         cid,
		})
	}

	return res
}

// Anonymous returns true if this identity provides anonymity
func (id *identity) Anonymous() bool {
	return false
}

// NewSerializedIdentity returns a serialized identity
// having as content the passed mspID and x509 certificate in PEM format.
// This method does not check the validity of certificate nor
// any consistency of the mspID with it.
func NewSerializedIdentity(mspID string, certPEM []byte) ([]byte, error) {
	// We serialize identities by prepending the MSPID
	// and appending the x509 cert in PEM format
	sId := &msp.SerializedIdentity{Mspid: mspID, IdBytes: certPEM}
	raw, err := proto.Marshal(sId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed serializing identity [%s][%X]", mspID, certPEM)
	}
	return raw, nil
}

// Verify 检查签名sig和消息msg
// 确定此标识是否产生了签名; 如果是，则返回nil，否则返回错误
func (id *identity) Verify(msg []byte, sig []byte) error {
	// mspIdentityLogger.Infof("Verifying signature")

	// 获取hash器
	hashOpt, err := id.getHashOpt(id.msp.cryptoConfig.SignatureHashFamily)
	if err != nil {
		return errors.WithMessage(err, "获取哈希算法选项失败")
	}

	digest, err := id.msp.bccsp.Hash(msg, hashOpt)
	if err != nil {
		return errors.WithMessage(err, "计算摘要hash失败")
	}

	if mspIdentityLogger.IsEnabledFor(zapcore.DebugLevel) {
		mspIdentityLogger.Debugf("验证: 签名者身份 (certificate subject=%s issuer=%s serialnumber=%d)", id.cert.Subject, id.cert.Issuer, id.cert.SerialNumber)
		// mspIdentityLogger.Debugf("Verify: digest = %s", hex.Dump(digest))
		// mspIdentityLogger.Debugf("Verify: sig = %s", hex.Dump(sig))
	}

	valid, err := id.msp.bccsp.Verify(id.pk, sig, digest, nil)
	if err != nil {
		return errors.WithMessage(err, "无法确定签名的有效性")
	} else if !valid {
		mspIdentityLogger.Warnf("签名对无效 (certificate subject=%s issuer=%s serialnumber=%d)", id.cert.Subject, id.cert.Issuer, id.cert.SerialNumber)
		return errors.New("签名无效")
	}

	return nil
}

// Serialize 返回此身份的字节数组表示形式。
func (id *identity) Serialize() ([]byte, error) {
	// 将证书转换为 PEM 格式
	pb := &pem.Block{Bytes: id.cert.Raw, Type: "CERTIFICATE"}
	pemBytes := pem.EncodeToMemory(pb)
	if pemBytes == nil {
		return nil, errors.New("x509 证书解析为 pem 格式失败")
	}

	// 通过在证书前添加 MSPID 并在末尾附加证书的 ASN.1 DER 内容来序列化身份
	sId := &msp.SerializedIdentity{Mspid: id.id.Mspid, IdBytes: pemBytes}
	idBytes, err := proto.Marshal(sId)
	if err != nil {
		return nil, errors.Wrapf(err, "无法编码 msp.SerializedIdentity 结构 %s", id.id)
	}

	return idBytes, nil
}

func (id *identity) getHashOpt(hashFamily string) (bccsp.HashOpts, error) {
	hashOpt, err := bccsp.GetHashOpt(hashFamily)
	if err != nil {
		return nil, errors.Errorf("无法识别哈希函数 [%s],只支持SHA2、SHA3", hashFamily)
	}
	return hashOpt, nil
}

// signingidentity 结构体表示一个签名身份，它嵌入了基本身份的所有属性。
type signingidentity struct {
	// identity 表示基本身份的对象(证书、私钥)
	identity

	// signer 对应可以使用此身份生成签名的对象(公钥/实现 crypto.Signer 接口的实例)
	signer crypto.Signer
}

// newSigningIdentity 函数创建一个新的签名身份实例。
// 输入参数：
//   - cert: x509.Certificate 对象，身份的证书
//   - pk: bccsp.Key 对象，身份的私钥
//   - signer: crypto.Signer 接口，用于签名操作的签名者
//   - msp: bccspmsp 对象，当前MSP的实例
//
// 返回值：
//   - SigningIdentity: 创建的签名身份实例
//   - error: 如果创建签名身份实例过程中出现错误，则返回相应的错误信息
func newSigningIdentity(cert *x509.Certificate, pk bccsp.Key, signer crypto.Signer, msp *bccspmsp) (SigningIdentity, error) {
	// mspIdentityLogger.Infof("为 ID %s 创建签名身份实例", id)
	mspId, err := newIdentity(cert, pk, msp)
	if err != nil {
		return nil, err
	}

	return &signingidentity{
		identity: identity{
			id:   mspId.(*identity).id,
			cert: mspId.(*identity).cert,
			msp:  mspId.(*identity).msp,
			pk:   mspId.(*identity).pk,
		},
		signer: signer,
	}, nil
}

// Sign 函数使用此实例对消息进行签名。
// 输入参数：
//   - msg: 要签名的消息的字节切片
//
// 返回值：
//   - []byte: 签名结果的字节切片
//   - error: 如果签名过程中出现错误，则返回相应的错误信息
func (id *signingidentity) Sign(msg []byte) ([]byte, error) {
	// mspIdentityLogger.Infof("对消息进行签名")

	// 计算哈希
	hashOpt, err := id.getHashOpt(id.msp.cryptoConfig.SignatureHashFamily)
	if err != nil {
		return nil, errors.WithMessage(err, "获取哈希函数选项失败")
	}

	// 对数据进行hash
	digest, err := id.msp.bccsp.Hash(msg, hashOpt)
	if err != nil {
		return nil, errors.WithMessage(err, "计算hash失败")
	}

	if len(msg) < 32 {
		mspIdentityLogger.Debugf("Sign: 明文: %X \n", msg)
	} else {
		mspIdentityLogger.Debugf("Sign: 明文: %X...%X \n", msg[0:16], msg[len(msg)-16:])
	}
	mspIdentityLogger.Debugf("Sign: 摘要: %X \n", digest)

	return id.signer.Sign(rand.Reader, digest, nil)
}

// GetPublicVersion returns the public version of this identity,
// namely, the one that is only able to verify messages and not sign them
func (id *signingidentity) GetPublicVersion() Identity {
	return &id.identity
}
