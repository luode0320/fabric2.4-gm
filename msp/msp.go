/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/msp"
)

// IdentityDeserializer 被 MSPManger 和 MSP 实现。
type IdentityDeserializer interface {
	// DeserializeIdentity 反序列化一个身份。
	// 如果身份与执行反序列化的 MSP 不同的 MSP 相关联，则反序列化将失败。
	DeserializeIdentity(serializedIdentity []byte) (Identity, error)

	// IsWellFormed 检查给定的身份是否可以反序列化为其特定于提供程序的形式。
	IsWellFormed(identity *msp.SerializedIdentity) error
}

// Membership service provider APIs for Hyperledger Fabric:
//
// By "membership service provider" we refer to an abstract component of the
// system that would provide (anonymous) credentials to clients, and peers for
// them to participate in Hyperledger/fabric network. Clients use these
// credentials to authenticate their transactions, and peers use these credentials
// to authenticate transaction processing results (endorsements). While
// strongly connected to the transaction processing components of the systems,
// this interface aims to have membership services components defined, in such
// a way such that alternate implementations of this can be smoothly plugged in
// without modifying the core of transaction processing components of the system.
//
// This file includes Membership service provider interface that covers the
// needs of a peer membership service provider interface.

// MSPManager is an interface defining a manager of one or more MSPs. This
// essentially acts as a mediator to MSP calls and routes MSP related calls
// to the appropriate MSP.
// This object is immutable, it is initialized once and never changed.
type MSPManager interface {

	// IdentityDeserializer interface needs to be implemented by MSPManager
	IdentityDeserializer

	// Setup the MSP manager instance according to configuration information
	Setup(msps []MSP) error

	// GetMSPs Provides a list of Membership Service providers
	GetMSPs() (map[string]MSP, error)
}

// MSP 是要实现以容纳对等功能的最小成员资格服务提供程序接口
// 主要的实现 msp.bccspmsp, 包含证书、中间证书、tls证书、tls中间证书、管理员证书、signer签名实例、CRL吊销证书、ous配置等
type MSP interface {

	// IdentityDeserializer 接口需要由MSP实现
	IdentityDeserializer

	// Setup 根据配置信息的MSP实例, 执行实现方法, 主要验证证书是否有效
	Setup(config *msp.MSPConfig) error

	// GetVersion 返回此MSP的版本
	GetVersion() MSPVersion

	// GetType 返回提供程序类型, 默认bccsp
	GetType() ProviderType

	// GetIdentifier 返回提供者标识符
	GetIdentifier() (string, error)

	// GetDefaultSigningIdentity 返回默认签名标识
	GetDefaultSigningIdentity() (SigningIdentity, error)

	// GetTLSRootCerts 返回此MSP的TLS根证书
	GetTLSRootCerts() [][]byte

	// GetTLSIntermediateCerts 返回此MSP的TLS中间根证书
	GetTLSIntermediateCerts() [][]byte

	// Validate 检查提供的标识是否有效
	Validate(id Identity) error

	// SatisfiesPrincipal 检查标识是否与MSPPrincipal中提供的描述匹配。
	// 检查可能涉及逐字节比较 (如果主体是序列化标识)，或者可能需要MSP验证
	SatisfiesPrincipal(id Identity, principal *msp.MSPPrincipal) error
}

// OUIdentifier 表示组织单位及其相关的信任链标识符。
type OUIdentifier struct {
	// CertifiersIdentifier 是与此组织单位相关的证书信任链的哈希(不是证书, 是证书的hash值)
	CertifiersIdentifier []byte
	// OrganizationUnitIdentifier 定义用MSPIdentifier标识的MSP下的组织单位
	OrganizationalUnitIdentifier string
}

// 从这一点开始，
// 有在成员资格服务提供者的对等和客户端API内共享的接口。

// Identity 定义与 “证书” 关联的操作的接口。
// 也就是说，身份的公共部分可以被认为是证书，
// 并仅提供签名验证功能。
// 这将在对等方验证用于签署交易的证书时使用，
// 并验证与这些证书对应的签名。
// 主要实现 msp.identity,包含证书、证书公钥、msp接口实例
type Identity interface {

	// ExpiresAt 返回标识过期的时间。
	// 如果返回的时间是零值，则表示标识未过期，或其过期时间未知
	ExpiresAt() time.Time

	// GetIdentifier 返回该标识的标识符(mspid, id)
	GetIdentifier() *IdentityIdentifier

	// GetMSPIdentifier 返回此实例的MSP Id
	GetMSPIdentifier() string

	// Validate 使用控制此标识的规则对其进行验证。
	// 例如，如果它是作为标识实现的结构TCert，
	// 验证将根据假定的根证书颁发机构检查TCert签名。
	Validate() error

	// GetOrganizationalUnits 返回零个或多个组织单位或部门，只要这是公共信息，该身份就与之相关。
	// 某些MSP实现可能使用与该身份公开关联的属性，
	// 或已在此证书上提供签名的根证书颁发机构的标识符。
	// 示例:
	//		-如果标识是x.509证书，
	//		 此函数返回一个或多个字符串，该字符串以OU类型的主题可分辨名称编码
	// TODO: 对于基于X.509的标识，
	// 		 检查我们是否需要OU的专用类型，其中证书OU由签名者的身份正确命名
	GetOrganizationalUnits() []*OUIdentifier

	// Anonymous 如果这是匿名标识，则返回true，否则返回false
	Anonymous() bool

	// Verify 使用此标识作为参考的某些消息上的签名
	Verify(msg []byte, sig []byte) error

	// Serialize 将标识转换为字节
	Serialize() ([]byte, error)

	// SatisfiesPrincipal 检查此实例是否与MSPPrincipal中提供的描述匹配。
	// 检查可能涉及逐字节比较 (如果主体是序列化身份)，或者可能需要MSP验证
	SatisfiesPrincipal(principal *msp.MSPPrincipal) error
}

// SigningIdentity 是 Identity 的扩展，用于涵盖签名功能。
// 例如，在客户端希望签署交易或 Fabric 背书者希望签署提案处理结果的情况下，应请求签名身份。
type SigningIdentity interface {

	// 继承自 Identity
	Identity

	// Sign 对消息进行签名
	Sign(msg []byte) ([]byte, error)

	// GetPublicVersion 返回此身份
	GetPublicVersion() Identity
}

// IdentityIdentifier 是特定的标识符的持有者身份，自然命名空间，由其提供者标识符。
type IdentityIdentifier struct {

	// Mspid 关联的成员资格服务提供程序的标识符
	Mspid string

	// Id 提供程序中标识的标识符
	Id string
}

// ProviderType 指示身份提供商的类型
type ProviderType int

// ProviderType 表示成员相对于成员 API 的 ProviderType。
const (
	FABRIC ProviderType = iota // MSP 是 FABRIC 类型
	IDEMIX                     // MSP 是 IDEMIX 类型
	OTHER                      // MSP 是其他类型

	// 注意：如果向此集合添加新类型，
	// 则必须扩展下面的 mspTypes 映射
)

var mspTypeStrings = map[ProviderType]string{
	FABRIC: "bccsp",
	IDEMIX: "idemix",
}

// Options 目前BCCSP是第3个版本MSPv1_4_3, Idemix遗留使用第一个版本MSPv1_1
var Options = map[string]NewOpts{
	ProviderTypeToString(FABRIC): &BCCSPNewOpts{NewBaseOpts: NewBaseOpts{Version: MSPv1_4_3}},
	ProviderTypeToString(IDEMIX): &IdemixNewOpts{NewBaseOpts: NewBaseOpts{Version: MSPv1_1}},
}

// ProviderTypeToString 函数用于将 ProviderType 整数转换为表示其字符串。
// 参数：
//   - id ProviderType：要转换的 ProviderType 整数。
//
// 返回值：
//   - string：表示 ProviderType 的字符串。
func ProviderTypeToString(id ProviderType) string {
	// 检查 mspTypeStrings 中是否存在对应的字符串表示
	if res, found := mspTypeStrings[id]; found {
		return res
	}

	return ""
}
