/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/msp"
)

// Org 存储常见的组织配置
type Org interface {
	// Name 返回此组织在config中引用的名称
	Name() string

	// MSPID 返回与此组织关联的MSP ID
	MSPID() string

	// MSP 返回此组织的MSP实现。
	MSP() msp.MSP
}

// ApplicationOrg stores the per org application config
type ApplicationOrg interface {
	Org

	// AnchorPeers returns the list of gossip anchor peers
	AnchorPeers() []*pb.AnchorPeer
}

// OrdererOrg 是一个接口，用于存储特定于组织的排序服务配置信息。
type OrdererOrg interface {
	// Org 接口可能包含通用的组织配置信息，这里 OrdererOrg 实现了该接口，意味着它继承了所有 Org 接口的方法和属性。
	Org

	// Endpoints 方法用于返回排序节点的端点列表，这些端点是客户端与排序服务通信的网络地址。
	// 返回的每个字符串都代表了一个排序节点的网络位置，例如可以是 "hostname:port" 形式的地址。
	Endpoints() []string
}

// Application stores the common shared application config
type Application interface {
	// Organizations returns a map of org ID to ApplicationOrg
	Organizations() map[string]ApplicationOrg

	// APIPolicyMapper returns a PolicyMapper that maps API names to policies
	APIPolicyMapper() PolicyMapper

	// Capabilities defines the capabilities for the application portion of a channel
	Capabilities() ApplicationCapabilities
}

// Channel gives read only access to the channel configuration
type Channel interface {
	// HashingAlgorithm returns the default algorithm to be used when hashing
	// such as computing block hashes, and CreationPolicy digests
	HashingAlgorithm() func(input []byte) []byte

	// BlockDataHashingStructureWidth returns the width to use when constructing the
	// Merkle tree to compute the BlockData hash
	BlockDataHashingStructureWidth() uint32

	// OrdererAddresses returns the list of valid orderer addresses to connect to to invoke Broadcast/Deliver
	OrdererAddresses() []string

	// Capabilities defines the capabilities for a channel
	Capabilities() ChannelCapabilities
}

// Consortiums represents the set of consortiums serviced by an ordering service
type Consortiums interface {
	// Consortiums returns the set of consortiums
	Consortiums() map[string]Consortium
}

// Consortium 表示一组可以一起创建频道的组织
type Consortium interface {
	// ChannelCreationPolicy 返回实例化此联盟的通道时要检查的策略
	ChannelCreationPolicy() *cb.Policy

	// Organizations 返回此联盟的组织
	Organizations() map[string]Org
}

// Orderer 存储了通用的、共享的排序服务配置信息。
type Orderer interface {
	// ConsensusType 返回配置的共识类型。
	ConsensusType() string

	// ConsensusMetadata 返回与共识类型关联的元数据。
	ConsensusMetadata() []byte

	// ConsensusState 返回共识类型的当前状态。
	ConsensusState() ab.ConsensusType_State

	// BatchSize 返回一个区块中应包含的最大消息数量。
	BatchSize() *ab.BatchSize

	// BatchTimeout 返回创建新区块前等待的最长时间。
	BatchTimeout() time.Duration

	// MaxChannelsCount 返回允许在网络中建立的通道最大数量。
	MaxChannelsCount() uint64

	// KafkaBrokers 返回一组“引导”Kafka代理的地址（采用IP:端口格式）。
	// 注意，这不一定包含所有用于排序服务的Kafka代理。
	KafkaBrokers() []string

	// Organizations 返回排序服务所涉及的组织信息。
	Organizations() map[string]OrdererOrg

	// Capabilities 定义了通道中与排序服务相关的功能集。
	Capabilities() OrdererCapabilities
}

// ChannelCapabilities defines the capabilities for a channel
type ChannelCapabilities interface {
	// Supported returns an error if there are unknown capabilities in this channel which are required
	Supported() error

	// MSPVersion specifies the version of the MSP this channel must understand, including the MSP types
	// and MSP principal types.
	MSPVersion() msp.MSPVersion

	// ConsensusTypeMigration return true if consensus-type migration is permitted in both orderer and peer.
	ConsensusTypeMigration() bool

	// OrgSpecificOrdererEndpoints return true if the channel config processing allows orderer orgs to specify their own endpoints
	OrgSpecificOrdererEndpoints() bool
}

// ApplicationCapabilities defines the capabilities for the application portion of a channel
type ApplicationCapabilities interface {
	// Supported returns an error if there are unknown capabilities in this channel which are required
	Supported() error

	// ForbidDuplicateTXIdInBlock specifies whether two transactions with the same TXId are permitted
	// in the same block or whether we mark the second one as TxValidationCode_DUPLICATE_TXID
	ForbidDuplicateTXIdInBlock() bool

	// ACLs returns true is ACLs may be specified in the Application portion of the config tree
	ACLs() bool

	// PrivateChannelData returns true if support for private channel data (a.k.a. collections) is enabled.
	// In v1.1, the private channel data is experimental and has to be enabled explicitly.
	// In v1.2, the private channel data is enabled by default.
	PrivateChannelData() bool

	// CollectionUpgrade returns true if this channel is configured to allow updates to
	// existing collection or add new collections through chaincode upgrade (as introduced in v1.2)
	CollectionUpgrade() bool

	// V1_1Validation returns true is this channel is configured to perform stricter validation
	// of transactions (as introduced in v1.1).
	V1_1Validation() bool

	// V1_2Validation returns true is this channel is configured to perform stricter validation
	// of transactions (as introduced in v1.2).
	V1_2Validation() bool

	// V1_3Validation returns true if this channel supports transaction validation
	// as introduced in v1.3. This includes:
	//  - policies expressible at a ledger key granularity, as described in FAB-8812
	//  - new chaincode lifecycle, as described in FAB-11237
	V1_3Validation() bool

	// StorePvtDataOfInvalidTx returns true if the peer needs to store the pvtData of
	// invalid transactions (as introduced in v142).
	StorePvtDataOfInvalidTx() bool

	// V2_0Validation returns true if this channel supports transaction validation
	// as introduced in v2.0. This includes:
	//  - new chaincode lifecycle
	//  - implicit per-org collections
	V2_0Validation() bool

	// LifecycleV20 indicates whether the peer should use the deprecated and problematic
	// v1.x lifecycle, or whether it should use the newer per channel approve/commit definitions
	// process introduced in v2.0.  Note, this should only be used on the endorsing side
	// of peer processing, so that we may safely remove all checks against it in v2.1.
	LifecycleV20() bool

	// MetadataLifecycle always returns false
	MetadataLifecycle() bool

	// KeyLevelEndorsement returns true if this channel supports endorsement
	// policies expressible at a ledger key granularity, as described in FAB-8812
	KeyLevelEndorsement() bool
}

// OrdererCapabilities 定义了通道中订单部分的功能特性接口
type OrdererCapabilities interface {
	// PredictableChannelTemplate 指定是否修复了v1.0中不期望的行为，该行为是将/Channel组的mod_policy设置为""
	// 并从订单系统通道配置复制版本。如果应修复此行为，则返回true。
	PredictableChannelTemplate() bool

	// Resubmission 指定是否通过重新提交经过重新验证的交易来修复v1.0中非确定性的交易提交问题。
	// 如果应启用此修复以确保交易提交的确定性，则返回true。
	Resubmission() bool

	// Supported 检查此通道中是否存在未知的功能特性，并在这些未知特性为必需时返回错误。
	Supported() error

	// ExpirationCheck 指定订单者在验证消息时是否执行标识过期检查。
	ExpirationCheck() bool

	// ConsensusTypeMigration 检查订单者是否允许进行共识类型迁移。
	ConsensusTypeMigration() bool

	// UseChannelCreationPolicyAsAdmins 检查订单者是否应使用更复杂的通道创建逻辑，
	// 即当创建交易看似支持时，使用通道创建策略作为Admins策略。
	UseChannelCreationPolicyAsAdmins() bool
}

// PolicyMapper is an interface for
type PolicyMapper interface {
	// PolicyRefForAPI takes the name of an API, and returns the policy name
	// or the empty string if the API is not found
	PolicyRefForAPI(apiName string) string
}

// Resources 是所有通道的通用配置资源集合。
// 根据链是在订购方还是对等方使用，可能还有其他配置资源可用。
type Resources interface {
	// ConfigtxValidator 返回通道的 configtx.Validator 配置验证器
	ConfigtxValidator() configtx.Validator

	// PolicyManager 返回通道的 policies.Manager 策略管理器
	PolicyManager() policies.Manager

	// ChannelConfig 返回链的 config.Channel 配置
	ChannelConfig() Channel

	// OrdererConfig 返回通道的 config.Orderer 配置以及是否存在 Orderer 配置
	OrdererConfig() (Orderer, bool)

	// ConsortiumsConfig 返回通道的 config.Consortiums 配置以及是否存在 consortiums 的配置
	ConsortiumsConfig() (Consortiums, bool)

	// ApplicationConfig 返回通道的 configtxapplication.SharedConfig 配置以及是否存在 Application 配置
	ApplicationConfig() (Application, bool)

	// MSPManager 返回链的 msp.MSPManager 配置
	MSPManager() msp.MSPManager

	// ValidateNew 如果新的配置资源集与当前配置不兼容，则返回错误
	ValidateNew(resources Resources) error
}
