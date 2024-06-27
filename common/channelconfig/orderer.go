/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/capabilities"
	"github.com/pkg/errors"
)

const (
	// OrdererGroupKey 是orderer配置的组名称。
	OrdererGroupKey = "Orderer"
)

const (
	// ConsensusTypeKey is the cb.ConfigItem type key name for the ConsensusType message.
	ConsensusTypeKey = "ConsensusType"

	// BatchSizeKey BatchSize消息的cb.ConfigItem类型键名称。
	BatchSizeKey = "BatchSize"

	// BatchTimeoutKey BatchTimeout消息的cb.ConfigItem类型键名称。
	BatchTimeoutKey = "BatchTimeout"

	// ChannelRestrictionsKey 是channelstrictions消息的键名称。
	ChannelRestrictionsKey = "ChannelRestrictions"

	// KafkaBrokersKey is the cb.ConfigItem type key name for the KafkaBrokers message.
	KafkaBrokersKey = "KafkaBrokers"

	// EndpointsKey Orderererorggroup中Endpoints消息的cb.COnfigValue键名称。
	EndpointsKey = "Endpoints"
)

// OrdererProtos 结构体用作排序服务配置 `OrdererConfig` 的源数据，它包含了排序服务的核心配置信息。
type OrdererProtos struct {
	// ConsensusType 字段表示共识算法的类型，定义了排序服务使用的共识机制。
	// 这个字段决定了网络中区块的生成和验证方式，常见的共识类型包括 Kafka、Raft、Solo 等。
	ConsensusType *ab.ConsensusType

	// BatchSize 字段定义了批处理大小，即每次打包交易形成新区块时包含的交易数量上限。
	// 这个字段对性能和资源使用有直接影响，较大的批处理大小可以提高吞吐量，但可能增加内存使用和区块传播延迟。
	BatchSize *ab.BatchSize

	// BatchTimeout 字段表示批处理超时时间，定义了排序服务在没有收到新的交易请求时等待的最长时间。
	// 如果在这个时间内没有新的交易请求，排序服务会强制打包当前收集的交易形成新区块。
	BatchTimeout *ab.BatchTimeout

	// KafkaBrokers 字段仅在使用 Kafka 作为共识机制时有效，它包含了 Kafka Broker 的地址列表。
	// 这个字段对于 Kafka 作为共识机制的排序服务来说是必需的，用于连接到 Kafka 集群进行消息传递。
	KafkaBrokers *ab.KafkaBrokers

	// ChannelRestrictions 字段表示通道限制，定义了排序服务对通道的一些限制性配置。
	// 这些限制可能包括通道的最大数量、最大区块大小等，用于控制网络资源的合理分配和使用。
	ChannelRestrictions *ab.ChannelRestrictions

	// Capabilities 字段表示排序服务支持的功能集，定义了排序服务具备的高级功能。
	// 这些功能可能包括事务原子性、隐私保护、智能合约支持等，用于区分不同版本和功能级别的排序服务。
	Capabilities *cb.Capabilities
}

// OrdererConfig 结构体用于保存排序服务的配置信息。
type OrdererConfig struct {
	// protos 字段保存了排序服务的原生配置信息，通常包含了更详细的配置参数。
	protos *OrdererProtos

	// orgs 字段是一个映射，用于保存与排序服务相关的组织信息，键为组织名称，值为OrdererOrg结构体。
	orgs map[string]OrdererOrg

	// batchTimeout 字段表示批处理超时时间，即排序服务在收集交易请求形成新区块时的等待时间。
	batchTimeout time.Duration
}

// OrdererOrgProtos are deserialized from the Orderer org config values
type OrdererOrgProtos struct {
	Endpoints *cb.OrdererAddresses
}

// OrdererOrgConfig 定义了排序节点组织的配置
type OrdererOrgConfig struct {
	*OrganizationConfig                   // 组织配置
	protos              *OrdererOrgProtos // 排序节点组织的协议定义
	name                string            // 组织名称
}

// Endpoints returns the set of addresses this ordering org exposes as orderers
func (oc *OrdererOrgConfig) Endpoints() []string {
	return oc.protos.Endpoints.Addresses
}

// NewOrdererOrgConfig 从给定的 ConfigGroup 构建一个 Orderer 组织配置。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - orgName：string，表示组织的名称
//   - orgGroup：*cb.ConfigGroup，表示组织配置组
//   - mspConfigHandler：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//   - channelCapabilities：ChannelCapabilities，表示通道的功能
//
// 返回值：
//   - *OrdererOrgConfig：表示 OrdererOrgConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewOrdererOrgConfig(orgName string, orgGroup *cb.ConfigGroup, mspConfigHandler *MSPConfigHandler, channelCapabilities ChannelCapabilities) (*OrdererOrgConfig, error) {
	// 检查 OrdererOrg 配置是否包含子组
	if len(orgGroup.Groups) > 0 {
		return nil, fmt.Errorf("OrdererOrg 配置不包含子组织")
	}

	// 检查通道功能是否允许组织特定的 Orderer 端点
	if !channelCapabilities.OrgSpecificOrdererEndpoints() {
		if _, ok := orgGroup.Values[EndpointsKey]; ok {
			return nil, errors.Errorf("Orderer 组织 %s 在启用V1_4_2功能之前，不包含端点值", orgName)
		}
	}

	// 创建 OrdererOrgProtos 和 OrganizationProtos 实例，并从组中反序列化值
	protos := &OrdererOrgProtos{}
	orgProtos := &OrganizationProtos{}

	// 从配置组中反序列化所有值的值。
	if err := DeserializeProtoValuesFromGroup(orgGroup, protos, orgProtos); err != nil {
		return nil, errors.Wrap(err, "orgGroup无法反序列化值")
	}

	// 创建 OrdererOrgConfig 实例，并初始化名称、protos 和 mspConfigHandler
	ooc := &OrdererOrgConfig{
		name:   orgName, // 组织名称
		protos: protos,  // 排序节点组织的协议定义
		OrganizationConfig: &OrganizationConfig{ // 组织配置
			name:             orgName,          // 组织名称
			protos:           orgProtos,        // 组织的协议定义
			mspConfigHandler: mspConfigHandler, // MSP 配置处理器
		},
	}

	if err := ooc.Validate(); err != nil {
		return nil, err
	}

	return ooc, nil
}

func (ooc *OrdererOrgConfig) Validate() error {
	return ooc.OrganizationConfig.Validate()
}

// NewOrdererConfig 创建一个新的 OrdererConfig 实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - ordererGroup：*cb.ConfigGroup，表示 Orderer 配置组
//   - mspConfig：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//   - channelCapabilities：ChannelCapabilities，表示通道的功能
//
// 返回值：
//   - *OrdererConfig：表示 OrdererConfig 的指针
//   - error：表示创建过程中可能出现的错误
func NewOrdererConfig(ordererGroup *cb.ConfigGroup, mspConfig *MSPConfigHandler, channelCapabilities ChannelCapabilities) (*OrdererConfig, error) {
	// 创建 OrdererConfig 实例，并初始化 protos 和 orgs
	oc := &OrdererConfig{
		protos: &OrdererProtos{},
		orgs:   make(map[string]OrdererOrg),
	}

	// 从 Orderer 配置组中反序列化值到 OrdererProtos 实例
	if err := DeserializeProtoValuesFromGroup(ordererGroup, oc.protos); err != nil {
		return nil, errors.Wrap(err, "Orderer无法反序列化值")
	}

	// 验证 OrdererConfig 的有效性
	if err := oc.Validate(); err != nil {
		return nil, err
	}

	// 遍历 Orderer 配置组中的每个组织，并为每个组织创建一个 OrdererOrgConfig 实例，并将其添加到 orgs 中
	for orgName, orgGroup := range ordererGroup.Groups {
		var err error
		// 从给定的 ConfigGroup 构建一个 Orderer 组织配置。
		if oc.orgs[orgName], err = NewOrdererOrgConfig(orgName, orgGroup, mspConfig, channelCapabilities); err != nil {
			return nil, err
		}
	}
	return oc, nil
}

// ConsensusType returns the configured consensus type.
func (oc *OrdererConfig) ConsensusType() string {
	return oc.protos.ConsensusType.Type
}

// ConsensusMetadata returns the metadata associated with the consensus type.
func (oc *OrdererConfig) ConsensusMetadata() []byte {
	return oc.protos.ConsensusType.Metadata
}

// ConsensusState return the consensus type state.
func (oc *OrdererConfig) ConsensusState() ab.ConsensusType_State {
	return oc.protos.ConsensusType.State
}

// BatchSize returns the maximum number of messages to include in a block.
func (oc *OrdererConfig) BatchSize() *ab.BatchSize {
	return oc.protos.BatchSize
}

// BatchTimeout returns the amount of time to wait before creating a batch.
func (oc *OrdererConfig) BatchTimeout() time.Duration {
	return oc.batchTimeout
}

// KafkaBrokers returns the addresses (IP:port notation) of a set of "bootstrap"
// Kafka brokers, i.e. this is not necessarily the entire set of Kafka brokers
// used for ordering.
func (oc *OrdererConfig) KafkaBrokers() []string {
	return oc.protos.KafkaBrokers.Brokers
}

// MaxChannelsCount returns the maximum count of channels this orderer supports.
func (oc *OrdererConfig) MaxChannelsCount() uint64 {
	return oc.protos.ChannelRestrictions.MaxCount
}

// Organizations returns a map of the orgs in the channel.
func (oc *OrdererConfig) Organizations() map[string]OrdererOrg {
	return oc.orgs
}

// Capabilities returns the capabilities the ordering network has for this channel.
func (oc *OrdererConfig) Capabilities() OrdererCapabilities {
	return capabilities.NewOrdererProvider(oc.protos.Capabilities.Capabilities)
}

func (oc *OrdererConfig) Validate() error {
	for _, validator := range []func() error{
		oc.validateBatchSize,
		oc.validateBatchTimeout,
		oc.validateKafkaBrokers,
	} {
		if err := validator(); err != nil {
			return err
		}
	}

	return nil
}

func (oc *OrdererConfig) validateBatchSize() error {
	if oc.protos.BatchSize.MaxMessageCount == 0 {
		return fmt.Errorf("Attempted to set the batch size max message count to an invalid value: 0")
	}
	if oc.protos.BatchSize.AbsoluteMaxBytes == 0 {
		return fmt.Errorf("Attempted to set the batch size absolute max bytes to an invalid value: 0")
	}
	if oc.protos.BatchSize.PreferredMaxBytes == 0 {
		return fmt.Errorf("Attempted to set the batch size preferred max bytes to an invalid value: 0")
	}
	if oc.protos.BatchSize.PreferredMaxBytes > oc.protos.BatchSize.AbsoluteMaxBytes {
		return fmt.Errorf("Attempted to set the batch size preferred max bytes (%v) greater than the absolute max bytes (%v).", oc.protos.BatchSize.PreferredMaxBytes, oc.protos.BatchSize.AbsoluteMaxBytes)
	}
	return nil
}

func (oc *OrdererConfig) validateBatchTimeout() error {
	var err error
	oc.batchTimeout, err = time.ParseDuration(oc.protos.BatchTimeout.Timeout)
	if err != nil {
		return fmt.Errorf("Attempted to set the batch timeout to a invalid value: %s", err)
	}
	if oc.batchTimeout <= 0 {
		return fmt.Errorf("Attempted to set the batch timeout to a non-positive value: %s", oc.batchTimeout)
	}
	return nil
}

func (oc *OrdererConfig) validateKafkaBrokers() error {
	for _, broker := range oc.protos.KafkaBrokers.Brokers {
		if !brokerEntrySeemsValid(broker) {
			return fmt.Errorf("Invalid broker entry: %s", broker)
		}
	}
	return nil
}

// This does just a barebones sanity check.
func brokerEntrySeemsValid(broker string) bool {
	if !strings.Contains(broker, ":") {
		return false
	}

	parts := strings.Split(broker, ":")
	if len(parts) > 2 {
		return false
	}

	host := parts[0]
	port := parts[1]

	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return false
	}

	// Valid hostnames may contain only the ASCII letters 'a' through 'z' (in a
	// case-insensitive manner), the digits '0' through '9', and the hyphen. IP
	// v4 addresses are  represented in dot-decimal notation, which consists of
	// four decimal numbers, each ranging from 0 to 255, separated by dots,
	// e.g., 172.16.254.1
	// The following regular expression:
	// 1. allows just a-z (case-insensitive), 0-9, and the dot and hyphen characters
	// 2. does not allow leading trailing dots or hyphens
	re, _ := regexp.Compile("^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9])$")
	matched := re.FindString(host)
	return len(matched) == len(host)
}
