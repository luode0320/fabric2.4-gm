/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"
	"io/ioutil"
	"math"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	mspprotos "github.com/hyperledger/fabric-protos-go/msp"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	// ReadersPolicyKey is the key used for the read policy
	ReadersPolicyKey = "Readers"

	// WritersPolicyKey is the key used for the read policy
	WritersPolicyKey = "Writers"

	// AdminsPolicyKey 是用于读取策略的键
	AdminsPolicyKey = "Admins"

	// todo luode 进行国密sm3的改造
	defaultHashingAlgorithm = bccsp.GMSM3

	defaultBlockDataHashingStructureWidth = math.MaxUint32
)

// ConfigValue defines a common representation for different *cb.ConfigValue values.
type ConfigValue interface {
	// Key is the key this value should be stored in the *cb.ConfigGroup.Values map.
	Key() string

	// Value is the message which should be marshaled to opaque bytes for the *cb.ConfigValue.value.
	Value() proto.Message
}

// StandardConfigValue 实现ConfigValue接口。
type StandardConfigValue struct {
	key   string
	value proto.Message
}

// Key is the key this value should be stored in the *cb.ConfigGroup.Values map.
func (scv *StandardConfigValue) Key() string {
	return scv.key
}

// Value is the message which should be marshaled to opaque bytes for the *cb.ConfigValue.value.
func (scv *StandardConfigValue) Value() proto.Message {
	return scv.value
}

// ConsortiumValue returns the config definition for the consortium name.
// It is a value for the channel group.
func ConsortiumValue(name string) *StandardConfigValue {
	return &StandardConfigValue{
		key: ConsortiumKey,
		value: &cb.Consortium{
			Name: name,
		},
	}
}

// HashingAlgorithmValue 返回默认哈希算法。
// 它是/Channel组的值。
func HashingAlgorithmValue() *StandardConfigValue {
	return &StandardConfigValue{
		key: HashingAlgorithmKey,
		value: &cb.HashingAlgorithm{
			Name: defaultHashingAlgorithm,
		},
	}
}

// BlockDataHashingStructureValue 返回当前唯一有效的块数据哈希结构。
// 它是/Channel组的值。
func BlockDataHashingStructureValue() *StandardConfigValue {
	return &StandardConfigValue{
		key: BlockDataHashingStructureKey,
		value: &cb.BlockDataHashingStructure{
			Width: defaultBlockDataHashingStructureWidth,
		},
	}
}

// OrdererAddressesValue returns the a config definition for the orderer addresses.
// It is a value for the /Channel group.
func OrdererAddressesValue(addresses []string) *StandardConfigValue {
	return &StandardConfigValue{
		key: OrdererAddressesKey,
		value: &cb.OrdererAddresses{
			Addresses: addresses,
		},
	}
}

// ConsensusTypeValue returns the config definition for the orderer consensus type.
// It is a value for the /Channel/Orderer group.
func ConsensusTypeValue(consensusType string, consensusMetadata []byte) *StandardConfigValue {
	return &StandardConfigValue{
		key: ConsensusTypeKey,
		value: &ab.ConsensusType{
			Type:     consensusType,
			Metadata: consensusMetadata,
		},
	}
}

// BatchSizeValue 返回orderer批处理大小的配置定义。
// 它是/Channel/Orderer组的值。
func BatchSizeValue(maxMessages, absoluteMaxBytes, preferredMaxBytes uint32) *StandardConfigValue {
	return &StandardConfigValue{
		key: BatchSizeKey,
		value: &ab.BatchSize{
			MaxMessageCount:   maxMessages,
			AbsoluteMaxBytes:  absoluteMaxBytes,
			PreferredMaxBytes: preferredMaxBytes,
		},
	}
}

// BatchTimeoutValue 返回orderer批处理超时的配置定义。
// 它是/Channel/Orderer组的值。
func BatchTimeoutValue(timeout string) *StandardConfigValue {
	return &StandardConfigValue{
		key: BatchTimeoutKey,
		value: &ab.BatchTimeout{
			Timeout: timeout,
		},
	}
}

// ChannelRestrictionsValue 返回orderer通道限制的配置定义。
// 它是/Channel/Orderer组的值。
func ChannelRestrictionsValue(maxChannelCount uint64) *StandardConfigValue {
	return &StandardConfigValue{
		key: ChannelRestrictionsKey,
		value: &ab.ChannelRestrictions{
			MaxCount: maxChannelCount,
		},
	}
}

// KafkaBrokersValue returns the config definition for the addresses of the ordering service's Kafka brokers.
// It is a value for the /Channel/Orderer group.
func KafkaBrokersValue(brokers []string) *StandardConfigValue {
	return &StandardConfigValue{
		key: KafkaBrokersKey,
		value: &ab.KafkaBrokers{
			Brokers: brokers,
		},
	}
}

// MSPValue 返回MSP的配置定义。
// 它是/Channel/Orderer/*，/Channel/Application/* 和/Channel/Consortiums/* 组的值。
func MSPValue(mspDef *mspprotos.MSPConfig) *StandardConfigValue {
	return &StandardConfigValue{
		key:   MSPKey,
		value: mspDef,
	}
}

// CapabilitiesValue returns the config definition for a a set of capabilities.
// It is a value for the /Channel/Orderer, Channel/Application/, and /Channel groups.
func CapabilitiesValue(capabilities map[string]bool) *StandardConfigValue {
	c := &cb.Capabilities{
		Capabilities: make(map[string]*cb.Capability),
	}

	for capability, required := range capabilities {
		if !required {
			continue
		}
		c.Capabilities[capability] = &cb.Capability{}
	}

	return &StandardConfigValue{
		key:   CapabilitiesKey,
		value: c,
	}
}

// EndpointsValue returns the config definition for the orderer addresses at an org scoped level.
// It is a value for the /Channel/Orderer/<OrgName> group.
func EndpointsValue(addresses []string) *StandardConfigValue {
	return &StandardConfigValue{
		key: EndpointsKey,
		value: &cb.OrdererAddresses{
			Addresses: addresses,
		},
	}
}

// AnchorPeersValue returns the config definition for an org's anchor peers.
// It is a value for the /Channel/Application/*.
func AnchorPeersValue(anchorPeers []*pb.AnchorPeer) *StandardConfigValue {
	return &StandardConfigValue{
		key:   AnchorPeersKey,
		value: &pb.AnchorPeers{AnchorPeers: anchorPeers},
	}
}

// ChannelCreationPolicyValue returns the config definition for a consortium's channel creation policy
// It is a value for the /Channel/Consortiums/*/*.
func ChannelCreationPolicyValue(policy *cb.Policy) *StandardConfigValue {
	return &StandardConfigValue{
		key:   ChannelCreationPolicyKey,
		value: policy,
	}
}

// ACLValues returns the config definition for an applications resources based ACL definitions.
// It is a value for the /Channel/Application/.
func ACLValues(acls map[string]string) *StandardConfigValue {
	a := &pb.ACLs{
		Acls: make(map[string]*pb.APIResource),
	}

	for apiResource, policyRef := range acls {
		a.Acls[apiResource] = &pb.APIResource{PolicyRef: policyRef}
	}

	return &StandardConfigValue{
		key:   ACLsKey,
		value: a,
	}
}

// ValidateCapabilities 验证对等节点是否能够满足给定配置块中的能力要求。
// 输入参数：
//   - block：要验证的配置块。
//   - bccsp：BCCSP（区块链加密服务提供者）实例。
//
// 返回值：
//   - error：如果验证过程中出现错误，则返回错误；否则返回nil。
func ValidateCapabilities(block *cb.Block, bccsp bccsp.BCCSP) error {
	// 提取通道配置, 从给定的块中提取通道配置。
	cc, err := extractChannelConfig(block, bccsp)
	if err != nil {
		return err
	}

	// 检查通道顶层的能力要求
	if err := cc.Capabilities().Supported(); err != nil {
		return err
	}

	// 检查应用程序的能力要求
	return cc.ApplicationConfig().Capabilities().Supported()
}

// ExtractMSPIDsForApplicationOrgs extracts MSPIDs for application organizations
func ExtractMSPIDsForApplicationOrgs(block *cb.Block, bccsp bccsp.BCCSP) ([]string, error) {
	cc, err := extractChannelConfig(block, bccsp)
	if err != nil {
		return nil, err
	}

	if cc.ApplicationConfig() == nil {
		return nil, errors.Errorf("could not get application config for the channel")
	}
	orgs := cc.ApplicationConfig().Organizations()
	mspids := make([]string, 0, len(orgs))
	for _, org := range orgs {
		mspids = append(mspids, org.MSPID())
	}
	return mspids, nil
}

// extractChannelConfig 从给定的块中提取通道配置。
// 输入参数：
//   - block：要提取通道配置的块。
//   - bccsp：BCCSP（区块链加密服务提供者）实例。
//
// 返回值：
//   - *ChannelConfig：提取的通道配置。
//   - error：如果提取过程中出现错误，则返回错误；否则返回nil。
func extractChannelConfig(block *cb.Block, bccsp bccsp.BCCSP) (*ChannelConfig, error) {
	// 提取配置块中的第一个envelope
	envelopeConfig, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, errors.WithMessage(err, "提取配置块中的第一个有效数据出错")
	}

	// 反序列化envelope为ConfigEnvelope对象
	configEnv := &cb.ConfigEnvelope{}
	_, err = protoutil.UnmarshalEnvelopeOfType(envelopeConfig, cb.HeaderType_CONFIG, configEnv)
	if err != nil {
		return nil, errors.WithMessage(err, "解析第一个区块出错")
	}

	// 检查ConfigEnvelope的Config字段是否为nil
	if configEnv.Config == nil {
		return nil, errors.New("第一个区块中的 Config 配置为空")
	}

	// 检查ConfigEnvelope的ChannelGroup字段是否为nil
	if configEnv.Config.ChannelGroup == nil {
		return nil, errors.New("第一个区块中 Config.ChannelGroup 通道组为空")
	}

	// 检查ConfigEnvelope的ChannelGroup的Groups字段是否为nil
	if configEnv.Config.ChannelGroup.Groups == nil {
		return nil, errors.New("第一个区块中没有可用的通道配置组 Config.ChannelGroup.Groups")
	}

	// 检查ConfigEnvelope的ChannelGroup的Groups字段中是否存在ApplicationGroupKey对应的配置组
	_, exists := configEnv.Config.ChannelGroup.Groups[ApplicationGroupKey]
	if !exists {
		return nil, errors.Errorf("无效的配置块, 缺失 %s 配置组", ApplicationGroupKey)
	}

	// 创建ChannelConfig对象
	cc, err := NewChannelConfig(configEnv.Config.ChannelGroup, bccsp)
	if err != nil {
		return nil, errors.WithMessage(err, "未找到有效的通道配置")
	}
	return cc, nil
}

// MarshalEtcdRaftMetadata serializes etcd RAFT metadata.
func MarshalEtcdRaftMetadata(md *etcdraft.ConfigMetadata) ([]byte, error) {
	copyMd := proto.Clone(md).(*etcdraft.ConfigMetadata)
	for _, c := range copyMd.Consenters {
		// Expect the user to set the config value for client/server certs to the
		// path where they are persisted locally, then load these files to memory.
		clientCert, err := ioutil.ReadFile(string(c.GetClientTlsCert()))
		if err != nil {
			return nil, fmt.Errorf("cannot load client cert for consenter %s:%d: %s", c.GetHost(), c.GetPort(), err)
		}
		c.ClientTlsCert = clientCert

		serverCert, err := ioutil.ReadFile(string(c.GetServerTlsCert()))
		if err != nil {
			return nil, fmt.Errorf("cannot load server cert for consenter %s:%d: %s", c.GetHost(), c.GetPort(), err)
		}
		c.ServerTlsCert = serverCert
	}
	return proto.Marshal(copyMd)
}
