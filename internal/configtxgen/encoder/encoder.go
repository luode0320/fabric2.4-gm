/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package encoder

import (
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/genesis"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/policydsl"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/configtxgen/genesisconfig"
	"github.com/hyperledger/fabric/internal/configtxlator/update"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	ordererAdminsPolicyName = "/Channel/Orderer/Admins"

	msgVersion = int32(0)
	epoch      = 0
)

var logger = flogging.MustGetLogger("common.tools.configtxgen.encoder")

const (
	// ConsensusTypeSolo 标识solo共识实现。
	ConsensusTypeSolo = "solo"
	// ConsensusTypeKafka 标识基于Kafka的共识实现。
	ConsensusTypeKafka = "kafka"
	// ConsensusTypeKafka 标识基于etcdraft的共识实现。
	ConsensusTypeEtcdRaft = "etcdraft"

	// BlockValidationPolicyKey TODO
	BlockValidationPolicyKey = "BlockValidation"

	// OrdererAdminsPolicy is the absolute path to the orderer admins policy
	OrdererAdminsPolicy = "/Channel/Orderer/Admins"

	// SignaturePolicyType 是签名策略的 “类型” 字符串
	SignaturePolicyType = "Signature"

	// ImplicitMetaPolicyType 是隐式元策略的 “类型” 字符串
	ImplicitMetaPolicyType = "ImplicitMeta"
)

func addValue(cg *cb.ConfigGroup, value channelconfig.ConfigValue, modPolicy string) {
	cg.Values[value.Key()] = &cb.ConfigValue{
		Value:     protoutil.MarshalOrPanic(value.Value()),
		ModPolicy: modPolicy,
	}
}

func addPolicy(cg *cb.ConfigGroup, policy policies.ConfigPolicy, modPolicy string) {
	cg.Policies[policy.Key()] = &cb.ConfigPolicy{
		Policy:    policy.Value(),
		ModPolicy: modPolicy,
	}
}

func AddOrdererPolicies(cg *cb.ConfigGroup, policyMap map[string]*genesisconfig.Policy, modPolicy string) error {
	switch {
	case policyMap == nil:
		return errors.Errorf("未定义策略")
	case policyMap[BlockValidationPolicyKey] == nil:
		return errors.Errorf("未定义BlockValidation策略")
	}

	return AddPolicies(cg, policyMap, modPolicy)
}

// AddPolicies 向 ConfigGroup 添加策略
func AddPolicies(cg *cb.ConfigGroup, policyMap map[string]*genesisconfig.Policy, modPolicy string) error {
	switch {
	case policyMap == nil:
		return errors.Errorf("未定义 Policies 通道策略")
	case policyMap[channelconfig.AdminsPolicyKey] == nil:
		return errors.Errorf("未定义 Policies 管理员策略")
	case policyMap[channelconfig.ReadersPolicyKey] == nil:
		return errors.Errorf("未定义 Policies 读取策略")
	case policyMap[channelconfig.WritersPolicyKey] == nil:
		return errors.Errorf("未定义 Policies 写入策略")
	}

	for policyName, policy := range policyMap {
		switch policy.Type {
		case ImplicitMetaPolicyType: // 隐式元策略的 “类型” 字符串
			// 从字符串中解析并创建一个 ImplicitMetaPolicy。
			imp, err := policies.ImplicitMetaFromString(policy.Rule)
			if err != nil {
				return errors.Wrapf(err, "无效的 ImplicitMeta 元策略规则: '%s'", policy.Rule)
			}
			cg.Policies[policyName] = &cb.ConfigPolicy{
				ModPolicy: modPolicy,
				Policy: &cb.Policy{
					Type:  int32(cb.Policy_IMPLICIT_META),
					Value: protoutil.MarshalOrPanic(imp),
				},
			}
		case SignaturePolicyType: // 签名策略的 “类型” 字符串
			sp, err := policydsl.FromString(policy.Rule)
			if err != nil {
				return errors.Wrapf(err, "签名 Signature 策略规则无效 '%s'", policy.Rule)
			}
			cg.Policies[policyName] = &cb.ConfigPolicy{
				ModPolicy: modPolicy,
				Policy: &cb.Policy{
					Type:  int32(cb.Policy_SIGNATURE),
					Value: protoutil.MarshalOrPanic(sp),
				},
			}
		default:
			return errors.Errorf("未知的策略类型: %s, 只支持Signature、ImplicitMeta", policy.Type)
		}
	}
	return nil
}

// NewChannelGroup 定义通道配置。它定义了基本的操作原理，例如用于块的哈希算法，以及订购服务的位置。
// 它将根据配置中是否设置了 neworderergup ，NewConsortiumsGroup 和 NewApplicationGroup 递归调用这些子元素。
// 此组的所有 mod_policy 值都设置为 “Admins”，但orderereraddresses值除外，该值设置为 “/Channel/Orderer/Admins”。
func NewChannelGroup(conf *genesisconfig.Profile) (*cb.ConfigGroup, error) {
	// 初始化创建一个空的通道组
	channelGroup := protoutil.NewConfigGroup()

	// 向 ConfigGroup 添加 conf.Policies 通道策略
	if err := AddPolicies(channelGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "将 Policies 通道策略添加到 channelGroup 通道组时出错")
	}

	// 返回默认哈希算法。
	addValue(channelGroup, channelconfig.HashingAlgorithmValue(), channelconfig.AdminsPolicyKey)
	// 返回当前唯一有效的块数据哈希结构。
	addValue(channelGroup, channelconfig.BlockDataHashingStructureValue(), channelconfig.AdminsPolicyKey)

	// 配置orderer 共识节点
	if conf.Orderer != nil && len(conf.Orderer.Addresses) > 0 {
		// 该值设置为 ordererAdminsPolicyName = “/Channel/Orderer/Admins”
		addValue(channelGroup, channelconfig.OrdererAddressesValue(conf.Orderer.Addresses), ordererAdminsPolicyName)
	}

	// 配置联盟
	if conf.Consortium != "" {
		addValue(channelGroup, channelconfig.ConsortiumValue(conf.Consortium), channelconfig.AdminsPolicyKey)
	}

	// 配置功能
	if len(conf.Capabilities) > 0 {
		addValue(channelGroup, channelconfig.CapabilitiesValue(conf.Capabilities), channelconfig.AdminsPolicyKey)
	}

	var err error
	if conf.Orderer != nil {
		// 返回通道配置的orderer组件。它定义了订购服务的参数，关于块应该有多大， 它们应该发出的频率等以及订购网络的组织。
		channelGroup.Groups[channelconfig.OrdererGroupKey], err = NewOrdererGroup(conf.Orderer)
		if err != nil {
			return nil, errors.Wrap(err, "无法创建 Orderer 排序组")
		}
	}

	if conf.Application != nil {
		// 返回通道配置的应用程序组件。
		channelGroup.Groups[channelconfig.ApplicationGroupKey], err = NewApplicationGroup(conf.Application)
		if err != nil {
			return nil, errors.Wrap(err, "无法创建 Application 应用程序组")
		}
	}

	if conf.Consortiums != nil {
		// 返回通道配置的consortiums组件。
		channelGroup.Groups[channelconfig.ConsortiumsGroupKey], err = NewConsortiumsGroup(conf.Consortiums)
		if err != nil {
			return nil, errors.Wrap(err, "无法创建 Consortiums 联盟组")
		}
	}

	channelGroup.ModPolicy = channelconfig.AdminsPolicyKey
	return channelGroup, nil
}

// NewOrdererGroup 返回通道配置的orderer组件。它定义了订购服务的参数，关于块应该有多大， 它们应该发出的频率等以及订购网络的组织。
// 它将所有元素的mod_policy设置为 “Admins”。此组始终存在于任何通道配置中。
func NewOrdererGroup(conf *genesisconfig.Orderer) (*cb.ConfigGroup, error) {
	ordererGroup := protoutil.NewConfigGroup()

	// 添加orderer组策略
	if err := AddOrdererPolicies(ordererGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "将 Orderer.Policies 策略添加到orderer组时出错")
	}

	// 返回orderer批处理大小的配置定义。
	addValue(ordererGroup, channelconfig.BatchSizeValue(
		conf.BatchSize.MaxMessageCount,
		conf.BatchSize.AbsoluteMaxBytes,
		conf.BatchSize.PreferredMaxBytes,
	), channelconfig.AdminsPolicyKey)
	// 返回orderer批处理超时的配置定义。
	addValue(ordererGroup, channelconfig.BatchTimeoutValue(conf.BatchTimeout.String()), channelconfig.AdminsPolicyKey)
	// 返回orderer通道数量限制的配置定义。
	addValue(ordererGroup, channelconfig.ChannelRestrictionsValue(conf.MaxChannels), channelconfig.AdminsPolicyKey)
	// 版本功能配置
	if len(conf.Capabilities) > 0 {
		addValue(ordererGroup, channelconfig.CapabilitiesValue(conf.Capabilities), channelconfig.AdminsPolicyKey)
	}

	var consensusMetadata []byte
	var err error

	// 共识类型
	switch conf.OrdererType {
	case ConsensusTypeSolo:
	case ConsensusTypeKafka:
		addValue(ordererGroup, channelconfig.KafkaBrokersValue(conf.Kafka.Brokers), channelconfig.AdminsPolicyKey)
	case ConsensusTypeEtcdRaft:
		if consensusMetadata, err = channelconfig.MarshalEtcdRaftMetadata(conf.EtcdRaft); err != nil {
			return nil, errors.Errorf("无法解析 orderer 共识类型的元数据 %s: %s", ConsensusTypeEtcdRaft, err)
		}
	default:
		return nil, errors.Errorf("未知的orderer 共识类型: %s, 只支持solo、kafka、etcdraft", conf.OrdererType)
	}

	addValue(ordererGroup, channelconfig.ConsensusTypeValue(conf.OrdererType, consensusMetadata), channelconfig.AdminsPolicyKey)

	// 组织配置
	for _, org := range conf.Organizations {
		var err error
		// 返回通道配置的orderer org组织组件。
		ordererGroup.Groups[org.Name], err = NewOrdererOrgGroup(org)
		if err != nil {
			return nil, errors.Wrap(err, "通道创建 orderer 组织失败")
		}
	}

	ordererGroup.ModPolicy = channelconfig.AdminsPolicyKey
	return ordererGroup, nil
}

// NewConsortiumsGroup returns an org component of the channel configuration.  It defines the crypto material for the
// organization (its MSP).  It sets the mod_policy of all elements to "Admins".
func NewConsortiumOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	consortiumsOrgGroup := protoutil.NewConfigGroup()
	consortiumsOrgGroup.ModPolicy = channelconfig.AdminsPolicyKey

	if conf.SkipAsForeign {
		return consortiumsOrgGroup, nil
	}

	mspConfig, err := msp.GetVerifyingMspConfig(conf.MSPDir, conf.ID, conf.MSPType)
	if err != nil {
		return nil, errors.Wrapf(err, "1 - Error loading MSP configuration for org: %s", conf.Name)
	}

	if err := AddPolicies(consortiumsOrgGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "error adding policies to consortiums org group '%s'", conf.Name)
	}

	addValue(consortiumsOrgGroup, channelconfig.MSPValue(mspConfig), channelconfig.AdminsPolicyKey)

	return consortiumsOrgGroup, nil
}

// NewOrdererOrgGroup 返回通道配置的orderer org组件。它定义了组织的加密材料 (其MSP)。
// 它将所有元素的mod_policy设置为 “Admins”。
func NewOrdererOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	ordererOrgGroup := protoutil.NewConfigGroup()
	ordererOrgGroup.ModPolicy = channelconfig.AdminsPolicyKey

	if conf.SkipAsForeign {
		return ordererOrgGroup, nil
	}

	// 返回给定目录、ID和类型的MSP配置
	mspConfig, err := msp.GetVerifyingMspConfig(conf.MSPDir, conf.ID, conf.MSPType)
	if err != nil {
		return nil, errors.Wrapf(err, "1-加载组织的MSP配置时出错: %s", conf.Name)
	}

	// 添加组织策略
	if err := AddPolicies(ordererOrgGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "向 orderer 组织组添加策略时出错 '%s'", conf.Name)
	}

	// 添加msp配置, 返回MSP的配置定义。
	addValue(ordererOrgGroup, channelconfig.MSPValue(mspConfig), channelconfig.AdminsPolicyKey)

	// 排序节点地址列表
	if len(conf.OrdererEndpoints) > 0 {
		addValue(ordererOrgGroup, channelconfig.EndpointsValue(conf.OrdererEndpoints), channelconfig.AdminsPolicyKey)
	}

	return ordererOrgGroup, nil
}

// NewApplicationGroup 返回通道配置的应用程序组件。
// 它定义了应用程序逻辑 (如链码) 中涉及的组织，以及这些成员如何与orderer交互。
// 它将所有元素的mod_policy设置为 “Admins”。
func NewApplicationGroup(conf *genesisconfig.Application) (*cb.ConfigGroup, error) {
	applicationGroup := protoutil.NewConfigGroup()

	// 添加 Application 策略
	if err := AddPolicies(applicationGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "将策略添加到 Application 应用程序组时出错")
	}

	// 权限acl配置
	if len(conf.ACLs) > 0 {
		addValue(applicationGroup, channelconfig.ACLValues(conf.ACLs), channelconfig.AdminsPolicyKey)
	}

	// 版本功能配置
	if len(conf.Capabilities) > 0 {
		addValue(applicationGroup, channelconfig.CapabilitiesValue(conf.Capabilities), channelconfig.AdminsPolicyKey)
	}

	// 组织配置
	for _, org := range conf.Organizations {
		var err error
		// 返回通道配置的应用程序组织组件。
		applicationGroup.Groups[org.Name], err = NewApplicationOrgGroup(org)
		if err != nil {
			return nil, errors.Wrap(err, "创建 Application 应用组织失败")
		}
	}

	applicationGroup.ModPolicy = channelconfig.AdminsPolicyKey
	return applicationGroup, nil
}

// NewApplicationOrgGroup 返回通道配置的应用程序组织组件。
// 它定义了组织 (其MSP) 及其锚对等体的加密材料，以供八卦网络使用。
// 它将所有元素的mod_policy设置为 “Admins”。
func NewApplicationOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	applicationOrgGroup := protoutil.NewConfigGroup()
	applicationOrgGroup.ModPolicy = channelconfig.AdminsPolicyKey

	// 跳过作为外部组织处理的标志
	if conf.SkipAsForeign {
		return applicationOrgGroup, nil
	}

	// 加载msp目录配置
	mspConfig, err := msp.GetVerifyingMspConfig(conf.MSPDir, conf.ID, conf.MSPType)
	if err != nil {
		return nil, errors.Wrapf(err, "1-加载组织 %s 的MSP配置时出错", conf.Name)
	}

	// 添加组织策略
	if err := AddPolicies(applicationOrgGroup, conf.Policies, channelconfig.AdminsPolicyKey); err != nil {
		return nil, errors.Wrapf(err, "将策略添加到应用程序组织组时出错 %s", conf.Name)
	}
	// 添加msp
	addValue(applicationOrgGroup, channelconfig.MSPValue(mspConfig), channelconfig.AdminsPolicyKey)

	// 添加锚节点
	var anchorProtos []*pb.AnchorPeer
	for _, anchorPeer := range conf.AnchorPeers {
		anchorProtos = append(anchorProtos, &pb.AnchorPeer{
			Host: anchorPeer.Host,
			Port: int32(anchorPeer.Port),
		})
	}

	// 避免在不需要时添加不必要的anchor peers元素。
	// 这有助于在计算更复杂的通道创建事务时防止来自orderer系统通道的增量
	if len(anchorProtos) > 0 {
		addValue(applicationOrgGroup, channelconfig.AnchorPeersValue(anchorProtos), channelconfig.AdminsPolicyKey)
	}

	return applicationOrgGroup, nil
}

// NewConsortiumsGroup 返回通道配置的consortiums组件。
// 此元素仅针对订购系统通道定义。
// 它将所有元素的mod_policy设置为 “/Channel/Orderer/Admins”。
func NewConsortiumsGroup(conf map[string]*genesisconfig.Consortium) (*cb.ConfigGroup, error) {
	consortiumsGroup := protoutil.NewConfigGroup()
	// 此策略在任何地方都没有引用，它仅用作通道级别隐式元策略规则的一部分，
	// 因此，此设置有效地降低了订购系统通道对订购管理员的控制
	addPolicy(consortiumsGroup, policies.SignaturePolicy(channelconfig.AdminsPolicyKey, policydsl.AcceptAllPolicy), ordererAdminsPolicyName)

	for consortiumName, consortium := range conf {
		var err error
		consortiumsGroup.Groups[consortiumName], err = NewConsortiumGroup(consortium)
		if err != nil {
			return nil, errors.Wrapf(err, "创建联盟失败 %s", consortiumName)
		}
	}

	consortiumsGroup.ModPolicy = ordererAdminsPolicyName
	return consortiumsGroup, nil
}

// NewConsortiums returns a consortiums component of the channel configuration.  Each consortium defines the organizations which may be involved in channel
// creation, as well as the channel creation policy the orderer checks at channel creation time to authorize the action.  It sets the mod_policy of all
// elements to "/Channel/Orderer/Admins".
func NewConsortiumGroup(conf *genesisconfig.Consortium) (*cb.ConfigGroup, error) {
	consortiumGroup := protoutil.NewConfigGroup()

	for _, org := range conf.Organizations {
		var err error
		consortiumGroup.Groups[org.Name], err = NewConsortiumOrgGroup(org)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create consortium org")
		}
	}

	addValue(consortiumGroup, channelconfig.ChannelCreationPolicyValue(policies.ImplicitMetaAnyPolicy(channelconfig.AdminsPolicyKey).Value()), ordererAdminsPolicyName)

	consortiumGroup.ModPolicy = ordererAdminsPolicyName
	return consortiumGroup, nil
}

// NewChannelCreateConfigUpdate 生成一个 ConfigUpdate，可以发送给 orderer 来创建一个新的通道。
// 可选地，可以传入订购系统通道的通道组，生成的 ConfigUpdate 将从该文件中提取适当的版本。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - channelID：string，表示通道的ID
//   - conf：*genesisconfig.Profile，表示通道的配置文件
//   - templateConfig：*cb.ConfigGroup，表示配置模板的 ConfigGroup
//
// 返回值：
//   - *cb.ConfigUpdate：表示生成的配置更新
//   - error：表示生成过程中可能发生的错误
func NewChannelCreateConfigUpdate(channelID string, conf *genesisconfig.Profile, templateConfig *cb.ConfigGroup) (*cb.ConfigUpdate, error) {
	// 检查是否定义了应用程序部分
	if conf.Application == nil {
		return nil, errors.New("无法定义没有 Application 应用程序的新通道")
	}

	if conf.Consortium == "" {
		return nil, errors.New("无法定义没有 Consortium 联盟的新通道")
	}

	// 将初始配置文件转换为通道组
	newChannelGroup, err := NewChannelGroup(conf)
	if err != nil {
		return nil, errors.Wrapf(err, "无法将分析配置文件转换为通道组")
	}

	// 计算通道配置更新
	updt, err := update.Compute(&cb.Config{ChannelGroup: templateConfig}, &cb.Config{ChannelGroup: newChannelGroup})
	if err != nil {
		return nil, errors.Wrapf(err, "计算通道配置更新失败")
	}

	// 将联盟名称添加到写集中，以创建所需的通道
	updt.ChannelId = channelID
	updt.ReadSet.Values[channelconfig.ConsortiumKey] = &cb.ConfigValue{Version: 0}
	updt.WriteSet.Values[channelconfig.ConsortiumKey] = &cb.ConfigValue{
		Version: 0,
		Value: protoutil.MarshalOrPanic(&cb.Consortium{
			Name: conf.Consortium,
		}),
	}

	return updt, nil
}

// DefaultConfigTemplate 根据输入的配置文件生成一个配置模板，假设该配置文件是一个通道创建模板，并且没有系统通道上下文可用。
// 输入参数：
//   - conf：*genesisconfig.Profile，表示通道的配置文件
//
// 返回值：
//   - *cb.ConfigGroup：表示生成的配置模板的 ConfigGroup
//   - error：表示生成过程中可能发生的错误
func DefaultConfigTemplate(conf *genesisconfig.Profile) (*cb.ConfigGroup, error) {
	// 根据配置文件创建一个新的 ChannelGroup 实例
	channelGroup, err := NewChannelGroup(conf)
	if err != nil {
		return nil, errors.WithMessage(err, "解析配置时出错")
	}

	// 检查是否存在应用程序部分
	if _, ok := channelGroup.Groups[channelconfig.ApplicationGroupKey]; !ok {
		return nil, errors.New("通道模板配置必须包含 Application 应用程序部分")
	}

	// 清空应用程序部分的 Values 和 Policies 字段, 因为后面会再次获取一次比较不同, 这里手动停止为不同
	channelGroup.Groups[channelconfig.ApplicationGroupKey].Values = nil
	channelGroup.Groups[channelconfig.ApplicationGroupKey].Policies = nil

	return channelGroup, nil
}

func ConfigTemplateFromGroup(conf *genesisconfig.Profile, cg *cb.ConfigGroup) (*cb.ConfigGroup, error) {
	template := proto.Clone(cg).(*cb.ConfigGroup)
	if template.Groups == nil {
		return nil, errors.Errorf("supplied system channel group has no sub-groups")
	}

	template.Groups[channelconfig.ApplicationGroupKey] = &cb.ConfigGroup{
		Groups: map[string]*cb.ConfigGroup{},
		Policies: map[string]*cb.ConfigPolicy{
			channelconfig.AdminsPolicyKey: {},
		},
	}

	consortiums, ok := template.Groups[channelconfig.ConsortiumsGroupKey]
	if !ok {
		return nil, errors.Errorf("supplied system channel group does not appear to be system channel (missing consortiums group)")
	}

	if consortiums.Groups == nil {
		return nil, errors.Errorf("system channel consortiums group appears to have no consortiums defined")
	}

	consortium, ok := consortiums.Groups[conf.Consortium]
	if !ok {
		return nil, errors.Errorf("supplied system channel group is missing '%s' consortium", conf.Consortium)
	}

	if conf.Application == nil {
		return nil, errors.Errorf("supplied channel creation profile does not contain an application section")
	}

	for _, organization := range conf.Application.Organizations {
		var ok bool
		template.Groups[channelconfig.ApplicationGroupKey].Groups[organization.Name], ok = consortium.Groups[organization.Name]
		if !ok {
			return nil, errors.Errorf("consortium %s does not contain member org %s", conf.Consortium, organization.Name)
		}
	}
	delete(template.Groups, channelconfig.ConsortiumsGroupKey)

	addValue(template, channelconfig.ConsortiumValue(conf.Consortium), channelconfig.AdminsPolicyKey)

	return template, nil
}

// MakeChannelCreationTransaction 用于创建通道创建的交易。 它假设调用者没有系统通道上下文，因此只关注应用程序部分。
// 输入参数：
//   - channelID：string，表示通道的ID
//   - signer：identity.SignerSerializer，表示签名者的序列化器
//   - conf：*genesisconfig.Profile，表示通道的配置文件
//
// 返回值：
//   - *cb.Envelope：表示创建的交易的封包
//   - error：表示创建过程中可能发生的错误
func MakeChannelCreationTransaction(
	channelID string,
	signer identity.SignerSerializer,
	conf *genesisconfig.Profile,
) (*cb.Envelope, error) {
	// 使用默认的配置模板生成配置文件, 根据输入的配置文件生成一个配置模板
	template, err := DefaultConfigTemplate(conf)
	if err != nil {
		return nil, errors.WithMessage(err, "无法生成默认 template 配置模板")
	}

	// 根据给定的模板创建一个用于创建通道的交易。
	return MakeChannelCreationTransactionFromTemplate(channelID, signer, conf, template)
}

// MakeChannelCreationTransactionWithSystemChannelContext is a utility function for creating channel creation txes.
// It requires a configuration representing the orderer system channel to allow more sophisticated channel creation
// transactions modifying pieces of the configuration like the orderer set.
func MakeChannelCreationTransactionWithSystemChannelContext(
	channelID string,
	signer identity.SignerSerializer,
	conf,
	systemChannelConf *genesisconfig.Profile,
) (*cb.Envelope, error) {
	cg, err := NewChannelGroup(systemChannelConf)
	if err != nil {
		return nil, errors.WithMessage(err, "could not parse system channel config")
	}

	template, err := ConfigTemplateFromGroup(conf, cg)
	if err != nil {
		return nil, errors.WithMessage(err, "could not create config template")
	}

	return MakeChannelCreationTransactionFromTemplate(channelID, signer, conf, template)
}

// MakeChannelCreationTransactionFromTemplate 根据给定的模板创建一个用于创建通道的交易。
// 它使用给定的模板生成配置更新集。通常，调用者会希望调用 MakeChannelCreationTransaction 或 MakeChannelCreationTransactionWithSystemChannelContext。
// 输入参数：
//   - channelID：string，表示通道的ID
//   - signer：identity.SignerSerializer，表示签名者的序列化器
//   - conf：*genesisconfig.Profile，表示通道的配置文件
//   - template：*cb.ConfigGroup，表示配置模板
//
// 返回值：
//   - *cb.Envelope：表示创建的交易的封包
//   - error：表示创建过程中可能发生的错误
func MakeChannelCreationTransactionFromTemplate(
	channelID string,
	signer identity.SignerSerializer,
	conf *genesisconfig.Profile,
	template *cb.ConfigGroup,
) (*cb.Envelope, error) {
	// 通过一个父模板, 生成一个 ConfigUpdate，可以发送给 orderer 来创建一个新的通道。
	newChannelConfigUpdate, err := NewChannelCreateConfigUpdate(channelID, conf, template)
	if err != nil {
		return nil, errors.Wrap(err, "配置更新生成失败")
	}

	newConfigUpdateEnv := &cb.ConfigUpdateEnvelope{
		ConfigUpdate: protoutil.MarshalOrPanic(newChannelConfigUpdate),
	}

	if signer != nil {
		sigHeader, err := protoutil.NewSignatureHeader(signer)
		if err != nil {
			return nil, errors.Wrap(err, "创建签名标头失败")
		}

		newConfigUpdateEnv.Signatures = []*cb.ConfigSignature{{
			SignatureHeader: protoutil.MarshalOrPanic(sigHeader),
		}}

		newConfigUpdateEnv.Signatures[0].Signature, err = signer.Sign(util.ConcatenateBytes(newConfigUpdateEnv.Signatures[0].SignatureHeader, newConfigUpdateEnv.ConfigUpdate))
		if err != nil {
			return nil, errors.Wrap(err, "配置更新上的签名失败")
		}

	}

	// 用于创建一个带有TLS绑定的已签名信封。
	return protoutil.CreateSignedEnvelope(cb.HeaderType_CONFIG_UPDATE, channelID, signer, newConfigUpdateEnv, msgVersion, epoch)
}

// HasSkippedForeignOrgs 用于检测配置文件是否包含不应解析的组织定义，
// 因为在当前上下文中，用户无法访问该组织的信息。
// 输入参数：
//   - conf：*genesisconfig.Profile，表示创世区块的配置文件
//
// 返回值：
//   - error：表示检测过程中可能发生的错误
func HasSkippedForeignOrgs(conf *genesisconfig.Profile) error {
	var organizations []*genesisconfig.Organization

	// 检查配置文件中是否存在 Orderer 部分，并将其中的组织添加到 organizations 切片中
	if conf.Orderer != nil {
		organizations = append(organizations, conf.Orderer.Organizations...)
	}

	// 检查配置文件中是否存在 Application 部分，并将其中的组织添加到 organizations 切片中
	if conf.Application != nil {
		organizations = append(organizations, conf.Application.Organizations...)
	}

	// 遍历配置文件中的 Consortiums 部分，并将其中的组织添加到 organizations 切片中
	for _, consortium := range conf.Consortiums {
		organizations = append(organizations, consortium.Organizations...)
	}

	// 遍历 organizations 切片，检查是否有被标记为跳过的外部组织
	for _, org := range organizations {
		if org.SkipAsForeign {
			return errors.Errorf("组织 %s 被标记为外部跳过", org.Name)
		}
	}

	return nil
}

// Bootstrapper 是一个围绕NewChannelConfigGroup的包装器，它可以产生创世块
type Bootstrapper struct {
	channelGroup *cb.ConfigGroup
}

// NewBootstrapper 创建一个 bootstrapper，但在出现错误时返回错误而不是 panic。
// 输入参数：
//   - config：*genesisconfig.Profile，表示创世区块的配置文件
//
// 返回值：
//   - *Bootstrapper：表示创建的 bootstrapper 实例
//   - error：表示创建过程中可能发生的错误
func NewBootstrapper(config *genesisconfig.Profile) (*Bootstrapper, error) {
	// 检查配置文件中是否存在跳过的外部组织, 检测配置文件是否包含不应解析的组织定义
	if err := HasSkippedForeignOrgs(config); err != nil {
		return nil, errors.WithMessage(err, "在 bootstrapper 引导期间，所有组织定义必须是本地的")
	}

	// 创建一个新的 ChannelGroup 实例
	channelGroup, err := NewChannelGroup(config)
	if err != nil {
		return nil, errors.WithMessage(err, "无法创建通道组")
	}

	return &Bootstrapper{
		channelGroup: channelGroup,
	}, nil
}

// New creates a new Bootstrapper for generating genesis blocks
func New(config *genesisconfig.Profile) *Bootstrapper {
	bs, err := NewBootstrapper(config)
	if err != nil {
		logger.Panicf("Error creating bootsrapper: %s", err)
	}
	return bs
}

// GenesisBlock produces a genesis block for the default test channel id
func (bs *Bootstrapper) GenesisBlock() *cb.Block {
	// TODO(mjs): remove
	return genesis.NewFactoryImpl(bs.channelGroup).Block("testchannelid")
}

// GenesisBlockForChannel 为给定的通道ID生成创世块
func (bs *Bootstrapper) GenesisBlockForChannel(channelID string) *cb.Block {
	// Block 促进创世纪块的创建。
	return genesis.NewFactoryImpl(bs.channelGroup).Block(channelID)
}
