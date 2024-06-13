/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policies

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/flogging"
	mspi "github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

const (
	// Path separator is used to separate policy names in paths
	PathSeparator = "/"

	// ChannelPrefix is used in the path of standard channel policy managers
	ChannelPrefix = "Channel"

	// ApplicationPrefix is used in the path of standard application policy paths
	ApplicationPrefix = "Application"

	// OrdererPrefix is used in the path of standard orderer policy paths
	OrdererPrefix = "Orderer"

	// ChannelReaders is the label for the channel's readers policy (encompassing both orderer and application readers)
	ChannelReaders = PathSeparator + ChannelPrefix + PathSeparator + "Readers"

	// ChannelWriters is the label for the channel's writers policy (encompassing both orderer and application writers)
	ChannelWriters = PathSeparator + ChannelPrefix + PathSeparator + "Writers"

	// ChannelApplicationReaders 是通道的应用程序读取器策略的标签
	ChannelApplicationReaders = PathSeparator + ChannelPrefix + PathSeparator + ApplicationPrefix + PathSeparator + "Readers"

	// ChannelApplicationWriters 是通道的应用程序编写器策略的标签
	ChannelApplicationWriters = PathSeparator + ChannelPrefix + PathSeparator + ApplicationPrefix + PathSeparator + "Writers"

	// ChannelApplicationAdmins 是通道的应用程序管理策略的标签
	ChannelApplicationAdmins = PathSeparator + ChannelPrefix + PathSeparator + ApplicationPrefix + PathSeparator + "Admins"

	// BlockValidation 是应验证通道的块签名的策略的标签
	BlockValidation = PathSeparator + ChannelPrefix + PathSeparator + OrdererPrefix + PathSeparator + "BlockValidation"

	// ChannelOrdererAdmins 是通道的orderer admin策略的标签
	ChannelOrdererAdmins = PathSeparator + ChannelPrefix + PathSeparator + OrdererPrefix + PathSeparator + "Admins"

	// ChannelOrdererWriters 是通道的orderer writers策略的标签
	ChannelOrdererWriters = PathSeparator + ChannelPrefix + PathSeparator + OrdererPrefix + PathSeparator + "Writers"

	// ChannelOrdererReaders 是通道的orderer readers的策略
	ChannelOrdererReaders = PathSeparator + ChannelPrefix + PathSeparator + OrdererPrefix + PathSeparator + "Readers"
)

var logger = flogging.MustGetLogger("policies")

// PrincipalSet is a collection of MSPPrincipals
type PrincipalSet []*msp.MSPPrincipal

// PrincipalSets aggregates PrincipalSets
type PrincipalSets []PrincipalSet

// ContainingOnly returns PrincipalSets that contain only principals of the given predicate
func (psSets PrincipalSets) ContainingOnly(f func(*msp.MSPPrincipal) bool) PrincipalSets {
	var res PrincipalSets
	for _, set := range psSets {
		if !set.ContainingOnly(f) {
			continue
		}
		res = append(res, set)
	}
	return res
}

// ContainingOnly returns whether the given PrincipalSet contains only Principals
// that satisfy the given predicate
func (ps PrincipalSet) ContainingOnly(f func(*msp.MSPPrincipal) bool) bool {
	for _, principal := range ps {
		if !f(principal) {
			return false
		}
	}
	return true
}

// UniqueSet returns a histogram that is induced by the PrincipalSet
func (ps PrincipalSet) UniqueSet() map[*msp.MSPPrincipal]int {
	// Create a histogram that holds the MSPPrincipals and counts them
	histogram := make(map[struct {
		cls       int32
		principal string
	}]int)
	// Now, populate the histogram
	for _, principal := range ps {
		key := struct {
			cls       int32
			principal string
		}{
			cls:       int32(principal.PrincipalClassification),
			principal: string(principal.Principal),
		}
		histogram[key]++
	}
	// Finally, convert to a histogram of MSPPrincipal pointers
	res := make(map[*msp.MSPPrincipal]int)
	for principal, count := range histogram {
		res[&msp.MSPPrincipal{
			PrincipalClassification: msp.MSPPrincipal_Classification(principal.cls),
			Principal:               []byte(principal.principal),
		}] = count
	}
	return res
}

// Converter represents a policy
// which may be translated into a SignaturePolicyEnvelope
type Converter interface {
	Convert() (*cb.SignaturePolicyEnvelope, error)
}

// Policy 用于确定签名是否有效。
type Policy interface {
	// EvaluateSignedData 接受一组 SignedData 并评估：
	// 1) 签名是否对相关消息有效；
	// 2) 签名身份是否满足策略。
	// 参数：
	//   - signatureSet: []*protoutil.SignedData 类型，表示签名数据集。
	// 返回值：
	//   - error: 如果签名无效或签名身份不满足策略，则返回错误。
	EvaluateSignedData(signatureSet []*protoutil.SignedData) error

	// EvaluateIdentities 接受一个身份数组并评估是否满足策略。
	// 参数：
	//   - identities: []mspi.Identity 类型，表示身份数组。
	// 返回值：
	//   - error: 如果身份不满足策略，则返回错误。
	EvaluateIdentities(identities []mspi.Identity) error
}

// InquireablePolicy is a Policy that one can inquire
type InquireablePolicy interface {
	// SatisfiedBy returns a slice of PrincipalSets that each of them
	// satisfies the policy.
	SatisfiedBy() []PrincipalSet
}

// Manager 是策略 ManagerImpl 的只读子集。
type Manager interface {
	// GetPolicy 返回指定策略和 true，如果是请求的策略，则返回 true；如果是默认策略，则返回 false。
	// 参数：
	//   - id: string 类型，表示策略的标识符。
	// 返回值：
	//   - Policy: 表示策略。
	//   - bool: 表示是否是请求的策略。
	GetPolicy(id string) (Policy, bool)

	// Manager 返回给定路径的子策略管理器和是否存在。
	// 参数：
	//   - path: []string 类型，表示路径。
	// 返回值：
	//   - Manager: 表示子策略管理器。
	//   - bool: 表示子策略管理器是否存在。
	Manager(path []string) (Manager, bool)
}

// Provider provides the backing implementation of a policy
type Provider interface {
	// NewPolicy creates a new policy based on the policy bytes
	NewPolicy(data []byte) (Policy, proto.Message, error)
}

// ChannelPolicyManagerGetter 是一个支持接口，用于访问给定渠道的策略管理器
type ChannelPolicyManagerGetter interface {
	// Manager 返回与指定通道关联的策略管理器。
	Manager(channelID string) Manager
}

// PolicyManagerGetterFunc is a function adapater for ChannelPolicyManagerGetter.
type PolicyManagerGetterFunc func(channelID string) Manager

func (p PolicyManagerGetterFunc) Manager(channelID string) Manager { return p(channelID) }

// ManagerImpl is an implementation of Manager and configtx.ConfigHandler
// In general, it should only be referenced as an Impl for the configtx.ConfigManager
type ManagerImpl struct {
	path     string // The group level path
	Policies map[string]Policy
	managers map[string]*ManagerImpl
}

// NewManagerImpl 使用给定的CryptoHelper创建新的ManagerImpl
func NewManagerImpl(path string, providers map[int32]Provider, root *cb.ConfigGroup) (*ManagerImpl, error) {
	var err error
	_, ok := providers[int32(cb.Policy_IMPLICIT_META)]
	if ok {
		logger.Panicf("ImplicitMetaPolicy类型必须由策略管理器提供")
	}

	managers := make(map[string]*ManagerImpl)

	for groupName, group := range root.Groups {
		managers[groupName], err = NewManagerImpl(path+PathSeparator+groupName, providers, group)
		if err != nil {
			return nil, err
		}
	}

	policies := make(map[string]Policy)
	for policyName, configPolicy := range root.Policies {
		policy := configPolicy.Policy
		if policy == nil {
			return nil, fmt.Errorf("策略 %s (位于路径 %s) 为nil", policyName, path)
		}

		var cPolicy Policy

		if policy.Type == int32(cb.Policy_IMPLICIT_META) {
			imp, err := NewImplicitMetaPolicy(policy.Value, managers)
			if err != nil {
				return nil, errors.Wrapf(err, "隐式策略 %s (位于路径 %s) 未编译nil", policyName, path)
			}
			cPolicy = imp
		} else {
			provider, ok := providers[int32(policy.Type)]
			if !ok {
				return nil, fmt.Errorf("策略 %s (位于路径 %s) 具有未知的策略类型: %v", policyName, path, policy.Type)
			}

			var err error
			cPolicy, _, err = provider.NewPolicy(policy.Value)
			if err != nil {
				return nil, errors.Wrapf(err, "策略 %s (位于路径 %s) 未编译", policyName, path)
			}
		}

		policies[policyName] = cPolicy

		logger.Debugf("建议的新策略 %s (用于 %s)", policyName, path)
	}

	for groupName, manager := range managers {
		for policyName, policy := range manager.Policies {
			policies[groupName+PathSeparator+policyName] = policy
		}
	}

	return &ManagerImpl{
		path:     path,
		Policies: policies,
		managers: managers,
	}, nil
}

type rejectPolicy string

func (rp rejectPolicy) EvaluateSignedData(signedData []*protoutil.SignedData) error {
	return errors.Errorf("no such policy: '%s'", rp)
}

func (rp rejectPolicy) EvaluateIdentities(identities []mspi.Identity) error {
	return errors.Errorf("no such policy: '%s'", rp)
}

// Manager returns the sub-policy manager for a given path and whether it exists
func (pm *ManagerImpl) Manager(path []string) (Manager, bool) {
	logger.Debugf("Manager %s looking up path %v", pm.path, path)
	for manager := range pm.managers {
		logger.Debugf("Manager %s has managers %s", pm.path, manager)
	}
	if len(path) == 0 {
		return pm, true
	}

	m, ok := pm.managers[path[0]]
	if !ok {
		return nil, false
	}

	return m.Manager(path[1:])
}

type PolicyLogger struct {
	Policy     Policy
	policyName string
}

// EvaluateSignedData 方法用于评估给定的签名数据集是否满足策略要求。
// 方法接收者：pl *PolicyLogger，表示 PolicyLogger 对象的指针。
// 输入参数：
//   - signatureSet []*protoutil.SignedData，表示签名数据集的指针数组。
//
// 返回值：
//   - error，表示评估过程中的错误，如果签名数据集满足策略要求则返回nil。
func (pl *PolicyLogger) EvaluateSignedData(signatureSet []*protoutil.SignedData) error {
	// 如果日志级别为 Debug，则打印策略评估的开始和结束信息
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.Debugf("== 评价 %T 策略Policy %s ==", pl.Policy, pl.policyName)
		defer logger.Debugf("== 已完成评估 %T 策略Policy %s", pl.Policy, pl.policyName)
	}

	// 调用策略对象的 EvaluateSignedData 方法进行签名数据集的评估
	err := pl.Policy.EvaluateSignedData(signatureSet)
	if err != nil {
		logger.Debugf("签名集不符合策略: %s", pl.policyName)
	} else {
		logger.Debugf("签名集满足策略: %s", pl.policyName)
	}
	return err
}

func (pl *PolicyLogger) EvaluateIdentities(identities []mspi.Identity) error {
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.Debugf("== Evaluating %T Policy %s ==", pl.Policy, pl.policyName)
		defer logger.Debugf("== Done Evaluating %T Policy %s", pl.Policy, pl.policyName)
	}

	err := pl.Policy.EvaluateIdentities(identities)
	if err != nil {
		logger.Debugf("Signature set did not satisfy policy %s", pl.policyName)
	} else {
		logger.Debugf("Signature set satisfies policy %s", pl.policyName)
	}
	return err
}

func (pl *PolicyLogger) Convert() (*cb.SignaturePolicyEnvelope, error) {
	logger.Debugf("== Converting %T Policy %s ==", pl.Policy, pl.policyName)

	convertiblePolicy, ok := pl.Policy.(Converter)
	if !ok {
		logger.Errorf("policy (name='%s',type='%T') is not convertible to SignaturePolicyEnvelope", pl.policyName, pl.Policy)
		return nil, errors.Errorf("policy (name='%s',type='%T') is not convertible to SignaturePolicyEnvelope", pl.policyName, pl.Policy)
	}

	cp, err := convertiblePolicy.Convert()
	if err != nil {
		logger.Errorf("== Error Converting %T Policy %s, err %s", pl.Policy, pl.policyName, err.Error())
	} else {
		logger.Debugf("== Done Converting %T Policy %s", pl.Policy, pl.policyName)
	}

	return cp, err
}

// GetPolicy returns a policy and true if it was the policy requested, or false if it is the default reject policy
func (pm *ManagerImpl) GetPolicy(id string) (Policy, bool) {
	if id == "" {
		logger.Errorf("Returning dummy reject all policy because no policy ID supplied")
		return rejectPolicy(id), false
	}
	var relpath string

	if strings.HasPrefix(id, PathSeparator) {
		if !strings.HasPrefix(id, PathSeparator+pm.path) {
			logger.Debugf("Requested absolute policy %s from %s, returning rejectAll", id, pm.path)
			return rejectPolicy(id), false
		}
		// strip off the leading slash, the path, and the trailing slash
		relpath = id[1+len(pm.path)+1:]
	} else {
		relpath = id
	}

	policy, ok := pm.Policies[relpath]
	if !ok {
		logger.Debugf("Returning dummy reject all policy because %s could not be found in %s/%s", id, pm.path, relpath)
		return rejectPolicy(relpath), false
	}

	return &PolicyLogger{
		Policy:     policy,
		policyName: PathSeparator + pm.path + PathSeparator + relpath,
	}, true
}

// SignatureSetToValidIdentities 函数接收一个指向签名数据的切片，检查签名和签名者的有效性，并返回相关联的身份切片。返回的身份切片是去重的。
// 方法接收者：无。
// 输入参数：
//   - signedData []*protoutil.SignedData，表示指向签名数据的切片。
//   - identityDeserializer mspi.IdentityDeserializer，表示身份反序列化器。
//
// 返回值：
//   - []mspi.Identity，表示相关联的身份切片。
func SignatureSetToValidIdentities(signedData []*protoutil.SignedData, identityDeserializer mspi.IdentityDeserializer) []mspi.Identity {
	// 用于存储已经出现过的身份的映射
	idMap := map[string]struct{}{}
	// 创建一个身份切片，初始容量为签名数据切片的长度
	identities := make([]mspi.Identity, 0, len(signedData))

	for i, sd := range signedData {
		identity, err := identityDeserializer.DeserializeIdentity(sd.Identity)
		if err != nil {
			logger.Warnw("无效的身份标识", "error", err.Error(), "identity", protoutil.LogMessageForSerializedIdentity(sd.Identity))
			continue
		}

		key := identity.GetIdentifier().Mspid + identity.GetIdentifier().Id

		// 在进行签名检查之前，我们检查此标识是否已经出现，以确保
		// 有人不能强迫我们浪费时间检查同一签名数千次
		if _, ok := idMap[key]; ok {
			logger.Warningf("正在删除重复的标识 [%s] (在签名集的索引 %d 处)", key, i)
			continue
		}

		err = identity.Verify(sd.Data, sd.Signature)
		if err != nil {
			logger.Warningf("身份标识 %d 的签名无效: %s", i, err)
			continue
		}
		logger.Debugf("身份 %d 的签名已验证", i)

		idMap[key] = struct{}{}
		identities = append(identities, identity)
	}

	return identities
}
