/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package aclmgmt

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/protoutil"
)

//--------- errors ---------

// PolicyNotFound cache for resource
type PolicyNotFound string

func (e PolicyNotFound) Error() string {
	return fmt.Sprintf("policy %s not found", string(e))
}

// InvalidIdInfo
type InvalidIdInfo string

func (e InvalidIdInfo) Error() string {
	return fmt.Sprintf("Invalid id for policy [%s]", string(e))
}

//---------- policyEvaluator ------

// policyEvalutor interface provides the interfaces for policy evaluation
type policyEvaluator interface {
	PolicyRefForAPI(resName string) string
	Evaluate(polName string, id []*protoutil.SignedData) error
}

// policyEvaluatorImpl implements policyEvaluator
type policyEvaluatorImpl struct {
	bundle channelconfig.Resources
}

func (pe *policyEvaluatorImpl) PolicyRefForAPI(resName string) string {
	app, exists := pe.bundle.ApplicationConfig()
	if !exists {
		return ""
	}

	pm := app.APIPolicyMapper()
	if pm == nil {
		return ""
	}

	return pm.PolicyRefForAPI(resName)
}

func (pe *policyEvaluatorImpl) Evaluate(polName string, sd []*protoutil.SignedData) error {
	policy, ok := pe.bundle.PolicyManager().GetPolicy(polName)
	if !ok {
		return PolicyNotFound(polName)
	}

	err := policy.EvaluateSignedData(sd)
	if err != nil {
		aclLogger.Warnw("EvaluateSignedData policy check failed", "error", err, "policyName", polName, policy, "policy", "signingIdentities", protoutil.LogMessageForSerializedIdentities(sd))
	}
	return err
}

//------ resourcePolicyProvider ----------

// aclmgmtPolicyProvider is the interface implemented by resource based ACL.
type aclmgmtPolicyProvider interface {
	// GetPolicyName returns policy name given resource name
	GetPolicyName(resName string) string

	// CheckACL backs ACLProvider interface
	CheckACL(polName string, idinfo interface{}) error
}

// aclmgmtPolicyProviderImpl holds the bytes from state of the ledger
type aclmgmtPolicyProviderImpl struct {
	pEvaluator policyEvaluator
}

// GetPolicyName returns the policy name given the resource string
func (rp *aclmgmtPolicyProviderImpl) GetPolicyName(resName string) string {
	return rp.pEvaluator.PolicyRefForAPI(resName)
}

// CheckACL implements AClProvider's CheckACL interface so it can be registered
// as a provider with aclmgmt
func (rp *aclmgmtPolicyProviderImpl) CheckACL(polName string, idinfo interface{}) error {
	aclLogger.Debugf("acl check(%s)", polName)

	// we will implement other identifiers. In the end we just need a SignedData
	var sd []*protoutil.SignedData
	switch idinfo := idinfo.(type) {
	case *pb.SignedProposal:
		signedProp := idinfo
		proposal, err := protoutil.UnmarshalProposal(signedProp.ProposalBytes)
		if err != nil {
			return fmt.Errorf("Failing extracting proposal during check policy with policy [%s]: [%s]", polName, err)
		}

		header, err := protoutil.UnmarshalHeader(proposal.Header)
		if err != nil {
			return fmt.Errorf("Failing extracting header during check policy [%s]: [%s]", polName, err)
		}

		shdr, err := protoutil.UnmarshalSignatureHeader(header.SignatureHeader)
		if err != nil {
			return fmt.Errorf("Invalid Proposal's SignatureHeader during check policy [%s]: [%s]", polName, err)
		}

		sd = []*protoutil.SignedData{{
			Data:      signedProp.ProposalBytes,
			Identity:  shdr.Creator,
			Signature: signedProp.Signature,
		}}

	case *common.Envelope:
		var err error
		sd, err = protoutil.EnvelopeAsSignedData(idinfo)
		if err != nil {
			return err
		}

	case *protoutil.SignedData:
		sd = []*protoutil.SignedData{idinfo}

	default:
		return InvalidIdInfo(polName)
	}

	err := rp.pEvaluator.Evaluate(polName, sd)
	if err != nil {
		return fmt.Errorf("failed evaluating policy on signed data during check policy [%s]: [%s]", polName, err)
	}

	return nil
}

//-------- resource provider - entry point API used by aclmgmtimpl for doing resource based ACL ----------

// ResourceGetter 资源getter获取channelconfig.Resources给定信道ID
type ResourceGetter func(channelID string) channelconfig.Resources

// 使用资源配置信息提供ACL支持的资源提供程序
type resourceProvider struct {
	// 资源getter获取channelconfig.Resources给定信道ID
	resGetter ResourceGetter

	// 用于未定义资源的默认提供程序
	defaultProvider defaultACLProvider
}

// 创建新的 resourceProvider
func newResourceProvider(rg ResourceGetter, defprov defaultACLProvider) *resourceProvider {
	return &resourceProvider{rg, defprov}
}

// enforceDefaultBehavior 强制执行默认行为，如果定义了p类型，则使用p类型。
// 方法接收者：rp（resourceProvider类型的指针）
// 输入参数：
//   - resName：资源名称。
//   - channelID：通道ID。
//   - idinfo：身份信息。
//
// 返回值：
//   - bool：如果使用p类型，则返回true；否则返回false。
func (rp *resourceProvider) enforceDefaultBehavior(resName string, channelID string, idinfo interface{}) bool {
	// 当前我们强制使用p类型，如果定义了p类型。将来我们将允许通过对等配置来覆盖p类型。
	return rp.defaultProvider.IsPtypePolicy(resName)
}

// CheckACL 实现了ACL（访问控制列表）。
// 方法接收者：rp（resourceProvider类型的指针）
// 输入参数：
//   - resName：资源名称。
//   - channelID：通道ID。
//   - idinfo：身份信息。
//
// 返回值：
//   - error：如果ACL检查失败，则返回错误；否则返回nil。
func (rp *resourceProvider) CheckACL(resName string, channelID string, idinfo interface{}) error {
	// 如果不强制执行默认行为，则执行以下逻辑
	if !rp.enforceDefaultBehavior(resName, channelID, idinfo) {
		// 获取资源配置
		resCfg := rp.resGetter(channelID)

		if resCfg != nil {
			// 创建ACL管理策略提供者
			pp := &aclmgmtPolicyProviderImpl{&policyEvaluatorImpl{resCfg}}
			// 获取资源的策略名称
			policyName := pp.GetPolicyName(resName)
			if policyName != "" {
				aclLogger.Debugf("acl policy 权限控制策略 %s 在资源的配置 config 中找到 %s", policyName, resName)
				// 检查ACL
				return pp.CheckACL(policyName, idinfo)
			}
			aclLogger.Debugf("acl policy 权限控制策略在资源的配置 config 中找不到 %s", resName)
		}
	}

	// 使用默认提供者检查权限 ACL
	return rp.defaultProvider.CheckACL(resName, channelID, idinfo)
}

// CheckACLNoChannel implements the ACLProvider interface function
func (rp *resourceProvider) CheckACLNoChannel(resName string, idinfo interface{}) error {
	if !rp.enforceDefaultBehavior(resName, "", idinfo) {
		return fmt.Errorf("cannot override peer type policy for channeless ACL check")
	}

	return rp.defaultProvider.CheckACLNoChannel(resName, idinfo)
}
