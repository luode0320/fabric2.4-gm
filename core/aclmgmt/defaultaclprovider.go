/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package aclmgmt

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/core/aclmgmt/resources"
	"github.com/hyperledger/fabric/core/policy"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	CHANNELREADERS = policies.ChannelApplicationReaders
	CHANNELWRITERS = policies.ChannelApplicationWriters
)

type defaultACLProvider interface {
	ACLProvider
	IsPtypePolicy(resName string) bool
}

// defaultACLProviderImpl 是在未提供基于资源的 ACL 提供程序或者如果它不包含命名资源的策略时使用的默认 ACL 提供程序。
type defaultACLProviderImpl struct {
	policyChecker      policy.PolicyChecker
	pResourcePolicyMap map[string]string // peer 级别的策略（目前未使用）
	cResourcePolicyMap map[string]string // 通道特定的策略
}

// newDefaultACLProvider _lifecycle、LSCC、QSCC、CSCC、事件的策略的权限定义
func newDefaultACLProvider(policyChecker policy.PolicyChecker) defaultACLProvider {
	d := &defaultACLProviderImpl{
		policyChecker:      policyChecker,
		pResourcePolicyMap: map[string]string{},
		cResourcePolicyMap: map[string]string{},
	}

	//-------------- _lifecycle 是一个系统链码，用于管理和控制链码的生命周期 --------------
	// 生命周期安装链码需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_InstallChaincode] = policy.Admins
	// 生命周期查询已安装链码需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_QueryInstalledChaincode] = policy.Admins
	// 生命周期获取已安装的链码包需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_GetInstalledChaincodePackage] = policy.Admins
	// 生命周期查询所有已安装链码需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_QueryInstalledChaincodes] = policy.Admins
	// 组织的生命周期批准链码定义需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_ApproveChaincodeDefinitionForMyOrg] = policy.Admins
	// 生命周期查询审核链码定义需要管理员
	d.pResourcePolicyMap[resources.Lifecycle_QueryApprovedChaincodeDefinition] = policy.Admins

	// 生命周期提交链码定义需要写入权限
	d.cResourcePolicyMap[resources.Lifecycle_CommitChaincodeDefinition] = CHANNELWRITERS
	// 生命周期查询Chaincode定义需要写入权限
	d.cResourcePolicyMap[resources.Lifecycle_QueryChaincodeDefinition] = CHANNELWRITERS
	// 生命周期查询链代码定义需要写入权限
	d.cResourcePolicyMap[resources.Lifecycle_QueryChaincodeDefinitions] = CHANNELWRITERS
	// 生命周期检查提交准备情况需要写入权限
	d.cResourcePolicyMap[resources.Lifecycle_CheckCommitReadiness] = CHANNELWRITERS

	//-------------- snapshot ---------------
	// 快照提交请求需要管理员
	d.pResourcePolicyMap[resources.Snapshot_submitrequest] = policy.Admins
	// 快照取消请求需要管理员
	d.pResourcePolicyMap[resources.Snapshot_cancelrequest] = policy.Admins
	// 快照列表挂起需要管理员
	d.pResourcePolicyMap[resources.Snapshot_listpending] = policy.Admins

	//-------------- LSCC LSCC 提供了与链码生命周期相关的功能，包括安装链码、批准链码定义、提交链码定义等--------------
	// p资源 (目前由链码实现)
	// Lscc安装需要管理员
	d.pResourcePolicyMap[resources.Lscc_Install] = policy.Admins
	// Lscc获取已安装的链代码需要管理员
	d.pResourcePolicyMap[resources.Lscc_GetInstalledChaincodes] = policy.Admins

	// c resources
	d.cResourcePolicyMap[resources.Lscc_Deploy] = ""  // 提案涵盖的ACL检查
	d.cResourcePolicyMap[resources.Lscc_Upgrade] = "" // 提案涵盖的ACL检查
	// Lscc链代码存在需要读取权限
	d.cResourcePolicyMap[resources.Lscc_ChaincodeExists] = CHANNELREADERS
	// Lscc获取部署规范需要读取权限
	d.cResourcePolicyMap[resources.Lscc_GetDeploymentSpec] = CHANNELREADERS
	// Lscc获取链码数据需要读取权限
	d.cResourcePolicyMap[resources.Lscc_GetChaincodeData] = CHANNELREADERS
	// Lscc获取实例化的链码需要读取权限
	d.cResourcePolicyMap[resources.Lscc_GetInstantiatedChaincodes] = CHANNELREADERS
	// Lscc获取集合配置需要读取权限
	d.cResourcePolicyMap[resources.Lscc_GetCollectionsConfig] = CHANNELREADERS

	//-------------- QSCC QSCC 提供了一组查询功能，包括查询区块、查询交易、查询链码等--------------
	//p resources (none)

	// c resources
	// Qscc获取链信息需要读取权限
	d.cResourcePolicyMap[resources.Qscc_GetChainInfo] = CHANNELREADERS
	// Qscc按编号获取块需要读取权限
	d.cResourcePolicyMap[resources.Qscc_GetBlockByNumber] = CHANNELREADERS
	// Qscc通过哈希获取块需要读取权限
	d.cResourcePolicyMap[resources.Qscc_GetBlockByHash] = CHANNELREADERS
	// Qscc按ID获取事务需要读取权限
	d.cResourcePolicyMap[resources.Qscc_GetTransactionByID] = CHANNELREADERS
	// Qscc按Tx ID获取块需要读取权限
	d.cResourcePolicyMap[resources.Qscc_GetBlockByTxID] = CHANNELREADERS

	//--------------- CSCC  提供了一组功能，包括添加组织、更新通道配置、加入通道等 -----------
	// p资源 (目前由链码实现)
	// Cscc加入链需要管理员权限
	d.pResourcePolicyMap[resources.Cscc_JoinChain] = policy.Admins
	// Cscc通过快照加入链需要管理员权限
	d.pResourcePolicyMap[resources.Cscc_JoinChainBySnapshot] = policy.Admins
	// Cscc按快照状态加入需要管理员权限
	d.pResourcePolicyMap[resources.Cscc_JoinBySnapshotStatus] = policy.Admins
	// Cscc获取频道需要用户权限
	d.pResourcePolicyMap[resources.Cscc_GetChannels] = policy.Members

	// c resources
	// Cscc 获取Config配置块需要读取权限
	d.cResourcePolicyMap[resources.Cscc_GetConfigBlock] = CHANNELREADERS
	// Cscc获取通道配置需要读取权限
	d.cResourcePolicyMap[resources.Cscc_GetChannelConfig] = CHANNELREADERS

	//---------------- non-scc resources ------------
	//Peer resources
	// peer提案需要写入权限
	d.cResourcePolicyMap[resources.Peer_Propose] = CHANNELWRITERS
	// peer链码到链码需要写入权限
	d.cResourcePolicyMap[resources.Peer_ChaincodeToChaincode] = CHANNELWRITERS

	// 事件 资源
	// 事件块需要读取权限
	d.cResourcePolicyMap[resources.Event_Block] = CHANNELREADERS
	// 事件筛选块需要读取权限
	d.cResourcePolicyMap[resources.Event_FilteredBlock] = CHANNELREADERS

	// 网关 资源
	// 网关提交状态需要读取权限
	d.cResourcePolicyMap[resources.Gateway_CommitStatus] = CHANNELREADERS
	// 网关链码事件需要读取权限
	d.cResourcePolicyMap[resources.Gateway_ChaincodeEvents] = CHANNELREADERS

	return d
}

func (d *defaultACLProviderImpl) IsPtypePolicy(resName string) bool {
	_, ok := d.pResourcePolicyMap[resName]
	return ok
}

// CheckACL 提供默认（v1.0）行为，通过将资源映射到其通道的ACL来检查ACL。
// 方法接收者：d（defaultACLProviderImpl类型的指针）
// 输入参数：
//   - resName：资源名称。
//   - channelID：通道ID。
//   - idinfo：身份信息。
//
// 返回值：
//   - error：如果ACL检查失败，则返回错误；否则返回nil。
func (d *defaultACLProviderImpl) CheckACL(resName string, channelID string, idinfo interface{}) error {
	// 默认行为是，如果定义了p类型，则使用p类型，并使用无通道策略检查
	policy := d.pResourcePolicyMap[resName]
	if policy != "" {
		channelID = ""
	} else {
		policy = d.cResourcePolicyMap[resName]
		if policy == "" {
			aclLogger.Errorf("未映射的策略: %s", resName)
			return fmt.Errorf("未映射的策略: %s", resName)
		}
	}
	aclLogger.Debugw("资源应用执行默认访问策略", "channel", channelID, "policy", policy, "resource", resName)

	switch typedData := idinfo.(type) {
	case *pb.SignedProposal: // 带有签名的提案
		return d.policyChecker.CheckPolicy(channelID, policy, typedData)
	case *common.Envelope: // 带有签名的有效负载 ，以便可以对消息进行身份验证
		sd, err := protoutil.EnvelopeAsSignedData(typedData)
		if err != nil {
			return err
		}
		return d.policyChecker.CheckPolicyBySignedData(channelID, policy, sd)
	case *protoutil.SignedData:
		return d.policyChecker.CheckPolicyBySignedData(channelID, policy, []*protoutil.SignedData{typedData})
	case []*protoutil.SignedData:
		return d.policyChecker.CheckPolicyBySignedData(channelID, policy, typedData)
	default:
		aclLogger.Errorf("checkACL 权限控制上未映射身份id %s", resName)
		return fmt.Errorf("checkACL 权限控制上的未知身份id %s", resName)
	}
}

// CheckACL provides default behavior by mapping channelless resources to their ACL.
func (d *defaultACLProviderImpl) CheckACLNoChannel(resName string, idinfo interface{}) error {
	policy := d.pResourcePolicyMap[resName]
	if policy == "" {
		aclLogger.Errorf("Unmapped channelless policy for %s", resName)
		return fmt.Errorf("Unmapped channelless policy for %s", resName)
	}

	switch typedData := idinfo.(type) {
	case *pb.SignedProposal:
		return d.policyChecker.CheckPolicyNoChannel(policy, typedData)
	case *common.Envelope:
		sd, err := protoutil.EnvelopeAsSignedData(typedData)
		if err != nil {
			return err
		}
		return d.policyChecker.CheckPolicyNoChannelBySignedData(policy, sd)
	case []*protoutil.SignedData:
		return d.policyChecker.CheckPolicyNoChannelBySignedData(policy, typedData)
	default:
		aclLogger.Errorf("Unmapped id on channelless checkACL %s", resName)
		return fmt.Errorf("Unknown id on channelless checkACL %s", resName)
	}
}
