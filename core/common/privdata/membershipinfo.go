/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/implicitcollection"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
)

var logger = flogging.MustGetLogger("common.privdata")

// MembershipProvider 可用于检查对等方是否有资格收集
type MembershipProvider struct {
	mspID                       string
	selfSignedData              protoutil.SignedData // 自签名数据
	IdentityDeserializerFactory func(chainID string) msp.IdentityDeserializer
	myImplicitCollectionName    string
}

// NewMembershipInfoProvider 返回 MembershipProvider 会员提供商
func NewMembershipInfoProvider(mspID string, selfSignedData protoutil.SignedData, identityDeserializerFunc func(chainID string) msp.IdentityDeserializer) *MembershipProvider {
	return &MembershipProvider{
		mspID:                       mspID,
		selfSignedData:              selfSignedData,                       // 自签名数据
		IdentityDeserializerFactory: identityDeserializerFunc,             // 根据通道名称获取对应的身份反序列化器，用于处理与身份相关的操作
		myImplicitCollectionName:    implicitcollection.NameForOrg(mspID), // 组织的隐式集合的名称
	}
}

// AmMemberOf checks whether the current peer is a member of the given collection config.
// If getPolicy returns an error, it will drop the error and return false - same as a RejectAll policy.
// It is used when a chaincode is upgraded to see if the peer's org has become eligible after	a collection
// change.
func (m *MembershipProvider) AmMemberOf(channelName string, collectionPolicyConfig *peer.CollectionPolicyConfig) (bool, error) {
	deserializer := m.IdentityDeserializerFactory(channelName)

	// Do a simple check to see if the mspid matches any principal identities in the SignaturePolicy - FAB-17059
	if collectionPolicyConfig.GetSignaturePolicy() != nil {
		memberOrgs := getMemberOrgs(collectionPolicyConfig.GetSignaturePolicy().GetIdentities(), deserializer)

		if _, ok := memberOrgs[m.mspID]; ok {
			return true, nil
		}
	}

	// Fall back to default access policy evaluation otherwise
	accessPolicy, err := getPolicy(collectionPolicyConfig, deserializer)
	if err != nil {
		// drop the error and return false - same as reject all policy
		logger.Errorf("Reject all due to error getting policy: %s", err)
		return false, nil
	}
	if err := accessPolicy.EvaluateSignedData([]*protoutil.SignedData{&m.selfSignedData}); err != nil {
		return false, nil
	}

	return true, nil
}

func (m *MembershipProvider) MyImplicitCollectionName() string {
	return m.myImplicitCollectionName
}
