/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"sync"

	"github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric-protos-go/peer"
)

// InstalledChaincode 定义了已安装链码的元数据。
type InstalledChaincode struct {
	PackageID  string                 // 链码包ID
	Hash       []byte                 // 链码哈希值
	Label      string                 // 链码标签
	References map[string][]*Metadata // 通道名称到链码的映射元数据，表示使用此已安装链码包的通道和链码定义

	// FIXME: 我们应该删除这两个字段，因为它们不是链码的属性（FAB-14561）
	Name    string // 链码名称
	Version string // 链码版本
}

// Metadata 定义了链码的通道范围元数据。
type Metadata struct {
	Name               string                        // 链码名称
	Version            string                        // 链码版本
	Policy             []byte                        // 链码背书策略
	CollectionPolicies map[string][]byte             // 集合策略，仅对于 _lifecycle 链码有效，存储集合名称到集合的背书策略的映射
	Id                 []byte                        // 链码ID
	CollectionsConfig  *peer.CollectionConfigPackage // 集合配置
	Approved           bool                          // 是否已批准，仅对于 _lifecycle 链码有效
	Installed          bool                          // 是否已安装，仅对于 _lifecycle 链码有效
}

// MetadataSet defines an aggregation of Metadata
type MetadataSet []Metadata

// AsChaincodes converts this MetadataSet to a slice of gossip.Chaincodes
func (ccs MetadataSet) AsChaincodes() []*gossip.Chaincode {
	var res []*gossip.Chaincode
	for _, cc := range ccs {
		res = append(res, &gossip.Chaincode{
			Name:    cc.Name,
			Version: cc.Version,
		})
	}
	return res
}

// MetadataMapping 定义从链码名称到元数据的映射
type MetadataMapping struct {
	sync.RWMutex
	mdByName map[string]Metadata
}

// NewMetadataMapping creates a new metadata mapping
func NewMetadataMapping() *MetadataMapping {
	return &MetadataMapping{
		mdByName: make(map[string]Metadata),
	}
}

// Lookup returns the Metadata that is associated with the given chaincode
func (m *MetadataMapping) Lookup(cc string) (Metadata, bool) {
	m.RLock()
	defer m.RUnlock()
	md, exists := m.mdByName[cc]
	return md, exists
}

// Update updates the chaincode metadata in the mapping
func (m *MetadataMapping) Update(ccMd Metadata) {
	m.Lock()
	defer m.Unlock()
	m.mdByName[ccMd.Name] = ccMd
}

// Aggregate aggregates all Metadata to a MetadataSet
func (m *MetadataMapping) Aggregate() MetadataSet {
	m.RLock()
	defer m.RUnlock()
	var set MetadataSet
	for _, md := range m.mdByName {
		set = append(set, md)
	}
	return set
}
