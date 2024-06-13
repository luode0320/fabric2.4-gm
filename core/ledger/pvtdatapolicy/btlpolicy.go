/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pvtdatapolicy

import (
	"math"
	"sync"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/common/privdata"
)

var defaultBTL uint64 = math.MaxUint64

// BTLPolicy BlockToLive policy for the pvt data
type BTLPolicy interface {
	// GetBTL returns BlockToLive for a given namespace and collection
	GetBTL(ns string, coll string) (uint64, error)
	// GetExpiringBlock returns the block number by which the pvtdata for given namespace,collection, and committingBlock should expire
	GetExpiringBlock(namespace string, collection string, committingBlock uint64) (uint64, error)
}

// LSCCBasedBTLPolicy 实现了 BTLPolicy 接口。
// 此实现从 lscc 命名空间加载 BTL 策略，该命名空间在链码初始化期间填充了集合配置。
type LSCCBasedBTLPolicy struct {
	collInfoProvider collectionInfoProvider // 集合信息提供者
	cache            map[btlkey]uint64      // 缓存
	lock             sync.Mutex             // 锁
}

type btlkey struct {
	ns   string
	coll string
}

// ConstructBTLPolicy 构造一个LSCCBasedBTLPolicy实例。
// 方法接收者：无（函数）
// 输入参数：
//   - collInfoProvider：collectionInfoProvider类型的实例，用于提供集合信息。
//
// 返回值：
//   - BTLPolicy：构造的BTLPolicy实例。
func ConstructBTLPolicy(collInfoProvider collectionInfoProvider) BTLPolicy {
	// 创建一个LSCCBasedBTLPolicy实例
	return &LSCCBasedBTLPolicy{
		collInfoProvider: collInfoProvider,
		cache:            make(map[btlkey]uint64),
	}
}

// GetBTL implements corresponding function in interface `BTLPolicyMgr`
func (p *LSCCBasedBTLPolicy) GetBTL(namespace string, collection string) (uint64, error) {
	var btl uint64
	var ok bool
	key := btlkey{namespace, collection}
	p.lock.Lock()
	defer p.lock.Unlock()
	btl, ok = p.cache[key]
	if !ok {
		collConfig, err := p.collInfoProvider.CollectionInfo(namespace, collection)
		if err != nil {
			return 0, err
		}
		if collConfig == nil {
			return 0, privdata.NoSuchCollectionError{Namespace: namespace, Collection: collection}
		}
		btlConfigured := collConfig.BlockToLive
		if btlConfigured > 0 {
			btl = uint64(btlConfigured)
		} else {
			btl = defaultBTL
		}
		p.cache[key] = btl
	}
	return btl, nil
}

// GetExpiringBlock implements function from the interface `BTLPolicy`
func (p *LSCCBasedBTLPolicy) GetExpiringBlock(namespace string, collection string, committingBlock uint64) (uint64, error) {
	btl, err := p.GetBTL(namespace, collection)
	if err != nil {
		return 0, err
	}
	return ComputeExpiringBlock(namespace, collection, committingBlock, btl), nil
}

func ComputeExpiringBlock(namespace, collection string, committingBlock, btl uint64) uint64 {
	expiryBlk := committingBlock + btl + uint64(1)
	if expiryBlk <= committingBlock { // committingBlk + btl overflows uint64-max
		expiryBlk = math.MaxUint64
	}
	return expiryBlk
}

type collectionInfoProvider interface {
	CollectionInfo(chaincodeName, collectionName string) (*peer.StaticCollectionConfig, error)
}

//go:generate counterfeiter -o mock/coll_info_provider.go -fake-name CollectionInfoProvider . collectionInfoProvider
