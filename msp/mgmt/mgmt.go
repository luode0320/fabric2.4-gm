/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mgmt

import (
	"sync"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/cache"
	"github.com/spf13/viper"
)

// FIXME: AS SOON AS THE CHAIN MANAGEMENT CODE IS COMPLETE,
// THESE MAPS AND HELPER FUNCTIONS SHOULD DISAPPEAR BECAUSE
// OWNERSHIP OF PER-CHAIN MSP MANAGERS WILL BE HANDLED BY IT;
// HOWEVER IN THE INTERIM, THESE HELPER FUNCTIONS ARE REQUIRED

var (
	m         sync.Mutex
	localMsp  msp.MSP
	mspMap    = make(map[string]msp.MSPManager)
	mspLogger = flogging.MustGetLogger("msp")
)

// TODO - this is a temporary solution to allow the peer to track whether the
// MSPManager has been setup for a channel, which indicates whether the channel
// exists or not
type mspMgmtMgr struct {
	msp.MSPManager
}

// GetManagerForChain returns the msp manager for the supplied
// chain; if no such manager exists, one is created
func GetManagerForChain(chainID string) msp.MSPManager {
	m.Lock()
	defer m.Unlock()

	mspMgr, ok := mspMap[chainID]
	if !ok {
		mspLogger.Debugf("Created new msp manager for channel `%s`", chainID)
		mspMgmtMgr := &mspMgmtMgr{msp.NewMSPManager()}
		mspMap[chainID] = mspMgmtMgr
		mspMgr = mspMgmtMgr
	}
	return mspMgr
}

// GetDeserializers 返回所有已注册的管理器
func GetDeserializers() map[string]msp.IdentityDeserializer {
	m.Lock()
	defer m.Unlock()

	clone := make(map[string]msp.IdentityDeserializer)

	for key, mspManager := range mspMap {
		clone[key] = mspManager
	}

	return clone
}

// XXXSetMSPManager is a stopgap solution to transition from the custom MSP config block
// parsing to the channelconfig.Resources interface, while preserving the problematic singleton
// nature of the MSP manager
func XXXSetMSPManager(chainID string, manager msp.MSPManager) {
	m.Lock()
	defer m.Unlock()

	mspMap[chainID] = &mspMgmtMgr{manager}
}

// GetLocalMSP 函数返回本地的 MSP（如果不存在则创建）。
// 参数：
//   - cryptoProvider bccsp.BCCSP：加密服务提供者。
//
// 返回值：
//   - msp.MSP：本地的 MSP。
func GetLocalMSP(cryptoProvider bccsp.BCCSP) msp.MSP {
	m.Lock()
	defer m.Unlock()

	// 如果本地的 MSP 已经存在，则直接返回
	if localMsp != nil {
		return localMsp
	}

	// 否则，加载本地的 MSP
	localMsp = loadLocalMSP(cryptoProvider)

	return localMsp
}

// loadLocalMSP 函数用于加载本地的 MSP。
// 参数：
//   - bccsp bccsp.BCCSP：加密服务提供者。
//
// 返回值：
//   - msp.MSP：加载到的本地 MSP。
func loadLocalMSP(bccsp bccsp.BCCSP) msp.MSP {
	// 确定 MSP 的类型（默认情况下，使用 bccsp MSP）
	mspType := viper.GetString("peer.localMspType")
	if mspType == "" {
		mspType = msp.ProviderTypeToString(msp.FABRIC)
	}

	// 根据 MSP 类型获取相应的选项, 主要msp版本兼容问题, 选择不同的版本实例化
	newOpts, found := msp.Options[mspType]
	if !found {
		mspLogger.Panicf("msp 类型 peer.localMspType = " + mspType + " 未知")
	}

	// 使用版本选项和加密服务提供者初始化 MSP
	mspInst, err := msp.New(newOpts, bccsp)
	if err != nil {
		mspLogger.Fatalf("初始化本地MSP失败, 收到错误 %+v", err)
	}

	// 根据 MSP 类型进行特定处理
	switch mspType {
	case msp.ProviderTypeToString(msp.FABRIC):
		// 对于 Fabric MSP，使用缓存包装 MSP 实例
		mspInst, err = cache.New(mspInst)
		if err != nil {
			mspLogger.Fatalf("初始化本地MSP失败, 收到错误 %+v", err)
		}
	case msp.ProviderTypeToString(msp.IDEMIX):
		// 对于 Idemix MSP，不进行任何处理
	default:
		panic("msp 类型 " + mspType + " 未知")
	}

	mspLogger.Debugf("已创建新的本地MSP")

	return mspInst
}

// GetIdentityDeserializer returns the IdentityDeserializer for the given chain
func GetIdentityDeserializer(chainID string, cryptoProvider bccsp.BCCSP) msp.IdentityDeserializer {
	if chainID == "" {
		return GetLocalMSP(cryptoProvider)
	}

	return GetManagerForChain(chainID)
}
