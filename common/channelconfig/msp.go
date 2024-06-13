/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channelconfig

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	mspprotos "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/cache"
	"github.com/pkg/errors"
)

type pendingMSPConfig struct {
	mspConfig *mspprotos.MSPConfig
	msp       msp.MSP
}

// MSPConfigHandler 处理 MSP 配置
type MSPConfigHandler struct {
	version msp.MSPVersion               // MSP 版本
	idMap   map[string]*pendingMSPConfig // MSP 配置映射
	bccsp   bccsp.BCCSP                  // BCCSP（区块链加密服务提供者）
}

// NewMSPConfigHandler 创建一个新的 MSPConfigHandler。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - mspVersion：msp.MSPVersion，表示 MSP 的版本
//   - bccsp：bccsp.BCCSP，表示 BCCSP（区块链加密服务提供者）
//
// 返回值：
//   - *MSPConfigHandler：表示 MSPConfigHandler 的指针
func NewMSPConfigHandler(mspVersion msp.MSPVersion, bccsp bccsp.BCCSP) *MSPConfigHandler {
	// 创建 MSPConfigHandler 实例，并初始化版本、MSP 配置映射和 BCCSP
	return &MSPConfigHandler{
		version: mspVersion,                         // MSP 版本
		idMap:   make(map[string]*pendingMSPConfig), // MSP 配置映射
		bccsp:   bccsp,                              // BCCSP（区块链加密服务提供者）
	}
}

// ProposeMSP called when an org defines an MSP
func (bh *MSPConfigHandler) ProposeMSP(mspConfig *mspprotos.MSPConfig) (msp.MSP, error) {
	var theMsp msp.MSP
	var err error

	switch mspConfig.Type {
	case int32(msp.FABRIC):
		// create the bccsp msp instance
		mspInst, err := msp.New(
			&msp.BCCSPNewOpts{NewBaseOpts: msp.NewBaseOpts{Version: bh.version}},
			bh.bccsp,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "创建MSP管理器失败")
		}

		// 在顶部添加缓存层
		theMsp, err = cache.New(mspInst)
		if err != nil {
			return nil, errors.WithMessage(err, "创建MSP缓存失败")
		}
	case int32(msp.IDEMIX):
		// create the idemix msp instance
		theMsp, err = msp.New(
			&msp.IdemixNewOpts{NewBaseOpts: msp.NewBaseOpts{Version: bh.version}},
			bh.bccsp,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "creating the MSP manager failed")
		}
	default:
		return nil, errors.New(fmt.Sprintf("Setup error: unsupported msp type %d", mspConfig.Type))
	}

	// 设置msp
	err = theMsp.Setup(mspConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "设置MSP管理器失败")
	}

	// 将MSP添加到待定MSP的映射中
	mspID, _ := theMsp.GetIdentifier()

	existingPendingMSPConfig, ok := bh.idMap[mspID]
	if ok && !proto.Equal(existingPendingMSPConfig.mspConfig, mspConfig) {
		return nil, errors.New(fmt.Sprintf("尝试定义两个不同版本、相同组织的MSP: %s", mspID))
	}

	if !ok {
		bh.idMap[mspID] = &pendingMSPConfig{
			mspConfig: mspConfig,
			msp:       theMsp,
		}
	}

	return theMsp, nil
}

// CreateMSPManager 创建一个 MSPManager 实例。
// 方法接收者：
//   - bh：*MSPConfigHandler，表示 MSPConfigHandler 的指针
//
// 输入参数：
//   - 无
//
// 返回值：
//   - msp.MSPManager：表示 MSPManager 实例
//   - error：表示创建过程中可能出现的错误
func (bh *MSPConfigHandler) CreateMSPManager() (msp.MSPManager, error) {
	// 创建一个 msp.MSP 切片，长度为 bh.idMap 的长度
	mspList := make([]msp.MSP, len(bh.idMap))
	i := 0
	// 遍历 bh.idMap 中的每个 pendingMSP，将其 msp 添加到 mspList 中
	for _, pendingMSP := range bh.idMap {
		mspList[i] = pendingMSP.msp
		i++
	}

	// 创建一个新的 MSPManager 实例
	manager := msp.NewMSPManager()
	// 使用 mspList 设置 MSPManager
	err := manager.Setup(mspList)
	return manager, err
}
