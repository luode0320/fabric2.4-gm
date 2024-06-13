/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cceventmgmt

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
)

// ChaincodeDefinition captures the info about chaincode
type ChaincodeDefinition struct {
	Name              string
	Hash              []byte
	Version           string
	CollectionConfigs *peer.CollectionConfigPackage
}

func (cdef *ChaincodeDefinition) String() string {
	if cdef != nil {
		return fmt.Sprintf("Name=%s, Version=%s, Hash=%x", cdef.Name, cdef.Version, cdef.Hash)
	}
	return "<nil>"
}

// ChaincodeLifecycleEventListener 接口使账本组件（主要是状态数据库）能够监听链码生命周期事件。
// 'dbArtifactsTar' 表示打包在 tar 文件中的特定于数据库的工件（例如索引规范）。
type ChaincodeLifecycleEventListener interface {
	// HandleChaincodeDeploy 在链码安装和定义为 true 时调用。
	// 预期的用法是创建所有必要的状态数据库结构（例如索引）并更新服务发现信息。
	// 此函数在包含链码定义的状态更改提交之前立即调用，或者在发生链码安装时调用。
	HandleChaincodeDeploy(chaincodeDefinition *ChaincodeDefinition, dbArtifactsTar []byte) error

	// ChaincodeDeployDone 在链码部署完成后调用 - `succeeded` 表示部署是否成功完成。
	ChaincodeDeployDone(succeeded bool)
}

// ChaincodeInfoProvider 接口使事件管理器能够为给定的链码检索链码信息。
type ChaincodeInfoProvider interface {
	// GetDeployedChaincodeInfo 检索部署的链码的详细信息。
	// 如果给定名称的链码未部署，或者部署的链码的版本或哈希与给定的版本和哈希不匹配，
	// 则此函数应返回 nil。
	GetDeployedChaincodeInfo(chainID string, chaincodeDefinition *ChaincodeDefinition) (*ledger.DeployedChaincodeInfo, error)

	// RetrieveChaincodeArtifacts 检查给定的链码是否已安装在对等节点上，
	// 如果是，则从链码包 tarball 中提取特定于状态数据库的工件。
	RetrieveChaincodeArtifacts(chaincodeDefinition *ChaincodeDefinition) (installed bool, dbArtifactsTar []byte, err error)
}
