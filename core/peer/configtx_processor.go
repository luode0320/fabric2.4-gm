/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peer

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	channelConfigKey = "CHANNEL_CONFIG_ENV_BYTES"
	peerNamespace    = ""
)

// ConfigTxProcessor 实现接口 'github.com/hyperledger/fabric/core/ledger/customtx/Processor'
type ConfigTxProcessor struct{}

// GenerateSimulationResults implements function in the interface 'github.com/hyperledger/fabric/core/ledger/customtx/Processor'
// This implementation processes CONFIG transactions which simply stores the config-envelope-bytes
func (tp *ConfigTxProcessor) GenerateSimulationResults(txEnv *common.Envelope, simulator ledger.TxSimulator, initializingLedger bool) error {
	payload := protoutil.UnmarshalPayloadOrPanic(txEnv.Payload)
	channelHdr := protoutil.UnmarshalChannelHeaderOrPanic(payload.Header.ChannelHeader)
	txType := common.HeaderType(channelHdr.GetType())

	switch txType {
	case common.HeaderType_CONFIG:
		peerLogger.Debugf("Processing CONFIG")
		if payload.Data == nil {
			return fmt.Errorf("channel config found nil")
		}
		return simulator.SetState(peerNamespace, channelConfigKey, payload.Data)
	default:
		return fmt.Errorf("tx type [%s] is not expected", txType)
	}
}

// retrieveChannelConfig 从查询执行器中检索通道配置。
// 输入参数：
//   - queryExecuter：ledger.QueryExecutor，表示查询执行器
//
// 返回值：
//   - *common.Config：表示通道配置的指针
//   - error：表示检索过程中可能出现的错误
func retrieveChannelConfig(queryExecuter ledger.QueryExecutor) (*common.Config, error) {
	// 从查询执行器中获取通道配置的字节数据, 获取给定命名空间和键的值。对于链码，命名空间对应于chaincodeId
	configBytes, err := queryExecuter.GetState(peerNamespace, channelConfigKey)
	if err != nil {
		return nil, err
	}

	// 如果通道配置的字节数据为空，则返回 nil
	if configBytes == nil {
		return nil, nil
	}

	// 创建 ConfigEnvelope 实例, 旨在为链包含 _all_配置，而不依赖于以前的配置事务。
	configEnvelope := &common.ConfigEnvelope{}

	// 反序列化通道配置的字节数据到 ConfigEnvelope 实例
	if err := proto.Unmarshal(configBytes, configEnvelope); err != nil {
		return nil, err
	}

	// 返回通道配置, 特定通道的配置
	return configEnvelope.Config, nil
}
