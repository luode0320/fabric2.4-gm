/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

// UnmarshalConfig attempts to unmarshal bytes to a *cb.Config
func UnmarshalConfig(data []byte) (*cb.Config, error) {
	config := &cb.Config{}
	err := proto.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// UnmarshalConfigOrPanic attempts to unmarshal bytes to a *cb.Config or panics on error
func UnmarshalConfigOrPanic(data []byte) *cb.Config {
	result, err := UnmarshalConfig(data)
	if err != nil {
		panic(err)
	}
	return result
}

// UnmarshalConfigUpdate attempts to unmarshal bytes to a *cb.ConfigUpdate
func UnmarshalConfigUpdate(data []byte) (*cb.ConfigUpdate, error) {
	configUpdate := &cb.ConfigUpdate{}
	err := proto.Unmarshal(data, configUpdate)
	if err != nil {
		return nil, err
	}
	return configUpdate, nil
}

// UnmarshalConfigUpdateOrPanic attempts to unmarshal bytes to a *cb.ConfigUpdate or panics on error
func UnmarshalConfigUpdateOrPanic(data []byte) *cb.ConfigUpdate {
	result, err := UnmarshalConfigUpdate(data)
	if err != nil {
		panic(err)
	}
	return result
}

// UnmarshalConfigUpdateEnvelope attempts to unmarshal bytes to a *cb.ConfigUpdate
func UnmarshalConfigUpdateEnvelope(data []byte) (*cb.ConfigUpdateEnvelope, error) {
	configUpdateEnvelope := &cb.ConfigUpdateEnvelope{}
	err := proto.Unmarshal(data, configUpdateEnvelope)
	if err != nil {
		return nil, err
	}
	return configUpdateEnvelope, nil
}

// UnmarshalConfigUpdateEnvelopeOrPanic attempts to unmarshal bytes to a *cb.ConfigUpdateEnvelope or panics on error
func UnmarshalConfigUpdateEnvelopeOrPanic(data []byte) *cb.ConfigUpdateEnvelope {
	result, err := UnmarshalConfigUpdateEnvelope(data)
	if err != nil {
		panic(err)
	}
	return result
}

// UnmarshalConfigEnvelope attempts to unmarshal bytes to a *cb.ConfigEnvelope
func UnmarshalConfigEnvelope(data []byte) (*cb.ConfigEnvelope, error) {
	configEnv := &cb.ConfigEnvelope{}
	err := proto.Unmarshal(data, configEnv)
	if err != nil {
		return nil, err
	}
	return configEnv, nil
}

// UnmarshalConfigEnvelopeOrPanic attempts to unmarshal bytes to a *cb.ConfigEnvelope or panics on error
func UnmarshalConfigEnvelopeOrPanic(data []byte) *cb.ConfigEnvelope {
	result, err := UnmarshalConfigEnvelope(data)
	if err != nil {
		panic(err)
	}
	return result
}

// UnmarshalConfigUpdateFromPayload 函数从给定的有效载荷中解析配置更新。
func UnmarshalConfigUpdateFromPayload(payload *cb.Payload) (*cb.ConfigUpdate, error) {
	// 从有效载荷数据中解析配置信封
	configEnv, err := UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, err
	}

	// 从配置信封的最后更新中提取配置更新
	configUpdateEnv, err := protoutil.EnvelopeToConfigUpdate(configEnv.LastUpdate)
	if err != nil {
		return nil, err
	}

	// 解析配置更新
	return UnmarshalConfigUpdate(configUpdateEnv.ConfigUpdate)
}
