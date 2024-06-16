/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
)

// Validator 提供了一种机制来提议配置更新、查看配置更新的结果以及验证配置更新的结果。
type Validator interface {
	// Validate 试图应用一个配置交易包以成为新的配置。如果应用成功则返回nil，否则返回错误。
	Validate(configEnv *cb.ConfigEnvelope) error

	// ProposeConfigUpdate 试图基于当前配置状态验证一个新的配置更新交易包，并返回更新后的配置交易包。
	// 如果验证成功，则返回新的配置交易包；如果验证失败，则返回错误。
	ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error)

	// ChannelID 返回与此管理者关联的通道ID。
	ChannelID() string

	// ConfigProto 返回当前配置的状态作为一个协议缓冲区（proto）消息。
	ConfigProto() *cb.Config

	// Sequence 返回当前配置的序列号。
	Sequence() uint64
}
