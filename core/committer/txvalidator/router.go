/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/channelconfig"
)

//go:generate mockery -dir . -name Validator -case underscore -output mocks

// Validator 定义了一个用于验证区块中交易有效性的接口
type Validator interface {
	// Validate 方法接收一个区块作为参数，如果验证成功，该方法会修改区块以反映其内部交易的有效性。
	// 如果验证无法成功完成，方法将返回一个错误。
	//
	// 参数:
	//   - block (*common.Block): 需要验证的区块指针。
	//
	// 返回:
	//   - error: 如果验证过程中出现任何问题，则返回相应的错误；否则返回nil。
	//
	// 注意:
	//   - 在验证成功的情况下，区块对象将被修改，以包含关于其内部交易有效性的信息。
	//   - 验证过程可能包括但不限于：检查交易签名、验证交易逻辑、确保交易顺序正确等。
	Validate(block *common.Block) error
}

//go:generate mockery -dir . -name CapabilityProvider -case underscore -output mocks

// CapabilityProvider contains functions to retrieve capability information for a channel
type CapabilityProvider interface {
	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

// ValidationRouter dynamically invokes the appropriate validator depending on the
// capabilities that are currently enabled in the channel.
type ValidationRouter struct {
	CapabilityProvider
	V20Validator Validator
	V14Validator Validator
}

// Validate 方法负责根据当前系统的版本能力，选择并执行适当的区块验证逻辑。
// 如果验证无法成功完成，该方法将返回一个错误。
// 在成功验证的情况下，区块将被修改，以反映其内部交易的有效性。
func (v *ValidationRouter) Validate(block *common.Block) error {
	// 根据系统版本能力判断应采用哪种验证逻辑
	switch {
	// 如果系统支持V2.0版本的验证能力
	case v.Capabilities().V2_0Validation():
		// 使用V2.0版本的验证器执行验证
		return v.V20Validator.Validate(block)
	// 如果系统不支持V2.0版本的验证能力（默认情况）
	default:
		// 使用V1.4版本的验证器执行验证
		return v.V14Validator.Validate(block)
	}
}
