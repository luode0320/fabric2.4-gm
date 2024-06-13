/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package decoration

import (
	"github.com/hyperledger/fabric-protos-go/peer"
)

// Decorator decorates a chaincode input
type Decorator interface {
	// Decorate decorates a chaincode input by changing it
	Decorate(proposal *peer.Proposal, input *peer.ChaincodeInput) *peer.ChaincodeInput
}

// Apply 在提供的顺序中应用装饰器。
// 输入参数：
//   - proposal：提案对象。
//   - input：链码输入对象。
//   - decorators：要应用的装饰器列表。
//
// 返回值：
//   - *peer.ChaincodeInput：应用装饰器后的链码输入对象。
func Apply(proposal *peer.Proposal, input *peer.ChaincodeInput, decorators ...Decorator) *peer.ChaincodeInput {
	for _, decorator := range decorators {
		input = decorator.Decorate(proposal, input)
	}

	return input
}
