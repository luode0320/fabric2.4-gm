/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cauthdsl

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

type provider struct {
	deserializer msp.IdentityDeserializer
}

// NewPolicyProvider provides a policy generator for cauthdsl type policies
func NewPolicyProvider(deserializer msp.IdentityDeserializer) policies.Provider {
	return &provider{
		deserializer: deserializer,
	}
}

// NewPolicy creates a new policy based on the policy bytes
func (pr *provider) NewPolicy(data []byte) (policies.Policy, proto.Message, error) {
	sigPolicy := &cb.SignaturePolicyEnvelope{}
	if err := proto.Unmarshal(data, sigPolicy); err != nil {
		return nil, nil, fmt.Errorf("Error unmarshalling to SignaturePolicy: %s", err)
	}

	if sigPolicy.Version != 0 {
		return nil, nil, fmt.Errorf("This evaluator only understands messages of version 0, but version was %d", sigPolicy.Version)
	}

	compiled, err := compile(sigPolicy.Rule, sigPolicy.Identities)
	if err != nil {
		return nil, nil, err
	}

	return &policy{
		evaluator:               compiled,
		deserializer:            pr.deserializer,
		signaturePolicyEnvelope: sigPolicy,
	}, sigPolicy, nil
}

// EnvelopeBasedPolicyProvider allows to create a new policy from SignaturePolicyEnvelope struct instead of []byte
type EnvelopeBasedPolicyProvider struct {
	Deserializer msp.IdentityDeserializer
}

// NewPolicy creates a new policy from the policy envelope
func (pp *EnvelopeBasedPolicyProvider) NewPolicy(sigPolicy *cb.SignaturePolicyEnvelope) (policies.Policy, error) {
	if sigPolicy == nil {
		return nil, errors.New("invalid arguments")
	}

	compiled, err := compile(sigPolicy.Rule, sigPolicy.Identities)
	if err != nil {
		return nil, err
	}

	return &policy{
		evaluator:               compiled,
		deserializer:            pp.Deserializer,
		signaturePolicyEnvelope: sigPolicy,
	}, nil
}

type policy struct {
	signaturePolicyEnvelope *cb.SignaturePolicyEnvelope
	evaluator               func([]msp.Identity, []bool) bool
	deserializer            msp.IdentityDeserializer
}

// EvaluateSignedData 方法接收一个签名数据集，并评估以下两个条件是否满足：
// 1) 签名是否对应相关消息有效；
// 2) 签名身份是否满足策略要求。
// 方法接收者：p *policy，表示 policy 结构体的指针。
// 输入参数：
//   - signatureSet []*protoutil.SignedData，表示签名数据集的指针数组。
//
// 返回值：
//   - error，表示评估过程中的错误，如果签名数据集满足策略要求则返回nil。
func (p *policy) EvaluateSignedData(signatureSet []*protoutil.SignedData) error {
	// 检查 policy 是否为 nil
	if p == nil {
		return errors.New("没有这样的策略")
	}

	// 函数接收一个指向签名数据的切片，检查签名和签名者的有效性，并返回相关联的身份切片。返回的身份切片是去重的
	ids := policies.SignatureSetToValidIdentities(signatureSet, p.deserializer)

	// 方法接收一个身份切片，并评估这些身份是否满足策略要求
	return p.EvaluateIdentities(ids)
}

// EvaluateIdentities 方法接收一个身份切片，并评估这些身份是否满足策略要求。
// 方法接收者：p *policy，表示 policy 结构体的指针。
// 输入参数：
//   - identities []msp.Identity，表示身份切片。
//
// 返回值：
//   - error，表示评估过程中的错误，如果身份切片满足策略要求则返回nil。
func (p *policy) EvaluateIdentities(identities []msp.Identity) error {
	// 检查 policy 是否为 nil
	if p == nil {
		return fmt.Errorf("没有这样的策略")
	}

	// 调用策略的 evaluator 方法评估身份切片是否满足策略要求
	ok := p.evaluator(identities, make([]bool, len(identities)))
	if !ok {
		return errors.New("签名集不符合策略")
	}
	return nil
}

func (p *policy) Convert() (*cb.SignaturePolicyEnvelope, error) {
	if p.signaturePolicyEnvelope == nil {
		return nil, errors.New("nil policy field")
	}

	return p.signaturePolicyEnvelope, nil
}
