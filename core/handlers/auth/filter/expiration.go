/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package filter

import (
	"context"
	"time"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/core/handlers/auth"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// NewExpirationCheckFilter creates a new Filter that checks identity expiration
func NewExpirationCheckFilter() auth.Filter {
	return &expirationCheckFilter{}
}

type expirationCheckFilter struct {
	next peer.EndorserServer
}

// Init 用下一个EndorserServer初始化过滤器
func (f *expirationCheckFilter) Init(next peer.EndorserServer) {
	f.next = next
}

// validateProposal 验证已签名提案的有效性, 签名者身份是否过期。
// 输入参数：
//   - signedProp：已签名的提案。
//
// 返回值：
//   - error：如果验证过程中出现错误，则返回错误；否则返回nil。
func validateProposal(signedProp *peer.SignedProposal) error {
	// 反序列化提案消息
	prop, err := protoutil.UnmarshalProposal(signedProp.ProposalBytes)
	if err != nil {
		return errors.Wrap(err, "反序列化提案消息失败")
	}

	// 反序列化提案头部
	hdr, err := protoutil.UnmarshalHeader(prop.Header)
	if err != nil {
		return errors.Wrap(err, "反序列化提案头部失败")
	}

	// 反序列化签名者
	sh, err := protoutil.UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return errors.Wrap(err, "反序列化签名者失败")
	}

	// 检查客户端签名者是否过期
	expirationTime := crypto.ExpiresAt(sh.Creator)
	if !expirationTime.IsZero() && time.Now().After(expirationTime) {
		return errors.New("提案客户端签名者已过期")
	}

	return nil
}

// ProcessProposal 处理已签名的提案
func (f *expirationCheckFilter) ProcessProposal(ctx context.Context, signedProp *peer.SignedProposal) (*peer.ProposalResponse, error) {
	// 验证已签名提案的有效性, 签名者身份是否过期
	if err := validateProposal(signedProp); err != nil {
		return nil, err
	}
	return f.next.ProcessProposal(ctx, signedProp)
}
