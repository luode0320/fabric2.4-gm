/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"bytes"
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/util"
	"github.com/pkg/errors"
)

// BindingInspector receives as parameters a gRPC context and an Envelope,
// and verifies whether the message contains an appropriate binding to the context
type BindingInspector func(context.Context, proto.Message) error

// CertHashExtractor 从 proto.Message 消息中提取证书
type CertHashExtractor func(proto.Message) []byte

// NewBindingInspector 函数根据是否配置了 mutualTLS 和一个从 proto 消息中提取 TLS 证书哈希的函数，
// 返回一个 BindingInspector。
// 方法接收者：无（函数）
// 输入参数：
//   - mutualTLS：bool 类型，表示是否配置了 mutualTLS。
//   - extractTLSCertHash：CertHashExtractor 类型，表示从 proto 消息中提取 TLS 证书哈希的函数。
//
// 返回值：
//   - BindingInspector：表示 BindingInspector 函数。
func NewBindingInspector(mutualTLS bool, extractTLSCertHash CertHashExtractor) BindingInspector {
	// 检查 extractTLSCertHash 参数是否为 nil
	if extractTLSCertHash == nil {
		panic(errors.New("ExtractTLSCertHash 提取TLS证书哈希参数为零"))
	}

	// 根据 mutualTLS 配置选择要使用的绑定检查函数
	inspectMessage := mutualTLSBinding
	if !mutualTLS {
		inspectMessage = noopBinding
	}

	// 返回 BindingInspector 函数

	return func(ctx context.Context, msg proto.Message) error {
		if msg == nil {
			return errors.New("msg 消息为nil")
		}
		return inspectMessage(ctx, extractTLSCertHash(msg))
	}
}

// mutualTLSBinding 方法用于强制客户端在消息中发送其TLS证书哈希，
// 然后将其与从gRPC上下文中派生的计算哈希进行比较。
// 如果它们不匹配，或者请求中缺少证书哈希，或者无法从gRPC上下文中提取TLS证书，
// 则返回错误。
// 方法接收者：无（函数）
// 输入参数：
//   - ctx：context.Context 类型，表示上下文。
//   - claimedTLScertHash：[]byte 类型，表示客户端声明的TLS证书哈希。
//
// 返回值：
//   - error：如果TLS证书哈希不匹配或缺失，则返回错误。
func mutualTLSBinding(ctx context.Context, claimedTLScertHash []byte) error {
	// 检查客户端是否包含其TLS证书哈希
	if len(claimedTLScertHash) == 0 {
		return errors.Errorf("客户端未包含其TLS证书哈希")
	}

	// 从gRPC上下文中提取实际的TLS证书哈希
	actualTLScertHash := util.ExtractCertificateHashFromContext(ctx)
	if len(actualTLScertHash) == 0 {
		return errors.Errorf("客户端未发送TLS证书")
	}

	// 比较客户端声明的TLS证书哈希和实际的TLS证书哈希
	if !bytes.Equal(actualTLScertHash, claimedTLScertHash) {
		return errors.Errorf("声明的TLS证书哈希为 %v，但实际的TLS证书哈希为 %v", claimedTLScertHash, actualTLScertHash)
	}

	return nil
}

// noopBinding is a BindingInspector that always returns nil
func noopBinding(_ context.Context, _ []byte) error {
	return nil
}
