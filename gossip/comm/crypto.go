/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"context"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls/gmcredentials"
	"github.com/hyperledger/fabric/common/util"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func certHashFromRawCert(rawCert []byte) []byte {
	if len(rawCert) == 0 {
		return nil
	}
	return util.ComputeSHA256(rawCert)
}

// ExtractCertificateHash 从上下文中提取证书的哈希值。
// 方法接收者：无（全局函数）
// 输入参数：
//   - ctx：上下文，用于从中提取对等节点信息。
//
// 返回值：
//   - []byte：表示证书的哈希值。
func extractCertificateHashFromContext(ctx context.Context) []byte {
	// 从上下文中获取对等节点信息
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	// 获取认证信息
	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}

	// 适配国密tls配置
	switch info := authInfo.(type) {
	case gmcredentials.TLSInfo:
		// 获取对等节点的证书列表
		certs := info.State.PeerCertificates
		if len(certs) == 0 {
			return nil
		}

		// 获取第一个证书的原始
		raw := certs[0].Raw
		return certHashFromRawCert(raw)
	case credentials.TLSInfo:
		// 获取对等节点的证书列表
		certs := info.State.PeerCertificates
		if len(certs) == 0 {
			return nil
		}

		// 获取第一个证书的原始
		raw := certs[0].Raw
		return certHashFromRawCert(raw)
	}

	return nil
}
