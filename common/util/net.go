/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"context"
	"crypto/x509"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls/gmcredentials"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ExtractRemoteAddress 根据上下文，解析远端地址
//
// @Author: zhaoruobo
// @Date: 2023/11/1
func ExtractRemoteAddress(ctx context.Context) string {
	var remoteAddress string
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	if address := p.Addr; address != nil {
		remoteAddress = address.String()
	}
	return remoteAddress
}

// ExtractCertificateHashFromContext 方法从给定的上下文中提取证书的哈希值。
// 如果证书不存在，则返回nil。
// 方法接收者：无（函数）
// 输入参数：
//   - ctx：context.Context 类型，表示上下文。
//
// 返回值：
//   - []byte：表示证书的哈希值。
func ExtractCertificateHashFromContext(ctx context.Context) []byte {
	// 从上下文中提取原始证书
	rawCert := ExtractRawCertificateFromContext(ctx)
	if len(rawCert) == 0 {
		return nil
	}
	// 改成使用国密hash
	return ComputeSHA256(rawCert)
}

// ExtractCertificateFromContext 返回TLS证书 (如果适用)
// 来自gRPC流的给定上下文
func ExtractCertificateFromContext(ctx context.Context) *x509.Certificate {
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}

	// 适配国密tls配置
	switch info := authInfo.(type) {
	case gmcredentials.TLSInfo:
		certs := info.State.PeerCertificates
		if len(certs) == 0 {
			return nil
		}
		return certs[0].ToX509Certificate()
	case credentials.TLSInfo:
		// todo luode 这里是会被执行了, 批准链码时走的是这里, 预计是orderer之间的请求, 没有使用gmtls
		certs := info.State.PeerCertificates
		if len(certs) == 0 {
			return nil
		}
		return certs[0]
	}
	return nil
}

// ExtractRawCertificateFromContext returns the raw TLS certificate (if applicable)
// from the given context of a gRPC stream
func ExtractRawCertificateFromContext(ctx context.Context) []byte {
	cert := ExtractCertificateFromContext(ctx)
	if cert == nil {
		return nil
	}
	return cert.Raw
}
