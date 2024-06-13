/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package accesscontrol

import (
	"context"
	"github.com/hyperledger/fabric/bccsp/common"
	"sync"
	"time"

	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var ttl = time.Minute * 10

// 证书hash值
type certHash string

// KeyGenFunc 用于生成证书和密钥对的函数
type KeyGenFunc func() (*tlsgen.CertKeyPair, error)

// certMapper 结构体用于管理证书哈希和链码名称的映射关系。
type certMapper struct {
	keyGen       KeyGenFunc          // 用于生成证书和密钥对
	sync.RWMutex                     // 读写锁
	m            map[certHash]string // 存储证书哈希和链码名称的对应关系
}

// newCertMapper 函数用于创建一个新的certMapper实例。
// 输入参数：
//   - keyGen：用于生成证书和密钥对的函数。
//
// 返回值：
//   - *certMapper：新创建的certMapper实例。
func newCertMapper(keyGen KeyGenFunc) *certMapper {
	// 创建一个新的certMapper实例，并初始化keyGen和m字段
	return &certMapper{
		keyGen: keyGen,
		m:      make(map[certHash]string),
	}
}

// lookup 用于查找指定证书哈希对应的链码名称。
// 输入参数：
//   - h：要查找的证书哈希。
//
// 返回值：
//   - string：指定证书哈希对应的证书名称。
func (r *certMapper) lookup(h certHash) string {
	r.RLock()
	defer r.RUnlock()

	// 返回指定证书哈希对应的链码名称
	return r.m[h]
}

// register方法用于将证书哈希和链码名称注册到映射中，并设置过期时间。
// 输入参数：
//   - hash：要注册的证书哈希。
//   - name：要注册的证书名称。
func (r *certMapper) register(hash certHash, name string) {
	r.Lock()
	defer r.Unlock()

	// 将证书哈希和链码名称注册到映射中
	r.m[hash] = name

	// 设置过期时间，过期后自动从映射中删除
	time.AfterFunc(ttl, func() {
		r.purge(hash)
	})
}

// purge 方法用于从映射中删除指定的证书哈希。
// 输入参数：
//   - hash：要删除的证书哈希。
func (r *certMapper) purge(hash certHash) {
	r.Lock()
	defer r.Unlock()
	delete(r.m, hash)
}

// genCert 用于生成证书和密钥对，并注册证书哈希和链码名称的映射关系。
// 输入参数：
//   - name：链码名称。
//
// 返回值：
//   - *tlsgen.CertKeyPair：生成的证书和密钥对。
//   - error：如果生成证书过程中出现错误，则返回相应的错误信息。
func (r *certMapper) genCert(name string) (*tlsgen.CertKeyPair, error) {
	// 生成证书和密钥对
	keyPair, err := r.keyGen()
	if err != nil {
		return nil, err
	}

	// 计算证书的哈希值
	hash, err := common.ComputeHASH(keyPair.TLSCert.Raw)
	if err != nil {
		return nil, err
	}

	// 注册 证书哈希 和 链码名称 的映射关系
	r.register(certHash(hash), name)

	return keyPair, nil
}

// extractCertificateHashFromContext 函数从grpc上下文中提取证书哈希值。
// 输入参数：
//   - ctx：上下文对象。
//
// 返回值：
//   - []byte：提取到的证书哈希值。
func extractCertificateHashFromContext(ctx context.Context) []byte {
	// 从上下文中提取对等节点信息
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	// 获取对等节点的认证信息
	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}
	// TODO luode 将认证信息转换为TLS连接信息, 这里国密的tls是否可以验证
	tlsInfo, isTLSConn := authInfo.(credentials.TLSInfo)
	if !isTLSConn {
		return nil
	}

	// 获取TLS连接中的对等证书列表
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return nil
	}

	// 获取第一个对等证书的原始字节数据
	raw := certs[0].Raw
	if len(raw) == 0 {
		return nil
	}

	// 计算原始字节数据的哈希值，并返回结果
	hash, err := common.ComputeHASH(raw)
	if err != nil {
		return nil
	}

	return hash
}
