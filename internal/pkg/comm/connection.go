/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls/gmcredentials"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"

	"github.com/hyperledger/fabric/msp"
	"google.golang.org/grpc/credentials"
	"sync"
)

var commLogger = flogging.MustGetLogger("comm")

// CredentialSupport 类型管理用于gRPC客户端连接的凭据。
type CredentialSupport struct {
	mutex             sync.RWMutex        // mutex 用于对 CredentialSupport 实例的读写操作进行同步。
	appRootCAsByChain map[string][][]byte // appRootCAsByChain 是一个映射，用于存储按证书链索引的应用程序根证书的字节切片。
	serverRootCAs     [][]byte            // serverRootCAs 是一个二维字节切片，表示服务器的根证书。
	clientCert        tls.Certificate     // clientCert 是一个tls.Certificate类型，表示客户端的证书。
}

// NewCredentialSupport 函数创建一个 CredentialSupport 实例。
//
// 参数：
//   - rootCAs: [][]byte 类型的可选参数，表示根证书的字节切片。
//
// 返回值：
//   - *CredentialSupport: 表示 CredentialSupport 实例。
func NewCredentialSupport(rootCAs ...[]byte) *CredentialSupport {
	return &CredentialSupport{
		appRootCAsByChain: make(map[string][][]byte),
		serverRootCAs:     rootCAs,
	}
}

// SetClientCertificate 方法设置用于gRPC客户端连接的tls.Certificate。
//
// 参数：
//   - cert: tls.Certificate 类型，表示要设置的TLS证书。
func (cs *CredentialSupport) SetClientCertificate(cert tls.Certificate) {
	cs.mutex.Lock()
	cs.clientCert = cert
	cs.mutex.Unlock()
}

// GetClientCertificate 返回CredentialSupport的客户端证书
func (cs *CredentialSupport) GetClientCertificate() tls.Certificate {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.clientCert
}

// GetPeerCredentials 返回用于与远程对等节点通信的 gRPC 传输凭证。
// 方法接收者：cs（CredentialSupport类型的指针）
// 返回值：
//   - credentials.TransportCredentials：表示 gRPC 传输凭证。
func (cs *CredentialSupport) GetPeerCredentials() credentials.TransportCredentials {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	// 创建证书池，并将根证书添加到池中
	var appRootCAs [][]byte
	appRootCAs = append(appRootCAs, cs.serverRootCAs...)
	for _, appRootCA := range cs.appRootCAsByChain {
		appRootCAs = append(appRootCAs, appRootCA...)
	}

	// 适配国密tls配置
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		certPool := x509GM.NewCertPool()
		for _, appRootCA := range appRootCAs {
			if !certPool.AppendCertsFromPEM(appRootCA) {
				commLogger.Warningf("将证书添加到 peer 的客户端TLS证书池失败")
			}
		}
		gmCert := gmtls.Certificate{
			Certificate: cs.clientCert.Certificate,
			PrivateKey:  cs.clientCert.PrivateKey,
		}
		// 创建 TLS 配置，并使用客户端证书和证书池初始化
		config := NewTLSConfig(&gmtls.Config{
			Certificates: []gmtls.Certificate{gmCert},
			RootCAs:      certPool,
		})
		return gmcredentials.NewTLS(config.gmconfig)

	case factory.SoftwareBasedFactoryName:
		certPool := x509.NewCertPool()
		for _, appRootCA := range appRootCAs {
			if !certPool.AppendCertsFromPEM(appRootCA) {
				commLogger.Warningf("将证书添加到 peer 的客户端TLS证书池失败")
			}
		}

		// 创建 TLS 配置，并使用客户端证书和证书池初始化
		config := NewTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cs.clientCert},
			RootCAs:      certPool,
		})
		return credentials.NewTLS(config.config)
	}

	return nil
}

func (cs *CredentialSupport) AppRootCAsByChain() map[string][][]byte {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.appRootCAsByChain
}

// BuildTrustedRootsForChain populates the appRootCAs and orderRootCAs maps by
// getting the root and intermediate certs for all msps associated with the
// MSPManager.
func (cs *CredentialSupport) BuildTrustedRootsForChain(cm channelconfig.Resources) {
	appOrgMSPs := make(map[string]struct{})
	if ac, ok := cm.ApplicationConfig(); ok {
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	ordOrgMSPs := make(map[string]struct{})
	if ac, ok := cm.OrdererConfig(); ok {
		for _, ordOrg := range ac.Organizations() {
			ordOrgMSPs[ordOrg.MSPID()] = struct{}{}
		}
	}

	cid := cm.ConfigtxValidator().ChannelID()
	msps, err := cm.MSPManager().GetMSPs()
	if err != nil {
		commLogger.Errorf("Error getting root CAs for channel %s (%s)", cid, err)
		return
	}

	var appRootCAs [][]byte
	for k, v := range msps {
		// we only support the fabric MSP
		if v.GetType() != msp.FABRIC {
			continue
		}

		for _, root := range v.GetTLSRootCerts() {
			// check to see of this is an app org MSP
			if _, ok := appOrgMSPs[k]; ok {
				commLogger.Debugf("adding app root CAs for MSP [%s]", k)
				appRootCAs = append(appRootCAs, root)
			}
		}
		for _, intermediate := range v.GetTLSIntermediateCerts() {
			// check to see of this is an app org MSP
			if _, ok := appOrgMSPs[k]; ok {
				commLogger.Debugf("adding app root CAs for MSP [%s]", k)
				appRootCAs = append(appRootCAs, intermediate)
			}
		}
	}

	cs.mutex.Lock()
	cs.appRootCAsByChain[cid] = appRootCAs
	cs.mutex.Unlock()
}
