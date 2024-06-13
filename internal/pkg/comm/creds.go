/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls/gmcredentials"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"net"
	"sync"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"google.golang.org/grpc/credentials"
)

var (
	ErrClientHandshakeNotImplemented = errors.New("core/comm: client handshakes are not implemented with serverCreds")
	ErrServerHandshakeNotImplemented = errors.New("core/comm: server handshakes are not implemented with clientCreds")
	ErrOverrideHostnameNotSupported  = errors.New("core/comm: OverrideServerName is not supported")

	// alpnProtoStr 是gRPC指定的应用层协议。
	alpnProtoStr = []string{"h2"}

	// TLS客户端连接记录器
	tlsClientLogger = flogging.MustGetLogger("comm.tls")
)

// NewServerTransportCredentials 根据给定的服务器配置和日志记录器创建一个 TransportCredentials 对象。
// 方法接收者：无（函数）
// 输入参数：
//   - serverConfig：服务器的 TLS 配置。
//   - logger：日志记录器。
//
// 返回值：
//   - credentials.TransportCredentials：创建的 TransportCredentials 对象。
func NewServerTransportCredentials(serverConfig *TLSConfig, logger *flogging.FabricLogger) credentials.TransportCredentials {
	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 适配国密工厂，更新服务器配置以支持国密
		serverConfig.gmconfig.GMSupport = &gmtls.GMSupport{}
		serverConfig.gmconfig.PreferServerCipherSuites = true
		serverConfig.gmconfig.CipherSuites = []uint16{gmtls.GMTLS_SM2_WITH_SM4_SM3, gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3}
		serverConfig.gmconfig.NextProtos = alpnProtoStr
		serverConfig.gmconfig.MinVersion = gmtls.VersionGMSSL
	case factory.SoftwareBasedFactoryName:
		// 适配软件工厂，更新服务器配置以支持软件证书
		serverConfig.config.NextProtos = alpnProtoStr
		serverConfig.config.MinVersion = tls.VersionTLS12
	}

	// 如果日志记录器为空，则使用默认的日志记录器
	if logger == nil {
		logger = tlsClientLogger
	}

	// 创建并返回一个 serverCreds 对象
	return &serverCreds{
		serverConfig: serverConfig,
		logger:       logger,
	}
}

// serverCreds 是 grpc/credentials.TransportCredentials 的实现。
type serverCreds struct {
	serverConfig *TLSConfig // 服务器 TLS 配置
	logger       *flogging.FabricLogger
}

// TLSConfig 包含 TLS 配置信息。
type TLSConfig struct {
	config   *tls.Config   // TLS 配置
	gmconfig *gmtls.Config // 补充国密 TLS 配置
	lock     sync.RWMutex  // 读写锁
}

// NewTLSConfig 根据给定的配置创建一个 TLSConfig 对象。
// 方法接收者：无（函数）
// 输入参数：
//   - config：要使用的配置。
//
// 返回值：
//   - *TLSConfig：创建的 TLSConfig 对象。
func NewTLSConfig(config interface{}) *TLSConfig {
	// 根据配置类型进行适配
	switch conf := config.(type) {
	case *gmtls.Config:
		conf.GMSupport = &gmtls.GMSupport{}  // 使用GMSSL（国密SSL）的支持
		conf.SessionTicketsDisabled = true   // 禁用会话票据（Session Tickets）
		conf.PreferServerCipherSuites = true // 优先使用服务器端的密码套件
		// 包含两个GMSSL密码套件的切片
		conf.CipherSuites = []uint16{gmtls.GMTLS_SM2_WITH_SM4_SM3, gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3}
		conf.NextProtos = alpnProtoStr       // 应用层协议（ALPN）
		conf.MinVersion = gmtls.VersionGMSSL // 最低支持的SSL版本
		return &TLSConfig{
			gmconfig: conf,
		}
	case gmtls.Config:
		conf.GMSupport = &gmtls.GMSupport{}  // 使用GMSSL（国密SSL）的支持
		conf.SessionTicketsDisabled = true   // 禁用会话票据（Session Tickets）
		conf.PreferServerCipherSuites = true // 优先使用服务器端的密码套件
		// 包含两个GMSSL密码套件的切片
		conf.CipherSuites = []uint16{gmtls.GMTLS_SM2_WITH_SM4_SM3, gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3}
		conf.NextProtos = alpnProtoStr       // 应用层协议（ALPN）
		conf.MinVersion = gmtls.VersionGMSSL // 最低支持的SSL版本
		return &TLSConfig{
			gmconfig: &conf,
		}
	case *tls.Config:
		conf.MinVersion = tls.VersionTLS12 // 最低支持的SSL版本
		conf.NextProtos = alpnProtoStr     // 应用层协议（ALPN）
		return &TLSConfig{
			config: conf,
		}
	case tls.Config:
		conf.MinVersion = tls.VersionTLS12 // 最低支持的SSL版本
		conf.NextProtos = alpnProtoStr     // 应用层协议（ALPN）
		return &TLSConfig{
			config: &conf,
		}
	default:
		// 如果配置类型不匹配，则创建一个空的 TLSConfig 对象
		return &TLSConfig{}
	}
}

// Config 返回 TLS 配置的副本。
// 方法接收者：TLSConfig
// 返回值：
//   - interface{}：TLS 配置的副本。
func (t *TLSConfig) Config() interface{} {
	// 获取读锁，确保线程安全
	t.lock.RLock()
	defer t.lock.RUnlock()

	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 适配国密工厂，返回国密配置的副本
		if t.gmconfig != nil {
			return *t.gmconfig.Clone()
		}
	case factory.SoftwareBasedFactoryName:
		// 适配软件工厂，返回软件配置的副本
		if t.config != nil {
			return *t.config.Clone()
		}
	}

	// 如果没有适配的配置，则返回一个空的 tls.Config 对象
	return tls.Config{}
}

// AddClientRootCA 用于向 TLS 配置中添加客户端根证书。
// 方法接收者：TLSConfig
// 输入参数：
//   - cert：要添加的客户端根证书。
func (t *TLSConfig) AddClientRootCA(cert *x509.Certificate) {
	// 获取锁，确保线程安全
	t.lock.Lock()
	defer t.lock.Unlock()

	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 适配国密工厂，将证书转换为国密格式并添加到客户端根证书列表中
		t.gmconfig.ClientCAs.AddCert(gm.ParseX509Certificate2Sm2(cert))
	case factory.SoftwareBasedFactoryName:
		// 适配软件工厂，直接将证书添加到客户端根证书列表中
		t.config.ClientCAs.AddCert(cert)
	}
}

// SetClientCAs 设置客户端根证书池。
// 方法接收者：TLSConfig
// 输入参数：
//   - certPool：要设置的客户端根证书池。
func (t *TLSConfig) SetClientCAs(certPool interface{}) {
	// 获取锁，确保线程安全
	t.lock.Lock()
	defer t.lock.Unlock()

	// 根据证书池类型进行适配
	switch pool := certPool.(type) {
	case *x509.CertPool:
		// 如果证书池类型是 *x509.CertPool，则将其设置为软件证书池
		t.config.ClientCAs = pool
	case *x509GM.CertPool:
		// 如果证书池类型是 *x509GM.CertPool，则将其设置为国密证书池
		t.gmconfig.ClientCAs = pool
	}
}

// ClientHandshake 未实现
func (sc *serverCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, ErrClientHandshakeNotImplemented
}

// ServerHandshake 用于为服务器进行身份验证握手。
// 方法接收者：serverCreds
// 输入参数：
//   - rawConn：原始的网络连接。
//
// 返回值：
//   - net.Conn：握手完成后的网络连接。
//   - credentials.AuthInfo：身份验证信息。
//   - error：握手过程中的错误，如果没有错误则为 nil。
func (sc *serverCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	serverConfig := sc.serverConfig.Config()

	// 根据服务器配置类型进行适配
	switch conf := serverConfig.(type) {
	case gmtls.Config:
		// 如果服务器配置类型是 gmtls.Config，则使用 gmtls.Server 进行握手
		server := gmtls.Server(rawConn, &conf)
		l := sc.logger.With("远程地址", server.RemoteAddr().String())

		start := time.Now()
		if err := server.Handshake(); err != nil {
			l.Errorf("服务器TLS握手失败 %s 有错误 %s", time.Since(start), err)
			return nil, nil, err
		}

		state := server.ConnectionState()
		return server, credentials.TLSInfo{State: convertGMToTLS(&state)}, nil
	case tls.Config:
		// 如果服务器配置类型是 tls.Config，则使用 tls.Server 进行握手
		conn := tls.Server(rawConn, &conf)
		l := sc.logger.With("远程地址", conn.RemoteAddr().String())

		start := time.Now()
		if err := conn.Handshake(); err != nil {
			l.Errorf("服务器TLS握手失败 %s 有错误 %s", time.Since(start), err)
			return nil, nil, err
		}

		return conn, credentials.TLSInfo{State: conn.ConnectionState()}, nil
	}

	return nil, credentials.TLSInfo{}, nil
}

// Info 返回此 TransportCredentials 的 ProtocolInfo。
// 方法接收者：serverCreds
// 返回值：
//   - credentials.ProtocolInfo：协议信息。
func (sc *serverCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
	}
}

// Clone 创建此 TransportCredentials 的副本。
// 方法接收者：serverCreds
// 返回值：
//   - credentials.TransportCredentials：创建的 TransportCredentials 副本。
func (sc *serverCreds) Clone() credentials.TransportCredentials {
	// 获取当前服务器配置
	config := sc.serverConfig.Config()
	// 创建服务器配置的副本
	serverConfig := NewTLSConfig(config)
	// 使用副本创建新的 TransportCredentials 对象
	return NewServerTransportCredentials(serverConfig, sc.logger)
}

// OverrideServerName 用于覆盖用于验证服务器返回证书的主机名。
// 方法接收者：serverCreds
// 输入参数：
//   - string：要覆盖的服务器名称。
//
// 返回值：
//   - error：如果不支持覆盖主机名，则返回 ErrOverrideHostnameNotSupported。
func (sc *serverCreds) OverrideServerName(string) error {
	return ErrOverrideHostnameNotSupported
}

// DynamicClientCredentials 动态客户端凭证的结构体
type DynamicClientCredentials struct {
	TLSConfig   *tls.Config   // TLS 配置
	GMTLSConfig *gmtls.Config // 补充国密 TLS 配置
}

// latestConfig 返回最新的配置。
// 方法接收者：DynamicClientCredentials
// 返回值：
//   - interface{}：最新的配置。
func (dtc *DynamicClientCredentials) latestConfig() interface{} {
	// 根据工厂名称选择适配的配置
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 如果工厂名称是 GuomiBasedFactoryName，则返回 GMTLSConfig 的副本
		return dtc.GMTLSConfig.Clone()
	case factory.SoftwareBasedFactoryName:
		// 如果工厂名称是 SoftwareBasedFactoryName，则返回 TLSConfig 的副本
		return dtc.TLSConfig.Clone()
	}
	// 默认情况下，返回 TLSConfig 的副本
	return dtc.TLSConfig.Clone()
}

// ClientHandshake 用于为客户端进行TLS握手。
// 方法接收者：DynamicClientCredentials
// 输入参数：
//   - ctx：上下文对象。
//   - authority：服务器的授权信息。
//   - rawConn：原始的网络连接。
//
// 返回值：
//   - net.Conn：握手完成后的网络连接。
//   - credentials.AuthInfo：身份验证信息。
//   - error：握手过程中的错误，如果没有错误则为 nil。
func (dtc *DynamicClientCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	l := tlsClientLogger.With("远程地址", rawConn.RemoteAddr().String())
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*gmtls.Config)

		// 创建 GMTLS TransportCredentials
		creds := gmcredentials.NewTLS(config)
		start := time.Now()
		// 进行客户端TLS握手
		conn, auth, err := creds.ClientHandshake(ctx, authority, rawConn)
		if err != nil {
			l.Errorf("客户端TLS握手在 %s 后失败，出现错误: %s", time.Since(start), err)
		}

		return conn, auth, err
	case factory.SoftwareBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*tls.Config)

		// 创建 TLS TransportCredentials
		creds := credentials.NewTLS(config)
		start := time.Now()
		// 进行客户端TLS握手
		conn, auth, err := creds.ClientHandshake(ctx, authority, rawConn)
		if err != nil {
			l.Errorf("客户端TLS握手在 %s 后失败，出现错误: %s", time.Since(start), err)
		}

		return conn, auth, err
	}

	return nil, nil, nil
}

// ServerHandshake 用于为服务器进行TLS握手。
// 方法接收者：DynamicClientCredentials
// 输入参数：
//   - rawConn：原始的网络连接。
//
// 返回值：
//   - net.Conn：握手完成后的网络连接。
//   - credentials.AuthInfo：身份验证信息。
//   - error：握手过程中的错误，如果没有错误则为 ErrServerHandshakeNotImplemented。
func (dtc *DynamicClientCredentials) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, ErrServerHandshakeNotImplemented
}

// Info 返回协议信息。
// 方法接收者：DynamicClientCredentials
// 返回值：
//   - credentials.ProtocolInfo：协议信息。
func (dtc *DynamicClientCredentials) Info() credentials.ProtocolInfo {
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*gmtls.Config)
		// 创建 GMTLS TransportCredentials
		creds := gmcredentials.NewTLS(config)
		// 返回协议信息
		return creds.Info()
	case factory.SoftwareBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*tls.Config)
		// 创建 TLS TransportCredentials
		creds := credentials.NewTLS(config)
		// 返回协议信息
		return creds.Info()
	}

	return credentials.ProtocolInfo{}
}

// Clone 用于克隆传输凭据。
// 方法接收者：DynamicClientCredentials
// 返回值：
//   - credentials.TransportCredentials：克隆后的传输凭据。
func (dtc *DynamicClientCredentials) Clone() credentials.TransportCredentials {
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*gmtls.Config)
		// 创建 GMTLS TransportCredentials
		return gmcredentials.NewTLS(config)
	case factory.SoftwareBasedFactoryName:
		// 获取最新的配置
		config := dtc.latestConfig().(*tls.Config)
		// 创建 TLS TransportCredentials
		return credentials.NewTLS(config)
	}
	return nil
}

// OverrideServerName 用于覆盖服务器名称。
// 方法接收者：DynamicClientCredentials
// 输入参数：
//   - name：要覆盖的服务器名称。
//
// 返回值：
//   - error：如果覆盖过程中出现错误，则返回错误；否则返回 nil。
func (dtc *DynamicClientCredentials) OverrideServerName(name string) error {
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 如果工厂名称是 GuomiBasedFactoryName，则将 GMTLSConfig 的 ServerName 属性设置为指定的名称
		dtc.GMTLSConfig.ServerName = name
	case factory.SoftwareBasedFactoryName:
		// 如果工厂名称是 SoftwareBasedFactoryName，则将 TLSConfig 的 ServerName 属性设置为指定的名称
		dtc.TLSConfig.ServerName = name
	}
	return nil
}

// convertGMToTLS gm ConnectionState 转 标准tls ConnectionState
func convertGMToTLS(gmState *gmtls.ConnectionState) tls.ConnectionState {
	tlsState := tls.ConnectionState{}
	// 如果使用会话票证或类似机制从以前的会话成功恢复此连接
	tlsState.DidResume = gmState.DidResume
	// NegotiatedProtocol是与ALPN协商的应用协议
	tlsState.NegotiatedProtocol = gmState.NegotiatedProtocol
	// 表示相互的NPN协商
	tlsState.NegotiatedProtocolIsMutual = gmState.NegotiatedProtocolIsMutual
	// 对等方通过叶证书的TLS握手提供的sct列表 (如果有的话)
	tlsState.SignedCertificateTimestamps = gmState.SignedCertificateTimestamps
	// 是对等体为叶证书 (如果有) 提供的装订联机证书状态协议 (OCSP) 响应
	tlsState.OCSPResponse = gmState.OCSPResponse
	// 包含 “tls-unique” 通道绑定值
	tlsState.TLSUnique = gmState.TLSUnique
	// 将TLS版本转换
	tlsState.Version = gmState.Version
	// 将密码套件转换
	tlsState.CipherSuite = gmState.CipherSuite
	// 将握手状态转换
	tlsState.HandshakeComplete = gmState.HandshakeComplete
	// 将证书链转换
	tlsState.PeerCertificates = make([]*x509.Certificate, 0)
	for _, certificate := range gmState.PeerCertificates {
		tlsState.PeerCertificates = append(tlsState.PeerCertificates, certificate.ToX509Certificate())
	}
	// 将客户端证书链转换
	tlsState.VerifiedChains = make([][]*x509.Certificate, 0)
	for _, chain := range gmState.VerifiedChains {
		tlsChain := make([]*x509.Certificate, 0)
		for _, cert := range chain {
			tlsChain = append(tlsChain, cert.ToX509Certificate())
		}
		tlsState.VerifiedChains = append(tlsState.VerifiedChains, tlsChain)
	}
	// 将服务器名称转换
	tlsState.ServerName = gmState.ServerName
	return tlsState
}
