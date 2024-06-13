/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"net"
	"sync"
	"sync/atomic"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCServer 是 gRPC 服务器的结构体。
type GRPCServer struct {
	// address 是服务器的监听地址，格式为 hostname:port。
	address string
	// listener 用于处理网络请求的监听器。
	listener net.Listener
	// server 是 gRPC 服务器。
	server *grpc.Server
	// serverCertificate 是服务器用于 TLS 通信的证书，以原子引用的形式存储。
	serverCertificate atomic.Value
	// lock 用于保护并发访问的追加/删除操作。
	lock *sync.Mutex
	// tls 是 gRPC 服务器使用的 TLS 配置。
	tls *TLSConfig
	// healthServer 是用于 gRPC 健康检查协议的服务器。
	healthServer *health.Server
}

// NewGRPCServer 创建一个给定监听地址的GRPCServer的新实现
func NewGRPCServer(address string, serverConfig ServerConfig) (*GRPCServer, error) {
	if address == "" {
		return nil, errors.New("缺少地址address参数配置")
	}
	// 创建一个tcp的监听器
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	// 在给定现有网络的情况下，创建GRPCServer的新实现
	return NewGRPCServerFromListener(lis, serverConfig)
}

// CreatTlsConfig 创建一个客户端tls配置
//
// @Author: 罗德
// @Date: 2023/11/22
func CreatTlsConfig(cert []tls.Certificate, rootCert *x509.Certificate) *TLSConfig {
	// 适配国密tls配置
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 配置tls服务器证书
		certificates := make([]gmtls.Certificate, 0)
		gmCert := gmtls.Certificate{
			Certificate: cert[0].Certificate,
			PrivateKey:  cert[0].PrivateKey,
		}
		certificates = append(certificates, gmCert, gmCert)

		tlsConfig := NewTLSConfig(&gmtls.Config{
			Certificates: certificates, // 证书
		})

		// 验证服务端证书的根证书
		tlsConfig.gmconfig.RootCAs = x509GM.NewCertPool()
		tlsConfig.gmconfig.RootCAs.AddCert(gm.ParseX509Certificate2Sm2(rootCert))

		return tlsConfig

	case factory.SoftwareBasedFactoryName:
		// gRPC 服务器使用的 TLS 配置
		tlsConfig := NewTLSConfig(&tls.Config{
			SessionTicketsDisabled: true, // 可以设置为true以禁用会话票证和PSK (恢复) 支持
			// 加载tls证书函数
			GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return &cert[0], nil
			},
		})

		// 验证服务端证书的根证书
		tlsConfig.config.RootCAs = x509.NewCertPool()
		tlsConfig.config.RootCAs.AddCert(rootCert)

		return tlsConfig
	}

	return nil
}

// NewGRPCServerFromListener 在给定现有网络的情况下，创建GRPCServer的新实现。
// 使用默认keepalive的侦听器实例
func NewGRPCServerFromListener(listener net.Listener, serverConfig ServerConfig) (*GRPCServer, error) {
	grpcServer := &GRPCServer{
		address:  listener.Addr().String(),
		listener: listener,
		lock:     &sync.Mutex{},
	}

	// 设置我们的服务器选项
	var serverOpts []grpc.ServerOption

	secureConfig := serverConfig.SecOpts
	if secureConfig.UseTLS {
		// 同时需要密钥和证书
		if secureConfig.Key != nil && secureConfig.Certificate != nil {
			// 读取并验证服务公私钥
			cert, err := common.X509KeyPair(secureConfig.Certificate, secureConfig.Key)
			if err != nil {
				return nil, err
			}

			// 配置tls服务器证书
			certificates := make([]gmtls.Certificate, 0)
			gmCert := gmtls.Certificate{
				Certificate: cert.Certificate,
				PrivateKey:  cert.PrivateKey,
			}
			certificates = append(certificates, gmCert, gmCert)

			// 加载证书到gRPC
			grpcServer.serverCertificate.Store(cert)

			// 适配国密tls配置
			switch factory.FactoryName {
			case factory.GuomiBasedFactoryName:
				// gRPC 服务器使用的 TLS 配置
				grpcServer.tls = NewTLSConfig(&gmtls.Config{
					Certificates:          certificates,                     // 证书
					VerifyPeerCertificate: secureConfig.VerifyCertificateGM, // TLS客户端或服务器进行正常证书验证后调用
				})

				// 时间
				if serverConfig.SecOpts.TimeShift > 0 {
					timeShift := serverConfig.SecOpts.TimeShift
					grpcServer.tls.gmconfig.Time = func() time.Time {
						return time.Now().Add((-1) * timeShift)
					}
				}

				// 用于TLS客户端身份验证的服务器策略。应在握手期间请求客户端证书，但不要求客户端发送任何证书。
				grpcServer.tls.gmconfig.ClientAuth = gmtls.RequestClientCert

				//判断在配置中，是否有配置需要客户端身份验证
				if secureConfig.RequireClientCert {
					// 要求tls的客户端授权
					grpcServer.tls.gmconfig.ClientAuth = gmtls.RequireAndVerifyClientCert
					// 如果在配置中，有配置root ca，那么在这里就创建一个certPool，把 root ca证书添加进来
					if len(secureConfig.ClientRootCAs) > 0 {
						grpcServer.tls.gmconfig.ClientCAs = x509GM.NewCertPool()
						for _, clientRootCA := range secureConfig.ClientRootCAs {
							err = grpcServer.appendClientRootCA(clientRootCA)
							if err != nil {
								return nil, err
							}
						}
					}
				}

			case factory.SoftwareBasedFactoryName:
				// gRPC 服务器使用的 TLS 配置
				grpcServer.tls = NewTLSConfig(&tls.Config{
					VerifyPeerCertificate:  secureConfig.VerifyCertificate, // TLS客户端或服务器进行正常证书验证后调用
					SessionTicketsDisabled: true,                           // 可以设置为true以禁用会话票证和PSK (恢复) 支持
					CipherSuites:           secureConfig.CipherSuites,      // 是已启用的TLS 1.0 1.2密码套件的列表
					// 加载tls证书函数
					GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
						cert := grpcServer.serverCertificate.Load().(tls.Certificate)
						return &cert, nil
					},
				})

				// 时间
				if serverConfig.SecOpts.TimeShift > 0 {
					timeShift := serverConfig.SecOpts.TimeShift
					grpcServer.tls.config.Time = func() time.Time {
						return time.Now().Add((-1) * timeShift)
					}
				}

				// 用于TLS客户端身份验证的服务器策略。应在握手期间请求客户端证书，但不要求客户端发送任何证书。
				grpcServer.tls.config.ClientAuth = tls.RequestClientCert

				//判断在配置中，是否有配置需要客户端身份验证
				if secureConfig.RequireClientCert {
					// 要求tls的客户端授权
					grpcServer.tls.config.ClientAuth = tls.RequireAndVerifyClientCert
					// 如果在配置中，有配置root ca，那么在这里就创建一个certPool，把root ca证书添加进来
					if len(secureConfig.ClientRootCAs) > 0 {
						grpcServer.tls.config.ClientCAs = x509.NewCertPool()
						for _, clientRootCA := range secureConfig.ClientRootCAs {
							err = grpcServer.appendClientRootCA(clientRootCA)
							if err != nil {
								return nil, err
							}
						}
					}
				}
			}

			// 创建凭据并添加到服务器选项
			creds := NewServerTransportCredentials(grpcServer.tls, serverConfig.Logger)

			serverOpts = append(serverOpts, grpc.Creds(creds))
		} else {
			return nil, errors.New("当 peer.tls.enabled为true时, peer.tls.cert、peer.tls.key必须包含证书和密钥")
		}
	}

	// 设置最大的发送/接受的信息大小
	maxSendMsgSize := DefaultMaxSendMsgSize
	if serverConfig.MaxSendMsgSize != 0 {
		maxSendMsgSize = serverConfig.MaxSendMsgSize
	}
	maxRecvMsgSize := DefaultMaxRecvMsgSize
	if serverConfig.MaxRecvMsgSize != 0 {
		maxRecvMsgSize = serverConfig.MaxRecvMsgSize
	}
	serverOpts = append(serverOpts, grpc.MaxSendMsgSize(maxSendMsgSize))
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(maxRecvMsgSize))

	// 设置keepalive选项
	serverOpts = append(serverOpts, serverConfig.KaOpts.ServerKeepaliveOptions()...)

	// 设置连接超时
	// 配置 keepalive 的相关选项
	serverOpts = append(serverOpts, serverConfig.KaOpts.ServerKeepaliveOptions()...)
	// 根据配置信息，设置链接超时时间
	if serverConfig.ConnectionTimeout <= 0 {
		serverConfig.ConnectionTimeout = DefaultConnectionTimeout
	}
	serverOpts = append(
		serverOpts,
		grpc.ConnectionTimeout(serverConfig.ConnectionTimeout))

	// 设置grpc的拦截器
	if len(serverConfig.StreamInterceptors) > 0 {
		serverOpts = append(
			serverOpts,
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(serverConfig.StreamInterceptors...)),
		)
	}

	// 定义grpc 一元方法的拦截器
	if len(serverConfig.UnaryInterceptors) > 0 {
		serverOpts = append(
			serverOpts,
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(serverConfig.UnaryInterceptors...)),
		)
	}

	// 报告连接的指标
	if serverConfig.ServerStatsHandler != nil {
		serverOpts = append(serverOpts, grpc.StatsHandler(serverConfig.ServerStatsHandler))
	}

	// 启动一个gRPC
	grpcServer.server = grpc.NewServer(serverOpts...)

	// 健康检查
	if serverConfig.HealthCheckEnabled {
		grpcServer.healthServer = health.NewServer()
		healthpb.RegisterHealthServer(grpcServer.server, grpcServer.healthServer)
	}

	return grpcServer, nil
}

// SetServerCertificate assigns the current TLS certificate to be the peer's server certificate
func (gServer *GRPCServer) SetServerCertificate(cert tls.Certificate) {
	gServer.serverCertificate.Store(cert)
}

// Address returns the listen address for this GRPCServer instance
func (gServer *GRPCServer) Address() string {
	return gServer.address
}

// Listener returns the net.Listener for the GRPCServer instance
func (gServer *GRPCServer) Listener() net.Listener {
	return gServer.listener
}

// Server 返回GRPCServer实例的grpc.Server
func (gServer *GRPCServer) Server() *grpc.Server {
	return gServer.server
}

// ServerCertificate returns the tls.Certificate used by the grpc.Server
func (gServer *GRPCServer) ServerCertificate() tls.Certificate {
	return gServer.serverCertificate.Load().(tls.Certificate)
}

// TLSEnabled  判断是否开启 GRPCServer 的实例
func (gServer *GRPCServer) TLSEnabled() bool {
	return gServer.tls != nil
}

// MutualTLSRequired 返回一个标识，表示这个GRPCServer实例是否需要客户端证书
func (gServer *GRPCServer) MutualTLSRequired() bool {
	config := gServer.tls.Config()

	// 适配国密
	switch conf := config.(type) {
	case *gmtls.Config:
		b := gServer.TLSEnabled() &&
			conf.ClientAuth == gmtls.RequireAndVerifyClientCert
		return b
	case gmtls.Config:
		b := gServer.TLSEnabled() &&
			conf.ClientAuth == gmtls.RequireAndVerifyClientCert
		return b
	case *tls.Config:
		b := gServer.TLSEnabled() &&
			conf.ClientAuth == tls.RequireAndVerifyClientCert
		return b
	case tls.Config:
		b := gServer.TLSEnabled() &&
			conf.ClientAuth == tls.RequireAndVerifyClientCert
		return b
	default:
		return false
	}

}

// Start 启动底层的 gRPC 服务
// 方法接收者：
//   - gServer：GRPCServer 结构体的指针，表示 gRPC 服务器
//
// 返回值：
//   - error：启动服务时可能发生的错误
func (gServer *GRPCServer) Start() error {
	// 如果启用了健康检查，则为所有已注册的服务设置健康状态
	if gServer.healthServer != nil {
		for name := range gServer.server.GetServiceInfo() {
			gServer.healthServer.SetServingStatus(
				name,
				healthpb.HealthCheckResponse_SERVING,
			)
		}

		gServer.healthServer.SetServingStatus(
			"",
			healthpb.HealthCheckResponse_SERVING,
		)
	}
	// 启动
	// 代码底层使用了阻塞I/O的方式进行连接的接受，即每次接受连接时会阻塞当前的goroutine，直到有新的连接到达或发生错误。
	// 这种方式适用于低并发的场景，如果需要处理大量并发连接，可以考虑使用NIO或其他非阻塞I/O的方式。
	return gServer.server.Serve(gServer.listener)
}

// Stop 停止底层的grpc服务
func (gServer *GRPCServer) Stop() {
	gServer.server.Stop()
}

// appendClientRootCA 是一个内部函数，用于添加一个 PEM 编码的 clientRootCA。
//
// 输入参数：
//   - clientRoot：PEM 编码的客户端根证书。
//
// 返回值：
//   - error：如果添加客户端根证书过程中出现错误，则返回相应的错误信息。
func (gServer *GRPCServer) appendClientRootCA(clientRoot []byte) error {
	// 将 PEM 编码的证书转换为 X.509 证书
	certs, err := pemToX509Certs(clientRoot)
	if err != nil {
		return errors.WithMessage(err, "将 PEM 编码的证书转换为 X.509 证书失败")
	}

	if len(certs) < 1 {
		return errors.New("未找到客户端根证书")
	}

	// 遍历证书列表，将每个证书添加到 gRPC 服务器的 TLS 配置中的客户端根证书列表中
	for _, cert := range certs {
		gServer.tls.AddClientRootCA(cert)
	}

	return nil
}

// 解析PEM编码的证书
func pemToX509Certs(pemCerts []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate

	// 有可能多个证书被编码
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}

		// 改为使用通用方法解析x509证书
		cert, err := common.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}

		certs = append(certs, cert)
	}

	return certs, nil
}

// SetClientRootCAs 根据一组 PEM 编码的 X509 证书机构列表，设置用于验证客户端证书的机构列表。
// 方法接收者：
//   - gServer：GRPCServer，表示 gRPC 服务器
//
// 输入参数：
//   - clientRoots：[][]byte，表示 PEM 编码的客户端根证书列表
//
// 返回值：
//   - error：表示设置客户端根证书时可能发生的错误
func (gServer *GRPCServer) SetClientRootCAs(clientRoots [][]byte) error {
	// 获取锁，确保线程安全
	gServer.lock.Lock()
	defer gServer.lock.Unlock()

	// 适配国密
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// 创建证书池
		certPool := x509GM.NewCertPool()
		// 遍历客户端根证书列表
		for _, clientRoot := range clientRoots {
			// 将 PEM 编码的证书添加到证书池中
			if !certPool.AppendCertsFromPEM(clientRoot) {
				return errors.New("无法设置客户端根证书")
			}
		}
		// 设置 gRPC 服务器的客户端 CA 列表
		gServer.tls.SetClientCAs(certPool)

	case factory.SoftwareBasedFactoryName:
		// 创建证书池
		certPool := x509.NewCertPool()
		// 遍历客户端根证书列表
		for _, clientRoot := range clientRoots {
			// 将 PEM 编码的证书添加到证书池中
			if !certPool.AppendCertsFromPEM(clientRoot) {
				return errors.New("无法设置客户端根证书")
			}
		}

		// 设置 gRPC 服务器的客户端 CA 列表
		gServer.tls.SetClientCAs(certPool)
	}

	return nil
}
