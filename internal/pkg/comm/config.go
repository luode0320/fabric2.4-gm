/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// 配置默认值

// grpc客户端和服务器的最大发送和接收字节数
const (
	DefaultMaxRecvMsgSize = 100 * 1024 * 1024
	DefaultMaxSendMsgSize = 100 * 1024 * 1024
)

var (
	// DefaultKeepaliveOptions 默认对等keepalive存活选项
	// 它可以确保连接在一段时间内保持活动状态，避免因为长时间没有活动而被关闭。
	DefaultKeepaliveOptions = KeepaliveOptions{
		// 指定对等节点作为客户端发送 keepalive 消息的时间间隔。在这个例子中，设置为 1 分钟
		ClientInterval: time.Duration(1) * time.Minute,
		// 指定对等节点作为客户端等待 keepalive 响应的超时时间。在这个例子中，设置为 20 秒
		ClientTimeout: time.Duration(20) * time.Second,
		// 指定对等节点作为服务器发送 keepalive 消息的时间间隔。在这个例子中，设置为 2 小时
		ServerInterval: time.Duration(2) * time.Hour,
		// 指定对等节点作为服务器等待 keepalive 响应的超时时间。在这个例子中，设置为 20 秒
		ServerTimeout: time.Duration(20) * time.Second,
		// 指定对等节点作为服务器发送 keepalive 消息的最小时间间隔。在这个例子中，设置为 1 分钟
		ServerMinInterval: time.Duration(1) * time.Minute,
	}
	// DefaultTLSCipherSuites 强大的TLS密码套件
	// 这里补充国密
	DefaultTLSCipherSuites = []uint16{
		// 使用 ECDHE 密钥交换和 AES 128 位 GCM 加密算法的 RSA 密码套件
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		// 使用 ECDHE 密钥交换和 AES 256 位 GCM 加密算法的 RSA 密码套件
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		// 使用 ECDHE 密钥交换和 AES 128 位 GCM 加密算法的 ECDSA 密码套件
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		// 使用 ECDHE 密钥交换和 AES 256 位 GCM 加密算法的 ECDSA 密码套件
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		// 使用 RSA 密钥交换和 AES 128 位 GCM 加密算法的 RSA 密码套件
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		// 使用 RSA 密钥交换和 AES 256 位 GCM 加密算法的 RSA 密码套件
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		// 国密提交的密码套件
		gmtls.GMTLS_SM2_WITH_SM4_SM3,
		gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3,
		// TLS_FALLBACK_SCSV 是一个指示客户端进行版本回退的标识符，不是标准的密码套件
		tls.TLS_FALLBACK_SCSV,
	}

	// DefaultConnectionTimeout 默认连接超时
	DefaultConnectionTimeout = 5 * time.Second
)

// ServerConfig 结构体定义了配置 GRPCServer 实例的参数。
type ServerConfig struct {
	// ConnectionTimeout 指定所有新连接的连接建立超时时间。
	ConnectionTimeout time.Duration
	// SecOpts 定义了安全参数。
	SecOpts SecureOptions
	// KaOpts 定义了Keepalive参数。
	KaOpts KeepaliveOptions
	// StreamInterceptors 指定要应用于流式RPC的拦截器列表。它们按顺序执行。
	StreamInterceptors []grpc.StreamServerInterceptor
	// UnaryInterceptors 指定要应用于一元RPC的拦截器列表。它们按顺序执行。
	UnaryInterceptors []grpc.UnaryServerInterceptor
	// Logger 指定服务器将使用的日志记录器。
	Logger *flogging.FabricLogger
	// HealthCheckEnabled 启用服务器的gRPC健康检查协议。
	HealthCheckEnabled bool
	// ServerStatsHandler 如果要报告连接的指标，则应设置ServerStatsHandler。
	ServerStatsHandler *ServerStatsHandler
	// MaxRecvMsgSize 服务器可以接收的最大消息大小。
	MaxRecvMsgSize int
	// MaxSendMsgSize 服务器可以发送的最大消息大小。
	MaxSendMsgSize int
}

// ClientConfig 定义了配置 GRPCClient 实例的参数。
type ClientConfig struct {
	SecOpts        SecureOptions    // 安全选项
	KaOpts         KeepaliveOptions // Keepalive 选项
	DialTimeout    time.Duration    // 建立连接的超时时间
	AsyncConnect   bool             // 异步连接，使连接创建非阻塞
	MaxRecvMsgSize int              // 客户端可以接收的最大消息大小
	MaxSendMsgSize int              // 客户端可以发送的最大消息大小
}

// DialOptions 将ClientConfig转换为grpc.DialOptions的适当集合。
func (cc ClientConfig) DialOptions() ([]grpc.DialOption, error) {
	// 配置gRPC客户端的选项
	var dialOpts []grpc.DialOption

	// 配置gRPC客户端的心跳保活机制，以确保客户端和服务器之间的连接保持活跃
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                cc.KaOpts.ClientInterval, // 客户端发送心跳的时间间隔
		Timeout:             cc.KaOpts.ClientTimeout,  // 户端等待服务器响应的超时时间
		PermitWithoutStream: true,                     // 客户端是否允许在没有活动流的情况下发送心跳
	}))

	// cc.AsyncConnect为false，则会将两个额外的gRPC客户端选项添加到dialOpts中
	if !cc.AsyncConnect {
		dialOpts = append(dialOpts,
			grpc.WithBlock(),                  //选项会使grpc.Dial方法在连接建立之前阻塞，直到连接成功建立或发生错误
			grpc.FailOnNonTempDialError(true), // 这个选项会使grpc.Dial方法在遇到非临时性的拨号错误时返回错误。例如连接被拒绝或找不到目标服务器。
		)
	}

	// 将发送/接收消息大小设置为包默认值
	maxRecvMsgSize := DefaultMaxRecvMsgSize
	if cc.MaxRecvMsgSize != 0 {
		maxRecvMsgSize = cc.MaxRecvMsgSize
	}
	maxSendMsgSize := DefaultMaxSendMsgSize
	if cc.MaxSendMsgSize != 0 {
		maxSendMsgSize = cc.MaxSendMsgSize
	}
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(maxRecvMsgSize),
		grpc.MaxCallSendMsgSize(maxSendMsgSize),
	))

	// 配置tls参数
	tlsConfig, err := cc.SecOpts.TLSConfig()
	if err != nil {
		return nil, err
	}

	// 国密改造兼容gmtls
	if tlsConfig != nil {
		var transportCreds *DynamicClientCredentials
		switch tlsconf := tlsConfig.(type) {
		case *tls.Config:
			transportCreds = &DynamicClientCredentials{TLSConfig: tlsconf}
		case tls.Config:
			transportCreds = &DynamicClientCredentials{TLSConfig: &tlsconf}
		case *gmtls.Config:
			transportCreds = &DynamicClientCredentials{GMTLSConfig: tlsconf}
		case gmtls.Config:
			transportCreds = &DynamicClientCredentials{GMTLSConfig: &tlsconf}
		}
		// 添加tls加密通讯
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(transportCreds))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	return dialOpts, nil
}

// Dial 使用给定的地址创建一个 gRPC 客户端连接。
// 方法接收者：ClientConfig
// 输入参数：
//   - address：要连接的地址。
//
// 返回值：
//   - *grpc.ClientConn：创建的 gRPC 客户端连接。
//   - error：创建过程中的错误，如果没有错误则为 nil。
func (cc ClientConfig) Dial(address string) (*grpc.ClientConn, error) {
	// 转换为 grpc 的tls配置
	grpcConfig, err := cc.DialOptions()
	if err != nil {
		return nil, err
	}

	// 创建一个带有超时的上下文, 并在超时时自动取消操作，以避免长时间的阻塞。
	ctx, cancel := context.WithTimeout(context.Background(), cc.DialTimeout)
	defer cancel()

	// 使用上下文和 grpc 选项创建 gRPC 客户端连接
	conn, err := grpc.DialContext(ctx, address, grpcConfig...)
	if err != nil {
		return nil, errors.Wrap(err, "创建 gRPC 新连接失败")
	}
	return conn, nil
}

// SecureOptions 结构体定义了GRPCServer或GRPCClient实例的TLS安全参数。
type SecureOptions struct {
	// VerifyCertificate 在TLS客户端或服务器进行正常证书验证后调用。
	VerifyCertificate func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
	// 补充国密的TLS客户端或服务器进行正常证书验证后调用。
	VerifyCertificateGM func(rawCerts [][]byte, verifiedChains [][]*x509GM.Certificate) error
	// 用于TLS通信的PEM编码的服务器证书
	Certificate []byte
	// 用于TLS通信的PEM编码的私钥
	Key []byte
	// 用于客户端验证服务器证书的一组PEM编码的X509证书颁发机构
	ServerRootCAs [][]byte
	// 用于服务器验证客户端证书的一组PEM编码的X509证书
	ClientRootCAs [][]byte
	// 是否使用TLS进行通信
	UseTLS bool
	// 是否要求TLS客户端提供证书进行身份验证
	RequireClientCert bool
	// TLS的支持的密码套件列表
	CipherSuites []uint16
	// 通过给定的持续时间使TLS握手时间采样向过去偏移
	TimeShift time.Duration
	// 用于验证返回的证书上的主机名。除非它是IP地址，否则它也包含在客户端的握手中以支持虚拟主机。
	ServerNameOverride string
}

// TLSConfig 创建tls配置
func (so SecureOptions) TLSConfig() (interface{}, error) {
	// 如果未启用TLS，则返回
	if !so.UseTLS {
		return nil, nil
	}

	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		// gRPC 服务器使用的 TLS 配置,NewTLSConfig 中有部分默认配置
		tlsConfig := NewTLSConfig(&gmtls.Config{
			ServerName: so.ServerNameOverride, //服务器名称
		})

		// 时间, TLS握手过程中使用偏移后的时间。这在某些情况下可能会用于调整时间戳，以解决时间不同步的问题。
		if so.TimeShift > 0 {
			tlsConfig.gmconfig.Time = func() time.Time {
				return time.Now().Add((-1) * so.TimeShift)
			}
		}

		// 根证书
		if len(so.ServerRootCAs) > 0 {
			tlsConfig.gmconfig.RootCAs = x509GM.NewCertPool()
			for _, certBytes := range so.ServerRootCAs {
				if !tlsConfig.gmconfig.RootCAs.AppendCertsFromPEM(certBytes) {
					return nil, errors.New("添加根证书时出错")
				}
			}
		}

		// 客户端证书, 如果服务端需要的话
		if so.RequireClientCert {
			cert, err := so.ClientCertificate()
			if err != nil {
				return nil, errors.WithMessage(err, "无法加载客户端证书")
			}

			// 完成的x509证书
			var certificate *x509GM.Certificate
			if cert.Leaf != nil {
				certificate = gm.ParseX509Certificate2Sm2(cert.Leaf)
			}

			// tls证书
			tlsCert := gmtls.Certificate{
				Certificate:                 cert.Certificate,
				PrivateKey:                  cert.PrivateKey,
				SignedCertificateTimestamps: cert.SignedCertificateTimestamps,
				Leaf:                        certificate,
			}
			tlsConfig.gmconfig.Certificates = append(tlsConfig.gmconfig.Certificates, tlsCert)
		}

		return tlsConfig.gmconfig, nil
	case factory.SoftwareBasedFactoryName:
		// gRPC 服务器使用的 TLS 配置, NewTLSConfig有部分默认配置
		tlsConfig := NewTLSConfig(&tls.Config{
			ServerName:            so.ServerNameOverride, //服务器名称
			VerifyPeerCertificate: so.VerifyCertificate,  // 在TLS客户端或服务器进行正常证书验证后调用。
		})

		// 时间, TLS握手过程中使用偏移后的时间。这在某些情况下可能会用于调整时间戳，以解决时间不同步的问题。
		if so.TimeShift > 0 {
			tlsConfig.config.Time = func() time.Time {
				return time.Now().Add((-1) * so.TimeShift)
			}
		}

		// 根证书
		if len(so.ServerRootCAs) > 0 {
			tlsConfig.config.RootCAs = x509.NewCertPool()
			for _, certBytes := range so.ServerRootCAs {
				if !tlsConfig.config.RootCAs.AppendCertsFromPEM(certBytes) {
					return nil, errors.New("添加根证书时出错")
				}
			}
		}

		// 客户端证书
		if so.RequireClientCert {
			cert, err := so.ClientCertificate()
			if err != nil {
				return nil, errors.WithMessage(err, "无法加载客户端证书")
			}
			tlsConfig.config.Certificates = append(tlsConfig.config.Certificates, cert)
		}

		return tlsConfig.config, nil
	}

	return nil, nil
}

// ClientCertificate 返回将用于TLS握手的客户端证书。
func (so SecureOptions) ClientCertificate() (tls.Certificate, error) {
	if so.Key == nil || so.Certificate == nil {
		return tls.Certificate{}, errors.New("使用TLS握手时需要密钥和证书")
	}
	cert, err := common.X509KeyPair(so.Certificate, so.Key)
	if err != nil {
		return tls.Certificate{}, errors.WithMessage(err, "创建密钥对失败")
	}
	return cert, nil
}

// KeepaliveOptions 用于为客户端和服务器设置gRPC keepalive设置
type KeepaliveOptions struct {
	// ClientInterval 是持续时间，在该持续时间之后，如果客户端没有看到来自服务器的任何活动，它将对服务器执行pings以查看其是否处于活动状态
	ClientInterval time.Duration
	// ClientTimeout 是客户端在关闭连接之前发送ping之后等待服务器响应的持续时间
	ClientTimeout time.Duration
	// ServerInterval 是持续时间，在该持续时间之后，如果服务器没有看到来自客户端的任何活动，它将对客户端执行pings以查看其是否处于活动状态
	ServerInterval time.Duration
	// ServerTimeout 是服务器在关闭连接之前发送ping之后等待客户端响应的持续时间
	ServerTimeout time.Duration
	// ServerMinInterval 是客户端之间的最短允许时间。
	// 如果客户端更频繁地发送pings，则服务器将断开它们的连接
	ServerMinInterval time.Duration
}

// ServerKeepaliveOptions returns gRPC keepalive options for a server.
func (ka KeepaliveOptions) ServerKeepaliveOptions() []grpc.ServerOption {
	var serverOpts []grpc.ServerOption
	kap := keepalive.ServerParameters{
		Time:    ka.ServerInterval,
		Timeout: ka.ServerTimeout,
	}
	serverOpts = append(serverOpts, grpc.KeepaliveParams(kap))
	kep := keepalive.EnforcementPolicy{
		MinTime: ka.ServerMinInterval,
		// allow keepalive w/o rpc
		PermitWithoutStream: true,
	}
	serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(kep))
	return serverOpts
}

// ClientKeepaliveOptions returns gRPC keepalive dial options for clients.
func (ka KeepaliveOptions) ClientKeepaliveOptions() []grpc.DialOption {
	var dialOpts []grpc.DialOption
	kap := keepalive.ClientParameters{
		Time:                ka.ClientInterval,
		Timeout:             ka.ClientTimeout,
		PermitWithoutStream: true,
	}
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(kap))
	return dialOpts
}

type Metrics struct {
	// OpenConnCounter keeps track of number of open connections
	OpenConnCounter metrics.Counter
	// ClosedConnCounter keeps track of number connections closed
	ClosedConnCounter metrics.Counter
}
