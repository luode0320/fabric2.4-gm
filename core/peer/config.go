/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// The 'viper' package for configuration handling is very flexible, but has
// been found to have extremely poor performance when configuration values are
// accessed repeatedly. The function CacheConfiguration() defined here caches
// all configuration values that are accessed frequently.  These parameters
// are now presented as function calls that access local configuration
// variables.  This seems to be the most robust way to represent these
// parameters in the face of the numerous ways that configuration files are
// loaded and used (e.g, normal usage vs. test cases).

// The CacheConfiguration() function is allowed to be called globally to
// ensure that the correct values are always cached; See for example how
// certain parameters are forced in 'ChaincodeDevMode' in main.go.

package peer

import (
	"crypto/tls"
	"fmt"
	"github.com/hyperledger/fabric/bccsp/common"
	"io/ioutil"
	"net"
	"path/filepath"
	"runtime"
	"time"

	"github.com/hyperledger/fabric/common/viperutil"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	gatewayconfig "github.com/hyperledger/fabric/internal/pkg/gateway/config"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// ExternalBuilder 表示链码外部构建器的配置结构体。
type ExternalBuilder struct {
	// TODO: 删除3.0中的环境
	// 已弃用: Environment 保留是为了向后兼容。
	// 新部署应使用新的 PropagateEnvironment 字段
	// Environment 表示环境白名单，用于传递给链码外部构建器的环境变量。
	Environment []string `yaml:"environmentWhitelist"`

	// PropagateEnvironment 表示要传递给链码外部构建器的环境变量。
	PropagateEnvironment []string `yaml:"propagateEnvironment"`

	// Name 表示链码外部构建器的名称。
	Name string `yaml:"name"`

	// Path 表示链码外部构建器的路径。
	Path string `yaml:"path"`
}

// Config 是定义peer配置的结构。
type Config struct {
	// LocalMSPID 是本地MSP的标识符。
	LocalMSPID string
	// ListenAddress 是对等体将侦听的本地地址。
	// 它必须格式化为 [host | ipaddr]:port。
	ListenAddress string
	// PeerID 提供此对等实例的名称。在命名docker资源以隔离结构网络和对等方时使用它。
	PeerID string
	// PeerAddress是其他对等体和客户端应用于与对等体通信的地址。它必须格式化为 [host | ipaddr]:port。
	// 当被CLI使用时，它表示目标对等端点。
	PeerAddress string
	// NetworkID 指定用于网络逻辑分离的名称。
	// 它在命名docker资源时用于隔离fabric网络和对等方。
	NetworkID string
	// ChaincodeListenAddress 此对等方将在其上侦听链码连接的终结点。
	// 如果省略，则默认为PeerAddress的主机部分和端口7052的节点。
	ChaincodeListenAddress string
	// ChaincodeAddress 指定由对等方启动的端点链码应用于连接到对等方。
	// 如果省略，则默认为 ChaincodeListenAddress 并回退到 ListenAddress 。
	ChaincodeAddress string
	// ValidatorPoolSize 指示将并行执行事务验证的goroutine的数量。
	// 如果省略，则默认为计算机上的硬件线程数。
	ValidatorPoolSize int

	// ----- Peer 传递客户端Keepalive -----
	// DeliverClientKeepaliveOptions 设置，用于与排序节点进行通信。
	DeliverClientKeepaliveOptions comm.KeepaliveOptions

	// ----- 配置文件 -----
	// TODO: create 用于配置文件配置的单独子结构。

	// ProfileEnabled 确定是否在对等端中启用了go ppprof端点。
	ProfileEnabled bool
	// ProfileListenAddress 是ppprof服务器应接受连接的地址。
	ProfileListenAddress string

	// ----- 发现 -----

	// 发现服务用于客户端查询对等方的信息，
	// 例如-哪些对等体加入了某个通道，最新的通道配置是什么，
	// 最重要的是-给定一个链码和一个通道，哪些可能的对等体集满足背书策略。
	// TODO: 创建用于发现配置的单独子结构。

	// DiscoveryEnabled 用于启用发现服务。
	DiscoveryEnabled bool
	// DiscoveryOrgMembersAllowed 允许非管理员执行非通道范围的查询。
	DiscoveryOrgMembersAllowed bool
	// DiscoveryAuthCacheEnabled 用于启用身份验证缓存。
	DiscoveryAuthCacheEnabled bool
	// DiscoveryAuthCacheMaxSize 设置身份验证缓存的最大大小。
	DiscoveryAuthCacheMaxSize int
	// DiscoveryAuthCachePurgeRetentionRatio 设置清除人口过剩后保留在缓存中的条目的比例。
	DiscoveryAuthCachePurgeRetentionRatio float64

	// ----- 限制 -----
	// 限制用于配置一些内部资源限制。
	// TODO: 为Limits config创建单独的子结构。

	// LimitsConcurrencyEndorserService 设置并发请求发送给负责链码部署、查询和调用的endorser service的限制，
	// 包括用户链代码和系统链代码。
	LimitsConcurrencyEndorserService int

	// LimitsConcurrencyDeliverService 设置为块和事务事件提供服务而注册的并发事件侦听器的限制。
	LimitsConcurrencyDeliverService int

	// LimitsConcurrencyGatewayService 设置对处理事务提交和评估的网关服务的并发请求的限制。
	LimitsConcurrencyGatewayService int

	// ----- TLS -----
	// 需要服务器端TLS。
	// TODO: 为PeerTLS配置创建单独的子结构。

	// PeerTLSEnabled 启用/禁用对等TLS。
	PeerTLSEnabled bool

	// ----- 身份验证 -----
	// Authentication 包含与验证客户端消息相关的配置参数。
	// TODO: 为身份验证配置创建单独的子结构。

	// AuthenticationTimeWindow 为客户端请求消息中指定的当前服务器时间和客户端时间设置可接受的持续时间。
	AuthenticationTimeWindow time.Duration

	// Endpoint vm管理系统。对于docker通常可以是以下之一
	// unix:///var/run/docker.sock
	// http://localhost:2375
	// https://localhost:2376
	// 如果您使用外部链码构建器，并且不需要默认的Docker链码构建器，
	// 应该取消配置端点，以便不会注册对等方的Docker运行状况检查器。
	VMEndpoint string

	// ----- vm.docker.tls -----
	// TODO: 为VM.Docker.TLS配置创建单独的子结构。

	// VMDockerTLSEnabled 为docker启用/禁用TLS。
	VMDockerTLSEnabled bool
	// 出于调试目的启用/禁用链码容器中的标准out/err
	VMDockerAttachStdout bool
	// VMNetworkMode 设置容器的网络模式。
	VMNetworkMode string

	// ChaincodePull 启用/禁用强制拉取基本docker映像。
	ChaincodePull bool
	// ExternalBuilders 表示链代码的生成器和启动器。
	// 外部构建器检测处理将按照下面指定的顺序迭代构建器。
	ExternalBuilders []ExternalBuilder

	// ----- Operations 配置 -----
	// TODO: 为操作配置创建单独的子结构。

	// OperationsListenAddress 为operations server提供主机和端口
	OperationsListenAddress string
	// OperationsTLSEnabled 为操作启用/禁用TLS。
	OperationsTLSEnabled bool
	// OperationsTLSCertFile 为operations server提供PEM编码的服务器证书的路径。
	OperationsTLSCertFile string
	// OperationsTLSKeyFile 为operations server提供PEM编码的服务器密钥的路径。
	OperationsTLSKeyFile string
	// OperationsTLSClientAuthRequired 启用/禁用TLS层的客户端证书身份验证要求以访问所有资源。
	OperationsTLSClientAuthRequired bool
	// OperationsTLSClientRootCAs 提供PEM编码的ca证书的路径以信任客户端身份验证。
	OperationsTLSClientRootCAs []string

	// ----- Metrics 指标配置 -----
	// TODO: 为指标配置创建单独的子结构。

	// MetricsProvider 提供度量提供程序的类别，其为statsd、prometheus或disabled之一。
	MetricsProvider string
	// StatsdNetwork 指示statsd metrics使用的网络类型。(tcp或udp)。
	StatsdNetwork string
	// StatsdAaddress 提供statsd服务器的地址。
	StatsdAaddress string
	// StatsdWriteInterval 设置推送本地缓存的计数器和仪表的时间间隔。
	StatsdWriteInterval time.Duration
	// StatsdPrefix 前缀附加到所有发出的statsd度量
	StatsdPrefix string

	// ----- Docker 配置 ------

	// DockerCert 是访问docker守护程序所需的PEM编码的TLS客户端证书的路径。
	DockerCert string
	// DockerKey 是访问docker守护程序所需的PEM编码密钥的路径。
	DockerKey string
	// DockerCA 是docker守护程序的PEM编码的CA证书的路径。
	DockerCA string

	// ----- Gateway 配置 -----

	// 客户端sdk使用网关服务来与fabric网络交互
	GatewayOptions gatewayconfig.Options
}

// GlobalConfig 函数从viper获取一组配置，构建并返回配置结构体。
//
// 返回值：
//   - *Config: Config 结构体指针，表示配置信息
//   - error: 如果加载配置过程中出现错误，则返回相应的错误信息
func GlobalConfig() (*Config, error) {
	c := &Config{}
	if err := c.load(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) load() error {
	// peer的 ip:port
	peerAddress, err := getLocalAddress()
	if err != nil {
		return err
	}
	// 获取给定配置文件路径的目录部分
	configDir := filepath.Dir(viper.ConfigFileUsed())

	c.PeerAddress = peerAddress
	c.PeerID = viper.GetString("peer.id")
	c.LocalMSPID = viper.GetString("peer.localMspId")
	c.ListenAddress = viper.GetString("peer.listenAddress")

	c.AuthenticationTimeWindow = viper.GetDuration("peer.authentication.timewindow")
	if c.AuthenticationTimeWindow == 0 {
		defaultTimeWindow := 15 * time.Minute
		logger.Warningf("`peer.authentication.timewindow` 当前服务器时间和客户端时间设置可接受的持续时间未设置; 默认为 %s", defaultTimeWindow)
		c.AuthenticationTimeWindow = defaultTimeWindow
	}

	c.PeerTLSEnabled = viper.GetBool("peer.tls.enabled")
	c.NetworkID = viper.GetString("peer.networkId")
	c.LimitsConcurrencyEndorserService = viper.GetInt("peer.limits.concurrency.endorserService")
	c.LimitsConcurrencyDeliverService = viper.GetInt("peer.limits.concurrency.deliverService")
	c.LimitsConcurrencyGatewayService = viper.GetInt("peer.limits.concurrency.gatewayService")
	c.ProfileEnabled = viper.GetBool("peer.profile.enabled")
	c.ProfileListenAddress = viper.GetString("peer.profile.listenAddress")
	c.DiscoveryEnabled = viper.GetBool("peer.discovery.enabled")
	c.DiscoveryOrgMembersAllowed = viper.GetBool("peer.discovery.orgMembersAllowedAccess")
	c.DiscoveryAuthCacheEnabled = viper.GetBool("peer.discovery.authCacheEnabled")
	c.DiscoveryAuthCacheMaxSize = viper.GetInt("peer.discovery.authCacheMaxSize")
	c.DiscoveryAuthCachePurgeRetentionRatio = viper.GetFloat64("peer.discovery.authCachePurgeRetentionRatio")
	c.ChaincodeListenAddress = viper.GetString("peer.chaincodeListenAddress")
	c.ChaincodeAddress = viper.GetString("peer.chaincodeAddress")

	c.ValidatorPoolSize = viper.GetInt("peer.validatorPoolSize")
	if c.ValidatorPoolSize <= 0 {
		c.ValidatorPoolSize = runtime.NumCPU()
	}

	// 对等keepalive存活选项
	c.DeliverClientKeepaliveOptions = comm.DefaultKeepaliveOptions
	if viper.IsSet("peer.keepalive.deliveryClient.interval") {
		c.DeliverClientKeepaliveOptions.ClientInterval = viper.GetDuration("peer.keepalive.deliveryClient.interval")
	}
	if viper.IsSet("peer.keepalive.deliveryClient.timeout") {
		c.DeliverClientKeepaliveOptions.ClientTimeout = viper.GetDuration("peer.keepalive.deliveryClient.timeout")
	}

	// 网关配置
	c.GatewayOptions = gatewayconfig.GetOptions(viper.GetViper())

	// 虚拟机管理系统
	c.VMEndpoint = viper.GetString("vm.endpoint")
	c.VMDockerTLSEnabled = viper.GetBool("vm.docker.tls.enabled")
	c.DockerCert = config.GetPath("vm.docker.tls.cert.file")
	c.DockerKey = config.GetPath("vm.docker.tls.key.file")
	c.DockerCA = config.GetPath("vm.docker.tls.ca.file")
	c.VMDockerAttachStdout = viper.GetBool("vm.docker.attachStdout")

	c.VMNetworkMode = viper.GetString("vm.docker.hostConfig.NetworkMode")
	if c.VMNetworkMode == "" {
		c.VMNetworkMode = "host"
	}

	// 是否拉取docker镜像
	c.ChaincodePull = viper.GetBool("chaincode.pull")

	// 外部链码
	var externalBuilders []ExternalBuilder
	err = viper.UnmarshalKey("chaincode.externalBuilders", &externalBuilders, viper.DecodeHook(viperutil.YamlStringToStructHook(externalBuilders)))
	if err != nil {
		return err
	}

	c.ExternalBuilders = externalBuilders
	for builderIndex, builder := range c.ExternalBuilders {
		if builder.Path == "" {
			return fmt.Errorf("外部链码生成器 chaincode.externalBuilders 配置无效, 一个或多个链码生成器中缺少路径 path 属性")
		}
		if builder.Name == "" {
			return fmt.Errorf("path: %s 处的外部链码构建器没有名称 name 属性", builder.Path)
		}
		if builder.Environment != nil && builder.PropagateEnvironment == nil {
			c.ExternalBuilders[builderIndex].PropagateEnvironment = builder.Environment
		}
	}

	// operations 运行配置
	c.OperationsListenAddress = viper.GetString("operations.listenAddress")
	c.OperationsTLSEnabled = viper.GetBool("operations.tls.enabled")
	c.OperationsTLSCertFile = config.GetPath("operations.tls.cert.file")
	c.OperationsTLSKeyFile = config.GetPath("operations.tls.key.file")
	c.OperationsTLSClientAuthRequired = viper.GetBool("operations.tls.clientAuthRequired")

	for _, rca := range viper.GetStringSlice("operations.tls.clientRootCAs.files") {
		c.OperationsTLSClientRootCAs = append(c.OperationsTLSClientRootCAs, config.TranslatePath(configDir, rca))
	}

	// metrics 指标
	c.MetricsProvider = viper.GetString("metrics.provider")
	c.StatsdNetwork = viper.GetString("metrics.statsd.network")
	c.StatsdAaddress = viper.GetString("metrics.statsd.address")
	c.StatsdWriteInterval = viper.GetDuration("metrics.statsd.writeInterval")
	c.StatsdPrefix = viper.GetString("metrics.statsd.prefix")

	return nil
}

// getLocalAddress 函数返回本地对等节点正在运行的地址和端口。受 env:peer.addressAutoDetect 环境变量的影响。
//
// 返回值：
//   - string: 本地对等节点的地址和端口
//   - error: 如果获取本地地址过程中出现错误，则返回相应的错误信息
func getLocalAddress() (string, error) {
	peerAddress := viper.GetString("peer.address")
	if peerAddress == "" {
		return "", fmt.Errorf("peer.address 未设置")
	}

	// 拆分主机端口
	host, port, err := net.SplitHostPort(peerAddress)
	if err != nil {
		return "", errors.Errorf("peer.address 不符合 host:port 格式: %s", peerAddress)
	}

	// 返回主机的回环本地IP地址, 回环本地IP地址是 127.0.0.1/localhost/本机ip
	localIP, err := getLocalIP()
	if err != nil {
		peerLogger.Errorf("本地IP地址不可自动检测: %s", err)
		return "", err
	}

	// 将主机和端口组合成 “host:port” 形式的网络地址
	autoDetectedIPAndPort := net.JoinHostPort(localIP, port)
	peerLogger.Info("自动检测到的对等地址:", autoDetectedIPAndPort)

	// 如果host是IPv4地址 "0.0.0.0" 或IPv6地址 "::"，
	// 然后回退到自动检测到的回环地址
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		peerLogger.Info("peer.address 配置 Host 是", host, ", 回退到自动检测到的地址:", autoDetectedIPAndPort)
		return autoDetectedIPAndPort, nil
	}

	if viper.GetBool("peer.addressAutoDetect") {
		peerLogger.Info("设置了自动检测标志 peer.addressAutoDetect, 返回", autoDetectedIPAndPort)
		return autoDetectedIPAndPort, nil
	}

	peerLogger.Info("使用配置生效的对等地址:", peerAddress)
	return peerAddress, nil
}

// getLocalIP 函数返回主机的回环本地IP地址, 回环本地IP地址是 127.0.0.1/localhost/本机ip
//
// 返回值：
//   - string: 主机的回环本地IP地址
//   - error: 如果获取本地IP地址过程中出现错误，则返回相应的错误信息
func getLocalIP() (string, error) {
	// 返回系统的单播接口地址列表
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// 检查地址类型，如果不是回环地址，则返回该地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.Errorf("不是回环地址 127.0.0.1/localhost, 检测到为IPv4接口")
}

// GetServerConfig 返回对等方的gRPC服务器配置
func GetServerConfig() (comm.ServerConfig, error) {
	serverConfig := comm.ServerConfig{
		ConnectionTimeout: viper.GetDuration("peer.connectiontimeout"),
		SecOpts: comm.SecureOptions{
			UseTLS: viper.GetBool("peer.tls.enabled"),
		},
	}
	if serverConfig.SecOpts.UseTLS {
		// 获取tls证书
		serverCert, err := ioutil.ReadFile(config.GetPath("peer.tls.cert.file"))
		if err != nil {
			return serverConfig, fmt.Errorf("加载 peer.tls.cert.file 证书时出错 (%s)", err)
		}

		// 获取tls私钥
		serverKey, err := ioutil.ReadFile(config.GetPath("peer.tls.key.file"))
		if err != nil {
			return serverConfig, fmt.Errorf("加载 peer.tls.key.file 密钥时出错 (%s)", err)
		}

		serverConfig.SecOpts.Certificate = serverCert
		serverConfig.SecOpts.Key = serverKey

		// 获取tls根证书
		if config.GetPath("peer.tls.rootcert.file") != "" {
			rootCert, err := ioutil.ReadFile(config.GetPath("peer.tls.rootcert.file"))
			if err != nil {
				return serverConfig, fmt.Errorf("加载 peer.tls.rootcert.file 根证书时出错 (%s)", err)
			}
			serverConfig.SecOpts.ServerRootCAs = [][]byte{rootCert}
		}

		serverConfig.SecOpts.RequireClientCert = viper.GetBool("peer.tls.clientAuthRequired")
		if serverConfig.SecOpts.RequireClientCert {
			var clientRoots [][]byte
			for _, file := range viper.GetStringSlice("peer.tls.clientRootCAs.files") {
				clientRoot, err := ioutil.ReadFile(
					config.TranslatePath(filepath.Dir(viper.ConfigFileUsed()), file))
				if err != nil {
					return serverConfig,
						fmt.Errorf("正在加载 peer.tls.clientRootCAs.files客户端根证书错误 (%s)", err)
				}
				clientRoots = append(clientRoots, clientRoot)
			}
			serverConfig.SecOpts.ClientRootCAs = clientRoots
		}
	}
	// 获取默认的keepalive选项
	serverConfig.KaOpts = comm.DefaultKeepaliveOptions
	// 检查是否为env设置了时间间隔
	if viper.IsSet("peer.keepalive.interval") {
		serverConfig.KaOpts.ServerInterval = viper.GetDuration("peer.keepalive.interval")
	}
	// 检查是否为env设置了超时
	if viper.IsSet("peer.keepalive.timeout") {
		serverConfig.KaOpts.ServerTimeout = viper.GetDuration("peer.keepalive.timeout")
	}
	// 检查是否为env设置了minInterval
	if viper.IsSet("peer.keepalive.minInterval") {
		serverConfig.KaOpts.ServerMinInterval = viper.GetDuration("peer.keepalive.minInterval")
	}

	// grpc客户端和服务器的最大发送和接收字节数
	serverConfig.MaxRecvMsgSize = comm.DefaultMaxRecvMsgSize
	serverConfig.MaxSendMsgSize = comm.DefaultMaxSendMsgSize

	if viper.IsSet("peer.maxRecvMsgSize") {
		serverConfig.MaxRecvMsgSize = int(viper.GetInt32("peer.maxRecvMsgSize"))
	}
	if viper.IsSet("peer.maxSendMsgSize") {
		serverConfig.MaxSendMsgSize = int(viper.GetInt32("peer.maxSendMsgSize"))
	}
	return serverConfig, nil
}

// GetClientCertificate 函数返回用于gRPC客户端连接的TLS证书。
//
// 返回值：
//   - tls.Certificate: 表示TLS证书。
//   - error: 表示获取证书过程中的错误。
func GetClientCertificate() (tls.Certificate, error) {
	cert := tls.Certificate{}

	keyPath := viper.GetString("peer.tls.clientKey.file")
	certPath := viper.GetString("peer.tls.clientCert.file")

	if keyPath != "" || certPath != "" {
		// 需要同时设置 keyPath 和 certPath
		if keyPath == "" || certPath == "" {
			return cert, errors.New("peer.tls.clientKey.file 和 " +
				"peer.tls.clientCert.file 必须同时设置或必须同时为空")
		}
		keyPath = config.GetPath("peer.tls.clientKey.file")
		certPath = config.GetPath("peer.tls.clientCert.file")

	} else {
		// 使用 TLS 服务器的密钥对
		keyPath = viper.GetString("peer.tls.key.file")
		certPath = viper.GetString("peer.tls.cert.file")

		if keyPath != "" || certPath != "" {
			// 需要同时设置 keyPath 和 certPath
			if keyPath == "" || certPath == "" {
				return cert, errors.New("peer.tls.key.file 和 " +
					"peer.tls.cert.file 必须同时设置或必须同时为空")
			}
			keyPath = config.GetPath("peer.tls.key.file")
			certPath = config.GetPath("peer.tls.cert.file")
		} else {
			return cert, errors.New("当 peer.tls.enabled 设置为true, 必须设置 " +
				"[peer.tls.key.file 和 peer.tls.cert.file] 或 " +
				"[peer.tls.clientKey.file 和 peer.tls.clientCert.file]")
		}
	}

	// 从一对文件中读取和解析公钥/私钥对。文件必须包含PEM编码的数据。
	// 证书文件可以包含叶子证书之后的中间证书，从而形成一个证书链。成功返回证书
	cert, err := common.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return cert, errors.WithMessage(err,
			"解析客户端TLS密钥对时出错")
	}
	return cert, nil
}
