// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package localconfig

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	bccsp "github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/viperutil"
	config2 "github.com/hyperledger/fabric/core/config"
	coreconfig "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"path/filepath"
	"sync"
	"time"
)

var logger = flogging.MustGetLogger("localconfig")

// TopLevel directly corresponds to the orderer config YAML.
type TopLevel struct {
	General              General
	FileLedger           FileLedger
	Kafka                Kafka
	Debug                Debug
	Consensus            Consensus
	Operations           Operations
	Metrics              Metrics
	ChannelParticipation ChannelParticipation
	Admin                Admin
}

// General 包含了应用于所有排序服务类型通用的配置信息。
type General struct {
	// ListenAddress 指定了排序服务监听的网络地址。
	ListenAddress string

	// ListenPort 指定了排序服务监听的端口号。
	ListenPort uint16

	// TLS 包含了Transport Layer Security (TLS) 相关的配置信息，用于安全通信。
	TLS TLS

	// Cluster 包含了集群间通信的配置细节。
	Cluster Cluster

	// Keepalive 定义了保持活动连接的策略，以防网络空闲时连接中断。
	Keepalive Keepalive

	// ConnectionTimeout 设定了对外部连接建立的超时时间。
	ConnectionTimeout time.Duration

	// GenesisMethod 是遗留字段，仅为了兼容性保留，未来将由BootstrapMethod替代，用于指示创世区块的生成方法。
	GenesisMethod string

	// GenesisFile 是遗留字段，仅为了兼容性保留，未来将由BootstrapFile替代，指定了创世区块或引导配置文件的路径。
	GenesisFile string

	// BootstrapMethod 指定了启动排序服务时使用的引导方法。
	BootstrapMethod string

	// BootstrapFile 指定了排序服务启动时加载的引导配置文件路径。
	BootstrapFile string

	// Profile 配置了性能剖析相关设置。
	Profile Profile

	// LocalMSPDir 指向了本地成员服务提供者(MSP)的目录路径。
	LocalMSPDir string

	// LocalMSPID 指定了排序服务所属的本地MSP的标识符。
	LocalMSPID string

	// BCCSP 指定了区块链加密服务提供者(BCCSP)的配置选项，用于加密和解密操作。
	BCCSP *bccsp.FactoryOpts

	// Authentication 包含了身份验证相关的配置信息。
	Authentication Authentication

	// MaxRecvMsgSize 设定了可接收消息的最大尺寸限制，单位为字节。
	MaxRecvMsgSize int32

	// MaxSendMsgSize 设定了可发送消息的最大尺寸限制，单位为字节。
	MaxSendMsgSize int32
}

type Cluster struct {
	ListenAddress                        string
	ListenPort                           uint16
	ServerCertificate                    string
	ServerPrivateKey                     string
	ClientCertificate                    string
	ClientPrivateKey                     string
	RootCAs                              []string
	DialTimeout                          time.Duration
	RPCTimeout                           time.Duration
	ReplicationBufferSize                int
	ReplicationPullTimeout               time.Duration
	ReplicationRetryTimeout              time.Duration
	ReplicationBackgroundRefreshInterval time.Duration
	ReplicationMaxRetries                int
	SendBufferSize                       int
	CertExpirationWarningThreshold       time.Duration
	TLSHandshakeTimeShift                time.Duration
}

// Keepalive contains configuration for gRPC servers.
type Keepalive struct {
	ServerMinInterval time.Duration
	ServerInterval    time.Duration
	ServerTimeout     time.Duration
}

// TLS contains configuration for TLS connections.
type TLS struct {
	Enabled               bool
	PrivateKey            string
	Certificate           string
	RootCAs               []string
	ClientAuthRequired    bool
	ClientRootCAs         []string
	TLSHandshakeTimeShift time.Duration
}

// SASLPlain contains configuration for SASL/PLAIN authentication
type SASLPlain struct {
	Enabled  bool
	User     string
	Password string
}

// Authentication contains configuration parameters related to authenticating
// client messages.
type Authentication struct {
	TimeWindow         time.Duration
	NoExpirationChecks bool
}

// Profile contains configuration for Go pprof profiling.
type Profile struct {
	Enabled bool
	Address string
}

// FileLedger contains configuration for the file-based ledger.
type FileLedger struct {
	Location string
	Prefix   string // For compatibility only. This setting is no longer supported.
}

// Kafka contains configuration for the Kafka-based orderer.
type Kafka struct {
	Retry     Retry
	Verbose   bool
	Version   sarama.KafkaVersion // TODO Move this to global config
	TLS       TLS
	SASLPlain SASLPlain
	Topic     Topic
}

// Retry contains configuration related to retries and timeouts when the
// connection to the Kafka cluster cannot be established, or when Metadata
// requests needs to be repeated (because the cluster is in the middle of a
// leader election).
type Retry struct {
	ShortInterval   time.Duration
	ShortTotal      time.Duration
	LongInterval    time.Duration
	LongTotal       time.Duration
	NetworkTimeouts NetworkTimeouts
	Metadata        Metadata
	Producer        Producer
	Consumer        Consumer
}

// NetworkTimeouts 包含了针对Kafka集群的网络请求的套接字超时设置。
// 这些超时设置分别应用于建立连接、读取数据和写入数据的操作。
type NetworkTimeouts struct {
	// DialTimeout 指定建立到Kafka代理的初始连接时的超时时间。
	DialTimeout time.Duration

	// ReadTimeout 设定了等待Kafka代理响应的读取操作超时时间。
	ReadTimeout time.Duration

	// WriteTimeout 定义了向Kafka代理发送数据的写入操作超时时间。
	WriteTimeout time.Duration
}

// Metadata contains configuration for the metadata requests to the Kafka
// cluster.
type Metadata struct {
	RetryMax     int
	RetryBackoff time.Duration
}

// Producer contains configuration for the producer's retries when failing to
// post a message to a Kafka partition.
type Producer struct {
	RetryMax     int
	RetryBackoff time.Duration
}

// Consumer contains configuration for the consumer's retries when failing to
// read from a Kafa partition.
type Consumer struct {
	RetryBackoff time.Duration
}

// Topic contains the settings to use when creating Kafka topics
type Topic struct {
	ReplicationFactor int16
}

// Debug contains configuration for the orderer's debug parameters.
type Debug struct {
	BroadcastTraceDir string
	DeliverTraceDir   string
}

// Operations configures the operations endpoint for the orderer.
type Operations struct {
	ListenAddress string
	TLS           TLS
}

// Metrics configures the metrics provider for the orderer.
type Metrics struct {
	Provider string
	Statsd   Statsd
}

// Statsd provides the configuration required to emit statsd metrics from the orderer.
type Statsd struct {
	Network       string
	Address       string
	WriteInterval time.Duration
	Prefix        string
}

// Admin configures the admin endpoint for the orderer.
type Admin struct {
	ListenAddress string
	TLS           TLS
}

// ChannelParticipation 为订购方提供通道参与 API 配置。
// 通道参与使用 Operations 服务的相同 ListenAddress 和 TLS 设置。
type ChannelParticipation struct {
	Enabled            bool   // 是否启用通道参与
	MaxRequestBodySize uint32 // 最大请求体大小
}

type Consensus struct {
	WALDir  string
	SnapDir string
}

// Defaults 携带默认的orderer配置值。
var Defaults = TopLevel{
	General: General{
		ListenAddress:   "127.0.0.1",
		ListenPort:      7050,
		BootstrapMethod: "file",
		BootstrapFile:   "genesisblock",
		Profile: Profile{
			Enabled: false,
			Address: "0.0.0.0:6060",
		},
		Cluster: Cluster{
			ReplicationMaxRetries:                12,
			RPCTimeout:                           time.Second * 7,
			DialTimeout:                          time.Second * 5,
			ReplicationBufferSize:                20971520,
			SendBufferSize:                       10,
			ReplicationBackgroundRefreshInterval: time.Minute * 5,
			ReplicationRetryTimeout:              time.Second * 5,
			ReplicationPullTimeout:               time.Second * 5,
			CertExpirationWarningThreshold:       time.Hour * 24 * 7,
		},
		LocalMSPDir: "msp",
		LocalMSPID:  "SampleOrg",
		BCCSP:       bccsp.GetDefaultOpts(),
		Authentication: Authentication{
			TimeWindow: time.Duration(15 * time.Minute),
		},
		MaxRecvMsgSize: comm.DefaultMaxRecvMsgSize,
		MaxSendMsgSize: comm.DefaultMaxSendMsgSize,
	},
	FileLedger: FileLedger{
		Location: "/var/hyperledger/production/orderer",
	},
	Kafka: Kafka{
		Retry: Retry{
			ShortInterval: 1 * time.Minute,
			ShortTotal:    10 * time.Minute,
			LongInterval:  10 * time.Minute,
			LongTotal:     12 * time.Hour,
			NetworkTimeouts: NetworkTimeouts{
				DialTimeout:  30 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			},
			Metadata: Metadata{
				RetryBackoff: 250 * time.Millisecond,
				RetryMax:     3,
			},
			Producer: Producer{
				RetryBackoff: 100 * time.Millisecond,
				RetryMax:     3,
			},
			Consumer: Consumer{
				RetryBackoff: 2 * time.Second,
			},
		},
		Verbose: false,
		Version: sarama.V0_10_2_0,
		TLS: TLS{
			Enabled: false,
		},
		Topic: Topic{
			ReplicationFactor: 3,
		},
	},
	Debug: Debug{
		BroadcastTraceDir: "",
		DeliverTraceDir:   "",
	},
	Operations: Operations{
		ListenAddress: "127.0.0.1:0",
	},
	Metrics: Metrics{
		Provider: "disabled",
	},
	ChannelParticipation: ChannelParticipation{
		Enabled:            false,
		MaxRequestBodySize: 1024 * 1024,
	},
	Admin: Admin{
		ListenAddress: "127.0.0.1:0",
	},
}

// Load parses the orderer YAML file and environment, producing
// a struct suitable for config use, returning error on failure.
func Load() (*TopLevel, error) {
	return cache.load()
}

// configCache stores marshalled bytes of config structures that produced from
// EnhancedExactUnmarshal. Cache key is the path of the configuration file that was used.
type configCache struct {
	mutex sync.Mutex
	cache map[string][]byte
}

var cache = &configCache{}

// Load will load the configuration and cache it on the first call; subsequent
// calls will return a clone of the configuration that was previously loaded.
func (c *configCache) load() (*TopLevel, error) {
	var uconf TopLevel

	config := viperutil.New()
	// 现在设置配置文件的名称。
	appNameWithoutExt := config2.GetConfig()
	fmt.Printf("使用配置文件: %s \n", appNameWithoutExt)
	// 如果用户使用与启动文件相同的名称, 并以yaml为文件后缀的配置, 我们优先考虑这个配置
	config.SetConfigName(appNameWithoutExt)
	if err := config.ReadInConfig(); err == nil {
		config.SetConfigName(appNameWithoutExt)
	} else {
		logger.Infof("使用默认配置文件: %s", "orderer")
		config.SetConfigName("orderer")
	}

	if err := config.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("Error reading configuration: %s", err)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	serializedConf, ok := c.cache[config.ConfigFileUsed()]
	if !ok {
		err := config.EnhancedExactUnmarshal(&uconf)
		if err != nil {
			return nil, fmt.Errorf("Error unmarshalling config into struct: %s", err)
		}

		serializedConf, err = json.Marshal(uconf)
		if err != nil {
			return nil, err
		}

		if c.cache == nil {
			c.cache = map[string][]byte{}
		}
		c.cache[config.ConfigFileUsed()] = serializedConf
	}

	err := json.Unmarshal(serializedConf, &uconf)
	if err != nil {
		return nil, err
	}
	uconf.completeInitialization(filepath.Dir(config.ConfigFileUsed()))

	return &uconf, nil
}

func (c *TopLevel) completeInitialization(configDir string) {
	defer func() {
		// Translate any paths for cluster TLS configuration if applicable
		if c.General.Cluster.ClientPrivateKey != "" {
			coreconfig.TranslatePathInPlace(configDir, &c.General.Cluster.ClientPrivateKey)
		}
		if c.General.Cluster.ClientCertificate != "" {
			coreconfig.TranslatePathInPlace(configDir, &c.General.Cluster.ClientCertificate)
		}
		c.General.Cluster.RootCAs = translateCAs(configDir, c.General.Cluster.RootCAs)
		// Translate any paths for general TLS configuration
		c.General.TLS.RootCAs = translateCAs(configDir, c.General.TLS.RootCAs)
		c.General.TLS.ClientRootCAs = translateCAs(configDir, c.General.TLS.ClientRootCAs)
		coreconfig.TranslatePathInPlace(configDir, &c.General.TLS.PrivateKey)
		coreconfig.TranslatePathInPlace(configDir, &c.General.TLS.Certificate)
		coreconfig.TranslatePathInPlace(configDir, &c.General.BootstrapFile)
		coreconfig.TranslatePathInPlace(configDir, &c.General.LocalMSPDir)
		// Translate file ledger location
		coreconfig.TranslatePathInPlace(configDir, &c.FileLedger.Location)

		coreconfig.TranslatePathInPlace(configDir, &c.Admin.TLS.Certificate)
		coreconfig.TranslatePathInPlace(configDir, &c.Admin.TLS.PrivateKey)

		// Translate paths for Consensus
		coreconfig.TranslatePathInPlace(configDir, &c.Consensus.WALDir)
		coreconfig.TranslatePathInPlace(configDir, &c.Consensus.SnapDir)

		c.Admin.TLS.ClientRootCAs = translateCAs(configDir, c.Admin.TLS.ClientRootCAs)
	}()

	for {
		switch {
		case c.General.ListenAddress == "":
			logger.Infof("General.ListenAddress unset, setting to %s", Defaults.General.ListenAddress)
			c.General.ListenAddress = Defaults.General.ListenAddress
		case c.General.ListenPort == 0:
			logger.Infof("General.ListenPort unset, setting to %v", Defaults.General.ListenPort)
			c.General.ListenPort = Defaults.General.ListenPort
		case c.General.BootstrapMethod == "":
			if c.General.GenesisMethod != "" {
				// This is to keep the compatibility with old config file that uses genesismethod
				logger.Warn("General.GenesisMethod should be replaced by General.BootstrapMethod")
				c.General.BootstrapMethod = c.General.GenesisMethod
			} else {
				c.General.BootstrapMethod = Defaults.General.BootstrapMethod
			}
		case c.General.BootstrapFile == "":
			if c.General.GenesisFile != "" {
				// This is to keep the compatibility with old config file that uses genesisfile
				logger.Warn("General.GenesisFile should be replaced by General.BootstrapFile")
				c.General.BootstrapFile = c.General.GenesisFile
			} else {
				c.General.BootstrapFile = Defaults.General.BootstrapFile
			}
		case c.General.Cluster.RPCTimeout == 0:
			c.General.Cluster.RPCTimeout = Defaults.General.Cluster.RPCTimeout
		case c.General.Cluster.DialTimeout == 0:
			c.General.Cluster.DialTimeout = Defaults.General.Cluster.DialTimeout
		case c.General.Cluster.ReplicationMaxRetries == 0:
			c.General.Cluster.ReplicationMaxRetries = Defaults.General.Cluster.ReplicationMaxRetries
		case c.General.Cluster.SendBufferSize == 0:
			c.General.Cluster.SendBufferSize = Defaults.General.Cluster.SendBufferSize
		case c.General.Cluster.ReplicationBufferSize == 0:
			c.General.Cluster.ReplicationBufferSize = Defaults.General.Cluster.ReplicationBufferSize
		case c.General.Cluster.ReplicationPullTimeout == 0:
			c.General.Cluster.ReplicationPullTimeout = Defaults.General.Cluster.ReplicationPullTimeout
		case c.General.Cluster.ReplicationRetryTimeout == 0:
			c.General.Cluster.ReplicationRetryTimeout = Defaults.General.Cluster.ReplicationRetryTimeout
		case c.General.Cluster.ReplicationBackgroundRefreshInterval == 0:
			c.General.Cluster.ReplicationBackgroundRefreshInterval = Defaults.General.Cluster.ReplicationBackgroundRefreshInterval
		case c.General.Cluster.CertExpirationWarningThreshold == 0:
			c.General.Cluster.CertExpirationWarningThreshold = Defaults.General.Cluster.CertExpirationWarningThreshold
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.Certificate == "":
			logger.Panicf("General.Kafka.TLS.Certificate must be set if General.Kafka.TLS.Enabled is set to true.")
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.PrivateKey == "":
			logger.Panicf("General.Kafka.TLS.PrivateKey must be set if General.Kafka.TLS.Enabled is set to true.")
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.RootCAs == nil:
			logger.Panicf("General.Kafka.TLS.CertificatePool must be set if General.Kafka.TLS.Enabled is set to true.")

		case c.Kafka.SASLPlain.Enabled && c.Kafka.SASLPlain.User == "":
			logger.Panic("General.Kafka.SASLPlain.User must be set if General.Kafka.SASLPlain.Enabled is set to true.")
		case c.Kafka.SASLPlain.Enabled && c.Kafka.SASLPlain.Password == "":
			logger.Panic("General.Kafka.SASLPlain.Password must be set if General.Kafka.SASLPlain.Enabled is set to true.")

		case c.General.Profile.Enabled && c.General.Profile.Address == "":
			logger.Infof("Profiling enabled and General.Profile.Address unset, setting to %s", Defaults.General.Profile.Address)
			c.General.Profile.Address = Defaults.General.Profile.Address

		case c.General.LocalMSPDir == "":
			logger.Infof("General.LocalMSPDir unset, setting to %s", Defaults.General.LocalMSPDir)
			c.General.LocalMSPDir = Defaults.General.LocalMSPDir
		case c.General.LocalMSPID == "":
			logger.Infof("General.LocalMSPID unset, setting to %s", Defaults.General.LocalMSPID)
			c.General.LocalMSPID = Defaults.General.LocalMSPID

		case c.General.Authentication.TimeWindow == 0:
			logger.Infof("General.Authentication.TimeWindow unset, setting to %s", Defaults.General.Authentication.TimeWindow)
			c.General.Authentication.TimeWindow = Defaults.General.Authentication.TimeWindow

		case c.Kafka.Retry.ShortInterval == 0:
			logger.Infof("Kafka.Retry.ShortInterval unset, setting to %v", Defaults.Kafka.Retry.ShortInterval)
			c.Kafka.Retry.ShortInterval = Defaults.Kafka.Retry.ShortInterval
		case c.Kafka.Retry.ShortTotal == 0:
			logger.Infof("Kafka.Retry.ShortTotal unset, setting to %v", Defaults.Kafka.Retry.ShortTotal)
			c.Kafka.Retry.ShortTotal = Defaults.Kafka.Retry.ShortTotal
		case c.Kafka.Retry.LongInterval == 0:
			logger.Infof("Kafka.Retry.LongInterval unset, setting to %v", Defaults.Kafka.Retry.LongInterval)
			c.Kafka.Retry.LongInterval = Defaults.Kafka.Retry.LongInterval
		case c.Kafka.Retry.LongTotal == 0:
			logger.Infof("Kafka.Retry.LongTotal unset, setting to %v", Defaults.Kafka.Retry.LongTotal)
			c.Kafka.Retry.LongTotal = Defaults.Kafka.Retry.LongTotal

		case c.Kafka.Retry.NetworkTimeouts.DialTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.DialTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.DialTimeout)
			c.Kafka.Retry.NetworkTimeouts.DialTimeout = Defaults.Kafka.Retry.NetworkTimeouts.DialTimeout
		case c.Kafka.Retry.NetworkTimeouts.ReadTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.ReadTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.ReadTimeout)
			c.Kafka.Retry.NetworkTimeouts.ReadTimeout = Defaults.Kafka.Retry.NetworkTimeouts.ReadTimeout
		case c.Kafka.Retry.NetworkTimeouts.WriteTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.WriteTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.WriteTimeout)
			c.Kafka.Retry.NetworkTimeouts.WriteTimeout = Defaults.Kafka.Retry.NetworkTimeouts.WriteTimeout

		case c.Kafka.Retry.Metadata.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Metadata.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Metadata.RetryBackoff)
			c.Kafka.Retry.Metadata.RetryBackoff = Defaults.Kafka.Retry.Metadata.RetryBackoff
		case c.Kafka.Retry.Metadata.RetryMax == 0:
			logger.Infof("Kafka.Retry.Metadata.RetryMax unset, setting to %v", Defaults.Kafka.Retry.Metadata.RetryMax)
			c.Kafka.Retry.Metadata.RetryMax = Defaults.Kafka.Retry.Metadata.RetryMax

		case c.Kafka.Retry.Producer.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Producer.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Producer.RetryBackoff)
			c.Kafka.Retry.Producer.RetryBackoff = Defaults.Kafka.Retry.Producer.RetryBackoff
		case c.Kafka.Retry.Producer.RetryMax == 0:
			logger.Infof("Kafka.Retry.Producer.RetryMax unset, setting to %v", Defaults.Kafka.Retry.Producer.RetryMax)
			c.Kafka.Retry.Producer.RetryMax = Defaults.Kafka.Retry.Producer.RetryMax

		case c.Kafka.Retry.Consumer.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Consumer.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Consumer.RetryBackoff)
			c.Kafka.Retry.Consumer.RetryBackoff = Defaults.Kafka.Retry.Consumer.RetryBackoff

		case c.Kafka.Version == sarama.KafkaVersion{}:
			logger.Infof("Kafka.Version unset, setting to %v", Defaults.Kafka.Version)
			c.Kafka.Version = Defaults.Kafka.Version

		case c.Admin.TLS.Enabled && !c.Admin.TLS.ClientAuthRequired:
			logger.Panic("Admin.TLS.ClientAuthRequired must be set to true if Admin.TLS.Enabled is set to true")

		case c.General.MaxRecvMsgSize == 0:
			logger.Infof("General.MaxRecvMsgSize is unset, setting to %v", Defaults.General.MaxRecvMsgSize)
			c.General.MaxRecvMsgSize = Defaults.General.MaxRecvMsgSize
		case c.General.MaxSendMsgSize == 0:
			logger.Infof("General.MaxSendMsgSize is unset, setting to %v", Defaults.General.MaxSendMsgSize)
			c.General.MaxSendMsgSize = Defaults.General.MaxSendMsgSize
		default:
			return
		}
	}
}

func translateCAs(configDir string, certificateAuthorities []string) []string {
	var results []string
	for _, ca := range certificateAuthorities {
		result := coreconfig.TranslatePath(configDir, ca)
		results = append(results, result)
	}
	return results
}
