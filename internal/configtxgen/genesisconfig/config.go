/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package genesisconfig

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/viperutil"
	cf "github.com/hyperledger/fabric/core/config"
	config2 "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/msp"
	"path/filepath"
	"sync"
	"time"
)

const (
	// EtcdRaft 基于etcd的RAFT共识的类型键。
	EtcdRaft = "etcdraft"
)

var logger = flogging.MustGetLogger("common.tools.configtxgen.localconfig")

const (
	// SampleInsecureSoloProfile references the sample profile which does not
	// include any MSPs and uses solo for ordering.
	SampleInsecureSoloProfile = "SampleInsecureSolo"
	// SampleDevModeSoloProfile references the sample profile which requires
	// only basic membership for admin privileges and uses solo for ordering.
	SampleDevModeSoloProfile = "SampleDevModeSolo"
	// SampleSingleMSPSoloProfile references the sample profile which includes
	// only the sample MSP and uses solo for ordering.
	SampleSingleMSPSoloProfile = "SampleSingleMSPSolo"

	// SampleInsecureKafkaProfile references the sample profile which does not
	// include any MSPs and uses Kafka for ordering.
	SampleInsecureKafkaProfile = "SampleInsecureKafka"
	// SampleDevModeKafkaProfile references the sample profile which requires only
	// basic membership for admin privileges and uses Kafka for ordering.
	SampleDevModeKafkaProfile = "SampleDevModeKafka"
	// SampleSingleMSPKafkaProfile references the sample profile which includes
	// only the sample MSP and uses Kafka for ordering.
	SampleSingleMSPKafkaProfile = "SampleSingleMSPKafka"

	// SampleDevModeEtcdRaftProfile references the sample profile used for testing
	// the etcd/raft-based ordering service.
	SampleDevModeEtcdRaftProfile = "SampleDevModeEtcdRaft"

	// SampleAppChannelInsecureSoloProfile references the sample profile which
	// does not include any MSPs and uses solo for ordering.
	SampleAppChannelInsecureSoloProfile = "SampleAppChannelInsecureSolo"
	// SampleApppChannelEtcdRaftProfile references the sample profile used for
	// testing the etcd/raft-based ordering service using the channel
	// participation API.
	SampleAppChannelEtcdRaftProfile = "SampleAppChannelEtcdRaft"

	// SampleSingleMSPChannelProfile 引用仅包含示例MSP并用于创建通道的示例配置文件
	SampleSingleMSPChannelProfile = "SampleSingleMSPChannel"

	// SampleConsortiumName 是来自示例configtx.yaml的示例联盟
	SampleConsortiumName = "SampleConsortium"
	// SampleOrgName 是示例配置文件中示例组织的名称
	SampleOrgName = "SampleOrg"

	// AdminRoleAdminPrincipal 设置为AdminRole，以使Admin类型的MSP角色用作admin主体默认值
	AdminRoleAdminPrincipal = "Role.ADMIN"
)

// TopLevel 是由 configtxgen 工具使用的结构体。
type TopLevel struct {
	Profiles      map[string]*Profile        `yaml:"Profiles"`      // 配置文件的配置文件
	Organizations []*Organization            `yaml:"Organizations"` // 组织列表
	Channel       *Profile                   `yaml:"Channel"`       // 通道配置
	Application   *Application               `yaml:"Application"`   // 应用程序配置
	Orderer       *Orderer                   `yaml:"Orderer"`       // 排序节点配置
	Capabilities  map[string]map[string]bool `yaml:"Capabilities"`  // 功能列表
}

// Profile 编码了 configtxgen 工具的排序节点/应用程序配置组合。
type Profile struct {
	Consortium   string                 `yaml:"Consortium"`   // 联盟名称
	Application  *Application           `yaml:"Application"`  // 应用程序配置
	Orderer      *Orderer               `yaml:"Orderer"`      // 排序节点配置
	Consortiums  map[string]*Consortium `yaml:"Consortiums"`  // 联盟列表
	Capabilities map[string]bool        `yaml:"Capabilities"` // 功能列表
	Policies     map[string]*Policy     `yaml:"Policies"`     // 策略列表
}

// Policy 编码了通道配置策略。
type Policy struct {
	Type string `yaml:"Type"` // 策略类型
	Rule string `yaml:"Rule"` // 策略规则
}

// Consortium 表示一组可以相互创建通道的组织。
type Consortium struct {
	Organizations []*Organization `yaml:"Organizations"` // 组织列表
}

// Application 编码了配置交易中所需的应用程序级配置。
type Application struct {
	Organizations []*Organization    `yaml:"Organizations"` // 组织列表
	Capabilities  map[string]bool    `yaml:"Capabilities"`  // 功能列表
	Policies      map[string]*Policy `yaml:"Policies"`      // 策略列表
	ACLs          map[string]string  `yaml:"ACLs"`          // ACL 列表
}

// Organization 编码了配置交易中所需的组织级配置。
type Organization struct {
	Name             string             `yaml:"Name"`             // 组织名称
	ID               string             `yaml:"ID"`               // 组织标识
	MSPDir           string             `yaml:"MSPDir"`           // MSP 目录
	MSPType          string             `yaml:"MSPType"`          // MSP 类型
	Policies         map[string]*Policy `yaml:"Policies"`         // 策略列表
	AnchorPeers      []*AnchorPeer      `yaml:"AnchorPeers"`      // 锚节点列表
	OrdererEndpoints []string           `yaml:"OrdererEndpoints"` // 排序节点地址列表
	AdminPrincipal   string             `yaml:"AdminPrincipal"`   // 管理员主体（已弃用）
	SkipAsForeign    bool               // 跳过作为外部组织处理的标志
}

// AnchorPeer 编码了标识锚节点所需的必要字段。
type AnchorPeer struct {
	Host string `yaml:"Host"` // 主机名
	Port int    `yaml:"Port"` // 端口号
}

// Orderer 包含与通道相关的配置。
type Orderer struct {
	OrdererType   string                   `yaml:"OrdererType"`   // 排序节点类型
	Addresses     []string                 `yaml:"Addresses"`     // 排序节点地址列表
	BatchTimeout  time.Duration            `yaml:"BatchTimeout"`  // 批处理超时时间
	BatchSize     BatchSize                `yaml:"BatchSize"`     // 批处理大小
	Kafka         Kafka                    `yaml:"Kafka"`         // Kafka 配置
	EtcdRaft      *etcdraft.ConfigMetadata `yaml:"EtcdRaft"`      // EtcdRaft 配置
	Organizations []*Organization          `yaml:"Organizations"` // 组织列表
	MaxChannels   uint64                   `yaml:"MaxChannels"`   // 最大通道数
	Capabilities  map[string]bool          `yaml:"Capabilities"`  // 功能列表
	Policies      map[string]*Policy       `yaml:"Policies"`      // 策略列表
}

// BatchSize 包含影响批处理大小的配置。
type BatchSize struct {
	MaxMessageCount   uint32 `yaml:"MaxMessageCount"`   // 最大消息数
	AbsoluteMaxBytes  uint32 `yaml:"AbsoluteMaxBytes"`  // 绝对最大字节数
	PreferredMaxBytes uint32 `yaml:"PreferredMaxBytes"` // 首选最大字节数
}

// Kafka contains configuration for the Kafka-based orderer.
type Kafka struct {
	Brokers []string `yaml:"Brokers"`
}

var genesisDefaults = TopLevel{
	Orderer: &Orderer{
		OrdererType:  "solo",
		BatchTimeout: 2 * time.Second,
		BatchSize: BatchSize{
			MaxMessageCount:   500,
			AbsoluteMaxBytes:  10 * 1024 * 1024,
			PreferredMaxBytes: 2 * 1024 * 1024,
		},
		Kafka: Kafka{
			Brokers: []string{"127.0.0.1:9092"},
		},
		EtcdRaft: &etcdraft.ConfigMetadata{
			Options: &etcdraft.Options{
				TickInterval:         "500ms",
				ElectionTick:         10,
				HeartbeatTick:        1,
				MaxInflightBlocks:    5,
				SnapshotIntervalSize: 16 * 1024 * 1024, // 16 MB
			},
		},
	},
}

// LoadTopLevel simply loads the configtx.yaml file into the structs above and
// completes their initialization. Config paths may optionally be provided and
// will be used in place of the FABRIC_CFG_PATH env variable.
//
// Note, for environment overrides to work properly within a profile, Load
// should be used instead.
func LoadTopLevel(configPaths ...string) *TopLevel {
	config := viperutil.New()
	config.AddConfigPaths(configPaths...)
	config.SetConfigName("configtx")

	err := config.ReadInConfig()
	if err != nil {
		logger.Panicf("Error reading configuration: %s", err)
	}
	logger.Debugf("Using config file: %s", config.ConfigFileUsed())

	uconf, err := cache.load(config, config.ConfigFileUsed())
	if err != nil {
		logger.Panicf("failed to load configCache: %s", err)
	}
	uconf.completeInitialization(filepath.Dir(config.ConfigFileUsed()))
	logger.Infof("Loaded configuration: %s", config.ConfigFileUsed())

	return uconf
}

// Load 返回与给定配置文件对应的 orderer/application 配置组合。
// 输入参数：
//   - profile：string，表示要加载的配置文件的配置文件名
//   - configPaths：[]string，表示要搜索配置文件的路径（可选）
//
// 返回值：
//   - *Profile：表示加载的配置文件对应的 orderer/application 配置组合
func Load(profile string, configPaths ...string) *Profile {
	// 创建一个新的 ConfigParser 配置文件
	config := viperutil.New()
	config.AddConfigPaths(configPaths...)
	// 现在设置配置文件的名称。
	appNameWithoutExt := config2.GetConfig()
	fmt.Printf("使用配置文件: %s \n", appNameWithoutExt)
	// 如果用户使用与启动文件相同的名称, 并以yaml为文件后缀的配置, 我们优先考虑这个配置
	config.SetConfigName(appNameWithoutExt)
	if err := config.ReadInConfig(); err == nil {
		config.SetConfigName(appNameWithoutExt)
	} else {
		logger.Infof("使用默认配置文件: %s", "configtx")
		config.SetConfigName("configtx")
	}

	// 读取yaml配置文件
	err := config.ReadInConfig()
	if err != nil {
		logger.Panicf("读取配置时出错: %s", err)
	}

	// 加载的 TopLevel 配置结构 (yaml配置文件的一对一结构)
	uconf, err := cache.load(config, config.ConfigFileUsed())
	if err != nil {
		logger.Panicf("从配置缓存加载配置时出错: %s", err)
	}

	// 获取指定配置文件名对应的配置组合
	result, ok := uconf.Profiles[profile]
	if !ok {
		logger.Panicf("找不到 profile 配置选项: %s", profile)
	}

	// 完成配置组合的初始化验证
	result.completeInitialization(filepath.Dir(config.ConfigFileUsed()))

	logger.Infof("已加载的配置: %s", config.ConfigFileUsed())

	return result
}

func (t *TopLevel) completeInitialization(configDir string) {
	for _, org := range t.Organizations {
		org.completeInitialization(configDir)
	}

	if t.Orderer != nil {
		t.Orderer.completeInitialization(configDir)
	}
}

func (p *Profile) completeInitialization(configDir string) {
	if p.Application != nil {
		for _, org := range p.Application.Organizations {
			// 设置bccsp, 设置相对路径为绝对路径
			org.completeInitialization(configDir)
		}
	}

	if p.Consortiums != nil {
		for _, consortium := range p.Consortiums {
			for _, org := range consortium.Organizations {
				// 设置bccsp, 设置相对路径为绝对路径
				org.completeInitialization(configDir)
			}
		}
	}

	if p.Orderer != nil {
		for _, org := range p.Orderer.Organizations {
			// 设置bccsp, 设置相对路径为绝对路径
			org.completeInitialization(configDir)
		}
		// 配置orderer配置, 共识节点配置等
		p.Orderer.completeInitialization(configDir)
	}
}

// completeInitialization 完成组织的初始化过程。
// 方法接收者：
//   - org：Organization，表示要完成初始化的组织
//
// 输入参数：
//   - configDir：string，表示配置文件的目录路径
//
// 返回值：
//   - 无
func (org *Organization) completeInitialization(configDir string) {
	// 设置 MSP 类型；如果未指定，则默认为 BCCSP
	if org.MSPType == "" {
		org.MSPType = msp.ProviderTypeToString(msp.FABRIC)
	}

	// 如果未指定 AdminPrincipal，则设置为 AdminRoleAdminPrincipal, （已弃用）
	if org.AdminPrincipal == "" {
		org.AdminPrincipal = AdminRoleAdminPrincipal
	}

	// 将配置文件路径进行转换，将相对路径补充为绝对路径
	translatePaths(configDir, org)
}

func (ord *Orderer) completeInitialization(configDir string) {
loop:
	for {
		switch {
		case ord.OrdererType == "":
			logger.Infof("Orderer.OrdererType 排序节点类型未设置, 设置为 %v", genesisDefaults.Orderer.OrdererType)
			ord.OrdererType = genesisDefaults.Orderer.OrdererType
		case ord.BatchTimeout == 0:
			logger.Infof("Orderer.BatchTimeout 批处理超时时间未设置, 设置为 %s", genesisDefaults.Orderer.BatchTimeout)
			ord.BatchTimeout = genesisDefaults.Orderer.BatchTimeout
		case ord.BatchSize.MaxMessageCount == 0:
			logger.Infof("Orderer.BatchSize.MaxMessageCount 最大消息数未设置, 设置为 %v", genesisDefaults.Orderer.BatchSize.MaxMessageCount)
			ord.BatchSize.MaxMessageCount = genesisDefaults.Orderer.BatchSize.MaxMessageCount
		case ord.BatchSize.AbsoluteMaxBytes == 0:
			logger.Infof("Orderer.BatchSize.AbsoluteMaxBytes 绝对最大字节数未设置, 设置为 %v", genesisDefaults.Orderer.BatchSize.AbsoluteMaxBytes)
			ord.BatchSize.AbsoluteMaxBytes = genesisDefaults.Orderer.BatchSize.AbsoluteMaxBytes
		case ord.BatchSize.PreferredMaxBytes == 0:
			logger.Infof("Orderer.BatchSize.PreferredMaxBytes 首选最大字节数未设置, 设置为 %v", genesisDefaults.Orderer.BatchSize.PreferredMaxBytes)
			ord.BatchSize.PreferredMaxBytes = genesisDefaults.Orderer.BatchSize.PreferredMaxBytes
		default:
			break loop
		}
	}

	logger.Infof("OrdererType 共识类型: %s", ord.OrdererType)
	// 另外，共识类型相关的初始化在这里
	// 也使用这个来panic未知的orderer类型。
	switch ord.OrdererType {
	case "solo":
		// 这里没有什么可做的
	case "kafka":
		if ord.Kafka.Brokers == nil {
			logger.Infof("Orderer.Kafka 未设置, 设置为 %v", genesisDefaults.Orderer.Kafka.Brokers)
			ord.Kafka.Brokers = genesisDefaults.Orderer.Kafka.Brokers
		}
	case EtcdRaft:
		if ord.EtcdRaft == nil {
			logger.Panicf("%s 配置丢失", EtcdRaft)
		}
		if ord.EtcdRaft.Options == nil {
			logger.Infof("Orderer.EtcdRaft.Options 选举策略未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options)
			ord.EtcdRaft.Options = genesisDefaults.Orderer.EtcdRaft.Options
		}
	second_loop:
		for {
			switch {
			case ord.EtcdRaft.Options.TickInterval == "":
				logger.Infof("Orderer.EtcdRaft.Options.TickInterval 两次调用之间的时间间隔未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options.TickInterval)
				ord.EtcdRaft.Options.TickInterval = genesisDefaults.Orderer.EtcdRaft.Options.TickInterval

			case ord.EtcdRaft.Options.ElectionTick == 0:
				logger.Infof("Orderer.EtcdRaft.Options.ElectionTick 选举开始最低调用次数未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options.ElectionTick)
				ord.EtcdRaft.Options.ElectionTick = genesisDefaults.Orderer.EtcdRaft.Options.ElectionTick

			case ord.EtcdRaft.Options.HeartbeatTick == 0:
				logger.Infof("Orderer.EtcdRaft.Options.HeartbeatTick 选举开始最低心跳次数未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options.HeartbeatTick)
				ord.EtcdRaft.Options.HeartbeatTick = genesisDefaults.Orderer.EtcdRaft.Options.HeartbeatTick

			case ord.EtcdRaft.Options.MaxInflightBlocks == 0:
				logger.Infof("Orderer.EtcdRaft.Options.MaxInflightBlocks 限制乐观复制阶段的最大动态追加块数未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options.MaxInflightBlocks)
				ord.EtcdRaft.Options.MaxInflightBlocks = genesisDefaults.Orderer.EtcdRaft.Options.MaxInflightBlocks

			case ord.EtcdRaft.Options.SnapshotIntervalSize == 0:
				logger.Infof("Orderer.EtcdRaft.Options.SnapshotIntervalSize 定义拍摄快照的字节数未设置, 设置为 %v", genesisDefaults.Orderer.EtcdRaft.Options.SnapshotIntervalSize)
				ord.EtcdRaft.Options.SnapshotIntervalSize = genesisDefaults.Orderer.EtcdRaft.Options.SnapshotIntervalSize

			case len(ord.EtcdRaft.Consenters) == 0:
				logger.Panicf("%s 配置未指定任何 consenter 共识节点", EtcdRaft)

			default:
				break second_loop
			}
		}

		if _, err := time.ParseDuration(ord.EtcdRaft.Options.TickInterval); err != nil {
			logger.Panicf("EtcdRaft.Options.TickInterval (%s) 必须采用持续时间格式", ord.EtcdRaft.Options.TickInterval)
		}

		// 验证选项的指定成员
		if ord.EtcdRaft.Options.ElectionTick <= ord.EtcdRaft.Options.HeartbeatTick {
			logger.Panicf("EtcdRaft.Options.ElectionTick 选举调用次数必须大于 EtcdRaft.Options.HeartbeatTick 心跳次数")
		}

		// 检测共识节点配置
		for _, c := range ord.EtcdRaft.GetConsenters() {
			if c.Host == "" {
				logger.Panicf("consenter 共识节点 %s 配置未指定 Host 主机", EtcdRaft)
			}
			if c.Port == 0 {
				logger.Panicf("consenter 共识节点 %s 配置未指定 port 主机节点", EtcdRaft)
			}
			if c.ClientTlsCert == nil {
				logger.Panicf("consenter 共识节点 %s 配置未指定 ClientTlsCert 客户端tls证书", EtcdRaft)
			}
			if c.ServerTlsCert == nil {
				logger.Panicf("consenter 共识节点 %s 配置未指定 ServerTlsCert 服务端tls证书", EtcdRaft)
			}

			// 相对路径转绝对路径
			clientCertPath := string(c.GetClientTlsCert())
			cf.TranslatePathInPlace(configDir, &clientCertPath)
			c.ClientTlsCert = []byte(clientCertPath)
			serverCertPath := string(c.GetServerTlsCert())
			cf.TranslatePathInPlace(configDir, &serverCertPath)
			c.ServerTlsCert = []byte(serverCertPath)
		}
	default:
		logger.Panicf("未知 Orderer.OrdererType: %s", ord.OrdererType)
	}
}

func translatePaths(configDir string, org *Organization) {
	cf.TranslatePathInPlace(configDir, &org.MSPDir)
}

// configCache stores marshalled bytes of config structures that produced from
// EnhancedExactUnmarshal. Cache key is the path of the configuration file that was used.
type configCache struct {
	mutex sync.Mutex
	cache map[string][]byte
}

var cache = &configCache{
	cache: make(map[string][]byte),
}

// load 从 configCache 中加载 TopLevel 配置结构。
// 如果加载不成功，则解组合一个配置文件，并使用序列化的 TopLevel 结构填充 configCache。
// 方法接收者：
//   - c：configCache，表示配置缓存
//
// 输入参数：
//   - config：*viperutil.ConfigParser，表示用于读取配置文件的配置解析器
//   - configPath：string，表示配置文件的路径
//
// 返回值：
//   - *TopLevel：表示加载的 TopLevel 配置结构
//   - error：表示加载过程中可能发生的错误
func (c *configCache) load(config *viperutil.ConfigParser, configPath string) (*TopLevel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 由 configtxgen 工具使用的结构体。
	conf := &TopLevel{}
	serializedConf, ok := c.cache[configPath]
	logger.Debugf("从缓存加载配置: %t", ok)
	if !ok {
		// 将配置文件解析为一个结构体
		err := config.EnhancedExactUnmarshal(conf)
		if err != nil {
			return nil, fmt.Errorf("将配置解码到结构体时出错: %s", err)
		}

		serializedConf, err = json.Marshal(conf)
		if err != nil {
			return nil, err
		}
		c.cache[configPath] = serializedConf
	}

	err := json.Unmarshal(serializedConf, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
