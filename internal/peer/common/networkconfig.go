/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"io/ioutil"

	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

// NetworkConfig 提供了一个 Hyperledger Fabric 网络的静态定义。
type NetworkConfig struct {
	Name                   string                          `yaml:"name"`                   // 网络名称
	Xtype                  string                          `yaml:"x-type"`                 // 网络类型
	Description            string                          `yaml:"description"`            // 网络描述
	Version                string                          `yaml:"version"`                // 网络版本
	Channels               map[string]ChannelNetworkConfig `yaml:"channels"`               // 通道配置
	Organizations          map[string]OrganizationConfig   `yaml:"organizations"`          // 组织配置
	Peers                  map[string]PeerConfig           `yaml:"peers"`                  // 节点配置
	Client                 ClientConfig                    `yaml:"client"`                 // 客户端配置
	Orderers               map[string]OrdererConfig        `yaml:"orderers"`               // 排序节点配置
	CertificateAuthorities map[string]CAConfig             `yaml:"certificateAuthorities"` // 证书颁发机构配置
}

// ClientConfig - CLI当前未使用
type ClientConfig struct {
	Organization    string              `yaml:"organization"`    // 组织名称
	Logging         LoggingType         `yaml:"logging"`         // 日志类型
	CryptoConfig    CCType              `yaml:"cryptoconfig"`    // 加密配置类型
	TLS             TLSType             `yaml:"tls"`             // TLS类型
	CredentialStore CredentialStoreType `yaml:"credentialStore"` // 凭据存储类型
}

// LoggingType - CLI当前未使用
type LoggingType struct {
	Level string `yaml:"level"` // 日志级别
}

// CCType - CLI当前未使用
type CCType struct {
	Path string `yaml:"path"` // 路径
}

// TLSType - CLI当前未使用
type TLSType struct {
	Enabled bool `yaml:"enabled"` // 是否启用TLS
}

// CredentialStoreType - CLI当前未使用
type CredentialStoreType struct {
	Path        string `yaml:"path"` // 路径
	CryptoStore struct {
		Path string `yaml:"path"` // 加密存储路径
	}
	Wallet string `yaml:"wallet"` // 钱包
}

// ChannelNetworkConfig 提供了网络中通道的定义。
type ChannelNetworkConfig struct {
	Orderers   []string                     `yaml:"orderers"`   // 排序节点列表
	Peers      map[string]PeerChannelConfig `yaml:"peers"`      // peer节点通道配置
	Chaincodes []string                     `yaml:"chaincodes"` // 链码列表
}

// PeerChannelConfig 定义了节点的能力。
type PeerChannelConfig struct {
	EndorsingPeer  bool `yaml:"endorsingPeer"`  // 是否为背书节点
	ChaincodeQuery bool `yaml:"chaincodeQuery"` // 是否支持链码查询
	LedgerQuery    bool `yaml:"ledgerQuery"`    // 是否支持账本查询
	EventSource    bool `yaml:"eventSource"`    // 是否为事件源
}

// OrganizationConfig 提供了网络中组织的定义。
// CLI当前未使用。
type OrganizationConfig struct {
	MspID                  string    `yaml:"mspid"`                  // 组织的MSP标识
	Peers                  []string  `yaml:"peers"`                  // 组织的节点列表
	CryptoPath             string    `yaml:"cryptoPath"`             // 组织的加密路径
	CertificateAuthorities []string  `yaml:"certificateAuthorities"` // 组织的证书颁发机构列表
	AdminPrivateKey        TLSConfig `yaml:"adminPrivateKey"`        // 管理员私钥配置
	SignedCert             TLSConfig `yaml:"signedCert"`             // 签名证书配置
}

// OrdererConfig 定义了一个orderer配置
// CLI当前未使用
type OrdererConfig struct {
	URL         string                 `yaml:"url"`         // URL
	GrpcOptions map[string]interface{} `yaml:"grpcOptions"` // gRPC选项
	TLSCACerts  TLSConfig              `yaml:"tlsCACerts"`  // TLS证书配置
}

// PeerConfig 定义了节点的配置。
type PeerConfig struct {
	URL         string                 `yaml:"url"`         // 节点的URL
	EventURL    string                 `yaml:"eventUrl"`    // 节点的事件URL
	GRPCOptions map[string]interface{} `yaml:"grpcOptions"` // 节点的GRPC选项
	TLSCACerts  TLSConfig              `yaml:"tlsCACerts"`  // 节点的TLS CA证书配置
}

// CAConfig 定义了一个CA配置
// CLI当前未使用
type CAConfig struct {
	URL         string                 `yaml:"url"`         // URL
	HTTPOptions map[string]interface{} `yaml:"httpOptions"` // HTTP选项
	TLSCACerts  MutualTLSConfig        `yaml:"tlsCACerts"`  // TLS证书配置
	Registrar   EnrollCredentials      `yaml:"registrar"`   // 注册者凭证
	CaName      string                 `yaml:"caName"`      // CA名称
}

// EnrollCredentials 包含用于注册的凭证
// CLI当前未使用
type EnrollCredentials struct {
	EnrollID     string `yaml:"enrollId"`     // 注册ID
	EnrollSecret string `yaml:"enrollSecret"` // 注册密码
}

// TLSConfig 提供了TLS配置。
type TLSConfig struct {
	Path string `yaml:"path"` // 证书根证书路径
	Pem  string `yaml:"pem"`  // 证书实际内容
}

// MutualTLSConfig 定义了双向TLS配置
// CLI当前未使用
type MutualTLSConfig struct {
	Pem    []string   `yaml:"pem"`    // PEM证书列表
	Path   string     `yaml:"path"`   // TLS验证的根证书路径（逗号分隔的路径列表）
	Client TLSKeyPair `yaml:"client"` // 客户端TLS信息
}

// TLSKeyPair 包含用于TLS加密的私钥和证书
// CLI当前未使用
type TLSKeyPair struct {
	Key  TLSConfig `yaml:"key"`  // 私钥配置
	Cert TLSConfig `yaml:"cert"` // 证书配置
}

// GetConfig 方法用于将提供的连接配置文件解析为网络配置结构。
// 方法接收者：无（全局函数）
// 输入参数：
//   - fileName：string 类型，表示连接配置文件名。
//
// 返回值：
//   - *NetworkConfig：表示解析后的网络配置结构。
//   - error：如果解析连接配置文件时出现错误，则返回错误。
func GetConfig(fileName string) (*NetworkConfig, error) {
	// 检查文件名是否为空
	if fileName == "" {
		return nil, errors.New("connectionProfile 连接配置文件名不能为空")
	}

	// 读取连接配置文件内容
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "读取 connectionProfile 连接配置文件时出错")
	}

	// 将文件内容转换为字符串
	configData := string(data)

	// 解析YAML格式的配置数据到NetworkConfig结构
	config := &NetworkConfig{}
	err = yaml.Unmarshal([]byte(configData), &config)
	if err != nil {
		return nil, errors.Wrap(err, "反序列化 connectionProfile.yaml 连接配置时出错")
	}

	return config, nil
}
