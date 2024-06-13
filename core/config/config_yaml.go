package config

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// 模板yaml
const (
	DEFAULT_CHANNEL           = "default/default_channel"
	DEFAULT_ORG               = "default/default_org"
	DEFAULT_PEER              = "default/default_peer"
	DEFAULT_ORDERER           = "default/default_orderer"
	DEFAULT_TLS_ORDERER       = "default/default_tls_orderer"
	DEFAULT_USER_PEER         = "default/default_user_peer"
	DEFAULT_USER_ORDERER      = "default/default_user_orderer"
	DEFAULT_USER_PEER_ORDERER = "default/default_user_peer_orderer"
)

// 新建yaml
const (
	CHANNEL_CREATE    = "configtxgen"
	CHAINCODE_PACKAGE = "chaincode.package."
	CHAINCODE_INSTALL = "chaincode.install."
	CHANNEL_JOIN      = "channel.join."
	CHANNEL_SIGN      = "channel.sign."
	CHANNEL_FETCH     = "channel.fetch."
	CHANNEL_UPDATE    = "channel.update."
	CHAINCODE_AGREE_  = "chaincode.approveformyorg."
	CHAINCODE_COMMIT  = "chaincode.commit."
	CHAINCODE_INIT    = "chaincode.init."
	CHAINCODE_INVOKE  = "chaincode.invoke."
)

func GetConfig() string {
	appName := filepath.Base(os.Args[0])
	appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
	appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")

	// 为了兼容idea本地启动
	// 所有命令都需要使用节点的全名(orderer0.dns.com/peer0.org1.dns.com) 或者, 使用 中文-全名 的格式, 全名用于映射配置文件
	if appNameWithoutExt[0] == '.' {
		// 使用字符串切片去掉第一个字符
		appNameWithoutExt = appNameWithoutExt[1:]
	}

	// ./configtxgen
	if CHANNEL_CREATE == appNameWithoutExt {
		return CHANNEL_CREATE
	}

	// 检查 os.Args 的长度是否足够
	if len(os.Args) < 4 {
		// 默认使用全名
		return appNameWithoutExt
	}

	arg1 := filepath.Base(os.Args[1])
	arg2 := filepath.Base(os.Args[2])
	arg3 := filepath.Base(os.Args[3])

	// channel join
	if CHANNEL_JOIN == arg1+"."+arg2+"." {
		return CHANNEL_JOIN + appNameWithoutExt
	}

	// lifecycle chaincode package
	if CHAINCODE_PACKAGE == arg2+"."+arg3+"." {
		return CHAINCODE_PACKAGE + appNameWithoutExt
	}

	// lifecycle chaincode install
	if CHAINCODE_INSTALL == arg2+"."+arg3+"." {
		return CHAINCODE_INSTALL + appNameWithoutExt
	}

	// lifecycle chaincode approveformyorg
	if CHAINCODE_AGREE_ == arg2+"."+arg3+"." {
		return CHAINCODE_AGREE_ + appNameWithoutExt
	}

	// lifecycle chaincode commit
	if CHAINCODE_COMMIT == arg2+"."+arg3+"." {
		return CHAINCODE_COMMIT + appNameWithoutExt
	}

	// chaincode invoke
	if CHAINCODE_INVOKE == arg1+"."+arg2+"." {
		// chaincode invoke --isInit=true
		for _, s := range os.Args {
			if strings.Contains(s, "isInit") {
				return CHAINCODE_INIT + appNameWithoutExt
			}
		}
		return CHAINCODE_INVOKE + appNameWithoutExt
	}

	// channel update
	if CHANNEL_UPDATE == arg1+"."+arg2+"." {
		return CHANNEL_UPDATE + appNameWithoutExt
	}

	// channel fetch
	if CHANNEL_FETCH == arg1+"."+arg2+"." {
		return CHANNEL_FETCH + appNameWithoutExt
	}

	// 没有匹配到, 默认使用全名
	return appNameWithoutExt
}

type PeerOrgs struct {
	MspID  string `mapstructure:"MspID"`
	Domain string `mapstructure:"Domain"`
	Specs  []Spec `mapstructure:"Specs"`
}

type OrdererOrgs struct {
	MspID  string `mapstructure:"MspID"`
	Domain string `mapstructure:"Domain"`
	Specs  []Spec `mapstructure:"Specs"`
}

type Spec struct {
	Host  string `mapstructure:"Host"`
	Port1 string `mapstructure:"port1"`
	Port2 string `mapstructure:"port2"`
	Port3 string `mapstructure:"port3"`
}

var defCertPath = "./crypto-config/ordererOrganizations/orderer.com/orderers/orderer0.orderer.com/tls/server.crt"

type OrdererEtcdRaft struct {
	Host          string `yaml:"Host"`
	Port          string `yaml:"Port"`
	ClientTLSCert string `yaml:"ClientTLSCert"`
	ServerTLSCert string `yaml:"ServerTLSCert"`
}

type Organization struct {
	Name        string       `yaml:"Name"`
	ID          string       `yaml:"ID"`
	MSPDir      string       `yaml:"MSPDir"`
	Policies    Policies     `yaml:"Policies"`
	AnchorPeers []AnchorPeer `yaml:"AnchorPeers"`
}

type Policies struct {
	Readers     Policy `yaml:"Readers"`
	Writers     Policy `yaml:"Writers"`
	Admins      Policy `yaml:"Admins"`
	Endorsement Policy `yaml:"Endorsement"`
}

type Policy struct {
	Type string `yaml:"Type"`
	Rule string `yaml:"Rule"`
}

type AnchorPeer struct {
	Host string `yaml:"Host"`
	Port int    `yaml:"Port"`
}

type Replace struct {
	Organizations []Organization `yaml:"Organizations"`
}

type Config struct {
	Replace Replace `yaml:"Replace"`
}

// Init 初始化生成配置信息
func Init() {
	// 初始化Viper
	v := viper.New()
	err := InitViperDefault(v, "config")
	if err != nil {
		fmt.Printf("无法读取配置文件: %s\n", err)
		return
	}

	// 读取配置文件
	err = v.ReadInConfig()
	if err != nil {
		fmt.Printf("无法读取配置文件: %s\n", err)
		return
	}

	// 解析配置文件到结构体
	var peerOrgs []PeerOrgs
	err = v.UnmarshalKey("PeerOrgs", &peerOrgs)
	if err != nil {
		fmt.Printf("反序列化配置失败: %s\n", err)
		return
	}

	// 解析配置文件到结构体
	var ordererOrgs []OrdererOrgs
	err = v.UnmarshalKey("OrdererOrgs", &ordererOrgs)
	if err != nil {
		fmt.Printf("反序列化配置失败: %s\n", err)
		return
	}

	// 创建新的peer.yaml
	createPeerYaml(v, peerOrgs)

	// 创建新的orderer.yaml
	createOrdererYaml(v, ordererOrgs)

	// 创建新的peer+orderer.yaml
	createPeerAndOrdererYaml(v, peerOrgs, ordererOrgs)

	// 创建新的channel_create.yaml
	createChannelCreateYaml(v, peerOrgs, ordererOrgs)
}

// 创建通道yaml
func createChannelCreateYaml(v *viper.Viper, peerOrgs []PeerOrgs, ordererOrgs []OrdererOrgs) {
	if len(peerOrgs) <= 0 {
		log.Fatalf("config.yaml 至少配置一个 PeerOrgs 组织")
	}

	// 取第一个
	appName := filepath.Base(os.Args[0])
	appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
	appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")

	// 打印解析结果
	for _, org := range ordererOrgs {
		// 创建channel.create.yaml
		originalFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_CHANNEL+".yaml")
		createChannel(v, originalFilePath, CHANNEL_CREATE, org, peerOrgs)
	}
}

func createChannel(v *viper.Viper, originalFilePath string, other string, ordererOrgs OrdererOrgs, peerOrgs []PeerOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), other+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	OrdererEtcdRafts := []OrdererEtcdRaft{}
	var OrdererAddressList string
	for _, specs := range ordererOrgs.Specs {
		Address := specs.Host + "." + ordererOrgs.Domain + ":" + specs.Port1
		OrdererAddressList = OrdererAddressList + Address + ","

		certPath := strings.ReplaceAll(defCertPath, "orderer.com", ordererOrgs.Domain)
		certPath = strings.ReplaceAll(certPath, "orderer0", specs.Host)
		ordererEtcdRaft := OrdererEtcdRaft{
			Host:          specs.Host + "." + ordererOrgs.Domain,
			Port:          specs.Port1,
			ClientTLSCert: certPath,
			ServerTLSCert: certPath,
		}
		OrdererEtcdRafts = append(OrdererEtcdRafts, ordererEtcdRaft)
	}

	// 转换为JSON格式
	jsonData, err := json.Marshal(OrdererEtcdRafts)
	if err != nil {
		fmt.Println("orderer共识列表转换json失败: ", err)
		return
	}

	// 移除最后一个逗号
	newContent = strings.ReplaceAll(newContent, "OrdererAddresseList", strings.TrimSuffix(OrdererAddressList, ","))
	newContent = strings.ReplaceAll(newContent, "ordererEtcdRaftList", string(jsonData))
	newContent = strings.ReplaceAll(newContent, "orderer.com", ordererOrgs.Domain)
	newContent = strings.ReplaceAll(newContent, "OrdererMSP", ordererOrgs.MspID)

	if len(peerOrgs) <= 0 {
		log.Fatalf("config.yaml 至少配置一个 PeerOrgs.Specs")
	}

	orgFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_ORG+".yaml")
	// 读取原始配置文件
	orgData, err := ioutil.ReadFile(orgFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 解析 YAML 文件
	var configs []Config
	for _, peerOrg := range peerOrgs {
		if len(peerOrg.Specs) <= 0 {
			log.Fatalf("config.yaml 至少配置一个 PeerOrgs.Specs")
		}

		var newOrgContent string
		newOrgContent = string(orgData)
		newOrgContent = strings.ReplaceAll(newOrgContent, "org.peer.com", peerOrg.Domain)
		newOrgContent = strings.ReplaceAll(newOrgContent, "OrgMSP", peerOrg.MspID)
		newOrgContent = strings.ReplaceAll(newOrgContent, "peer0", peerOrg.Specs[0].Host)
		newOrgContent = strings.ReplaceAll(newOrgContent, "7150", peerOrg.Specs[0].Port1)
		var config Config
		err = yaml.Unmarshal([]byte(newOrgContent), &config)
		if err != nil {
			log.Fatal(err)
		}
		configs = append(configs, config)
	}

	var mergedOrganizations []Organization
	for _, config := range configs {
		mergedOrganizations = append(mergedOrganizations, config.Replace.Organizations...)
	}

	// 创建新的 Application 结构体
	newApplication := Replace{
		Organizations: mergedOrganizations,
	}

	// 创建新的 Config 结构体
	newConfig := Config{
		Replace: newApplication,
	}

	// 将新的 Config 结构体转换为 YAML 格式
	newOrgConfigYAML, err := yaml.Marshal(newConfig)
	if err != nil {
		log.Fatal(err)
	}

	// 替换
	newContent = strings.ReplaceAll(newContent, "OrgConfig:", string(newOrgConfigYAML))
	newContent = strings.ReplaceAll(newContent, "Replace:", "")

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

func createPeerAndOrdererYaml(v *viper.Viper, peerOrgs []PeerOrgs, ordererOrgs []OrdererOrgs) {
	if len(ordererOrgs) <= 0 {
		log.Fatalf("config.yaml 至少配置一个 OrdererOrgs 组织")
	}

	// 取第一个orderer
	ordererOrg := ordererOrgs[0]
	// 打印解析结果
	for _, org := range peerOrgs {
		for _, spec := range org.Specs {
			appName := filepath.Base(os.Args[0])
			appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
			appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")
			// 模板文件路径
			if strings.Contains(appNameWithoutExt, spec.Host+"."+org.Domain) {
				// 创建channel.fetch.peer.org.dns.com.yaml
				originalFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHANNEL_FETCH, spec, org, ordererOrg)

				// 创建chaincode.agree.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHAINCODE_AGREE_, spec, org, ordererOrg)

				// 创建chaincode.commit.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHAINCODE_COMMIT, spec, org, ordererOrg)

				// 创建chaincode.init.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHAINCODE_INIT, spec, org, ordererOrg)

				// 创建chaincode.invoke.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHAINCODE_INVOKE, spec, org, ordererOrg)

				// 创建channel.update.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER_ORDERER+".yaml")
				createPeerAndOrderer(v, originalFilePath, CHANNEL_UPDATE, spec, org, ordererOrg)
			}
		}
	}

	// 取第一个peer
	peerOrg := peerOrgs[0]

	for _, orderOrg := range ordererOrgs {
		for _, spec := range orderOrg.Specs {
			appName := filepath.Base(os.Args[0])
			appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
			appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")
			// 创建channel.update.orderer0.dns.com.yaml
			if strings.Contains(appNameWithoutExt, spec.Host+"."+orderOrg.Domain) {
				originalFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_ORDERER+".yaml")
				createUpdateOrderer(v, originalFilePath, CHANNEL_UPDATE, spec, peerOrg, ordererOrg)
			}
		}
	}

}

func createPeerAndOrderer(v *viper.Viper, originalFilePath string, other string, spec Spec, org PeerOrgs, ordererOrg OrdererOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), other+spec.Host+"."+org.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "org.peer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "OrgMSP", org.MspID)
	newContent = strings.ReplaceAll(newContent, "peer0", spec.Host)
	newContent = strings.ReplaceAll(newContent, "7150", spec.Port1)
	newContent = strings.ReplaceAll(newContent, "7250", spec.Port2)
	newContent = strings.ReplaceAll(newContent, "7350", spec.Port3)

	if len(ordererOrg.Specs) <= 0 {
		log.Fatalf("config.yaml 至少配置一个 OrdererOrgs.Specs")
	}

	newContent = strings.ReplaceAll(newContent, "orderer.com", ordererOrg.Domain)
	newContent = strings.ReplaceAll(newContent, "orderer0", ordererOrg.Specs[0].Host)
	newContent = strings.ReplaceAll(newContent, "7051", ordererOrg.Specs[0].Port1)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

// createPeerYaml 创建新的yaml
//
// @Author: 罗德
// @Date: 2023/11/29
func createPeerYaml(v *viper.Viper, peerOrgs []PeerOrgs) {
	// 打印解析结果
	for _, org := range peerOrgs {
		for _, spec := range org.Specs {
			appName := filepath.Base(os.Args[0])
			appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
			appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")
			// 模板文件路径
			if strings.Contains(appNameWithoutExt, spec.Host+"."+org.Domain) {
				// 创建peer.org.dns.com.yaml
				originalFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_PEER+".yaml")
				createPeer(v, originalFilePath, spec, org)

				// 创建channel.join.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER+".yaml")
				createOtherPeer(v, originalFilePath, CHANNEL_JOIN, spec, org)

				// 创建chaincode.package.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER+".yaml")
				createOtherPeer(v, originalFilePath, CHAINCODE_PACKAGE, spec, org)

				// 创建chaincode.install.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER+".yaml")
				createOtherPeer(v, originalFilePath, CHAINCODE_INSTALL, spec, org)

				// 创建channel.sign.peer.org.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_USER_PEER+".yaml")
				createOtherPeer(v, originalFilePath, CHANNEL_SIGN, spec, org)
			}
		}
	}

}

// 创建其他名称的yaml
func createOtherPeer(v *viper.Viper, originalFilePath string, other string, spec Spec, org PeerOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), other+spec.Host+"."+org.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "org.peer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "OrgMSP", org.MspID)
	newContent = strings.ReplaceAll(newContent, "peer0", spec.Host)
	newContent = strings.ReplaceAll(newContent, "7150", spec.Port1)
	newContent = strings.ReplaceAll(newContent, "7250", spec.Port2)
	newContent = strings.ReplaceAll(newContent, "7350", spec.Port3)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

// 创建peer.org.dns.com.yaml
func createPeer(v *viper.Viper, originalFilePath string, spec Spec, org PeerOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), spec.Host+"."+org.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "org.peer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "OrgMSP", org.MspID)
	newContent = strings.ReplaceAll(newContent, "peer0", spec.Host)
	newContent = strings.ReplaceAll(newContent, "7150", spec.Port1)
	newContent = strings.ReplaceAll(newContent, "7250", spec.Port2)
	newContent = strings.ReplaceAll(newContent, "7350", spec.Port3)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

// createOrdererYaml 创建新的yaml
//
// @Author: 罗德
// @Date: 2023/11/29
func createOrdererYaml(v *viper.Viper, ordererOrgs []OrdererOrgs) {

	// 打印解析结果
	for _, org := range ordererOrgs {
		for _, spec := range org.Specs {
			appName := filepath.Base(os.Args[0])
			appNameWithoutExt := strings.TrimSuffix(appName, filepath.Ext(appName))
			appNameWithoutExt = strings.ReplaceAll(appNameWithoutExt, "_", ".")
			// 模板文件路径
			if strings.Contains(appNameWithoutExt, spec.Host+"."+org.Domain) {
				// 创建orderer.dns.com.yaml
				originalFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_ORDERER+".yaml")
				createOrderer(v, originalFilePath, spec, org)

				// 创建channel.join.orderer.dns.com.yaml
				originalFilePath = TranslatePath(filepath.Dir(v.ConfigFileUsed()), DEFAULT_TLS_ORDERER+".yaml")
				createTlsOrdelsrer(v, originalFilePath, CHANNEL_JOIN, spec, org)
			}
		}
	}

}

func createUpdateOrderer(v *viper.Viper, originalFilePath string, other string, spec Spec, org PeerOrgs, ordererOrg OrdererOrgs) {
	if len(org.Specs) <= 0 {
		log.Fatalf("config.yaml 至少配置一个 PeerOrgs.Specs")
	}

	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), other+spec.Host+"."+ordererOrg.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "org.peer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "peer0", org.Specs[0].Host)
	newContent = strings.ReplaceAll(newContent, "7150", org.Specs[0].Port1)
	newContent = strings.ReplaceAll(newContent, "7250", org.Specs[0].Port2)
	newContent = strings.ReplaceAll(newContent, "7350", org.Specs[0].Port3)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "orderer.com", ordererOrg.Domain)
	newContent = strings.ReplaceAll(newContent, "OrdererMSP", ordererOrg.MspID)
	newContent = strings.ReplaceAll(newContent, "orderer0", ordererOrg.Specs[0].Host)
	newContent = strings.ReplaceAll(newContent, "7051", ordererOrg.Specs[0].Port1)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

// 创建orderer.dns.com.yaml
func createOrderer(v *viper.Viper, originalFilePath string, spec Spec, org OrdererOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), spec.Host+"."+org.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "orderer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "OrdererMSP", org.MspID)
	newContent = strings.ReplaceAll(newContent, "orderer0", spec.Host)
	newContent = strings.ReplaceAll(newContent, "7051", spec.Port1)
	newContent = strings.ReplaceAll(newContent, "8443", spec.Port2)
	newContent = strings.ReplaceAll(newContent, "9443", spec.Port3)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}

// 创建其他yaml
func createTlsOrdelsrer(v *viper.Viper, originalFilePath string, other string, spec Spec, org OrdererOrgs) {
	// 新文件路径
	newFilePath := TranslatePath(filepath.Dir(v.ConfigFileUsed()), other+spec.Host+"."+org.Domain+".yaml")

	// 读取原始配置文件
	originalData, err := ioutil.ReadFile(originalFilePath)
	if err != nil {
		log.Fatalf("无法读取配置文件: %v", err)
	}

	// 将字节切片转换为字符串
	var newContent string
	newContent = string(originalData)

	// 替换字段值
	newContent = strings.ReplaceAll(newContent, "orderer.com", org.Domain)
	newContent = strings.ReplaceAll(newContent, "orderer0", spec.Host)
	newContent = strings.ReplaceAll(newContent, "9443", spec.Port3)

	// 复制原始配置文件为新文件
	err = ioutil.WriteFile(newFilePath, []byte(newContent), 0644)
	if err != nil {
		log.Fatalf("创建配置文件失败: %v", err)
	}
}
