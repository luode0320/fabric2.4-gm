/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package main

import (
	"bytes"
	"crypto"
	"fmt"
	"github.com/hyperledger/fabric/bccsp/common"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"text/template"

	"github.com/hyperledger/fabric/internal/cryptogen/ca"
	"github.com/hyperledger/fabric/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric/internal/cryptogen/msp"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

const (
	userBaseName            = "User"
	adminBaseName           = "Admin"
	defaultHostnameTemplate = "{{.Prefix}}{{.Index}}"
	defaultCNTemplate       = "{{.Hostname}}.{{.Domain}}"
)

type HostnameData struct {
	Prefix string
	Index  int
	Domain string
}

type SpecData struct {
	Hostname   string
	Domain     string
	CommonName string
}

type NodeTemplate struct {
	Count    int      `yaml:"Count"`
	Start    int      `yaml:"Start"`
	Hostname string   `yaml:"Hostname"`
	SANS     []string `yaml:"SANS"`
}

type NodeSpec struct {
	isAdmin            bool
	Hostname           string   `yaml:"Hostname"`
	CommonName         string   `yaml:"CommonName"`
	Country            string   `yaml:"Country"`
	Province           string   `yaml:"Province"`
	Locality           string   `yaml:"Locality"`
	OrganizationalUnit string   `yaml:"OrganizationalUnit"`
	StreetAddress      string   `yaml:"StreetAddress"`
	PostalCode         string   `yaml:"PostalCode"`
	SANS               []string `yaml:"SANS"`
}

type UsersSpec struct {
	Count int `yaml:"Count"`
}

type OrgSpec struct {
	Name          string       `yaml:"Name"`
	Domain        string       `yaml:"Domain"`
	EnableNodeOUs bool         `yaml:"EnableNodeOUs"`
	CA            NodeSpec     `yaml:"CA"`
	Template      NodeTemplate `yaml:"Template"`
	Specs         []NodeSpec   `yaml:"Specs"`
	Users         UsersSpec    `yaml:"Users"`
}

type Config struct {
	OrdererOrgs []OrgSpec `yaml:"OrdererOrgs"`
	PeerOrgs    []OrgSpec `yaml:"PeerOrgs"`
}

var defaultConfig = `
# ---------------------------------------------------------------------------
# "OrdererOrgs" - Definition of organizations managing orderer nodes
# ---------------------------------------------------------------------------
OrdererOrgs:
  # ---------------------------------------------------------------------------
  # Orderer
  # ---------------------------------------------------------------------------
  - Name: Orderer
    Domain: example.com
    EnableNodeOUs: false

    # ---------------------------------------------------------------------------
    # "Specs" - See PeerOrgs below for complete description
    # ---------------------------------------------------------------------------
    Specs:
      - Hostname: orderer

# ---------------------------------------------------------------------------
# "PeerOrgs" - Definition of organizations managing peer nodes
# ---------------------------------------------------------------------------
PeerOrgs:
  # ---------------------------------------------------------------------------
  # Org1
  # ---------------------------------------------------------------------------
  - Name: Org1
    Domain: org1.example.com
    EnableNodeOUs: false

    # ---------------------------------------------------------------------------
    # "CA"
    # ---------------------------------------------------------------------------
    # Uncomment this section to enable the explicit definition of the CA for this
    # organization.  This entry is a Spec.  See "Specs" section below for details.
    # ---------------------------------------------------------------------------
    # CA:
    #    Hostname: ca # implicitly ca.org1.example.com
    #    Country: US
    #    Province: California
    #    Locality: San Francisco
    #    OrganizationalUnit: Hyperledger Fabric
    #    StreetAddress: address for org # default nil
    #    PostalCode: postalCode for org # default nil

    # ---------------------------------------------------------------------------
    # "Specs"
    # ---------------------------------------------------------------------------
    # Uncomment this section to enable the explicit definition of hosts in your
    # configuration.  Most users will want to use Template, below
    #
    # Specs is an array of Spec entries.  Each Spec entry consists of two fields:
    #   - Hostname:   (Required) The desired hostname, sans the domain.
    #   - CommonName: (Optional) Specifies the template or explicit override for
    #                 the CN.  By default, this is the template:
    #
    #                              "{{.Hostname}}.{{.Domain}}"
    #
    #                 which obtains its values from the Spec.Hostname and
    #                 Org.Domain, respectively.
    #   - SANS:       (Optional) Specifies one or more Subject Alternative Names
    #                 to be set in the resulting x509. Accepts template
    #                 variables {{.Hostname}}, {{.Domain}}, {{.CommonName}}. IP
    #                 addresses provided here will be properly recognized. Other
    #                 values will be taken as DNS names.
    #                 NOTE: Two implicit entries are created for you:
    #                     - {{ .CommonName }}
    #                     - {{ .Hostname }}
    # ---------------------------------------------------------------------------
    # Specs:
    #   - Hostname: foo # implicitly "foo.org1.example.com"
    #     CommonName: foo27.org5.example.com # overrides Hostname-based FQDN set above
    #     SANS:
    #       - "bar.{{.Domain}}"
    #       - "altfoo.{{.Domain}}"
    #       - "{{.Hostname}}.org6.net"
    #       - 172.16.10.31
    #   - Hostname: bar
    #   - Hostname: baz

    # ---------------------------------------------------------------------------
    # "Template"
    # ---------------------------------------------------------------------------
    # Allows for the definition of 1 or more hosts that are created sequentially
    # from a template. By default, this looks like "peer%d" from 0 to Count-1.
    # You may override the number of nodes (Count), the starting index (Start)
    # or the template used to construct the name (Hostname).
    #
    # Note: Template and Specs are not mutually exclusive.  You may define both
    # sections and the aggregate nodes will be created for you.  Take care with
    # name collisions
    # ---------------------------------------------------------------------------
    Template:
      Count: 1
      # Start: 5
      # Hostname: {{.Prefix}}{{.Index}} # default
      # SANS:
      #   - "{{.Hostname}}.alt.{{.Domain}}"

    # ---------------------------------------------------------------------------
    # "Users"
    # ---------------------------------------------------------------------------
    # Count: The number of user accounts _in addition_ to Admin
    # ---------------------------------------------------------------------------
    Users:
      Count: 1

  # ---------------------------------------------------------------------------
  # Org2: See "Org1" for full specification
  # ---------------------------------------------------------------------------
  - Name: Org2
    Domain: org2.example.com
    EnableNodeOUs: false
    Template:
      Count: 1
    Users:
      Count: 1
`

// command line flags
var (
	app = kingpin.New("cryptogen", "Utility for generating Hyperledger Fabric key material")

	gen           = app.Command("generate", "Generate key material")
	outputDir     = gen.Flag("output", "The output directory in which to place artifacts").Default("crypto-config").String()
	genConfigFile = gen.Flag("config", "The configuration template to use").File()

	showtemplate = app.Command("showtemplate", "Show the default configuration template")

	version       = app.Command("version", "Show version information")
	ext           = app.Command("extend", "Extend existing network")
	inputDir      = ext.Flag("input", "The input directory in which existing network place").Default("crypto-config").String()
	extConfigFile = ext.Flag("config", "The configuration template to use").File()
)

func main() {
	kingpin.Version("0.0.1")
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	// "generate" 命令
	case gen.FullCommand():
		// 生成证书
		generate()
	// "extend" 命令
	case ext.FullCommand():
		// 扩展命令
		extend()
	// "showtemplate" 命令
	case showtemplate.FullCommand():
		// 打印默认配置模板
		fmt.Print(defaultConfig)
		os.Exit(0)
	// "version" 命令
	case version.FullCommand():
		// 打印版本
		printVersion()
	}
}

// getConfig 获取crypto-config.yaml配置
func getConfig() (*Config, error) {
	var configData string

	if *genConfigFile != nil {
		data, err := ioutil.ReadAll(*genConfigFile)
		if err != nil {
			return nil, fmt.Errorf("读取配置时出错: %s", err)
		}

		configData = string(data)
	} else if *extConfigFile != nil {
		data, err := ioutil.ReadAll(*extConfigFile)
		if err != nil {
			return nil, fmt.Errorf("读取配置时出错: %s", err)
		}

		configData = string(data)
	} else {
		configData = defaultConfig
	}

	config := &Config{}
	err := yaml.Unmarshal([]byte(configData), &config)
	if err != nil {
		return nil, fmt.Errorf("取消编组时出错 YAML: %s", err)
	}

	return config, nil
}

// extend 扩展已经存在的组织节点证书
func extend() {
	config, err := getConfig()
	if err != nil {
		fmt.Printf("读取配置时出错: %s", err)
		os.Exit(-1)
	}

	for _, orgSpec := range config.PeerOrgs {
		err = renderOrgSpec(&orgSpec, "peer")
		if err != nil {
			fmt.Printf("处理对等配置时出错: %s", err)
			os.Exit(-1)
		}
		// 扩展存在的组织的peer节点新证书
		extendPeerOrg(orgSpec)
	}

	for _, orgSpec := range config.OrdererOrgs {
		err = renderOrgSpec(&orgSpec, "orderer")
		if err != nil {
			fmt.Printf("处理orderer配置时出错: %s", err)
			os.Exit(-1)
		}
		// 扩展存在的组织的oderer节点新证书
		extendOrdererOrg(orgSpec)
	}
}

// extendPeerOrg 扩展存在的组织的peer节点新证书
func extendPeerOrg(orgSpec OrgSpec) {
	orgName := orgSpec.Domain
	orgDir := filepath.Join(*inputDir, "peerOrganizations", orgName)
	if _, err := os.Stat(orgDir); os.IsNotExist(err) {
		generatePeerOrg(*inputDir, orgSpec)
		return
	}

	peersDir := filepath.Join(orgDir, "peers")
	usersDir := filepath.Join(orgDir, "users")
	caDir := filepath.Join(orgDir, "ca")
	tlscaDir := filepath.Join(orgDir, "tlsca")
	// 获取之前创建完成的ca证书与tls证书
	signCA := getCA(caDir, orgSpec, orgSpec.CA.CommonName)
	tlsCA := getCA(tlscaDir, orgSpec, "tls"+orgSpec.CA.CommonName)
	// 生成节点证书
	generateNodes(peersDir, orgSpec.Specs, signCA, tlsCA, msp.PEER, orgSpec.EnableNodeOUs)

	adminUser := NodeSpec{
		isAdmin:    true,
		CommonName: fmt.Sprintf("%s@%s", adminBaseName, orgName),
	}
	// 将管理证书复制到每个组织对等体的MSP管理证书
	for _, spec := range orgSpec.Specs {
		err := copyAdminCert(usersDir,
			filepath.Join(peersDir, spec.CommonName, "msp", "admincerts"), adminUser.CommonName)
		if err != nil {
			fmt.Printf("复制组织的管理证书时出错 %s peer %s:\n%v\n",
				orgName, spec.CommonName, err)
			os.Exit(1)
		}
	}

	// TODO: 添加指定用户名的功能
	users := []NodeSpec{}
	for j := 1; j <= orgSpec.Users.Count; j++ {
		user := NodeSpec{
			CommonName: fmt.Sprintf("%s%d@%s", userBaseName, j, orgName),
		}

		users = append(users, user)
	}
	// 生成普通用户
	generateNodes(usersDir, users, signCA, tlsCA, msp.CLIENT, orgSpec.EnableNodeOUs)
}

// extendOrdererOrg 扩展存在的组织的oderer节点新证书
func extendOrdererOrg(orgSpec OrgSpec) {
	orgName := orgSpec.Domain

	orgDir := filepath.Join(*inputDir, "ordererOrganizations", orgName)
	caDir := filepath.Join(orgDir, "ca")
	usersDir := filepath.Join(orgDir, "users")
	tlscaDir := filepath.Join(orgDir, "tlsca")
	orderersDir := filepath.Join(orgDir, "orderers")
	if _, err := os.Stat(orgDir); os.IsNotExist(err) {
		generateOrdererOrg(*inputDir, orgSpec)
		return
	}
	// 获取之前创建完成的ca证书与tls证书
	signCA := getCA(caDir, orgSpec, orgSpec.CA.CommonName)
	tlsCA := getCA(tlscaDir, orgSpec, "tls"+orgSpec.CA.CommonName)
	// 生成节点证书
	generateNodes(orderersDir, orgSpec.Specs, signCA, tlsCA, msp.ORDERER, orgSpec.EnableNodeOUs)

	adminUser := NodeSpec{
		isAdmin:    true,
		CommonName: fmt.Sprintf("%s@%s", adminBaseName, orgName),
	}
	// 将管理证书复制到每个组织对等体的MSP管理证书
	for _, spec := range orgSpec.Specs {
		err := copyAdminCert(usersDir,
			filepath.Join(orderersDir, spec.CommonName, "msp", "admincerts"), adminUser.CommonName)
		if err != nil {
			fmt.Printf("复制组织的管理证书时出错 %s orderer %s:\n%v\n",
				orgName, spec.CommonName, err)
			os.Exit(1)
		}
	}
}

// generate 生成证书
func generate() {
	config, err := getConfig()
	if err != nil {
		fmt.Printf("读取配置时出错: %s", err)
		os.Exit(-1)
	}

	for _, orgSpec := range config.PeerOrgs {
		err = renderOrgSpec(&orgSpec, "peer")
		if err != nil {
			fmt.Printf("处理对等配置时出错: %s", err)
			os.Exit(-1)
		}
		// 生成peer组织和节点证书
		generatePeerOrg(*outputDir, orgSpec)
	}

	for _, orgSpec := range config.OrdererOrgs {
		err = renderOrgSpec(&orgSpec, "orderer")
		if err != nil {
			fmt.Printf("处理orderer配置时出错: %s", err)
			os.Exit(-1)
		}
		// 生成Orderer组织和节点证书
		generateOrdererOrg(*outputDir, orgSpec)
	}
}

// parseTemplate 函数将给定的输入字符串作为模板进行解析，并使用提供的数据进行渲染。
// 输入参数：
//   - input: 要解析的模板字符串
//   - data: 用于渲染模板的数据
//
// 返回值：
//   - string: 渲染后的字符串结果
//   - error: 如果解析或执行模板时出现错误，则返回相应的错误信息
func parseTemplate(input string, data interface{}) (string, error) {
	// 创建一个新的模板，并解析输入字符串
	t, err := template.New("parse").Parse(input)
	if err != nil {
		return "", fmt.Errorf("解析模板时出错: %s", err)
	}
	// 创建一个缓冲区用于存储渲染后的输出
	output := new(bytes.Buffer)
	// 使用提供的数据渲染模板，并将结果写入缓冲区
	err = t.Execute(output, data)
	if err != nil {
		return "", fmt.Errorf("执行模板时出错: %s", err)
	}
	// 将缓冲区中的内容转换为字符串并返回
	return output.String(), nil
}

// 是否使用默认值解析配置模板
func parseTemplateWithDefault(input, defaultInput string, data interface{}) (string, error) {
	// 如果输入为空字符串，则使用默认值
	if len(input) == 0 {
		input = defaultInput
	}
	// 解析
	return parseTemplate(input, data)
}

// renderNodeSpec函数用于渲染节点规范（NodeSpec）中的模板字段，并更新相关字段的值。
// 输入参数：
//   - domain: 节点所属的域名
//   - spec: 节点规范，包含了需要渲染的模板字段和相关数据
//
// 返回值：
//   - error: 如果在渲染过程中出现错误，则返回相应的错误信息
func renderNodeSpec(domain string, spec *NodeSpec) error {
	// 创建SpecData结构体，用于存储模板渲染所需的数据
	data := SpecData{
		Hostname: spec.Hostname,
		Domain:   domain,
	}

	// 使用默认值解析配置模板，获取CommonName字段的值
	cn, err := parseTemplateWithDefault(spec.CommonName, defaultCNTemplate, data)
	if err != nil {
		return err
	}
	// 更新spec的CommonName字段和data的CommonName字段为渲染后的值
	spec.CommonName = cn
	data.CommonName = cn

	// 保存原始的、未处理的SANS条目
	origSANS := spec.SANS

	// 为CN/Hostname设置我们的隐式san条目
	spec.SANS = []string{cn, spec.Hostname}

	// 处理任何剩余的SANS条目
	for _, _san := range origSANS {
		// 使用data渲染SANS条目模板，并将渲染后的值添加到spec的SANS字段中
		san, err := parseTemplate(_san, data)
		if err != nil {
			return err
		}

		spec.SANS = append(spec.SANS, san)
	}

	return nil
}

// 读取 prefix 配置中的属性, 赋值到 orgSpec
func renderOrgSpec(orgSpec *OrgSpec, prefix string) error {
	// 首先处理我们所有的模板化节点
	for i := 0; i < orgSpec.Template.Count; i++ {
		data := HostnameData{
			Prefix: prefix,
			Index:  i + orgSpec.Template.Start,
			Domain: orgSpec.Domain,
		}
		// 使用默认值解析模板
		hostname, err := parseTemplateWithDefault(orgSpec.Template.Hostname, defaultHostnameTemplate, data)
		if err != nil {
			return err
		}

		spec := NodeSpec{
			Hostname: hostname,
			SANS:     orgSpec.Template.SANS,
		}
		orgSpec.Specs = append(orgSpec.Specs, spec)
	}

	// 润屏所有常规节点规范以添加域
	for idx, spec := range orgSpec.Specs {
		err := renderNodeSpec(orgSpec.Domain, &spec)
		if err != nil {
			return err
		}

		orgSpec.Specs[idx] = spec
	}

	// 以相同的方式处理CA node-spec
	if len(orgSpec.CA.Hostname) == 0 {
		orgSpec.CA.Hostname = "ca"
	}
	err := renderNodeSpec(orgSpec.Domain, &orgSpec.CA)
	if err != nil {
		return err
	}

	return nil
}

// generatePeerOrg 生成peer组织和节点证书
func generatePeerOrg(baseDir string, orgSpec OrgSpec) {
	orgName := orgSpec.Domain

	// 生成 CAs
	// samplewindowsconfig\peerOrganizations\org.dns.com
	orgDir := filepath.Join(baseDir, "peerOrganizations", orgName)
	// samplewindowsconfig\peerOrganizations\org.dns.com\ca
	caDir := filepath.Join(orgDir, "ca")
	// samplewindowsconfig\peerOrganizations\org.dns.com\tlsca
	tlsCADir := filepath.Join(orgDir, "tlsca")
	// samplewindowsconfig\peerOrganizations\org.dns.com\msp
	mspDir := filepath.Join(orgDir, "msp")
	// samplewindowsconfig\peerOrganizations\org.dns.com\peers
	peersDir := filepath.Join(orgDir, "peers")
	// samplewindowsconfig\peerOrganizations\org.dns.com\users
	usersDir := filepath.Join(orgDir, "users")
	// samplewindowsconfig\peerOrganizations\org.dns.com\msp\admincerts
	adminCertsDir := filepath.Join(mspDir, "admincerts")
	// 生成 签名 CA
	signCA, err := ca.NewCA(caDir, orgName, orgSpec.CA.CommonName, orgSpec.CA.Country, orgSpec.CA.Province, orgSpec.CA.Locality, orgSpec.CA.OrganizationalUnit, orgSpec.CA.StreetAddress, orgSpec.CA.PostalCode)
	if err != nil {
		fmt.Printf("为组织生成signCA时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}
	// 生成 TLS CA
	tlsCA, err := ca.NewCA(tlsCADir, orgName, "tls"+orgSpec.CA.CommonName, orgSpec.CA.Country, orgSpec.CA.Province, orgSpec.CA.Locality, orgSpec.CA.OrganizationalUnit, orgSpec.CA.StreetAddress, orgSpec.CA.PostalCode)
	if err != nil {
		fmt.Printf("为组织生成tlsCA时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}
	// 生成msp
	err = msp.GenerateVerifyingMSP(mspDir, signCA, tlsCA, orgSpec.EnableNodeOUs)
	if err != nil {
		fmt.Printf("为组织生成MSP时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}
	// 生成peers节点证书
	generateNodes(peersDir, orgSpec.Specs, signCA, tlsCA, msp.PEER, orgSpec.EnableNodeOUs)

	// TODO: 添加指定用户名的功能
	users := []NodeSpec{}
	for j := 1; j <= orgSpec.Users.Count; j++ {
		user := NodeSpec{
			CommonName: fmt.Sprintf("%s%d@%s", userBaseName, j, orgName),
		}
		users = append(users, user)
	}
	// 添加管理员用户
	adminUser := NodeSpec{
		isAdmin:    true,
		CommonName: fmt.Sprintf("%s@%s", adminBaseName, orgName),
	}

	users = append(users, adminUser)
	// 生成users证书
	generateNodes(usersDir, users, signCA, tlsCA, msp.CLIENT, orgSpec.EnableNodeOUs)

	// 将管理证书复制到组织的MSP admincerts
	err = copyAdminCert(usersDir, adminCertsDir, adminUser.CommonName)
	if err != nil {
		fmt.Printf("复制组织的管理证书时出错 %s:\n%v\n",
			orgName, err)
		os.Exit(1)
	}

	// 将管理证书复制到每个组织对等体的MSP管理证书
	for _, spec := range orgSpec.Specs {
		err = copyAdminCert(usersDir,
			filepath.Join(peersDir, spec.CommonName, "msp", "admincerts"), adminUser.CommonName)
		if err != nil {
			fmt.Printf("复制组织的管理证书时出错 %s peer %s:\n%v\n",
				orgName, spec.CommonName, err)
			os.Exit(1)
		}
	}
}

// 将管理证书复制到组织的MSP
func copyAdminCert(usersDir, adminCertsDir, adminUserName string) error {
	// 删除admincerts的内容
	err := os.RemoveAll(adminCertsDir)
	if err != nil {
		return err
	}
	// 重新创建admincerts目录
	err = os.MkdirAll(adminCertsDir, 0o755)
	if err != nil {
		return err
	}
	err = copyFile(filepath.Join(usersDir, adminUserName, "msp", "signcerts",
		adminUserName+"-cert.pem"), filepath.Join(adminCertsDir,
		adminUserName+"-cert.pem"))
	if err != nil {
		return err
	}
	return nil
}

// 生成节点的MSP目录（成员服务提供者）
func generateNodes(baseDir string, nodes []NodeSpec, signCA *ca.CA, tlsCA *ca.CA, nodeType int, nodeOUs bool) {
	for _, node := range nodes {
		// 创建一个节点目录nodeDir
		nodeDir := filepath.Join(baseDir, node.CommonName)
		// 代码检查节点目录是否已存在。如果节点目录不存在，则继续执行下面的步骤；否则，跳过该节点的处理
		if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
			currentNodeType := nodeType
			if node.isAdmin && nodeOUs {
				currentNodeType = msp.ADMIN
			}
			err := msp.GenerateLocalMSP(nodeDir, node.CommonName, node.SANS, signCA, tlsCA, currentNodeType, nodeOUs)
			if err != nil {
				fmt.Printf("生成本地MSP表单时出错 %v:\n%v\n", node, err)
				os.Exit(1)
			}
		}
	}
}

// generateOrdererOrg 生成Orderer组织和节点证书
func generateOrdererOrg(baseDir string, orgSpec OrgSpec) {
	orgName := orgSpec.Domain

	// 生成 CAs

	// samplewindowsconfig\ordererOrganizations\dns.com
	orgDir := filepath.Join(baseDir, "ordererOrganizations", orgName)
	// samplewindowsconfig\ordererOrganizations\dns.com\ca
	caDir := filepath.Join(orgDir, "ca")
	// samplewindowsconfig\ordererOrganizations\dns.com\tlsca
	tlsCADir := filepath.Join(orgDir, "tlsca")
	// samplewindowsconfig\ordererOrganizations\dns.com\msp
	mspDir := filepath.Join(orgDir, "msp")
	// samplewindowsconfig\ordererOrganizations\dns.com\orderers
	orderersDir := filepath.Join(orgDir, "orderers")
	// samplewindowsconfig\ordererOrganizations\dns.com\users
	usersDir := filepath.Join(orgDir, "users")
	// samplewindowsconfig\ordererOrganizations\dns.com\msp\admincerts
	adminCertsDir := filepath.Join(mspDir, "admincerts")

	// 生成签名CA
	signCA, err := ca.NewCA(caDir, orgName, orgSpec.CA.CommonName, orgSpec.CA.Country, orgSpec.CA.Province, orgSpec.CA.Locality, orgSpec.CA.OrganizationalUnit, orgSpec.CA.StreetAddress, orgSpec.CA.PostalCode)
	if err != nil {
		fmt.Printf("为组织生成signCA时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}
	// generate TLS CA
	tlsCA, err := ca.NewCA(tlsCADir, orgName, "tls"+orgSpec.CA.CommonName, orgSpec.CA.Country, orgSpec.CA.Province, orgSpec.CA.Locality, orgSpec.CA.OrganizationalUnit, orgSpec.CA.StreetAddress, orgSpec.CA.PostalCode)
	if err != nil {
		fmt.Printf("为组织生成tlsCA时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}

	err = msp.GenerateVerifyingMSP(mspDir, signCA, tlsCA, orgSpec.EnableNodeOUs)
	if err != nil {
		fmt.Printf("为组织生成MSP时出错 %s:\n%v\n", orgName, err)
		os.Exit(1)
	}
	// 生成orderer节点证书
	generateNodes(orderersDir, orgSpec.Specs, signCA, tlsCA, msp.ORDERER, orgSpec.EnableNodeOUs)

	adminUser := NodeSpec{
		isAdmin:    true,
		CommonName: fmt.Sprintf("%s@%s", adminBaseName, orgName),
	}

	// 为orderer组织生成管理员
	users := []NodeSpec{}
	// 添加管理员用户
	users = append(users, adminUser)
	generateNodes(usersDir, users, signCA, tlsCA, msp.CLIENT, orgSpec.EnableNodeOUs)

	// 将管理证书复制到组织的MSP admincerts
	err = copyAdminCert(usersDir, adminCertsDir, adminUser.CommonName)
	if err != nil {
		fmt.Printf("复制组织的管理证书时出错 %s:\n%v\n",
			orgName, err)
		os.Exit(1)
	}

	// 将管理证书复制到每个组织的orderers的MSP admincerts
	for _, spec := range orgSpec.Specs {
		err = copyAdminCert(usersDir,
			filepath.Join(orderersDir, spec.CommonName, "msp", "admincerts"), adminUser.CommonName)
		if err != nil {
			fmt.Printf("复制组织的管理证书时出错 %s orderer %s:\n%v\n",
				orgName, spec.CommonName, err)
			os.Exit(1)
		}
	}
}

// 复制文件
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	cerr := out.Close()
	if err != nil {
		return err
	}
	return cerr
}

// 打印版本
func printVersion() {
	fmt.Println(metadata.GetVersionInfo())
}

// 根据目录路径获取ca
func getCA(caDir string, spec OrgSpec, name string) *ca.CA {
	// 从 keystorePath 目录, 获取私钥
	key, _ := common.GetPrivateKey(caDir, common.SUFFIX_SK)
	// 从 certPath 目录, 获取x509证书, 文件类型.pem
	cert, _ := common.GetCertificate(caDir)

	return &ca.CA{
		Name:               name,
		Signer:             key.Key().(crypto.Signer),
		SignCert:           cert,
		Country:            spec.CA.Country,
		Province:           spec.CA.Province,
		Locality:           spec.CA.Locality,
		OrganizationalUnit: spec.CA.OrganizationalUnit,
		StreetAddress:      spec.CA.StreetAddress,
		PostalCode:         spec.CA.PostalCode,
	}
}
