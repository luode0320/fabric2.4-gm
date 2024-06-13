/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp/common"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/IBM/idemix"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// Configuration 结构体表示 MSP 可以配置的附件配置。
// 默认情况下，这个配置存储在一个 yaml 文件中。
type Configuration struct {
	// OrganizationalUnitIdentifiers 是一个组织单位标识符的列表。
	// 如果设置了这个字段，MSP 将只认为包含至少一个这些组织单位标识符的身份是有效的。
	OrganizationalUnitIdentifiers []*OrganizationalUnitIdentifiersConfiguration `yaml:"OrganizationalUnitIdentifiers,omitempty"`

	// NodeOUs 允许 MSP 根据身份的组织单位（OU）来区分客户端、对等节点和排序节点。
	NodeOUs *NodeOUs `yaml:"NodeOUs,omitempty"`
}

// NodeOUs 结构体包含了根据组织单位（OU）来区分客户端、对等节点和排序节点的信息。
// 如果启用了 OU 检查（通过将 Enabled 设置为 true），MSP 将只认为身份是有效的，如果它是客户端、对等节点或排序节点的身份。
// 一个身份应该只有这些特殊 OU 中的一个。
type NodeOUs struct {
	// Enable 激活 OU 强制检查。
	Enable bool `yaml:"Enable,omitempty"`

	// ClientOUIdentifier 指定如何通过 OU 来识别客户端。
	ClientOUIdentifier *OrganizationalUnitIdentifiersConfiguration `yaml:"ClientOUIdentifier,omitempty"`

	// PeerOUIdentifier 指定如何通过 OU 来识别对等节点。
	PeerOUIdentifier *OrganizationalUnitIdentifiersConfiguration `yaml:"PeerOUIdentifier,omitempty"`

	// AdminOUIdentifier 指定如何通过 OU 来识别管理员。
	AdminOUIdentifier *OrganizationalUnitIdentifiersConfiguration `yaml:"AdminOUIdentifier,omitempty"`

	// OrdererOUIdentifier 指定如何通过 OU 来识别排序节点。
	OrdererOUIdentifier *OrganizationalUnitIdentifiersConfiguration `yaml:"OrdererOUIdentifier,omitempty"`
}

// OrganizationalUnitIdentifiersConfiguration 结构体用于表示一个组织单位（OU）和关联的受信任证书。
type OrganizationalUnitIdentifiersConfiguration struct {
	// Certificate 是根或中间证书的路径。
	Certificate string `yaml:"Certificate,omitempty"`

	// OrganizationalUnitIdentifier 是 OU 的名称。
	OrganizationalUnitIdentifier string `yaml:"OrganizationalUnitIdentifier,omitempty"`
}

func readFile(file string) ([]byte, error) {
	fileCont, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrapf(err, "无法读取文件 %s", file)
	}

	return fileCont, nil
}

// ReadPemFile 函数用于从指定的文件中读取 PEM 格式的内容。
// 参数：
//   - file string：文件的路径。
//
// 返回值：
//   - []byte：读取到的 PEM 内容。
//   - error：读取过程中的错误，如果没有错误则为 nil。
func readPemFile(file string) ([]byte, error) {
	bytes, err := readFile(file)
	if err != nil {
		return nil, errors.Wrapf(err, "从文件读取 %s 失败", file)
	}
	// 检查是否是 pem 编码
	b, _ := pem.Decode(bytes)
	if b == nil { // TODO: also check that the type is what we expect (cert vs key..)
		return nil, errors.Errorf("文件没有pem内容 %s", file)
	}

	return bytes, nil
}

// getPemMaterialFromDir 函数从指定的目录中读取 PEM 格式的材料。
// 参数：
//   - dir string：目录的路径。
//
// 返回值：
//   - [][]byte：读取到的 PEM 材料。
//   - error：读取过程中的错误，如果没有错误则为 nil。
func getPemMaterialFromDir(dir string) ([][]byte, error) {
	mspLogger.Debugf("正在读取目录 %s", dir)

	// 检查目录是否存在
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, err
	}
	// 读取目录并返回files的列表, 目录内容files, 按文件名排序
	content := make([][]byte, 0)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "无法读取目录 %s", dir)
	}

	// 遍历目录文件
	for _, f := range files {
		fullName := filepath.Join(dir, f.Name())
		// 获取文件的信息
		f, err := os.Stat(fullName)
		if err != nil {
			mspLogger.Warningf("获取文件的信息失败 %s: %s", fullName, err)
			continue
		}
		if f.IsDir() {
			continue
		}

		mspLogger.Debugf("检查文件 %s", fullName)

		// 读取文件并解码为 pem 字节数组
		item, err := readPemFile(fullName)
		if err != nil {
			mspLogger.Warningf("读取文件失败 %s: %s", fullName, err)
			continue
		}

		content = append(content, item)
	}

	return content, nil
}

const (
	// 存放根证书和中间证书的目录。这些证书用于验证网络中的节点和客户端的身份。
	cacerts = "cacerts"
	// 存放管理员证书的目录。管理员证书用于管理和操作网络。
	admincerts = "admincerts"
	// 存放实体证书的目录。实体证书用于标识网络中的节点和客户端。
	signcerts = "signcerts"
	// 存放私钥的目录。私钥用于对通信进行加密和解密。
	keystore = "keystore"
	// 存放中间证书的目录。中间证书用于构建证书链，验证节点和客户端的身份。
	intermediatecerts = "intermediatecerts"
	// 存放证书吊销列表 (CRL) 的目录。CRL 用于标识已被吊销的证书。
	crlsfolder = "crls"
	// 配置文件的名称。该文件包含了网络的配置信息。
	configfilename = "config.yaml"
	// 存放 TLS 根证书和中间证书的目录。这些证书用于在网络中进行安全的传输。
	tlscacerts = "tlscacerts"
	// 存放 TLS 中间证书的目录。这些证书用于构建 TLS 证书链，验证网络中的节点和客户端的身份。
	tlsintermediatecerts = "tlsintermediatecerts"
)

// SetupBCCSPKeystoreConfig 函数用于设置 BCCSP 配置中的密钥库路径。
// 如果传入的 BCCSP 配置为 nil，则使用默认配置。
// 如果传入的 BCCSP 配置中的 SW 配置为 nil，则使用默认配置的 SW 配置。
// 如果传入的 BCCSP 配置中的 SW.FileKeystore 为 nil 或 KeyStorePath 为空，则将其覆盖为指定的密钥库路径。
// 参数：
//   - bccspConfig *factory.FactoryOpts：BCCSP 配置的指针。
//   - keystoreDir string：密钥库目录的路径。
//
// 返回值：
//   - *factory.FactoryOpts：更新后的 BCCSP 配置。
func SetupBCCSPKeystoreConfig(bccspConfig *factory.FactoryOpts, keystoreDir string) *factory.FactoryOpts {
	// 如果传入的 BCCSP 配置为 nil，则使用默认配置
	if bccspConfig == nil {
		bccspConfig = factory.GetDefaultOpts()
	}

	// 如果传入的 BCCSP 配置中的 SW 配置为 nil，则使用默认配置的 SW 配置
	if bccspConfig.SW == nil {
		bccspConfig.SW = factory.GetDefaultOpts().SW
	}

	// 如果传入的 BCCSP 配置中的 SW.FileKeystore 为 nil 或 KeyStorePath 为空，则将其覆盖为指定的密钥库路径
	if bccspConfig.SW.FileKeystore == nil ||
		bccspConfig.SW.FileKeystore.KeyStorePath == "" {
		bccspConfig.SW.FileKeystore = &factory.FileKeystoreOpts{KeyStorePath: keystoreDir}
	}

	return bccspConfig
}

// GetLocalMspConfigWithType 函数根据指定的msp目录、bccsp配置、mspID 和 msp类型，返回指定目录中 MSP 的本地配置。
// 参数：
//   - dir string：MSP 目录的路径。
//   - bccspConfig *factory.FactoryOpts：BCCSP 配置的指针。
//   - ID string：MSP 的 ID。
//   - mspType string：MSP 的类型。
//
// 返回值：
//   - *msp.MSPConfig：MSP 的本地配置。
//   - error：获取配置过程中的错误，如果没有错误则为 nil。
func GetLocalMspConfigWithType(dir string, bccspConfig *factory.FactoryOpts, ID, mspType string) (*msp.MSPConfig, error) {
	// 根据 msp类型 加载配置
	// 本地MSP的类型-默认情况下，它的类型为bccsp=FABRIC
	// 目前版本已经固定为bccsp, 旧版本才中可使用idemix=IDEMIX
	switch mspType {
	case ProviderTypeToString(FABRIC):
		// 根据指定的msp目录、BCCSP 配置和 mspID，返回指定目录中 MSP 的本地配置
		return GetLocalMspConfig(dir, bccspConfig, ID)
	case ProviderTypeToString(IDEMIX):
		// 调用 idemix.GetIdemixMspConfig 函数获取指定目录中 Idemix MSP 的本地配置
		return idemix.GetIdemixMspConfig(dir, ID)
	default:
		// 如果指定的 MSP 类型未知，则返回错误
		return nil, errors.Errorf("未知 MSP 类型 '%s'", mspType)
	}
}

// GetLocalMspConfig 函数根据指定的msp目录、BCCSP 配置和 mspID，返回指定目录中 MSP 的本地配置。
// 参数：
//   - dir string：MSP 目录的路径。
//   - bccspConfig *factory.FactoryOpts：BCCSP 配置的指针。
//   - ID string：MSP 的 ID。
//
// 返回值：
//   - *msp.MSPConfig：MSP 的本地配置。
//   - error：获取配置过程中的错误，如果没有错误则为 nil。
func GetLocalMspConfig(dir string, bccspConfig *factory.FactoryOpts, ID string) (*msp.MSPConfig, error) {
	// 拼接签名证书 signcerts 目录和密钥库 keystore 目录的路径
	signcertDir := filepath.Join(dir, signcerts)
	keystoreDir := filepath.Join(dir, keystore)

	// 使用 密钥库 目录更新 BCCSP 配置
	bccspConfig = SetupBCCSPKeystoreConfig(bccspConfig, keystoreDir)

	// 初始化 BCCSP 工厂, 可以使用 factory.GetDefault() 获取初始化后的工厂实例
	err := factory.InitFactories(bccspConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "无法初始化BCCSP工厂,"+
			"检查 peer.BCCSP、peer.mspConfigPath、peer.localMspId、peer.localMspType 配置")
	}

	// 从签名证书目录中获取 PEM 格式的证书材料, 返回一个 pem 数组
	signcert, err := getPemMaterialFromDir(signcertDir)
	if err != nil || len(signcert) == 0 {
		return nil, errors.Wrapf(err, "无法从目录加载有效的 signcerts 签名者证书: %s", signcertDir)
	}

	/* FIXME: 现在我们做以下假设
	 * 1. 只有一个签名证书
	 * 2. BCCSP的密钥库具有与SKI匹配的私钥签名证书
	 */
	if len(signcert) > 1 {
		return nil, errors.Wrapf(err, "目录的 signcerts 签名者证书预期只有一个, 但出现了多个: %s", signcertDir)
	}

	// 创建一个 SigningIdentityInfo 结构体，其中 PublicSigner 为签名证书，PrivateSigner 为 nil
	sigid := &msp.SigningIdentityInfo{PublicSigner: signcert[0], PrivateSigner: nil}

	// 根据签名证书, 调用 getMspConfig 函数获取 MSP 的配置
	return getMspConfig(dir, ID, sigid)
}

// GetVerifyingMspConfig 返回给定目录、ID和类型的MSP配置
func GetVerifyingMspConfig(dir, ID, mspType string) (*msp.MSPConfig, error) {
	switch mspType {
	case ProviderTypeToString(FABRIC):
		return getMspConfig(dir, ID, nil)
	case ProviderTypeToString(IDEMIX):
		return idemix.GetIdemixMspConfig(dir, ID)
	default:
		return nil, errors.Errorf("未知的MSP类型 '%s', 只支持bccsp、idemix", mspType)
	}
}

// getMspConfig 函数用于从指定目录中获取指定 ID 的 MSP 配置。
// 参数：
//   - dir string：目录的路径。
//   - ID string：MSP 的 ID。
//   - sigid *msp.SigningIdentityInfo：签名标识信息的指针。
//
// 返回值：
//   - *msp.MSPConfig：获取到的 MSP 配置。
//   - error：获取过程中的错误，如果没有错误则为 nil。
func getMspConfig(dir string, ID string, sigid *msp.SigningIdentityInfo) (*msp.MSPConfig, error) {
	// 存放根证书和中间证书的目录。这些证书用于验证网络中的节点和客户端的身份。
	cacertDir := filepath.Join(dir, cacerts)
	// 存放管理员证书的目录。管理员证书用于管理和操作网络。
	admincertDir := filepath.Join(dir, admincerts)
	// 存放中间证书的目录。中间证书用于构建证书链，验证节点和客户端的身份。
	intermediatecertsDir := filepath.Join(dir, intermediatecerts)
	// 存放证书吊销列表 (CRL) 的目录。CRL 用于标识已被吊销的证书。
	crlsDir := filepath.Join(dir, crlsfolder)
	// 配置文件的名称。该文件包含了网络的配置信息。
	configFile := filepath.Join(dir, configfilename)
	// 存放 TLS 根证书和中间证书的目录。这些证书用于在网络中进行安全的传输。
	tlscacertDir := filepath.Join(dir, tlscacerts)
	// 存放 TLS 中间证书的目录。这些证书用于构建 TLS 证书链，验证网络中的节点和客户端的身份。
	tlsintermediatecertsDir := filepath.Join(dir, tlsintermediatecerts)

	// 从指定的 cacertDir 目录中读取 PEM 格式的材料。
	cacerts, err := getPemMaterialFromDir(cacertDir)
	if err != nil || len(cacerts) == 0 {
		return nil, errors.WithMessagef(err, "无法从目录加载有效的cacerts根证书: %s", cacertDir)
	}

	// 从指定的 admincertDir 目录中读取 PEM 格式的材料。
	admincert, err := getPemMaterialFromDir(admincertDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.WithMessagef(err, "无法从目录加载有效的admincert管理员证书: %s", admincertDir)
	}

	// 从指定的 intermediatecertsDir 目录中读取 PEM 格式的材料。
	intermediatecerts, err := getPemMaterialFromDir(intermediatecertsDir)
	if os.IsNotExist(err) {
		mspLogger.Debugf("未在中找到intermediatecerts中间证书文件夹: [%s]. 正在跳过. [%s]", intermediatecertsDir, err)
	} else if err != nil {
		return nil, errors.WithMessagef(err, "加载intermediatecerts中间证书失败: [%s]", intermediatecertsDir)
	}

	// 从指定的 tlscacertDir 目录中读取 PEM 格式的材料。
	tlsCACerts, err := getPemMaterialFromDir(tlscacertDir)
	tlsIntermediateCerts := [][]byte{}
	if os.IsNotExist(err) {
		mspLogger.Debugf("在未找到tlsCACerts证书文件夹: [%s]. 跳过并忽略tlsIntermediateCerts中间证书文件夹. [%s]", tlsintermediatecertsDir, err)
	} else if err != nil {
		return nil, errors.WithMessagef(err, "在以下位置加载TLS证书失败: [%s]", tlsintermediatecertsDir)
	} else if len(tlsCACerts) != 0 {
		// 如果tls证书有多个, 尝试加载tls中间证书
		tlsIntermediateCerts, err = getPemMaterialFromDir(tlsintermediatecertsDir)
		if os.IsNotExist(err) {
			mspLogger.Debugf("在未找到tlsIntermediateCerts中间证书文件夹: [%s]. 正在跳过. [%s]", tlsintermediatecertsDir, err)
		} else if err != nil {
			return nil, errors.WithMessagef(err, "加载tlsIntermediateCerts中间证书失败: [%s]", tlsintermediatecertsDir)
		}
	} else {
		mspLogger.Debugf("位于的tlsIntermediateCerts中间证书文件夹: [%s] 是空的. 正在跳过.", tlsintermediatecertsDir)
	}

	// 从指定的 crlsDir 目录中读取 PEM 格式的材料。
	crls, err := getPemMaterialFromDir(crlsDir)
	if os.IsNotExist(err) {
		mspLogger.Debugf("在未找到crls撤销证书文件夹: [%s]. 正在跳过. [%s]", crlsDir, err)
	} else if err != nil {
		return nil, errors.WithMessagef(err, "在以下位置加载crl撤销证书失败: [%s]", crlsDir)
	}

	var ouis []*msp.FabricOUIdentifier
	var nodeOUs *msp.FabricNodeOUs
	// 加载配置文件, 如果配置文件在那里，然后加载它, 否则跳过它
	_, err = os.Stat(configFile)
	if err == nil {
		// 加载文件，如果加载失败，则
		// 返回一个错误
		raw, err := ioutil.ReadFile(configFile)
		if err != nil {
			return nil, errors.Wrapf(err, "在加载配置文件失败: [%s]", configFile)
		}

		// 读取配置到结构体
		configuration := Configuration{}
		err = yaml.Unmarshal(raw, &configuration)
		if err != nil {
			return nil, errors.Wrapf(err, "配置出现错误, 无法解析配置文件: [%s]", configFile)
		}

		// 准备组织机构标识符, 组织机构可以不配置。组织机构标识符用于标识和区分不同的组织
		if len(configuration.OrganizationalUnitIdentifiers) > 0 {
			for _, ouID := range configuration.OrganizationalUnitIdentifiers {
				// 读取配置的证书
				f := filepath.Join(dir, ouID.Certificate)
				raw, err = readFile(f)
				if err != nil {
					return nil, errors.Wrapf(err, "未能在加载OrganizationalUnit证书: [%s]", f)
				}

				oui := &msp.FabricOUIdentifier{
					Certificate:                  raw,                               // 证书
					OrganizationalUnitIdentifier: ouID.OrganizationalUnitIdentifier, // OU 的名称
				}
				ouis = append(ouis, oui)
			}
		}

		// 准备NodeOUs节点组织单元。节点组织单元用于标识和区分不同类型的节点。
		if configuration.NodeOUs != nil && configuration.NodeOUs.Enable {
			mspLogger.Debug("正在加载 NodeOUs")
			nodeOUs = &msp.FabricNodeOUs{
				Enable: true,
			}

			// 检查配置中的各个节点组织单元标识符, 并将相应的组织单元标识符赋值给该结构体的 OrganizationalUnitIdentifier 字段
			if configuration.NodeOUs.ClientOUIdentifier != nil && len(configuration.NodeOUs.ClientOUIdentifier.OrganizationalUnitIdentifier) != 0 {
				nodeOUs.ClientOuIdentifier = &msp.FabricOUIdentifier{OrganizationalUnitIdentifier: configuration.NodeOUs.ClientOUIdentifier.OrganizationalUnitIdentifier}
			}
			if configuration.NodeOUs.PeerOUIdentifier != nil && len(configuration.NodeOUs.PeerOUIdentifier.OrganizationalUnitIdentifier) != 0 {
				nodeOUs.PeerOuIdentifier = &msp.FabricOUIdentifier{OrganizationalUnitIdentifier: configuration.NodeOUs.PeerOUIdentifier.OrganizationalUnitIdentifier}
			}
			if configuration.NodeOUs.AdminOUIdentifier != nil && len(configuration.NodeOUs.AdminOUIdentifier.OrganizationalUnitIdentifier) != 0 {
				nodeOUs.AdminOuIdentifier = &msp.FabricOUIdentifier{OrganizationalUnitIdentifier: configuration.NodeOUs.AdminOUIdentifier.OrganizationalUnitIdentifier}
			}
			if configuration.NodeOUs.OrdererOUIdentifier != nil && len(configuration.NodeOUs.OrdererOUIdentifier.OrganizationalUnitIdentifier) != 0 {
				nodeOUs.OrdererOuIdentifier = &msp.FabricOUIdentifier{OrganizationalUnitIdentifier: configuration.NodeOUs.OrdererOUIdentifier.OrganizationalUnitIdentifier}
			}

			// 读取证书 (如果已定义), 读取配置中指定的证书文件，并将读取到的证书内容赋值给相应的节点组织单元标识符的 Certificate 字段。

			// ClientOU
			if nodeOUs.ClientOuIdentifier != nil {
				nodeOUs.ClientOuIdentifier.Certificate = loadCertificateAt(dir, configuration.NodeOUs.ClientOUIdentifier.Certificate, "ClientOU")
			}
			// PeerOU
			if nodeOUs.PeerOuIdentifier != nil {
				nodeOUs.PeerOuIdentifier.Certificate = loadCertificateAt(dir, configuration.NodeOUs.PeerOUIdentifier.Certificate, "PeerOU")
			}
			// AdminOU
			if nodeOUs.AdminOuIdentifier != nil {
				nodeOUs.AdminOuIdentifier.Certificate = loadCertificateAt(dir, configuration.NodeOUs.AdminOUIdentifier.Certificate, "AdminOU")
			}
			// OrdererOU
			if nodeOUs.OrdererOuIdentifier != nil {
				nodeOUs.OrdererOuIdentifier.Certificate = loadCertificateAt(dir, configuration.NodeOUs.OrdererOUIdentifier.Certificate, "OrdererOU")
			}
		}
	} else {
		mspLogger.Debugf("在中找不到MSP配置文件: [%s]. [%s]", configFile, err)
	}

	// 设置Fabric哈希配置
	cryptoConfig := &msp.FabricCryptoConfig{
		SignatureHashFamily:            common.SignatureHashFamily(),
		IdentityIdentifierHashFunction: common.SignatureHashFamily(),
	}

	// 编写Fabric MSP 配置
	fmspconf := &msp.FabricMSPConfig{
		Admins:                        admincert,            // 管理员证书
		RootCerts:                     cacerts,              // 根证书
		IntermediateCerts:             intermediatecerts,    // 中间证书
		SigningIdentity:               sigid,                // 签名证书
		Name:                          ID,                   // msp id
		OrganizationalUnitIdentifiers: ouis,                 // 组织机构标识符。组织机构标识符用于标识和区分不同的组织
		RevocationList:                crls,                 // 撤销证书
		CryptoConfig:                  cryptoConfig,         // 哈希函数配置
		TlsRootCerts:                  tlsCACerts,           // tls证书
		TlsIntermediateCerts:          tlsIntermediateCerts, // tls中间证书
		FabricNodeOus:                 nodeOUs,              // 节点组织单元。节点组织单元用于标识和区分不同类型的节点。
	}
	// 序列化为字节数组
	fmpsjs, err := proto.Marshal(fmspconf)
	if err != nil {
		return nil, err
	}

	return &msp.MSPConfig{Config: fmpsjs, Type: int32(FABRIC)}, nil
}

// loadCertificateAt 函数用于加载指定路径下的证书文件。
// 参数：
//   - dir string：目录的路径。
//   - certificatePath string：证书文件的路径。
//   - ouType string：组织单位类型(无实际意义, 主要作为日志说明)。
//
// 返回值：
//   - []byte：加载到的证书内容。
func loadCertificateAt(dir, certificatePath string, ouType string) []byte {
	// 如果证书路径为空，则打印日志并返回 nil
	if certificatePath == "" {
		mspLogger.Debugf("%s 未配置证书路径", ouType)
		return nil
	}

	// 拼接完整的证书文件路径
	f := filepath.Join(dir, certificatePath)

	// 读取证书文件内容
	raw, err := readFile(f)
	if err != nil {
		mspLogger.Warnf(" %s 证书加载失败: [%s]. [%s]", ouType, f, err)
	} else {
		return raw
	}

	return nil
}
