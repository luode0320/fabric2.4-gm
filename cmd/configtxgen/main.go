/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/internal/peer/channel"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-config/protolator"
	"github.com/hyperledger/fabric-config/protolator/protoext/ordererext"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/configtxgen/encoder"
	"github.com/hyperledger/fabric/internal/configtxgen/genesisconfig"
	"github.com/hyperledger/fabric/internal/configtxgen/metadata"
	"github.com/hyperledger/fabric/internal/configtxlator/update"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("common.tools.configtxgen")

// doOutputBlock 生成并输出创世区块。
// 输入参数：
//   - config：*genesisconfig.Profile，表示创世区块的配置文件
//   - channelID：string，表示通道的ID
//   - outputBlock：string，表示输出创世区块的文件路径
//
// 返回值：
//   - error：表示生成和输出创世区块过程中可能发生的错误
func doOutputBlock(config *genesisconfig.Profile, channelID string, outputBlock string) error {
	// 创建一个新的 bootstrapper 实例
	pgen, err := encoder.NewBootstrapper(config)
	if err != nil {
		return errors.WithMessage(err, "无法创建 bootstrapper 通道组")
	}

	logger.Info("生成创世块...")

	// 检查配置文件中是否存在 Orderer 部分
	if config.Orderer == nil {
		return errors.New("拒绝生成创世块, 缺少orderer配置")
	}

	// 检查配置文件中是否存在 Consortiums 联盟列表
	if config.Consortiums != nil {
		logger.Info("创建系统通道创世块")
	} else {
		// 检查配置文件中是否存在 Application 部分
		if config.Application == nil {
			return errors.New("拒绝生成缺少 Application 应用程序部分的应用程序创世通道块")
		}
		logger.Info("创建应用程序通道创世块")
	}

	// 生成指定通道的创世区块
	genesisBlock := pgen.GenesisBlockForChannel(channelID)

	logger.Info("正在写入创世块...")

	// 将创世区块写入文件
	file := filepath.Join(os.Getenv("FABRIC_CFG_PATH"), outputBlock)
	err = writeFile(file, protoutil.MarshalOrPanic(genesisBlock), 0o640)
	if err != nil {
		return fmt.Errorf("写入创世块时出错: %s", err)
	}

	logger.Info("生成创世块完成: ", file)

	return nil
}

// doOutputChannelCreateTx 生成并输出新的通道创建交易。
// 输入参数：
//   - conf：*genesisconfig.Profile，表示通道的配置文件
//   - baseProfile：*genesisconfig.Profile，表示基础配置文件
//   - channelID：string，表示通道的ID
//   - outputChannelCreateTx：string，表示输出通道创建交易的文件路径
//
// 返回值：
//   - error：表示生成和输出通道创建交易过程中可能发生的错误
func doOutputChannelCreateTx(conf, baseProfile *genesisconfig.Profile, channelID string, outputChannelCreateTx string) error {
	logger.Info("生成新的创建通道的交易记录...")

	var configtx *cb.Envelope
	var err error

	// 如果基础配置文件为空，则使用 MakeChannelCreationTransaction 函数生成通道创建交易
	if baseProfile == nil {
		// 创建一个用于创建通道的交易。
		configtx, err = encoder.MakeChannelCreationTransaction(channelID, nil, conf)
	} else {
		// 如果基础配置文件不为空，则使用 MakeChannelCreationTransactionWithSystemChannelContext 函数生成通道创建交易
		configtx, err = encoder.MakeChannelCreationTransactionWithSystemChannelContext(channelID, nil, conf, baseProfile)
	}

	if err != nil {
		return err
	}

	logger.Info("正在写入新的创建通道的交易记录...")

	// 将创建通道的交易写入文件
	file := filepath.Join(os.Getenv("FABRIC_CFG_PATH"), outputChannelCreateTx)
	err = writeFile(file, protoutil.MarshalOrPanic(configtx), 0o640)
	if err != nil {
		return fmt.Errorf("写入创建通道的交易记录时出错: %s", err)
	}

	logger.Info("生成新的创建通道的交易记录完成: ", file)
	return nil
}

func doOutputAnchorPeersUpdate(conf *genesisconfig.Profile, channelID string, outputAnchorPeersUpdate string, asOrg string) error {
	logger.Info("Generating anchor peer update")
	if asOrg == "" {
		return fmt.Errorf("must specify an organization to update the anchor peer for")
	}

	if conf.Application == nil {
		return fmt.Errorf("cannot update anchor peers without an application section")
	}

	original, err := encoder.NewChannelGroup(conf)
	if err != nil {
		return errors.WithMessage(err, "error parsing profile as channel group")
	}
	original.Groups[channelconfig.ApplicationGroupKey].Version = 1

	updated := proto.Clone(original).(*cb.ConfigGroup)

	originalOrg, ok := original.Groups[channelconfig.ApplicationGroupKey].Groups[asOrg]
	if !ok {
		return errors.Errorf("org with name '%s' does not exist in config", asOrg)
	}

	if _, ok = originalOrg.Values[channelconfig.AnchorPeersKey]; !ok {
		return errors.Errorf("org '%s' does not have any anchor peers defined", asOrg)
	}

	delete(originalOrg.Values, channelconfig.AnchorPeersKey)

	updt, err := update.Compute(&cb.Config{ChannelGroup: original}, &cb.Config{ChannelGroup: updated})
	if err != nil {
		return errors.WithMessage(err, "could not compute update")
	}
	updt.ChannelId = channelID

	newConfigUpdateEnv := &cb.ConfigUpdateEnvelope{
		ConfigUpdate: protoutil.MarshalOrPanic(updt),
	}

	updateTx, err := protoutil.CreateSignedEnvelope(cb.HeaderType_CONFIG_UPDATE, channelID, nil, newConfigUpdateEnv, 0, 0)
	if err != nil {
		return errors.WithMessage(err, "could not create signed envelope")
	}

	logger.Info("Writing anchor peer update")
	err = writeFile(outputAnchorPeersUpdate, protoutil.MarshalOrPanic(updateTx), 0o640)
	if err != nil {
		return fmt.Errorf("Error writing channel anchor peer update: %s", err)
	}
	return nil
}

// Block 过滤json前缀
type Block struct {
	Data struct {
		Data []struct {
			Payload struct {
				Data struct {
					Config interface{} `json:"config"`
				} `json:"data"`
			} `json:"payload"`
		} `json:"data"`
	} `json:"data"`
}

func doInspectBlock(inspectBlock string) error {
	logger.Info("检查 block 创世块")
	inspectBlock = filepath.Join(os.Getenv("FABRIC_CFG_PATH"), inspectBlock)
	data, err := ioutil.ReadFile(inspectBlock)
	if err != nil {
		return fmt.Errorf("无法读取块 %s", inspectBlock)
	}

	// 构建文件路径
	// 将字符串中的".block"替换为".json"
	filePath := strings.Replace(inspectBlock, ".block", ".json", 1)

	logger.Info("解析 block 创世块")
	block, err := protoutil.UnmarshalBlock(data)
	if err != nil {
		return fmt.Errorf("反序列化到 block 块时出错: %s", err)
	}

	// 打开文件，如果文件不存在则创建，如果文件存在则覆盖写入
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s", err)
	}
	defer file.Close()

	// 将JSON写入文件
	err = protolator.DeepMarshalJSON(file, block)
	if err != nil {
		return fmt.Errorf("生成通道 json 文件失败: %s", err)
	}

	// 读取文件内容
	file, err = os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer file.Close()

	data, err = ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("读取文件失败: %s\n", err)
	}

	// 解析JSON数据
	var blockFilter Block
	err = json.Unmarshal(data, &blockFilter)
	if err != nil {
		return fmt.Errorf("解析JSON失败: %s\n", err)
	}

	// 提取需要的数据
	configData := blockFilter.Data.Data[0].Payload.Data.Config

	// 将数据转换为JSON格式
	filteredData, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return fmt.Errorf("转换为JSON失败: %s\n", err)
	}

	// 将过滤后的数据写入原始文件
	err = ioutil.WriteFile(filePath, filteredData, 0644)
	if err != nil {
		return fmt.Errorf("写入文件失败: %s\n", err)
	}

	logger.Infof("生成通道 json 文件: %s", filePath)

	// 再次读取 json 文件内容
	file, err = os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer file.Close()

	// 构建文件路径
	// 将字符串中的".block"替换为".json"
	filePath = strings.Replace(inspectBlock, ".block", ".pb", 1)

	// 读取文件内容
	pbfile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer pbfile.Close()

	err = channel.EncodeProto(file, pbfile)
	if err != nil {
		return fmt.Errorf("生成通道 pb 文件失败: %s", filePath)
	}

	logger.Infof("生成通道 pb 文件: %s", filePath)
	return nil
}

func doInspectChannelCreateTx(inspectChannelCreateTx string) error {
	logger.Info("Inspecting transaction")
	data, err := ioutil.ReadFile(inspectChannelCreateTx)
	if err != nil {
		return fmt.Errorf("could not read channel create tx: %s", err)
	}

	logger.Info("Parsing transaction")
	env, err := protoutil.UnmarshalEnvelope(data)
	if err != nil {
		return fmt.Errorf("Error unmarshalling envelope: %s", err)
	}

	err = protolator.DeepMarshalJSON(os.Stdout, env)
	if err != nil {
		return fmt.Errorf("malformed transaction contents: %s", err)
	}

	return nil
}

func doPrintOrg(t *genesisconfig.TopLevel, printOrg string) error {
	for _, org := range t.Organizations {
		if org.Name == printOrg {
			og, err := encoder.NewOrdererOrgGroup(org)
			if err != nil {
				return errors.Wrapf(err, "bad org definition for org %s", org.Name)
			}

			if err := protolator.DeepMarshalJSON(os.Stdout, &ordererext.DynamicOrdererOrgGroup{ConfigGroup: og}); err != nil {
				return errors.Wrapf(err, "malformed org definition for org: %s", org.Name)
			}
			return nil
		}
	}
	return errors.Errorf("organization %s not found", printOrg)
}

func writeFile(filename string, data []byte, perm os.FileMode) error {
	dirPath := filepath.Dir(filename)
	exists, err := dirExists(dirPath)
	if err != nil {
		return err
	}
	if !exists {
		err = os.MkdirAll(dirPath, 0o750)
		if err != nil {
			return err
		}
	}
	return ioutil.WriteFile(filename, data, perm)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func main() {
	var outputBlock, outputChannelCreateTx, channelCreateTxBaseProfile, profile, configPath, channelID, inspectBlock, inspectChannelCreateTx, outputAnchorPeersUpdate, asOrg, printOrg string

	flag.StringVar(&outputBlock, "outputBlock", "", "输出创世区块的文件路径(如果设置)")
	flag.StringVar(&channelID, "channelID", "", "此通道ID(必须配置)")
	flag.StringVar(&outputChannelCreateTx, "outputCreateChannelTx", "", "配置文件输出到此目录(如果设置)")
	flag.StringVar(&channelCreateTxBaseProfile, "channelCreateTxBaseProfile", "", "指定要视为orderer系统通道当前状态的配置文件，以允许在通道创建tx生成期间修改非应用程序参数. 仅与 “outputCreateChannelTx配置” 一起有效(如果设置)")
	flag.StringVar(&profile, "profile", "", "configtx.yaml中用于生成的Profiles配置选项(必须配置)")
	flag.StringVar(&configPath, "configPath", "", "指定要使用的配置configtx.yaml路径(如果设置)")
	flag.StringVar(&inspectBlock, "inspectBlock", "", "打印指定路径的块中包含的配置(如果设置)")
	flag.StringVar(&inspectChannelCreateTx, "inspectChannelCreateTx", "", "打印指定路径的事务中包含的配置(如果设置)")
	flag.StringVar(&outputAnchorPeersUpdate, "outputAnchorPeersUpdate", "", "[已弃用] 创建配置更新以更新锚点对等体 (仅适用于默认通道创建，并且仅适用于第一次更新)")
	flag.StringVar(&asOrg, "asOrg", "", "作为特定组织 (按名称) 执行配置生成，仅包括org (可能) 有权设置的写入集中的值(如果设置)")
	flag.StringVar(&printOrg, "printOrg", "", "将组织的定义打印为JSON。(对于手动将组织添加到渠道非常有用)(如果设置)")

	version := flag.Bool("version", false, "显示版本信息")

	flag.Parse()

	if channelID == "" && (outputBlock != "" || outputChannelCreateTx != "" || outputAnchorPeersUpdate != "") {
		logger.Fatalf("缺少channelID通道ID，请用 “-channelID” 指定")
	}

	// 显示版本
	if *version {
		printVersion()
		os.Exit(0)
	}

	// 通过命令行运行时解决panic
	defer func() {
		if err := recover(); err != nil {
			if strings.Contains(fmt.Sprint(err), "读取配置时出错: 不支持的配置类型") {
				logger.Error("找不到 configtx.yaml. " +
					"请确保 FABRIC_CFG_PATH 或 -configPath 设置为路径 " +
					"其中包含 configtx.yaml")
				os.Exit(1)
			}
			if strings.Contains(fmt.Sprint(err), "找不到配置文件") {
				logger.Error(fmt.Sprint(err) + ". " +
					"请确保 FABRIC_CFG_PATH 或 -configPath 设置为路径 " +
					",其中包含具有指定配置文件的 configtx.yaml ")
				os.Exit(1)
			}
			logger.Panic(err)
		}
	}()

	logger.Info("加载配置")
	// 函数用于在使用工厂接口之前初始化 BCCSP 工厂。
	err := factory.InitFactories(nil)
	if err != nil {
		logger.Fatalf("初始化 BCCSP 工厂错误: %s", err)
	}
	var profileConfig *genesisconfig.Profile
	if outputBlock != "" || outputChannelCreateTx != "" || outputAnchorPeersUpdate != "" {
		if profile == "" {
			logger.Fatalf("当 '-outputblock' 输出此创世块的路径时, 必须需要 '-profile'、'-outputChannelCreateTx' 配置")
		}

		if configPath != "" {
			profileConfig = genesisconfig.Load(profile, configPath)
		} else {
			profileConfig = genesisconfig.Load(profile)
		}
	}

	var baseProfile *genesisconfig.Profile
	if channelCreateTxBaseProfile != "" {
		if outputChannelCreateTx == "" {
			logger.Warning("指定 'channelCreateTxBaseProfile', 但没有指定 'outputChannelCreateTx', 'channelCreateTxBaseProfile' 不会影响输出.")
		}
		if configPath != "" {
			baseProfile = genesisconfig.Load(channelCreateTxBaseProfile, configPath)
		} else {
			baseProfile = genesisconfig.Load(channelCreateTxBaseProfile)
		}
	}

	if outputBlock != "" {
		// 生成并输出创世区块
		if err := doOutputBlock(profileConfig, channelID, outputBlock); err != nil {
			logger.Fatalf("生成并输出创世区块 outputBlock 错误: %s", err)
		}
	}

	if outputChannelCreateTx != "" {
		// 生成并输出新的通道创建交易, 创建一个用于创建通道的交易。
		if err := doOutputChannelCreateTx(profileConfig, baseProfile, channelID, outputChannelCreateTx); err != nil {
			logger.Fatalf("创建一个用于创建通道的交易 outputChannelCreateTx 错误: %s", err)
		}
	}

	// 输出为json+pb文件
	if inspectBlock != "" {
		if err := doInspectBlock(inspectBlock); err != nil {
			logger.Fatalf("打印指定路径的块中包含的配置 inspectBlock 错误: %s", err)
		}
	}

	if inspectChannelCreateTx != "" {
		if err := doInspectChannelCreateTx(inspectChannelCreateTx); err != nil {
			logger.Fatalf("打印指定路径的事务中包含的配置 inspectChannelCreateTx 错误: %s", err)
		}
	}

	// 已弃用
	if outputAnchorPeersUpdate != "" {
		if err := doOutputAnchorPeersUpdate(profileConfig, channelID, outputAnchorPeersUpdate, asOrg); err != nil {
			logger.Fatalf("Error on inspectChannelCreateTx: %s", err)
		}
	}

	if printOrg != "" {
		var topLevelConfig *genesisconfig.TopLevel
		if configPath != "" {
			topLevelConfig = genesisconfig.LoadTopLevel(configPath)
		} else {
			topLevelConfig = genesisconfig.LoadTopLevel()
		}

		if err := doPrintOrg(topLevelConfig, printOrg); err != nil {
			logger.Fatalf("将组织的定义打印为JSON错误: %s", err)
		}
	}
}

func printVersion() {
	fmt.Println(metadata.GetVersionInfo())
}

// 配置文件目录
var peerLinuxConfig = "sampleconfig"
var peerWindowsConfig = "samplewindowsconfig"

/**
 * 初始化
 */
func init() {
	// 检查 FABRIC_CFG_PATH 环境变量是否已设置
	cfgPath := os.Getenv("FABRIC_CFG_PATH")

	if cfgPath == "" {
		// 获取当前工作目录
		currentDir, err := os.Getwd()
		if err != nil {
			fmt.Println("无法获取当前路径:", err)
			return
		}
		// 根据操作系统确定 peerConfig 的路径
		var peerConfig string
		switch runtime.GOOS {
		case "linux":
			peerConfig = peerLinuxConfig
		case "windows":
			peerConfig = peerWindowsConfig
		default:
			fmt.Println("不支持的操作系统")
			return
		}
		// 构建 peerConfigPath 的路径
		cfgPath = filepath.Join(currentDir, peerConfig)
		if _, err := os.Stat(cfgPath); err == nil {
			os.Setenv("FABRIC_CFG_PATH", cfgPath)
		} else {
			os.Setenv("FABRIC_CFG_PATH", currentDir)
		}
	}
	fmt.Println("FABRIC_CFG_PATH: ", os.Getenv("FABRIC_CFG_PATH"))
}

// 初始化创建配置文件
func init() {
	config.Init()
}
