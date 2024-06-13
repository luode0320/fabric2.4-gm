/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/internal/peer/packaging"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	chainFuncName = "chaincode"
	chainCmdDes   = "Operate a chaincode: install|instantiate|invoke|package|query|signpackage|upgrade|list."
)

var logger = flogging.MustGetLogger("chaincodeCmd")

// XXX This is a terrible singleton hack, however
// it simply making a latent dependency explicit.
// It should be removed along with the other package
// scoped variables
var platformRegistry = packaging.NewRegistry(packaging.SupportedPlatforms...)

func addFlags(cmd *cobra.Command) {
	common.AddOrdererFlags(cmd)
	flags := cmd.PersistentFlags()
	flags.StringVarP(&transient, "transient", "", "", "Transient map of arguments in JSON encoding")
}

// Cmd 返回Chaincode的cobra命令
func Cmd(cf *ChaincodeCmdFactory, cryptoProvider bccsp.BCCSP) *cobra.Command {
	addFlags(chaincodeCmd)

	chaincodeCmd.AddCommand(installCmd(cf, nil, cryptoProvider))
	chaincodeCmd.AddCommand(instantiateCmd(cf, cryptoProvider))
	chaincodeCmd.AddCommand(invokeCmd(cf, cryptoProvider))
	chaincodeCmd.AddCommand(packageCmd(cf, nil, nil, cryptoProvider))
	chaincodeCmd.AddCommand(queryCmd(cf, cryptoProvider))
	chaincodeCmd.AddCommand(signpackageCmd(cf, cryptoProvider))
	chaincodeCmd.AddCommand(upgradeCmd(cf, cryptoProvider))
	chaincodeCmd.AddCommand(listCmd(cf, cryptoProvider))

	return chaincodeCmd
}

// 与链码相关的变量。
var (
	chaincodeLang         string        // 链码语言
	chaincodeCtorJSON     string        // 链码构造函数的JSON表示
	chaincodePath         string        // 链码路径
	chaincodeName         string        // 链码名称
	chaincodeUsr          string        // 未使用
	chaincodeQueryRaw     bool          // 是否以原始格式查询链码
	chaincodeQueryHex     bool          // 是否以十六进制格式查询链码
	channelID             string        // 通道ID
	chaincodeVersion      string        // 链码版本
	policy                string        // 策略
	escc                  string        // 背书系统链码
	vscc                  string        // 验证系统链码
	policyMarshalled      []byte        // 序列化的策略
	transient             string        // 临时数据
	isInit                bool          // 是否为初始化
	collectionsConfigFile string        // 配置集合文件
	collectionConfigBytes []byte        // 集合配置字节
	peerAddresses         []string      // 节点地址列表
	tlsRootCertFiles      []string      // TLS根证书文件列表
	connectionProfile     string        // 连接配置文件
	waitForEvent          bool          // 是否等待事件
	waitForEventTimeout   time.Duration // 等待事件超时时间
)

var chaincodeCmd = &cobra.Command{
	Use:   chainFuncName,
	Short: fmt.Sprint(chainCmdDes),
	Long:  fmt.Sprint(chainCmdDes),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		common.InitCmd(cmd, args)
		common.SetOrdererEnv(cmd, args)
	},
}

var flags *pflag.FlagSet

func init() {
	resetFlags()
}

// 显式定义方法以方便测试
func resetFlags() {
	flags = &pflag.FlagSet{}

	flags.StringVarP(&chaincodeLang, "lang", "l", "golang",
		fmt.Sprintf("编写 %s 的 lang 语言", chainFuncName))
	flags.StringVarP(&chaincodeCtorJSON, "ctor", "c", "{}",
		fmt.Sprintf("JSON格式的 %s 构造函数 ctor 消息", chainFuncName))
	flags.StringVarP(&chaincodePath, "path", "p", common.UndefinedParamValue,
		fmt.Sprintf("%s 的 path 路径", chainFuncName))
	flags.StringVarP(&chaincodeName, "name", "n", common.UndefinedParamValue,
		"链码的 name 名称")
	flags.StringVarP(&chaincodeVersion, "version", "v", common.UndefinedParamValue,
		"install/instantiate/upgrade 命令中指定的链码的版本")
	flags.StringVarP(&chaincodeUsr, "username", "u", common.UndefinedParamValue,
		"启用安全性时链码操作的 username 用户名")
	flags.StringVarP(&channelID, "channelID", "C", "",
		"应执行此命令的 channelID 通道")
	flags.StringVarP(&policy, "policy", "P", common.UndefinedParamValue,
		"此链码关联的 policy 背书策略")
	flags.StringVarP(&escc, "escc", "E", common.UndefinedParamValue,
		"要用于此链码的背书系统链码 escc 的名称")
	flags.StringVarP(&vscc, "vscc", "V", common.UndefinedParamValue,
		"要用于此链码的验证系统链码 vscc 的名称")
	flags.BoolVarP(&isInit, "isInit", "I", false,
		"这是init初始化函数的调用(对于在新的生命周期中支持旧的链码很有用)")
	flags.BoolVarP(&getInstalledChaincodes, "installed", "", false,
		"获取 peer 上已安装的链码")
	flags.BoolVarP(&getInstantiatedChaincodes, "instantiated", "", false,
		"获取通道上已实例化的链码")
	flags.StringVar(&collectionsConfigFile, "collections-config", common.UndefinedParamValue,
		"The fully qualified path to the collection JSON file including the file name")
	flags.StringArrayVarP(&peerAddresses, "peerAddresses", "", []string{common.UndefinedParamValue},
		"要连接到的 peer 的地址(数组)")
	flags.StringArrayVarP(&tlsRootCertFiles, "tlsRootCertFiles", "", []string{common.UndefinedParamValue},
		"如果启用了TLS, 则要配置连接的 peer 的TLS根证书文件的路径, 指定的证书的顺序和数量应与 --peerAddresses标志匹配(数组)")
	flags.StringVarP(&connectionProfile, "connectionProfile", "", common.UndefinedParamValue,
		"为网络提供必要连接信息的连接配置文件, 注意: 目前仅支持提供 peer 连接信息")
	flags.BoolVar(&waitForEvent, "waitForEvent", false,
		"是否等待来自每个 peer 的交付服务的事件, 表示 '调用' 事务已成功提交")
	flags.DurationVar(&waitForEventTimeout, "waitForEventTimeout", 30*time.Second,
		"等待来自每个 peer 的交付服务的事件的时间, 该事件表示 '调用' 事务已成功提交")
	flags.BoolVarP(&createSignedCCDepSpec, "cc-package", "s", false,
		"create CC deployment spec for owner endorsements instead of raw CC deployment spec")
	flags.BoolVarP(&signCCDepSpec, "sign", "S", false,
		"如果为所有者背书创建CC部署规范包 cc-package , 还请使用本地MSP对其进行 sign 签名")
	flags.StringVarP(&instantiationPolicy, "instantiate-policy", "i", "",
		"链码的实例化策略")
}

func attachFlags(cmd *cobra.Command, names []string) {
	cmdFlags := cmd.Flags()
	for _, name := range names {
		if flag := flags.Lookup(name); flag != nil {
			cmdFlags.AddFlag(flag)
		} else {
			logger.Fatalf("Could not find flag '%s' to attach to command '%s'", name, cmd.Name())
		}
	}
}
