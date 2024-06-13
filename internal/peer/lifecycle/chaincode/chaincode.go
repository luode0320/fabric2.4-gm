/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"time"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	lifecycleName                = "_lifecycle"
	approveFuncName              = "ApproveChaincodeDefinitionForMyOrg"
	commitFuncName               = "CommitChaincodeDefinition"
	checkCommitReadinessFuncName = "CheckCommitReadiness"
)

var logger = flogging.MustGetLogger("cli.lifecycle.chaincode")

func addFlags(cmd *cobra.Command) {
	common.AddOrdererFlags(cmd)
}

// Cmd 返回Chaincode的cobra命令
func Cmd(cryptoProvider bccsp.BCCSP) *cobra.Command {
	addFlags(chaincodeCmd)

	chaincodeCmd.AddCommand(PackageCmd(nil))
	chaincodeCmd.AddCommand(CalculatePackageIDCmd(nil))
	chaincodeCmd.AddCommand(InstallCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(QueryInstalledCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(GetInstalledPackageCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(ApproveForMyOrgCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(QueryApprovedCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(CheckCommitReadinessCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(CommitCmd(nil, cryptoProvider))
	chaincodeCmd.AddCommand(QueryCommittedCmd(nil, cryptoProvider))

	return chaincodeCmd
}

// 与链码相关的变量。
var (
	chaincodeLang         string        // 链码语言
	chaincodePath         string        // 链码路径
	chaincodeName         string        // 链码名称
	channelID             string        // 通道ID
	chaincodeVersion      string        // 链码版本
	packageLabel          string        // 包标签
	signaturePolicy       string        // 签名策略
	channelConfigPolicy   string        // 通道配置策略
	endorsementPlugin     string        // 背书插件
	validationPlugin      string        // 验证插件
	collectionsConfigFile string        // 集合配置文件
	peerAddresses         []string      // 背书节点地址
	tlsRootCertFiles      []string      // TLS根证书文件
	connectionProfilePath string        // 连接配置文件路径
	targetPeer            string        // 目标背书节点
	waitForEvent          bool          // 是否等待事件
	waitForEventTimeout   time.Duration // 等待事件超时时间
	packageID             string        // 包ID
	sequence              int           // 序列号
	initRequired          bool          // 是否需要初始化
	output                string        // 输出
	outputDirectory       string        // 输出目录
)

var chaincodeCmd = &cobra.Command{
	Use:   "chaincode",
	Short: "执行链码操作: package|install|queryinstalled|getinstalledpackage|calculatepackageid|approveformyorg|queryapproved|checkcommitreadiness|commit|querycommitted",
	Long:  "执行链码操作: package|install|queryinstalled|getinstalledpackage|calculatepackageid|approveformyorg|queryapproved|checkcommitreadiness|commit|querycommitted",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		common.InitCmd(cmd, args)       // 初始化msp配置
		common.SetOrdererEnv(cmd, args) // 初始化orderer配置
	},
}

var flags *pflag.FlagSet

func init() {
	ResetFlags()
}

// ResetFlags resets the values of these flags to facilitate tests
func ResetFlags() {
	flags = &pflag.FlagSet{}

	flags.StringVarP(&chaincodeLang, "lang", "l", "golang", "编写链码的 lang 语言")
	flags.StringVarP(&chaincodePath, "path", "p", "", "链码的 path 路径")
	flags.StringVarP(&chaincodeName, "name", "n", "", "链码的 name 名称")
	flags.StringVarP(&chaincodeVersion, "version", "v", "", "链码的 version 版本")
	flags.StringVarP(&packageLabel, "label", "", "", "链码的 label 标签描述")
	flags.StringVarP(&channelID, "channelID", "C", "", "应执行此命令的 channelID 通道")
	flags.StringVarP(&signaturePolicy, "signature-policy", "", "", "The endorsement policy associated to this chaincode specified as a signature policy")
	flags.StringVarP(&channelConfigPolicy, "channel-config-policy", "", "", "The endorsement policy associated to this chaincode specified as a channel config policy reference")
	flags.StringVarP(&endorsementPlugin, "endorsement-plugin", "E", "", "要用于此链码的背书插件的名称")
	flags.StringVarP(&validationPlugin, "validation-plugin", "V", "", "要用于此链码的验证插件的名称")
	flags.StringVar(&collectionsConfigFile, "collections-config", "", "The fully qualified path to the collection JSON file including the file name")
	flags.StringArrayVarP(&peerAddresses, "peerAddresses", "", []string{""}, "要连接到的 peer 的地址(数组)")
	flags.StringArrayVarP(&tlsRootCertFiles, "tlsRootCertFiles", "", []string{""},
		"如果启用了TLS, 则要配置连接的 peer 的TLS根证书文件的路径, 指定的证书的顺序和数量应与 --peerAddresses 标志匹配(数组)")
	flags.StringVarP(&connectionProfilePath, "connectionProfile", "", "",
		"连接的配置文件路径, 该路径为网络提供必要的连接信息. 注意: 目前仅支持提供 peer 的连接信息")
	flags.StringVarP(&targetPeer, "targetPeer", "", "",
		"使用连接配置文件时，此操作的目标 peer 的名称")
	flags.BoolVar(&waitForEvent, "waitForEvent", true,
		"是否等待来自每个 peer 的交付服务的事件, 表示事务已成功提交")
	flags.DurationVar(&waitForEventTimeout, "waitForEventTimeout", 30*time.Second,
		"等待来自每个 peer 的交付服务的事件的时间, 该事件表示 '调用' 事务已成功提交")
	flags.StringVarP(&packageID, "package-id", "", "", "链码安装包的 package-id 标识符")
	flags.IntVarP(&sequence, "sequence", "", 0, "通道的链码定义的序列号")
	flags.BoolVarP(&initRequired, "init-required", "", false, "链码是否需要调用 'init' 初始化函数")
	flags.StringVarP(&output, "output", "O", "", "查询结果的输出格式, 默认为人类可读的纯文本. json是目前唯一支持的格式.")
	flags.StringVarP(&outputDirectory, "output-directory", "", "", "将链码安装包写入磁盘时要使用的输出目录. 默认为当前工作目录.")
}

func attachFlags(cmd *cobra.Command, names []string) {
	cmdFlags := cmd.Flags()
	for _, name := range names {
		if flag := flags.Lookup(name); flag != nil {
			cmdFlags.AddFlag(flag)
		} else {
			logger.Fatalf("找不到要附加到命令 '%s' 的标志 '%s'", name, cmd.Name())
		}
	}
}
