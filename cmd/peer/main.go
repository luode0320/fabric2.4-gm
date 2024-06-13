// 该main函数是peer二进制文件的运行入口
package main

import (
	"fmt"
	"github.com/hyperledger/fabric/core/config"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/internal/peer/chaincode"
	"github.com/hyperledger/fabric/internal/peer/channel"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/internal/peer/lifecycle"
	"github.com/hyperledger/fabric/internal/peer/node"
	"github.com/hyperledger/fabric/internal/peer/snapshot"
	"github.com/hyperledger/fabric/internal/peer/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// 主命令描述服务 和 默认打印帮助消息。
var mainCmd = &cobra.Command{Use: "peer"}

// peer配置文件目录
var peerLinuxConfig = "sampleconfig"
var peerWindowsConfig = "samplewindowsconfig"

/**
 * 1. viper 是一个流行的 Go 库，用于处理配置文件和环境变量
 * 2. mainCmd 属于命令程序, 用于识别和处理启动的追加命令
 */
func main() {
	// 设置环境变量前缀, 并设置get默认自动获取这个前缀的环境变量
	viper.SetEnvPrefix(common.CmdRoot)
	viper.AutomaticEnv()
	// 设置将 . 转换为 _ 的环境变量key转换器
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	// 定义对所有peer对等命令有效的命令行标志，和命令。
	mainFlags := mainCmd.PersistentFlags()

	// 设置传统日志记录级别
	mainFlags.String("logging-level", "", "Legacy logging level flag")
	// 命令行参数与 viper 绑定,使用 viper.Get() 或 viper.GetString() 获取配置值时，viper 会自动从命令行参数中读取该配置项的值
	viper.BindPFlag("logging_level", mainFlags.Lookup("logging-level"))
	// 该命令行参数在帮助信息中不会显示出来，但仍然可以通过命令行传递和使用
	mainFlags.MarkHidden("logging-level")

	// 获取默认的加密提供程序
	cryptoProvider := factory.GetDefault()

	// 添加子命令, version版本、node节点、chaincode链码、channel通道、lifecycle生命周期、snapshot快照
	mainCmd.AddCommand(version.Cmd())
	mainCmd.AddCommand(node.Cmd())
	mainCmd.AddCommand(chaincode.Cmd(nil, cryptoProvider))
	mainCmd.AddCommand(channel.Cmd(nil))
	mainCmd.AddCommand(lifecycle.Cmd(cryptoProvider))
	mainCmd.AddCommand(snapshot.Cmd(cryptoProvider))

	// 执行主命令
	if mainCmd.Execute() != nil {
		// 失败时，会打印使用消息和错误字符串，因此我们仅需要以非 0 状态退出
		os.Exit(1)
	}
}

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
