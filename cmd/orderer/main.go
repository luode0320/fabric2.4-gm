// 该main函数是orderer二进制文件的运行入口
package main

import (
	"fmt"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/orderer/common/server"
	"os"
	"path/filepath"
	"runtime"
)

// peer配置文件目录
var peerLinuxConfig = "sampleconfig"
var peerWindowsConfig = "samplewindowsconfig"

func main() {
	server.Main()
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
