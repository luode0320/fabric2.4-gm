/*
Copyright IBM Corp. 2016-2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package common

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	OrderingEndpoint           string        // 排序节点的终端点
	tlsEnabled                 bool          // 是否启用TLS
	clientAuth                 bool          // 是否启用客户端认证
	caFile                     string        // CA证书文件
	keyFile                    string        // 私钥文件
	certFile                   string        // 证书文件
	ordererTLSHostnameOverride string        // 排序节点的TLS主机名覆盖
	connTimeout                time.Duration // 连接超时时间
	tlsHandshakeTimeShift      time.Duration // TLS握手时间偏移量
)

// SetOrdererEnv 向全局Viper环境添加特定于orderer的设置。
// 方法接收者：无
// 输入参数：
//   - cmd：命令对象。
//   - args：命令行参数。
func SetOrdererEnv(cmd *cobra.Command, args []string) {
	// 读取旧的日志记录级别设置，如果设置，
	// 通知用户 FABRIC_LOGGING_SPEC env变量
	var loggingLevel string
	if viper.GetString("logging_level") != "" {
		loggingLevel = viper.GetString("logging_level")
	} else {
		loggingLevel = viper.GetString("logging.level")
	}
	if loggingLevel != "" {
		mainLogger.Warning("不再支持 CORE_LOGGING_LEVEL , 请使用 FABRIC_LOGGING_SPEC 环境变量")
	}

	// 配置tls根证书
	if !viper.IsSet("orderer.tls.rootcert.file") {
		viper.Set("orderer.tls.rootcert.file", caFile)
	}
	// 配置tls签名证书密钥
	if !viper.IsSet("orderer.tls.clientKey.file") {
		viper.Set("orderer.tls.clientKey.file", keyFile)
	}
	// 配置tls签名证书
	if !viper.IsSet("orderer.tls.clientCert.file") {
		viper.Set("orderer.tls.clientCert.file", certFile)
	}
	// 配置orderer地址
	if !viper.IsSet("orderer.address") {
		viper.Set("orderer.address", OrderingEndpoint)
	}
	// 配置TLS主机名覆盖
	if !viper.IsSet("orderer.tls.serverhostoverride") {
		viper.Set("orderer.tls.serverhostoverride", ordererTLSHostnameOverride)
	}
	// 是否开启tls
	if !viper.IsSet("orderer.tls.enabled") {
		viper.Set("orderer.tls.enabled", tlsEnabled)
	}
	// 是否开始客户端验证
	if !viper.IsSet("orderer.tls.clientAuthRequired") {
		viper.Set("orderer.tls.clientAuthRequired", clientAuth)
	}
	// 超时时间
	if !viper.IsSet("orderer.client.connTimeout") {
		viper.Set("orderer.client.connTimeout", connTimeout)
	}
	// 握手时间偏移量
	if !viper.IsSet("orderer.tls.handshakeTimeShift") {
		viper.Set("orderer.tls.handshakeTimeShift", tlsHandshakeTimeShift)
	}
}

// AddOrdererFlags 为与orderer相关的命令添加标志
func AddOrdererFlags(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()

	flags.StringVarP(&OrderingEndpoint, "orderer", "o", "",
		"orderer 服务节点")
	flags.BoolVarP(&tlsEnabled, "tls", "", false,
		"与 orderer 节点通信时是否开启TLS")
	flags.BoolVarP(&clientAuth, "clientauth", "", false,
		"与 orderer 节点通信时是否开启客户端TLS")
	flags.StringVarP(&caFile, "cafile", "", "",
		"包含 orderer 节点的PEM编码的 tls 根证书的文件路径")
	flags.StringVarP(&keyFile, "keyfile", "", "",
		"包含用于与 orderer 节点进行 TLS 通信的PEM编码私钥文件的路径")
	flags.StringVarP(&certFile, "certfile", "", "",
		"包含用于与 orderer 节点进行 TLS 通信的PEM编码的证书的路径")
	flags.StringVarP(&ordererTLSHostnameOverride, "ordererTLSHostnameOverride", "", "",
		"验证与 orderer 的TLS连接时要使用的覆盖主机名")
	flags.DurationVarP(&connTimeout, "connTimeout", "", 3*time.Second,
		"客户端连接超时")
	flags.DurationVarP(&tlsHandshakeTimeShift, "tlsHandshakeTimeShift", "", 0,
		"在与 orderer 节点进行 TLS 握手期间, 向后移动以进行证书过期检查的时间偏移量")
}
