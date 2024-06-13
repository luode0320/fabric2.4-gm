/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"bytes"
	"github.com/hyperledger/fabric/core/config"
	"github.com/spf13/viper"
	"path/filepath"
	"runtime"
	//"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	gmx509 "github.com/Hyperledger-TWGC/tjfoc-gm/x509"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	bccspcommon "github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/internal/osnadmin"
	"github.com/hyperledger/fabric/protoutil"
	"gopkg.in/alecthomas/kingpin.v2"
)

// peer配置文件目录
var peerLinuxConfig = "sampleconfig"
var peerWindowsConfig = "samplewindowsconfig"

var orderer *string
var caFile *string
var clientCert *string
var clientKey *string

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

func main() {
	kingpin.Version("0.0.1")

	output, exit, err := executeForArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("解析参数: %s. Try --help", err)
	}
	fmt.Println(output)
	os.Exit(exit)
}

func executeForArgs(args []string) (output string, exit int, err error) {
	//
	// 相关命令行标签
	//
	app := kingpin.New("osnadmin", "全称：Orderer Service Node (OSN) administration. orderer 服务节点管理")

	// 现在设置配置文件的名称。
	appNameWithoutExt := config.GetConfig()
	// 如果用户使用与启动文件相同的名称, 并以yaml为文件后缀的配置, 我们优先考虑这个配置
	v := viper.New()
	config.AddLocalViperConfigPaths(v)
	v.SetConfigName(appNameWithoutExt)
	if err := v.ReadInConfig(); err == nil {
		fmt.Printf("使用配置文件: %s \n", appNameWithoutExt)
		path := v.GetString("orderer.address")
		orderer = &path
		cafile := config.GetLocalPath(v, "orderer.tls.rootcert.file")
		caFile = &cafile
		clientfile := config.GetLocalPath(v, "orderer.tls.cert.file")
		clientCert = &clientfile
		keyfile := config.GetLocalPath(v, "orderer.tls.key.file")
		clientKey = &keyfile
	} else {
		fmt.Printf("使用命令行参数 \n")
		orderer = app.Flag("orderer-address", "OSN的管理端点").Short('o').Required().String()
		caFile = app.Flag("ca-file", "tls的根证书").String()
		clientCert = app.Flag("client-cert", "tls签名证书").String()
		clientKey = app.Flag("client-key", "tls签名证书私钥").String()
	}

	noStatus := app.Flag("no-status", "将命令回显中的HTTP状态信息删除").Default("false").Bool()
	channel := app.Command("channel", "通道相关的操作")

	join := channel.Command("join", "将OSN(orderer服务节点)加入通道。如果通道还不存在，将创建它。")
	joinChannelID := join.Flag("channelID", "通道ID").Short('c').Required().String()
	configBlockPath := join.Flag("config-block", "包含通道的最新配置块的文件的路径").Short('b').Required().String()

	list := channel.Command("list", "列出OSN (Ordering Service Node)的通道信息。如果设置了channelID标志，将为该通道提供更详细的信息.")
	listChannelID := list.Flag("channelID", "通道ID").Short('c').String()

	remove := channel.Command("remove", "将orderer服务节点(OSN)从通道中移除")
	removeChannelID := remove.Flag("channelID", "通道ID").Short('c').Required().String()

	command, err := app.Parse(args)
	if err != nil {
		return "", 1, err
	}

	//
	// 标签验证
	//
	var (
		osnURL          string
		gmCaCertPool    *gmx509.CertPool
		gmTlsClientCert gmtls.Certificate
		x509CaCertPool  *x509.CertPool
	)
	// 启用TLS
	if *caFile != "" {
		osnURL = fmt.Sprintf("https://%s", *orderer)
		var err error
		gmCaCertPool = gmx509.NewCertPool()
		caFilePEM, err := ioutil.ReadFile(*caFile)
		if err != nil {
			return "", 1, fmt.Errorf("reading orderer CA certificate: %s", err)
		}

		//_, ok := bccspcommon.AddGMPEMCerts2X509CertPool(caFilePEM, caCertPool)
		//if !ok {
		//	return "", 1, fmt.Errorf("failed to add ca-file PEM to cert pool")
		//}
		if !gmCaCertPool.AppendCertsFromPEM(caFilePEM) {
			return "", 1, fmt.Errorf("failed to add ca-file PEM to cert pool")
		}

		// 通过common.LoadX509KeyPair 将国密证书 转成tls证书
		gmTlsClientCert, err = gmtls.LoadX509KeyPair(*clientCert, *clientKey)
		if err != nil {
			return "", 1, fmt.Errorf("loading client cert/key pair: %s", err)
		}

		x509CaCertPool = x509.NewCertPool()
		x509CaCertPool, _ = bccspcommon.AddGMPEMCerts2X509CertPool(caFilePEM, x509CaCertPool)
	} else { // 禁用 TLS
		osnURL = fmt.Sprintf("http://%s", *orderer)
	}

	var marshaledConfigBlock []byte
	if *configBlockPath != "" {
		configBlockPath := filepath.Join(os.Getenv("FABRIC_CFG_PATH"), *configBlockPath)
		marshaledConfigBlock, err = ioutil.ReadFile(configBlockPath)
		if err != nil {
			return "", 1, fmt.Errorf("reading config block: %s", err)
		}

		err = validateBlockChannelID(marshaledConfigBlock, *joinChannelID)
		if err != nil {
			return "", 1, err
		}
	}

	//
	// 调用底层的实现
	//
	var resp *http.Response

	switch command {
	case join.FullCommand():
		resp, err = osnadmin.Join(osnURL, *orderer, marshaledConfigBlock, gmCaCertPool, gmTlsClientCert)
	case list.FullCommand():
		if *listChannelID != "" {
			resp, err = osnadmin.ListSingleChannel(osnURL, *orderer, *listChannelID, gmCaCertPool, gmTlsClientCert)
			break
		}
		resp, err = osnadmin.ListAllChannels(osnURL, *orderer, gmCaCertPool, gmTlsClientCert)
	case remove.FullCommand():
		resp, err = osnadmin.Remove(osnURL, *removeChannelID, *orderer, gmCaCertPool, gmTlsClientCert)
	}
	if err != nil {
		return errorOutput(err), 1, nil
	}

	bodyBytes, err := readBodyBytes(resp.Body)
	if err != nil {
		return errorOutput(err), 1, nil
	}

	output, err = responseOutput(!*noStatus, resp.StatusCode, bodyBytes)
	if err != nil {
		return errorOutput(err), 1, nil
	}

	return output, 0, nil
}

func responseOutput(showStatus bool, statusCode int, responseBody []byte) (string, error) {
	var buffer bytes.Buffer
	if showStatus {
		fmt.Fprintf(&buffer, "Status: %d\n", statusCode)
	}
	if len(responseBody) != 0 {
		if err := json.Indent(&buffer, responseBody, "", "\t"); err != nil {
			return "", err
		}
	}
	return buffer.String(), nil
}

func readBodyBytes(body io.ReadCloser) ([]byte, error) {
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading http response body: %s", err)
	}
	body.Close()

	return bodyBytes, nil
}

func errorOutput(err error) string {
	return fmt.Sprintf("Error: %s\n", err)
}

// 校验配置块的通道ID
func validateBlockChannelID(blockBytes []byte, channelID string) error {
	block := &common.Block{}
	err := proto.Unmarshal(blockBytes, block)
	if err != nil {
		return fmt.Errorf("unmarshalling block: %s", err)
	}

	blockChannelID, err := protoutil.GetChannelIDFromBlock(block)
	if err != nil {
		return err
	}

	// 根据传入的通道ID和解析出来的通道ID判断这个通道是否是想要加入的通道
	if channelID != blockChannelID {
		return fmt.Errorf("specified --channelID %s does not match channel ID %s in config block", channelID, blockChannelID)
	}

	return nil
}
