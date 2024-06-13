/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/spf13/cobra"
)

// updateCmd 函数用于创建一个名为 "update" 的 Cobra 命令，该命令用于发送配置更新文件到通道。
// 方法接收者：无。
// 输入参数：
//   - cf *ChannelCmdFactory，表示 ChannelCmdFactory 对象的指针。
//
// 返回值：
//   - *cobra.Command，表示创建的 Cobra 命令。
func updateCmd(cf *ChannelCmdFactory) *cobra.Command {
	// 创建 "update" 命令
	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "发送 configtx 更新.",
		Long:  "签名并将提供的 configtx 更新文件发送到通道. 需要 '-f', '-o', '-c'.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return update(cmd, args, cf)
		},
	}

	// 定义需要附加到 "update" 命令的标志列表
	flagList := []string{
		"channelID",
		"file",
	}

	// 将标志列表附加到 "update" 命令
	attachFlags(updateCmd, flagList)

	return updateCmd
}

// update 函数用于执行通道的配置更新操作。
// 方法接收者：无。
// 输入参数：
//   - cmd *cobra.Command，表示执行的Cobra命令。
//   - args []string，表示命令的参数。
//   - cf *ChannelCmdFactory，表示ChannelCmdFactory对象的指针。
//
// 返回值：
//   - error，表示执行过程中的错误，如果没有错误则返回nil。
func update(cmd *cobra.Command, args []string, cf *ChannelCmdFactory) error {
	// 检查是否提供了通道ID
	if channelID == common.UndefinedParamValue {
		return errors.New("必须提供 --channelID 通道ID")
	}

	// 检查是否提供了配置更新文件名
	if channelTxFile == "" {
		return InvalidCreateTx("未提供 --file 通道提交更新文件名")
	}

	// 静默使用命令行参数，不显示命令的使用说明
	cmd.SilenceUsage = true

	var err error
	if cf == nil {
		// 初始化ChannelCmdFactory对象
		cf, err = InitCmdFactory(EndorserNotRequired, PeerDeliverNotRequired, OrdererNotRequired)
		if err != nil {
			return err
		}
	}

	// 读取配置更新文件的内容
	channelTxFile = filepath.Join(os.Getenv("FABRIC_CFG_PATH"), channelTxFile)
	fileData, err := ioutil.ReadFile(channelTxFile)
	if err != nil {
		return ConfigTxFileNotFound(err.Error())
	}

	// 解析配置更新文件为Envelope对象
	ctxEnv, err := protoutil.UnmarshalEnvelope(fileData)
	if err != nil {
		return err
	}

	// 对配置更新进行检查和签名
	sCtxEnv, err := sanityCheckAndSignConfigTx(ctxEnv, cf.Signer)
	if err != nil {
		return err
	}

	// 获取广播客户端
	var broadcastClient common.BroadcastClient
	broadcastClient, err = cf.BroadcastFactory()
	if err != nil {
		return fmt.Errorf("获取 orderer 广播客户端时出错: %s", err)
	}

	defer broadcastClient.Close()

	// 发送配置更新到通道
	err = broadcastClient.Send(sCtxEnv)
	if err != nil {
		return err
	}

	logger.Info("已成功提交通道更新")
	return nil
}
