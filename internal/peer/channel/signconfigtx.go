/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"io/ioutil"

	"github.com/hyperledger/fabric/protoutil"
	"github.com/spf13/cobra"
)

func signconfigtxCmd(cf *ChannelCmdFactory) *cobra.Command {
	signconfigtxCmd := &cobra.Command{
		Use:   "signconfigtx",
		Short: "签名 configtx 更新.",
		Long:  "在文件系统上对提供的 configtx 更新文件进行签名. 需要 '--file'.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return sign(cmd, args, cf)
		},
	}
	flagList := []string{
		"file",
	}
	attachFlags(signconfigtxCmd, flagList)

	return signconfigtxCmd
}

// sign 方法用于对通道配置交易进行签名。
// 方法接收者：无，是一个独立的函数。
// 输入参数：
//   - cmd *cobra.Command，表示命令对象。
//   - args []string，表示命令行参数。
//   - cf *ChannelCmdFactory，表示通道命令工厂对象。
//
// 返回值：
//   - error，表示签名过程中的错误，如果签名成功则返回nil。
func sign(cmd *cobra.Command, args []string, cf *ChannelCmdFactory) error {
	// 检查是否提供了配置交易文件名
	if channelTxFile == "" {
		return InvalidCreateTx("未提供 configtx 文件名. 使用 '--file'")
	}
	// 解析命令行参数完成后，禁用 cmd 的使用说明
	cmd.SilenceUsage = true

	var err error
	// 如果通道命令工厂对象为nil，则初始化一个通道命令工厂对象
	if cf == nil {
		cf, err = InitCmdFactory(EndorserNotRequired, PeerDeliverNotRequired, OrdererNotRequired)
		if err != nil {
			return err
		}
	}

	logger.Infof("读取签名文件 [%s]", channelTxFile)

	// 读取配置交易文件的内容
	fileData, err := ioutil.ReadFile(channelTxFile)
	if err != nil {
		return ConfigTxFileNotFound(err.Error())
	}

	// 反序列化配置交易文件内容为 Envelope 对象
	ctxEnv, err := protoutil.UnmarshalEnvelope(fileData)
	if err != nil {
		return err
	}

	// 对配置交易进行检查和签名
	sCtxEnv, err := sanityCheckAndSignConfigTx(ctxEnv, cf.Signer)
	if err != nil {
		return err
	}

	// 序列化签名后的配置交易为字节数据
	sCtxEnvData := protoutil.MarshalOrPanic(sCtxEnv)

	// 将签名后的配置交易写入文件
	err = ioutil.WriteFile(channelTxFile, sCtxEnvData, 0o660)

	logger.Infof("签名完成写入原文件 [%s]", channelTxFile)

	return err

}
