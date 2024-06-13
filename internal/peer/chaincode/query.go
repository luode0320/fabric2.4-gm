/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/spf13/cobra"
)

var chaincodeQueryCmd *cobra.Command

// queryCmd 返回Chaincode查询的cobra命令
func queryCmd(cf *ChaincodeCmdFactory, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeQueryCmd = &cobra.Command{
		Use:       "query",
		Short:     fmt.Sprintf("使用指定的查询 %s.", chainFuncName),
		Long:      fmt.Sprintf("获取 %s 函数调用的背书结果并打印. 它不会生成事务.", chainFuncName),
		ValidArgs: []string{"1"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return chaincodeQuery(cmd, cf, cryptoProvider)
		},
	}
	flagList := []string{
		"ctor",
		"name",
		"channelID",
		"peerAddresses",
		"tlsRootCertFiles",
		"connectionProfile",
	}
	attachFlags(chaincodeQueryCmd, flagList)

	chaincodeQueryCmd.Flags().BoolVarP(&chaincodeQueryRaw, "raw", "r", false,
		"If true, output the query value as raw bytes, otherwise format as a printable string")
	chaincodeQueryCmd.Flags().BoolVarP(&chaincodeQueryHex, "hex", "x", false,
		"If true, output the query value byte array in hexadecimal. Incompatible with --raw")

	return chaincodeQueryCmd
}

func chaincodeQuery(cmd *cobra.Command, cf *ChaincodeCmdFactory, cryptoProvider bccsp.BCCSP) error {
	if channelID == "" {
		return errors.New("必需的参数 '--channelID' 为空. 请使用--channelID 手动添加通道名称")
	}
	// 命令行的解析完成，所以沉默cmd的使用
	cmd.SilenceUsage = true

	var err error
	if cf == nil {
		// 方法用于使用默认客户端启动 ChaincodeCmdFactory
		cf, err = InitCmdFactory(cmd.Name(), true, false, cryptoProvider)
		if err != nil {
			return err
		}
	}

	// 方法用于调用或查询链码
	return chaincodeInvokeOrQuery(cmd, false, cf)
}
