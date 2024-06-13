/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"fmt"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var chaincodeInvokeCmd *cobra.Command

// invokeCmd 返回用于Chaincode Invoke的cobra命令
func invokeCmd(cf *ChaincodeCmdFactory, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeInvokeCmd = &cobra.Command{
		Use:       "invoke",
		Short:     fmt.Sprintf("调用指定的 %s.", chainFuncName),
		Long:      fmt.Sprintf("调用指定的 %s. 它将尝试将认可的事务提交给网络.", chainFuncName),
		ValidArgs: []string{"1"},
		RunE: func(cmd *cobra.Command, args []string) error {
			// 方法用于执行链码的调用操作
			return chaincodeInvoke(cmd, cf, cryptoProvider)
		},
	}
	flagList := []string{
		"name",
		"ctor",
		"isInit",
		"channelID",
		"peerAddresses",
		"tlsRootCertFiles",
		"connectionProfile",
		"waitForEvent",
		"waitForEventTimeout",
	}
	attachFlags(chaincodeInvokeCmd, flagList)

	return chaincodeInvokeCmd
}

// chaincodeInvoke 方法用于执行链码的调用操作。
// 方法接收者：无（全局函数）
// 输入参数：
//   - cmd：*cobra.Command 类型，表示命令对象。
//   - cf：*ChaincodeCmdFactory 类型，表示链码命令工厂。
//   - cryptoProvider：bccsp.BCCSP 类型，表示密码提供者。
//
// 返回值：
//   - error：如果执行链码调用操作时出现错误，则返回错误。
func chaincodeInvoke(cmd *cobra.Command, cf *ChaincodeCmdFactory, cryptoProvider bccsp.BCCSP) error {
	// 检查是否提供了channelID参数
	if channelID == "" {
		return errors.New("所需的参数 'channelID' 为空. Rerun the command with -C flag")
	}

	// 命令行的解析完成，所以沉默cmd的使用
	cmd.SilenceUsage = true

	var err error
	// 初始化 ChaincodeCmdFactory 链码执行工厂, 获取 背书、提交、广播、签名 实例
	if cf == nil {
		cf, err = InitCmdFactory(cmd.Name(), true, true, cryptoProvider)
		if err != nil {
			return err
		}
	}
	// 关闭广播客户端
	defer cf.BroadcastClient.Close()

	// 执行链码的调用操作
	return chaincodeInvokeOrQuery(cmd, true, cf)
}
