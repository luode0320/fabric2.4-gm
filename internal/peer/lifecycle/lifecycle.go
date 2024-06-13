/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lifecycle

import (
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/internal/peer/lifecycle/chaincode"
	"github.com/spf13/cobra"
)

// Cmd 返回生命周期的cobra命令
func Cmd(cryptoProvider bccsp.BCCSP) *cobra.Command {
	lifecycleCmd := &cobra.Command{
		Use:   "lifecycle",
		Short: "执行命令 _lifecycle 操作链码的生命周期",
		Long:  "执行命令 _lifecycle 操作链码的生命周期",
	}
	// 包装链码的命令, 可以在 lifecycle 命令之后直接跟上链码的命令如: peer lifecycle chaincode install ...
	lifecycleCmd.AddCommand(chaincode.Cmd(cryptoProvider))

	return lifecycleCmd
}
