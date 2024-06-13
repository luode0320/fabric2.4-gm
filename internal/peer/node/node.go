/*
Copyright IBM Corp. 2016 All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/spf13/cobra"
)

const (
	nodeFuncName = "node"
	nodeCmdDes   = "操作对等节点: start|reset|rollback|pause|resume|rebuild-dbs|unjoin|upgrade-dbs."
)

var logger = flogging.MustGetLogger("nodeCmd")

// Cmd 函数用于返回节点的 Cobra 命令。
// 返回值：
//   - *cobra.Command：节点的 Cobra 命令。
func Cmd() *cobra.Command {
	// 向节点命令添加子命令
	nodeCmd.AddCommand(startCmd())
	nodeCmd.AddCommand(resetCmd())
	nodeCmd.AddCommand(rollbackCmd())
	nodeCmd.AddCommand(pauseCmd())
	nodeCmd.AddCommand(resumeCmd())
	nodeCmd.AddCommand(rebuildDBsCmd())
	nodeCmd.AddCommand(unjoinCmd())
	nodeCmd.AddCommand(upgradeDBsCmd())

	return nodeCmd
}

// nodeCmd 是一个用于表示节点命令的 Cobra 命令。
var nodeCmd = &cobra.Command{
	Use:              nodeFuncName,           // 命令的使用方式
	Short:            fmt.Sprint(nodeCmdDes), // 命令的简短描述
	Long:             fmt.Sprint(nodeCmdDes), // 命令的详细描述
	PersistentPreRun: common.InitCmd,         // 在运行命令之前执行的预处理函数
}
