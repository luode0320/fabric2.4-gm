/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"strings"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var logger = flogging.MustGetLogger("channelCmd")

const (
	EndorserRequired       bool = true
	EndorserNotRequired    bool = false
	OrdererRequired        bool = true
	OrdererNotRequired     bool = false
	PeerDeliverRequired    bool = true
	PeerDeliverNotRequired bool = false
)

var (
	genesisBlockPath string        // 创世区块路径
	snapshotPath     string        // 快照路径
	channelID        string        // 通道ID
	channelTxFile    string        // 通道交易文件
	outputBlock      string        // 输出区块文件
	timeout          time.Duration // 超时时间
	bestEffort       bool          // 尽力而为
)

// Cmd returns the cobra command for Node
func Cmd(cf *ChannelCmdFactory) *cobra.Command {
	AddFlags(channelCmd)

	channelCmd.AddCommand(createCmd(cf))
	channelCmd.AddCommand(fetchCmd(cf))
	channelCmd.AddCommand(joinCmd(cf))
	channelCmd.AddCommand(joinBySnapshotCmd(cf))
	channelCmd.AddCommand(joinBySnapshotStatusCmd(cf))
	channelCmd.AddCommand(listCmd(cf))
	channelCmd.AddCommand(updateCmd(cf))
	channelCmd.AddCommand(signconfigtxCmd(cf))
	channelCmd.AddCommand(getinfoCmd(cf))

	return channelCmd
}

// AddFlags 为创建和加入添加标志
func AddFlags(cmd *cobra.Command) {
	common.AddOrdererFlags(cmd)
}

var flags *pflag.FlagSet

func init() {
	resetFlags()
}

// 显式定义方法以方便测试
func resetFlags() {
	flags = &pflag.FlagSet{}

	flags.StringVarP(&genesisBlockPath, "blockpath", "b", common.UndefinedParamValue, "包含创世块的文件的路径")
	flags.StringVarP(&snapshotPath, "snapshotpath", "", common.UndefinedParamValue, "快照目录的路径")
	flags.StringVarP(&channelID, "channelID", "c", common.UndefinedParamValue, "在newChain命令的情况下，要创建的通道ID。它必须全部为小写，长度小于250个字符，并与正则表达式匹配: [a-z][a-z0-9.-]*")
	flags.StringVarP(&channelTxFile, "file", "f", "", "configtxgen等工具生成的用于提交给orderer的配置事务文件")
	flags.StringVarP(&outputBlock, "outputBlock", "", common.UndefinedParamValue, `写入通道的创世块的路径. (default ./<channelID>.block)`)
	flags.DurationVarP(&timeout, "timeout", "t", 10*time.Second, "通道创建超时时间")
	flags.BoolVarP(&bestEffort, "bestEffort", "", false, "请求是否应忽略错误并尽最大努力返回块")
}

// attachFlags 用于将标志附加到命令对象。
// 方法接收者：无（函数）
// 输入参数：
//   - cmd：要附加标志的命令对象。
//   - names：要附加的标志名称列表。
func attachFlags(cmd *cobra.Command, names []string) {
	// 获取命令对象的标志集合
	cmdFlags := cmd.Flags()
	// 遍历要附加的标志名称列表
	for _, name := range names {
		// 查找标志对象
		if flag := flags.Lookup(name); flag != nil {
			// 将标志对象添加到命令对象的标志集合中
			cmdFlags.AddFlag(flag)
		} else {
			// 如果找不到标志对象，则输出错误信息
			logger.Fatalf("找不到要附加到命令 '%s' 的标志 '%s'", name, cmd.Name())
		}
	}
}

var channelCmd = &cobra.Command{
	Use:   "channel",
	Short: "操作一个频道: create|fetch|join|joinbysnapshot|joinbysnapshotstatus|list|update|signconfigtx|getinfo.",
	Long:  "操作一个频道: create|fetch|join|joinbysnapshot|joinbysnapshotstatus|list|update|signconfigtx|getinfo.",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		common.InitCmd(cmd, args)
		common.SetOrdererEnv(cmd, args)
	},
}

type BroadcastClientFactory func() (common.BroadcastClient, error)

type deliverClientIntf interface {
	GetSpecifiedBlock(num uint64) (*cb.Block, error)
	GetOldestBlock() (*cb.Block, error)
	GetNewestBlock() (*cb.Block, error)
	Close() error
}

// ChannelCmdFactory holds the clients used by ChannelCmdFactory
type ChannelCmdFactory struct {
	EndorserClient   pb.EndorserClient
	Signer           msp.SigningIdentity
	BroadcastClient  common.BroadcastClient
	DeliverClient    deliverClientIntf
	BroadcastFactory BroadcastClientFactory
}

// InitCmdFactory 根据参数，初始化 背书/提交/排序 节点连接
func InitCmdFactory(isEndorserRequired, isPeerDeliverRequired, isOrdererRequired bool) (*ChannelCmdFactory, error) {
	if isPeerDeliverRequired && isOrdererRequired {
		// 这可能是在开发过程中添加新cmd导致的错误
		return nil, errors.New("错误-当前仅支持单个传递源")
	}

	var err error
	cf := &ChannelCmdFactory{}

	// 获取签名证书的签名实例
	cf.Signer, err = common.GetDefaultSignerFnc()
	if err != nil {
		return nil, errors.WithMessage(err, "获取默认 bccsp 签名者时出错")
	}

	cf.BroadcastFactory = func() (common.BroadcastClient, error) {
		return common.GetBroadcastClientFnc()
	}

	// 是否需要背书节点支持
	if isEndorserRequired {
		// 通过创建EndorserClient对象，可以与背书节点进行背书服务的通信。这通常用于发送交易并获取背书结果。
		cf.EndorserClient, err = common.GetEndorserClientFnc(common.UndefinedParamValue, common.UndefinedParamValue)
		if err != nil {
			return nil, errors.WithMessage(err, "获取通道的背书节点连接时出错")
		}
	}

	// 是否需要提交节点支持
	if isPeerDeliverRequired {
		// 通过创建DeliverClient对象，可以与对等节点进行交付服务的通信。这通常用于接收区块或交易等数据。
		cf.DeliverClient, err = common.NewDeliverClientForPeer(channelID, cf.Signer, bestEffort)
		if err != nil {
			return nil, errors.WithMessage(err, "获取通道的提交节点连接时出错")
		}
	}

	// 是否需要排序节点支持
	if isOrdererRequired {
		if len(strings.Split(common.OrderingEndpoint, ":")) != 2 {
			return nil, errors.Errorf("排序服务节点 %s 无效或丢失", common.OrderingEndpoint)
		}

		// 通过创建DeliverClient对象，可以与排序服务进行通信，例如获取区块或提交交易。
		cf.DeliverClient, err = common.NewDeliverClientForOrderer(channelID, cf.Signer, bestEffort)
		if err != nil {
			return nil, err
		}
	}

	logger.Infof("初始化 背书/提交/排序 节点连接")
	return cf, nil
}
