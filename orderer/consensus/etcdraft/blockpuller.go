/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"encoding/pem"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/pkg/errors"
)

// LedgerBlockPuller pulls blocks upon demand, or fetches them from the ledger
type LedgerBlockPuller struct {
	BlockPuller
	BlockRetriever cluster.BlockRetriever
	Height         func() uint64
}

func (lp *LedgerBlockPuller) PullBlock(seq uint64) *common.Block {
	lastSeq := lp.Height() - 1
	if lastSeq >= seq {
		return lp.BlockRetriever.Block(seq)
	}
	return lp.BlockPuller.PullBlock(seq)
}

// EndpointconfigFromSupport 方法用于从共识支持对象中提取 TLS CA 证书和端点配置信息。
// 参数：
// support consensus.ConsenterSupport // 共识层支持接口，用于获取最新的配置区块
// bccsp bccsp.BCCSP // BCCSP (Blockchain Cryptographic Service Provider)，用于加密和解密操作
// 功能描述：
// 从共识支持对象中获取最新的配置区块。
// 使用获取的最新配置区块和 BCCSP 对象来解析和提取 TLS CA 证书和端点配置信息。
// 返回提取的端点配置信息列表和任何可能发生的错误。
func EndpointconfigFromSupport(support consensus.ConsenterSupport, bccsp bccsp.BCCSP) ([]cluster.EndpointCriteria, error) {
	// 从共识支持对象中获取最新的配置区块
	lastConfigBlock, err := lastConfigBlockFromSupport(support)
	if err != nil {
		return nil, err
	}

	// 使用获取的最新配置区块和 BCCSP 对象来解析和提取 TLS CA 证书和端点配置信息
	endpointconf, err := cluster.EndpointconfigFromConfigBlock(lastConfigBlock, bccsp)
	if err != nil {
		return nil, err
	}

	// 返回提取的端点配置信息列表
	return endpointconf, nil
}

// lastConfigBlockFromSupport 方法用于从共识层支持接口中获取最新的配置区块。
// 参数：
// support consensus.ConsenterSupport // 共识层支持接口，提供了对共识层操作的支持，如获取区块高度和区块数据等
// 功能描述：
// 获取共识层支持接口中的当前区块高度，减去1以获取上一个区块的序号。
// 使用上一个区块的序号从共识层支持接口中获取对应的区块数据。
// 如果获取的区块数据为 nil，则返回错误，表明无法获取指定序号的区块。
// 使用获取的区块数据和共识层支持接口来查找并返回最新的配置区块。
func lastConfigBlockFromSupport(support consensus.ConsenterSupport) (*common.Block, error) {
	// 获取共识层支持接口中的当前区块高度，减去1以获取上一个区块的序号
	lastBlockSeq := support.Height() - 1

	// 使用上一个区块的序号从共识层支持接口中获取对应的区块数据
	lastBlock := support.Block(lastBlockSeq)
	if lastBlock == nil {
		// 如果获取的区块数据为 nil，则返回错误，表明无法获取指定序号的区块
		return nil, errors.Errorf("无法检索块 [%d]", lastBlockSeq)
	}

	// 使用获取的区块数据和共识层支持接口来查找并返回最新的配置区块
	lastConfigBlock, err := cluster.LastConfigBlock(lastBlock, support)
	if err != nil {
		return nil, err
	}

	// 返回最新的配置区块
	return lastConfigBlock, nil
}

// NewBlockPuller 方法用于创建一个新的区块拉取器（Block Puller）实例。
// 参数：
// support consensus.ConsenterSupport // 提供共识相关支持的接口
// baseDialer *cluster.PredicateDialer // 基础的条件拨号器
// clusterConfig localconfig.Cluster // 集群配置
// bccsp bccsp.BCCSP // BCCSP (Blockchain Cryptographic Service Provider) 加密服务提供者
// 功能描述：
// 定义一个区块序列验证函数，用于验证拉取的区块序列。
// 创建一个标准拨号器，用于与远程排序节点建立连接。
// 从配置中提取 TLS CA 证书和端点信息。
// 解码拨号器配置中的 TLS 证书。
// 创建一个 BlockPuller 实例，初始化其属性，包括区块序列验证函数、日志记录器、重试超时时间、最大缓冲字节数、拉取超时时间、端点列表、签名者、TLS 证书、通道 ID、拨号器和停止通道。
// 返回一个 LedgerBlockPuller 实例，它封装了高度、区块检索器和 BlockPuller 实例。
func NewBlockPuller(support consensus.ConsenterSupport,
	baseDialer *cluster.PredicateDialer,
	clusterConfig localconfig.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	// 定义一个区块序列验证函数，用于验证拉取的区块序列
	verifyBlockSequence := func(blocks []*common.Block, _ string) error {
		return cluster.VerifyBlocks(blocks, support)
	}

	// 创建一个标准拨号器，用于与远程排序节点建立连接
	stdDialer := &cluster.StandardDialer{
		Config: baseDialer.Config,
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// 从配置中提取 TLS CA 证书和端点信息
	endpoints, err := EndpointconfigFromSupport(support, bccsp)
	if err != nil {
		return nil, err
	}

	// 解码拨号器配置中的 TLS 证书
	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("客户端证书不是PEM格式: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	// 创建并初始化 BlockPuller 实例，用于从远程节点拉取区块数据
	bp := &cluster.BlockPuller{
		// 设置区块序列验证函数
		VerifyBlockSequence: verifyBlockSequence,
		// 设置日志记录器，用于记录 puller 相关的日志信息，其中包含了通道 ID
		Logger: flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", support.ChannelID()),
		// 设置重试超时时间，当拉取区块失败时，等待一段时间后重新尝试
		RetryTimeout: clusterConfig.ReplicationRetryTimeout,
		// 设置最大的总缓冲字节数，用于控制拉取区块时的内存使用
		MaxTotalBufferBytes: clusterConfig.ReplicationBufferSize,
		// 设置拉取区块的超时时间
		FetchTimeout: clusterConfig.ReplicationPullTimeout,
		// 设置远程节点的连接信息，用于确定与哪些节点进行通信
		Endpoints: endpoints,
		// 设置签名者，用于在与远程节点通信时进行身份验证
		Signer: support,
		// 设置本地节点的 TLS 证书，用于在与远程节点通信时进行加密和身份验证
		TLSCert: der.Bytes,
		// 设置当前通道的 ID，用于标识通信上下文
		Channel: support.ChannelID(),
		// 设置标准的网络连接建立器，用于创建与远程节点的网络连接
		Dialer: stdDialer,
		// 设置停止通道，用于在需要时停止 puller 的运行
		StopChannel: make(chan struct{}),
	}

	// 返回一个 LedgerBlockPuller 实例，它结合了本地账本信息和远程区块拉取能力
	return &LedgerBlockPuller{
		// 设置当前账本的高度，用于判断是否需要拉取更多的区块
		Height: support.Height,
		// 设置区块检索器，用于从本地账本中检索区块
		BlockRetriever: support,
		// 设置 BlockPuller 实例，用于从远程节点拉取区块
		BlockPuller: bp,
	}, nil
}
