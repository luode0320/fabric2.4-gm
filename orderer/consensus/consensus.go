/*
版权所有 IBM Corp. 2017保留所有权利。

SPDX-License-Identifier: Apache-2.0
*/

package consensus // 包含共识相关接口和类型定义

import (
	// 引入必要的依赖库
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/orderer/common/blockcutter"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/protoutil"
)

// Consenter 定义了底层排序机制的接口。
type Consenter interface {
	// HandleChain 应基于给定的资源集合创建并返回链的引用。
	// 对于每个进程中的特定链，此方法只会被调用一次。通常情况下，错误会被视为不可恢复的，并导致系统关闭。
	// 有关更多详细信息，请参阅Chain的描述。
	// HandleChain的第二个参数是指向ORDERER槽中上一个提交到此链账本的区块的元数据指针。
	// 对于新链或迁移后的链，此元数据将为nil（或包含零长度的Value），因为没有先前的元数据可报告。
	HandleChain(support ConsenterSupport, metadata *cb.Metadata) (Chain, error)
}

// ClusterConsenter 定义由集群类型共识者实现的方法。
type ClusterConsenter interface {
	// IsChannelMember 检查加入区块并检测它是否表明此排序器是通道成员。
	// 如果排序器属于共识者集合，则返回true；否则返回false。此方法还会检查共识类型元数据的有效性。
	// 如果因处理区块错误而无法确定成员资格，则返回错误。
	IsChannelMember(joinBlock *cb.Block) (bool, error)
	// RemoveInactiveChainRegistry 停止并移除不活动的链注册表。
	// 这在删除系统通道时使用。
	RemoveInactiveChainRegistry()
}

// MetadataValidator 定义了对通道配置更新期间ConsensusMetadata更新的验证操作。
// 注意：我们期望Consenter实现可选择性地实现MetadataValidator接口。
// 如果Consenter未实现MetadataValidator，我们将默认使用一个无操作的MetadataValidator。
type MetadataValidator interface {
	// ValidateConsensusMetadata 在通道上的配置更新期间决定ConsensusMetadata更新的有效性。
	// 由于ConsensusMetadata特定于共识实现（独立于特定链），此验证也需要由特定的共识实现来执行。
	ValidateConsensusMetadata(oldOrdererConfig, newOrdererConfig channelconfig.Orderer, newChannel bool) error
}

// Chain 定义了一种注入消息进行排序的方式。
// 注意，为了允许实现的灵活性，由实现者负责将已排序的消息通过HandleChain提供的blockcutter.Receiver进行切割成块，
// 并最终写入也是由HandleChain提供的账本。这设计支持两种主要流程：
// 1. 消息按顺序进入流，流被切割成块，块被提交（例如solo、kafka）
// 2. 消息被切割成块，块被排序，然后块被提交（例如sbft）
type Chain interface {
	// Order 接受在给定configSeq下处理的消息。
	// 如果configSeq前进，共识者有责任重新验证并可能丢弃消息。
	// 共识者可能会返回错误，表示消息未被接受。
	Order(env *cb.Envelope, configSeq uint64) error

	// Configure 接受重新配置通道的消息，并会在提交时触发configSeq的更新。
	// 配置必须由ConfigUpdate消息触发。如果config序列前进，共识者有责任重新计算结果配置，
	// 如果重新配置不再有效，则丢弃消息。共识者可能会返回错误，表示消息未被接受。
	Configure(config *cb.Envelope, configSeq uint64) error

	// WaitReady 阻塞等待共识者准备好接受新消息。
	// 当共识者需要暂时阻止入口消息以便消耗在途消息时，这很有用。如果不需要此阻塞行为，共识者可以简单地返回nil。
	WaitReady() error

	// Errored 返回一个通道，当发生错误时将关闭。
	// 这对于必须在共识者未及时更新时终止等待客户端的Deliver客户端尤其有用。
	Errored() <-chan struct{}

	// Start 应分配资源以保持与链的最新状态。
	// 通常，这涉及创建一个线程，从排序源读取消息，将这些消息传递给区块切割器，并将产生的区块写入账本。
	Start()

	// Halt 释放为此Chain分配的资源。
	Halt()
}

// ConsenterSupport 提供给Consenter实现可用的资源接口。
type ConsenterSupport interface {
	identity.SignerSerializer // 签名和序列化接口
	msgprocessor.Processor    // 消息处理器接口

	// VerifyBlockSignature 验证带有可选配置（可为nil）的区块签名。
	VerifyBlockSignature([]*protoutil.SignedData, *cb.ConfigEnvelope) error

	// BlockCutter 返回此通道的区块切割助手。
	BlockCutter() blockcutter.Receiver

	// SharedConfig 提供来自通道当前配置块的共享配置。
	SharedConfig() channelconfig.Orderer

	// ChannelConfig 提供来自通道当前配置块的通道配置。
	ChannelConfig() channelconfig.Channel

	// CreateNextBlock 根据账本中最高块号的区块创建下一个区块。
	// 注意，在第二次调用此方法之前，必须先调用WriteBlock或WriteConfigBlock。
	CreateNextBlock(messages []*cb.Envelope) *cb.Block

	// Block 返回具有给定编号的区块，如果不存在这样的区块则返回nil。
	Block(number uint64) *cb.Block

	// WriteBlock 将区块提交到账本。
	WriteBlock(block *cb.Block, encodedMetadataValue []byte)

	// WriteConfigBlock 将区块提交到账本，并应用其中的配置更新。
	WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte)

	// Sequence 返回当前的配置序列号。
	Sequence() uint64

	// ChannelID 返回与此支持关联的通道ID。
	ChannelID() string

	// Height 返回与此支持关联的链中的区块数量。
	Height() uint64

	// Append 以原始形式将新块追加到账本中，
	// 与同时修改其元数据的WriteBlock不同。
	Append(block *cb.Block) error
}

// NoOpMetadataValidator 实现了一个始终不考虑输入而返回nil错误的MetadataValidator。
type NoOpMetadataValidator struct{}

// ValidateConsensusMetadata 在通道配置更新期间确定ConsensusMetadata更新的有效性，
// 对于NoOpMetadataValidator实现，它始终返回nil错误。
func (n NoOpMetadataValidator) ValidateConsensusMetadata(oldChannelConfig, newChannelConfig channelconfig.Orderer, newChannel bool) error {
	return nil
}
