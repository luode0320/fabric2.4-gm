/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fileledger

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
)

var logger = flogging.MustGetLogger("common.ledger.blockledger.file")

// FileLedger is a struct used to interact with a node's ledger
type FileLedger struct {
	blockStore FileLedgerBlockStore
	signal     chan struct{}
}

// FileLedgerBlockStore 定义了当使用文件账本时，与deliver服务交互的接口规范。
type FileLedgerBlockStore interface {
	// AddBlock 用于向账本中添加一个新的区块。
	AddBlock(block *cb.Block) error

	// GetBlockchainInfo 获取区块链的当前信息，包括当前高度和最新区块的哈希。
	GetBlockchainInfo() (*cb.BlockchainInfo, error)

	// RetrieveBlocks 从指定的起始区块号开始，检索一系列区块。
	// 返回一个迭代器用于遍历这些区块。
	RetrieveBlocks(startBlockNumber uint64) (ledger.ResultsIterator, error)

	// Shutdown 关闭BlockStore，释放相关资源。
	Shutdown()

	// RetrieveBlockByNumber 根据区块编号检索单个区块。
	RetrieveBlockByNumber(blockNum uint64) (*cb.Block, error)
}

// NewFileLedger creates a new FileLedger for interaction with the ledger
func NewFileLedger(blockStore FileLedgerBlockStore) *FileLedger {
	return &FileLedger{blockStore: blockStore, signal: make(chan struct{})}
}

type fileLedgerIterator struct {
	ledger         *FileLedger
	blockNumber    uint64
	commonIterator ledger.ResultsIterator
}

// Next blocks until there is a new block available, or until Close is called.
// It returns an error if the next block is no longer retrievable.
func (i *fileLedgerIterator) Next() (*cb.Block, cb.Status) {
	result, err := i.commonIterator.Next()
	if err != nil {
		logger.Error(err)
		return nil, cb.Status_SERVICE_UNAVAILABLE
	}
	// Cover the case where another thread calls Close on the iterator.
	if result == nil {
		return nil, cb.Status_SERVICE_UNAVAILABLE
	}
	return result.(*cb.Block), cb.Status_SUCCESS
}

// Close releases resources acquired by the Iterator
func (i *fileLedgerIterator) Close() {
	i.commonIterator.Close()
}

// Iterator returns an Iterator, as specified by an ab.SeekInfo message, and its
// starting block number
func (fl *FileLedger) Iterator(startPosition *ab.SeekPosition) (blockledger.Iterator, uint64) {
	var startingBlockNumber uint64
	switch start := startPosition.Type.(type) {
	case *ab.SeekPosition_Oldest:
		startingBlockNumber = 0
	case *ab.SeekPosition_Newest:
		info, err := fl.blockStore.GetBlockchainInfo()
		if err != nil {
			logger.Panic(err)
		}
		newestBlockNumber := info.Height - 1
		if info.BootstrappingSnapshotInfo != nil && newestBlockNumber == info.BootstrappingSnapshotInfo.LastBlockInSnapshot {
			newestBlockNumber = info.Height
		}
		startingBlockNumber = newestBlockNumber
	case *ab.SeekPosition_Specified:
		startingBlockNumber = start.Specified.Number
		height := fl.Height()
		if startingBlockNumber > height {
			return &blockledger.NotFoundErrorIterator{}, 0
		}
	case *ab.SeekPosition_NextCommit:
		startingBlockNumber = fl.Height()
	default:
		return &blockledger.NotFoundErrorIterator{}, 0
	}

	iterator, err := fl.blockStore.RetrieveBlocks(startingBlockNumber)
	if err != nil {
		logger.Warnw("Failed to initialize block iterator", "blockNum", startingBlockNumber, "error", err)
		return &blockledger.NotFoundErrorIterator{}, 0
	}

	return &fileLedgerIterator{ledger: fl, blockNumber: startingBlockNumber, commonIterator: iterator}, startingBlockNumber
}

// Height 返回账本中的区块数量
func (fl *FileLedger) Height() uint64 {
	// 尝试从blockStore中获取区块链的信息
	info, err := fl.blockStore.GetBlockchainInfo()
	if err != nil {
		// 如果获取过程中发生错误，则记录严重错误日志并终止程序运行
		logger.Panic(err)
	}
	// 返回区块链的高度，即账本中的区块总数
	return info.Height
}

// Append 向账本追加一个新区块
func (fl *FileLedger) Append(block *cb.Block) error {
	// 调用blockStore的AddBlock方法将新区块添加到账本存储中
	err := fl.blockStore.AddBlock(block)
	if err == nil {
		// 如果区块添加成功，关闭当前的信号通道并重新初始化一个新的信号通道
		// 这通常用于通知等待的goroutine区块追加已完成
		close(fl.signal)
		fl.signal = make(chan struct{})
	}
	// 返回添加区块操作的错误状态
	return err
}

func (fl *FileLedger) RetrieveBlockByNumber(blockNumber uint64) (*cb.Block, error) {
	return fl.blockStore.RetrieveBlockByNumber(blockNumber)
}
