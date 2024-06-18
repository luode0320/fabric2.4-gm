/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/snapshot"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

// BlockStore 是基于文件系统的 `BlockStore` 实现。
type BlockStore struct {
	id      string        // BlockStore 的标识
	conf    *Conf         // 配置信息
	fileMgr *blockfileMgr // blockfileMgr 的实例
	stats   *ledgerStats  // 账本统计信息
}

// newBlockStore 构造一个基于文件系统的 `BlockStore` 实现。
// 方法接收者：无（函数）
// 输入参数：
//   - id：块存储的ID。
//   - conf：配置信息。
//   - indexConfig：索引配置信息。
//   - dbHandle：数据库句柄。
//   - stats：统计信息。
//
// 返回值：
//   - *BlockStore：构造的块存储。
//   - error：如果构造块存储失败，则返回错误；否则返回nil。
func newBlockStore(id string, conf *Conf, indexConfig *IndexConfig,
	dbHandle *leveldbhelper.DBHandle, stats *stats) (*BlockStore, error) {
	// 创建将管理用于块持久性的文件的新管理器。 此管理器管理文件系统FS
	fileMgr, err := newBlockfileMgr(id, conf, indexConfig, dbHandle)
	if err != nil {
		return nil, err
	}

	// 创建ledgerStats并初始化blockchain_height统计信息, 定义了区块链统计信息(高度、时间)，适用于排序服务和节点
	ledgerStats := stats.ledgerStats(id)
	// 包含有关区块链分类帐的信息，例如高度，当前块哈希和上一个块哈希
	info := fileMgr.getBlockchainInfo()
	// 更新区块链高度统计信息
	ledgerStats.updateBlockchainHeight(info.Height)

	return &BlockStore{id, conf, fileMgr, ledgerStats}, nil
}

// AddBlock 向存储中添加一个新区块
func (store *BlockStore) AddBlock(block *common.Block) error {
	// 记录开始添加区块的时间，以便计算区块提交所需时间
	startBlockCommit := time.Now()

	// 调用fileMgr的addBlock方法实际执行区块的添加操作
	result := store.fileMgr.addBlock(block)

	// 计算添加区块操作消耗的时间
	elapsedBlockCommit := time.Since(startBlockCommit)

	// 更新区块统计信息，包括区块高度及区块提交耗时
	store.updateBlockStats(block.Header.Number, elapsedBlockCommit)

	// 返回添加区块操作的结果，即是否有错误发生
	return result
}

// GetBlockchainInfo 返回有关区块链的当前信息(高度、当前hash、上一个hsash)
func (store *BlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return store.fileMgr.getBlockchainInfo(), nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (store *BlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return store.fileMgr.retrieveBlocks(startNum)
}

// RetrieveBlockByHash returns the block for given block-hash
func (store *BlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByHash(blockHash)
}

// RetrieveBlockByNumber 根据给定的区块链高度返回区块。
// 方法接收者：store（BlockStore类型的指针）
// 输入参数：
//   - blockNum：要获取的区块的高度。
//
// 返回值：
//   - *common.Block：指定高度的区块。
//   - error：如果获取区块时出错，则返回错误。
func (store *BlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	// 调用fileMgr的retrieveBlockByNumber方法获取指定高度的区块
	return store.fileMgr.retrieveBlockByNumber(blockNum)
}

// TxIDExists returns true if a transaction with the txID is ever committed
func (store *BlockStore) TxIDExists(txID string) (bool, error) {
	return store.fileMgr.txIDExists(txID)
}

// RetrieveTxByID returns a transaction for given transaction id
func (store *BlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	return store.fileMgr.retrieveTransactionByID(txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for the given <blockNum, tranNum>
func (store *BlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	return store.fileMgr.retrieveTransactionByBlockNumTranNum(blockNum, tranNum)
}

// RetrieveBlockByTxID returns the block for the specified txID
func (store *BlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByTxID(txID)
}

// RetrieveTxValidationCodeByTxID returns validation code and blocknumber for the specified txID
func (store *BlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	return store.fileMgr.retrieveTxValidationCodeByTxID(txID)
}

// ExportTxIds creates two files in the specified dir and returns a map that contains
// the mapping between the names of the files and their hashes.
// Technically, the TxIDs appear in the sort order of radix-sort/shortlex. However,
// since practically all the TxIDs are of same length, so the sort order would be the lexical sort order
func (store *BlockStore) ExportTxIds(dir string, newHashFunc snapshot.NewHashFunc) (map[string][]byte, error) {
	return store.fileMgr.index.exportUniqueTxIDs(dir, newHashFunc)
}

// Shutdown shuts down the block store
func (store *BlockStore) Shutdown() {
	logger.Debugf("closing fs blockStore:%s", store.id)
	store.fileMgr.close()
}

func (store *BlockStore) updateBlockStats(blockNum uint64, blockstorageCommitTime time.Duration) {
	store.stats.updateBlockchainHeight(blockNum + 1)
	store.stats.updateBlockstorageCommitTime(blockstorageCommitTime)
}
