/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	blockfilePrefix                   = "blockfile_"
	bootstrappingSnapshotInfoFile     = "bootstrappingSnapshot.info"
	bootstrappingSnapshotInfoTempFile = "bootstrappingSnapshotTemp.info"
)

var blkMgrInfoKey = []byte("blkMgrInfo")

// blockfileMgr 是一个管理区块文件的结构体。
type blockfileMgr struct {
	rootDir                   string                     // 根目录
	conf                      *Conf                      // 配置信息
	db                        *leveldbhelper.DBHandle    // LevelDB 数据库处理函数
	index                     *blockIndex                // 区块索引
	blockfilesInfo            *blockfilesInfo            // 区块文件信息
	bootstrappingSnapshotInfo *BootstrappingSnapshotInfo // 启动快照信息(最后一个区块的编号,最后一个区块的哈希值,上一个区块的哈希值)
	blkfilesInfoCond          *sync.Cond                 // 区块文件信息条件变量
	currentFileWriter         *blockfileWriter           // 当前块文件写入器
	bcInfo                    atomic.Value               // 区块链信息
}

/*
创建将管理用于块持久性的文件的新管理器。
此管理器管理文件系统FS，包括

	-- 存储文件的目录
	-- 存储块的单个文件
	-- 跟踪要保留到的最新文件的 blockfilesInfo
	-- 跟踪什么块和事务在什么文件中的索引

当启动新的 blockfile 管理器时 (即仅在启动时)，
它检查此启动是否是系统第一次启动，或者这是系统的重新启动。

块文件管理器将数据块存储到文件系统中。
该文件存储是通过创建配置大小的顺序编号的文件来完成的
即blockfile_000000、blockfile_000001等。

块中的每个事务都存储有关该事务中字节数的信息

	将tx [1:0] 的txLoc [fileSuffixNum = 0，offset = 3，bytesLength = 104] 添加到索引
	将tx [1:1] 的txLoc [fileSuffixNum = 0，offset = 107，bytesLength = 104] 添加到索引

每个块与该块的总编码长度以及tx位置偏移一起被存储。

请记住，这些步骤仅在系统启动时执行一次。
在启动新管理时:

	  *) 检查用于存储文件的目录是否存在，如果不存在，则创建目录
	  *) 检查键值数据库是否存在，如果不存在，则创建一个
	       (将创建一个db dir)
	  *) 确定用于存储的 blockfilesInfo
			-- 从db加载 (如果存在), 如果不实例化一个新的 blockfilesInfo
			-- 如果 blockfilesInfo 是从db加载的，则与FS进行比较
			-- 如果 blockfilesInfo 和 文件系统 不同步, 从FS同步blockfilesInfo
	  *) 启动新的文件编写器
			-- 截断每个块文件 filesinfo 以删除任何超过最后一个块的多余文件
	  *) 确定用于在文件 blkstorage 中查找tx和块的索引信息
			-- 实例化一个新的blockIdxInfo
			-- 如果存在，则从db加载索引
			-- syncIndex 将索引的最后一个块与FS中的块进行比较
			-- 如果索引和文件系统不同步，则从FS同步索引
	  *)  更新api使用的区块链信息
*/
func newBlockfileMgr(id string, conf *Conf, indexConfig *IndexConfig, indexStore *leveldbhelper.DBHandle) (*blockfileMgr, error) {
	logger.Debugf("newBlockfileMgr() 为通道账本初始化基于文件的块存储: %s ", id)

	// 返回给定账本ID的账本块目录路径
	rootDir := conf.getLedgerBlockDir(id)
	// 确保目录存在，并创建目录返回目录是否为空
	_, err := fileutil.CreateDirIfMissing(rootDir)
	if err != nil {
		panic(fmt.Sprintf("创建块存储根目录 [%s] 时出错: %s", rootDir, err))
	}

	// 一个管理区块文件的结构体
	mgr := &blockfileMgr{rootDir: rootDir, conf: conf, db: indexStore}
	// 从数据库中获取当前存储的块文件信息
	blockfilesInfo, err := mgr.loadBlkfilesInfo()
	if err != nil {
		panic(fmt.Sprintf("无法从 db 获取当前块文件的块文件信息: %s", err))
	}

	if blockfilesInfo == nil {
		logger.Info(`从块存储获取块信息`)
		if blockfilesInfo, err = constructBlockfilesInfo(rootDir); err != nil {
			panic(fmt.Sprintf("无法从块文件生成块 blockfilesInfo 信息: %s", err))
		}

		logger.Debugf("通过扫描块目录构造的块信息 = %s", spew.Sdump(blockfilesInfo))
	} else {
		logger.Debug(`从块存储同步块信息 (如果需要)`)
		syncBlockfilesInfoFromFS(rootDir, blockfilesInfo)
	}

	// 将块文件信息保存到数据库中
	err = mgr.saveBlkfilesInfo(blockfilesInfo, true)
	if err != nil {
		panic(fmt.Sprintf("无法将下一个块文件信息保存到db: %s", err))
	}

	// 创建一个新的块文件写入器
	currentFileWriter, err := newBlockfileWriter(deriveBlockfilePath(rootDir, blockfilesInfo.latestFileNumber))
	if err != nil {
		panic(fmt.Sprintf("无法打开当前文件的块文件写入器: %s", err))
	}

	// 将文件截断为目标大小, latestFileSize 最新文件的大小
	err = currentFileWriter.truncateFile(blockfilesInfo.latestFileSize)
	if err != nil {
		panic(fmt.Sprintf("无法将当前文件截断为db中的已知大小: %s", err))
	}

	// 创建一个新的块索引, indexStore=数据库的处理函数
	if mgr.index, err = newBlockIndex(indexConfig, indexStore); err != nil {
		panic(fmt.Sprintf("块索引错误: %s", err))
	}

	mgr.blockfilesInfo = blockfilesInfo
	// 加载引导快照信息(最后一个区块的编号,最后一个区块的哈希值,上一个区块的哈希值)
	bsi, err := loadBootstrappingSnapshotInfo(rootDir)
	if err != nil {
		return nil, err
	}

	mgr.bootstrappingSnapshotInfo = bsi
	mgr.currentFileWriter = currentFileWriter
	mgr.blkfilesInfoCond = sync.NewCond(&sync.Mutex{})

	if err := mgr.syncIndex(); err != nil {
		return nil, err
	}

	// 包含有关区块链账本的信息，例如高度，当前块哈希和上一个块哈希。
	bcInfo := &common.BlockchainInfo{}
	if mgr.bootstrappingSnapshotInfo != nil {
		bcInfo.Height = mgr.bootstrappingSnapshotInfo.LastBlockNum + 1             // 最后一个区块的编号 + 1 = 区块高度
		bcInfo.CurrentBlockHash = mgr.bootstrappingSnapshotInfo.LastBlockHash      // 最后一个区块的哈希值
		bcInfo.PreviousBlockHash = mgr.bootstrappingSnapshotInfo.PreviousBlockHash // 上一个区块的哈希值
		bcInfo.BootstrappingSnapshotInfo = &common.BootstrappingSnapshotInfo{}
		bcInfo.BootstrappingSnapshotInfo.LastBlockInSnapshot = mgr.bootstrappingSnapshotInfo.LastBlockNum // 最后一个区块的编号
	}

	// 是否没有区块文件
	if !blockfilesInfo.noBlockFiles {
		// 根据块编号检索块头(区块号、数据哈希、上一个哈希), lastPersistedBlock=最后持久化的区块
		lastBlockHeader, err := mgr.retrieveBlockHeaderByNumber(blockfilesInfo.lastPersistedBlock)
		if err != nil {
			panic(fmt.Sprintf("无法检索最后一个块头(区块号、数据哈希、上一个哈希): %s", err))
		}

		// 使用 lastpersitedblock 更新 bcInfo
		bcInfo.Height = blockfilesInfo.lastPersistedBlock + 1                // 高度
		bcInfo.CurrentBlockHash = protoutil.BlockHeaderHash(lastBlockHeader) // 当前块hash
		bcInfo.PreviousBlockHash = lastBlockHeader.PreviousHash              // 是一个hash
	}

	// 存储
	mgr.bcInfo.Store(bcInfo)
	return mgr, nil
}

func bootstrapFromSnapshottedTxIDs(
	ledgerID string,
	snapshotDir string,
	snapshotInfo *SnapshotInfo,
	conf *Conf,
	indexStore *leveldbhelper.DBHandle,
) error {
	rootDir := conf.getLedgerBlockDir(ledgerID)
	isEmpty, err := fileutil.CreateDirIfMissing(rootDir)
	if err != nil {
		return err
	}
	if !isEmpty {
		return errors.Errorf("dir %s not empty", rootDir)
	}

	bsi := &BootstrappingSnapshotInfo{
		LastBlockNum:      snapshotInfo.LastBlockNum,
		LastBlockHash:     snapshotInfo.LastBlockHash,
		PreviousBlockHash: snapshotInfo.PreviousBlockHash,
	}

	bsiBytes, err := proto.Marshal(bsi)
	if err != nil {
		return err
	}

	if err := fileutil.CreateAndSyncFileAtomically(
		rootDir,
		bootstrappingSnapshotInfoTempFile,
		bootstrappingSnapshotInfoFile,
		bsiBytes,
		0o644,
	); err != nil {
		return err
	}
	if err := fileutil.SyncDir(rootDir); err != nil {
		return err
	}
	if err := importTxIDsFromSnapshot(snapshotDir, snapshotInfo.LastBlockNum, indexStore); err != nil {
		return err
	}
	return nil
}

func syncBlockfilesInfoFromFS(rootDir string, blkfilesInfo *blockfilesInfo) {
	logger.Debugf("Starting blockfilesInfo=%s", blkfilesInfo)
	// Checks if the file suffix of where the last block was written exists
	filePath := deriveBlockfilePath(rootDir, blkfilesInfo.latestFileNumber)
	exists, size, err := fileutil.FileExists(filePath)
	if err != nil {
		panic(fmt.Sprintf("Error in checking whether file [%s] exists: %s", filePath, err))
	}
	logger.Debugf("status of file [%s]: exists=[%t], size=[%d]", filePath, exists, size)
	// Test is !exists because when file number is first used the file does not exist yet
	// checks that the file exists and that the size of the file is what is stored in blockfilesInfo
	// status of file [<blkstorage_location>/blocks/blockfile_000000]: exists=[false], size=[0]
	if !exists || int(size) == blkfilesInfo.latestFileSize {
		// blockfilesInfo is in sync with the file on disk
		return
	}
	// Scan the file system to verify that the blockfilesInfo stored in db is correct
	_, endOffsetLastBlock, numBlocks, err := scanForLastCompleteBlock(
		rootDir, blkfilesInfo.latestFileNumber, int64(blkfilesInfo.latestFileSize))
	if err != nil {
		panic(fmt.Sprintf("Could not open current file for detecting last block in the file: %s", err))
	}
	blkfilesInfo.latestFileSize = int(endOffsetLastBlock)
	if numBlocks == 0 {
		return
	}
	// Updates the blockfilesInfo for the actual last block number stored and it's end location
	if blkfilesInfo.noBlockFiles {
		blkfilesInfo.lastPersistedBlock = uint64(numBlocks - 1)
	} else {
		blkfilesInfo.lastPersistedBlock += uint64(numBlocks)
	}
	blkfilesInfo.noBlockFiles = false
	logger.Debugf("blockfilesInfo after updates by scanning the last file segment:%s", blkfilesInfo)
}

func deriveBlockfilePath(rootDir string, suffixNum int) string {
	return rootDir + "/" + blockfilePrefix + fmt.Sprintf("%06d", suffixNum)
}

func (mgr *blockfileMgr) close() {
	mgr.currentFileWriter.close()
}

func (mgr *blockfileMgr) moveToNextFile() {
	blkfilesInfo := &blockfilesInfo{
		latestFileNumber:   mgr.blockfilesInfo.latestFileNumber + 1,
		latestFileSize:     0,
		lastPersistedBlock: mgr.blockfilesInfo.lastPersistedBlock,
	}

	nextFileWriter, err := newBlockfileWriter(
		deriveBlockfilePath(mgr.rootDir, blkfilesInfo.latestFileNumber))
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to next file: %s", err))
	}
	mgr.currentFileWriter.close()
	err = mgr.saveBlkfilesInfo(blkfilesInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}
	mgr.currentFileWriter = nextFileWriter
	mgr.updateBlockfilesInfo(blkfilesInfo)
}

func (mgr *blockfileMgr) addBlock(block *common.Block) error {
	bcInfo := mgr.getBlockchainInfo()
	if block.Header.Number != bcInfo.Height {
		return errors.Errorf(
			"块高度应该是 %d, 但却是 %d",
			mgr.getBlockchainInfo().Height, block.Header.Number,
		)
	}

	// 添加以前的哈希检查-虽然不是必需的，但可能不是一个坏主意
	// 验证块中是否存在字段 “block.Header.Previoushash”。
	// 此检查是一个简单的字节比较，因此不会导致任何可观察到的性能损失
	// 如果 orderer 服务中有任何错误，可能有助于检测罕见的情况。
	if !bytes.Equal(block.Header.PreviousHash, bcInfo.CurrentBlockHash) {
		return errors.Errorf(
			"上一个意外的块哈希, 预期PreviousHash = [%x], PreviousHash在最新的块中引用= [%x]",
			bcInfo.CurrentBlockHash, block.Header.PreviousHash,
		)
	}
	blockBytes, info, err := serializeBlock(block)
	if err != nil {
		return errors.WithMessage(err, "error serializing block")
	}
	blockHash := protoutil.BlockHeaderHash(block.Header)
	// Get the location / offset where each transaction starts in the block and where the block ends
	txOffsets := info.txOffsets
	currentOffset := mgr.blockfilesInfo.latestFileSize

	blockBytesLen := len(blockBytes)
	blockBytesEncodedLen := proto.EncodeVarint(uint64(blockBytesLen))
	totalBytesToAppend := blockBytesLen + len(blockBytesEncodedLen)

	// Determine if we need to start a new file since the size of this block
	// exceeds the amount of space left in the current file
	if currentOffset+totalBytesToAppend > mgr.conf.maxBlockfileSize {
		mgr.moveToNextFile()
		currentOffset = 0
	}
	// append blockBytesEncodedLen to the file
	err = mgr.currentFileWriter.append(blockBytesEncodedLen, false)
	if err == nil {
		// append the actual block bytes to the file
		err = mgr.currentFileWriter.append(blockBytes, true)
	}
	if err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(mgr.blockfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Could not truncate current file to known size after an error during block append: %s", err))
		}
		return errors.WithMessage(err, "error appending block to file")
	}

	// Update the blockfilesInfo with the results of adding the new block
	currentBlkfilesInfo := mgr.blockfilesInfo
	newBlkfilesInfo := &blockfilesInfo{
		latestFileNumber:   currentBlkfilesInfo.latestFileNumber,
		latestFileSize:     currentBlkfilesInfo.latestFileSize + totalBytesToAppend,
		noBlockFiles:       false,
		lastPersistedBlock: block.Header.Number,
	}
	// save the blockfilesInfo in the database
	if err = mgr.saveBlkfilesInfo(newBlkfilesInfo, false); err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(currentBlkfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Error in truncating current file to known size after an error in saving blockfiles info: %s", err))
		}
		return errors.WithMessage(err, "error saving blockfiles file info to db")
	}

	// Index block file location pointer updated with file suffex and offset for the new block
	blockFLP := &fileLocPointer{fileSuffixNum: newBlkfilesInfo.latestFileNumber}
	blockFLP.offset = currentOffset
	// shift the txoffset because we prepend length of bytes before block bytes
	for _, txOffset := range txOffsets {
		txOffset.loc.offset += len(blockBytesEncodedLen)
	}
	// save the index in the database
	if err = mgr.index.indexBlock(&blockIdxInfo{
		blockNum: block.Header.Number, blockHash: blockHash,
		flp: blockFLP, txOffsets: txOffsets, metadata: block.Metadata,
	}); err != nil {
		return err
	}

	// update the blockfilesInfo (for storage) and the blockchain info (for APIs) in the manager
	mgr.updateBlockfilesInfo(newBlkfilesInfo)
	mgr.updateBlockchainInfo(blockHash, block)
	return nil
}

func (mgr *blockfileMgr) syncIndex() error {
	nextIndexableBlock := uint64(0)
	lastBlockIndexed, err := mgr.index.getLastBlockIndexed()
	if err != nil {
		if err != errIndexSavePointKeyNotPresent {
			return err
		}
	} else {
		nextIndexableBlock = lastBlockIndexed + 1
	}

	if nextIndexableBlock == 0 && mgr.bootstrappedFromSnapshot() {
		// This condition can happen only if there was a peer crash or failure during
		// bootstrapping the ledger from a snapshot or the index is dropped/corrupted afterward
		return errors.Errorf(
			"cannot sync index with block files. blockstore is bootstrapped from a snapshot and first available block=[%d]",
			mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}

	if mgr.blockfilesInfo.noBlockFiles {
		logger.Debug("No block files present. This happens when there has not been any blocks added to the ledger yet")
		return nil
	}

	if nextIndexableBlock == mgr.blockfilesInfo.lastPersistedBlock+1 {
		logger.Debug("Both the block files and indices are in sync.")
		return nil
	}

	startFileNum := 0
	startOffset := 0
	skipFirstBlock := false
	endFileNum := mgr.blockfilesInfo.latestFileNumber

	firstAvailableBlkNum, err := retrieveFirstBlockNumFromFile(mgr.rootDir, 0)
	if err != nil {
		return err
	}

	if nextIndexableBlock > firstAvailableBlkNum {
		logger.Debugf("Last block indexed [%d], Last block present in block files [%d]", lastBlockIndexed, mgr.blockfilesInfo.lastPersistedBlock)
		var flp *fileLocPointer
		if flp, err = mgr.index.getBlockLocByBlockNum(lastBlockIndexed); err != nil {
			return err
		}
		startFileNum = flp.fileSuffixNum
		startOffset = flp.locPointer.offset
		skipFirstBlock = true
	}

	logger.Infof("Start building index from block [%d] to last block [%d]", nextIndexableBlock, mgr.blockfilesInfo.lastPersistedBlock)

	// open a blockstream to the file location that was stored in the index
	var stream *blockStream
	if stream, err = newBlockStream(mgr.rootDir, startFileNum, int64(startOffset), endFileNum); err != nil {
		return err
	}
	var blockBytes []byte
	var blockPlacementInfo *blockPlacementInfo

	if skipFirstBlock {
		if blockBytes, _, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			return errors.Errorf("block bytes for block num = [%d] should not be nil here. The indexes for the block are already present",
				lastBlockIndexed)
		}
	}

	// Should be at the last block already, but go ahead and loop looking for next blockBytes.
	// If there is another block, add it to the index.
	// This will ensure block indexes are correct, for example if peer had crashed before indexes got updated.
	blockIdxInfo := &blockIdxInfo{}
	for {
		if blockBytes, blockPlacementInfo, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			break
		}
		info, err := extractSerializedBlockInfo(blockBytes)
		if err != nil {
			return err
		}

		// The blockStartOffset will get applied to the txOffsets prior to indexing within indexBlock(),
		// therefore just shift by the difference between blockBytesOffset and blockStartOffset
		numBytesToShift := int(blockPlacementInfo.blockBytesOffset - blockPlacementInfo.blockStartOffset)
		for _, offset := range info.txOffsets {
			offset.loc.offset += numBytesToShift
		}

		// Update the blockIndexInfo with what was actually stored in file system
		blockIdxInfo.blockHash = protoutil.BlockHeaderHash(info.blockHeader)
		blockIdxInfo.blockNum = info.blockHeader.Number
		blockIdxInfo.flp = &fileLocPointer{
			fileSuffixNum: blockPlacementInfo.fileNum,
			locPointer:    locPointer{offset: int(blockPlacementInfo.blockStartOffset)},
		}
		blockIdxInfo.txOffsets = info.txOffsets
		blockIdxInfo.metadata = info.metadata

		logger.Debugf("syncIndex() indexing block [%d]", blockIdxInfo.blockNum)
		if err = mgr.index.indexBlock(blockIdxInfo); err != nil {
			return err
		}
		if blockIdxInfo.blockNum%10000 == 0 {
			logger.Infof("Indexed block number [%d]", blockIdxInfo.blockNum)
		}
	}
	logger.Infof("Finished building index. Last block indexed [%d]", blockIdxInfo.blockNum)
	return nil
}

func (mgr *blockfileMgr) getBlockchainInfo() *common.BlockchainInfo {
	return mgr.bcInfo.Load().(*common.BlockchainInfo)
}

func (mgr *blockfileMgr) updateBlockfilesInfo(blkfilesInfo *blockfilesInfo) {
	mgr.blkfilesInfoCond.L.Lock()
	defer mgr.blkfilesInfoCond.L.Unlock()
	mgr.blockfilesInfo = blkfilesInfo
	logger.Debugf("Broadcasting about update blockfilesInfo: %s", blkfilesInfo)
	mgr.blkfilesInfoCond.Broadcast()
}

func (mgr *blockfileMgr) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo := mgr.getBlockchainInfo()
	newBCInfo := &common.BlockchainInfo{
		Height:                    currentBCInfo.Height + 1,
		CurrentBlockHash:          latestBlockHash,
		PreviousBlockHash:         latestBlock.Header.PreviousHash,
		BootstrappingSnapshotInfo: currentBCInfo.BootstrappingSnapshotInfo,
	}

	mgr.bcInfo.Store(newBCInfo)
}

func (mgr *blockfileMgr) retrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	logger.Debugf("retrieveBlockByHash() - blockHash = [%#v]", blockHash)
	loc, err := mgr.index.getBlockLocByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

// retrieveBlockByNumber 根据给定的区块链高度返回区块。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：
//   - blockNum：要获取的区块的高度。
//
// 返回值：
//   - *common.Block：指定高度的区块。
//   - error：如果获取区块时出错，则返回错误。
func (mgr *blockfileMgr) retrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	logger.Debugf("retrieveBlockByNumber() - blockNum = [%d]", blockNum)

	// 如果blockNum等于math.MaxUint64，则将其解释为请求最后一个区块
	if blockNum == math.MaxUint64 {
		blockNum = mgr.getBlockchainInfo().Height - 1
	}

	// 如果blockNum小于block files中第一个可能的区块号，则返回错误
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"无法使用块高度 [%d]. 账本是从快照引导的, 第一个可用块高度 = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}

	// 根据blockNum获取区块在block files中的位置(后缀编号、偏移量、长度)
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}

	// 根据给定的文件位置指针获取下一个区块数据
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockByTxID(txID string) (*common.Block, error) {
	logger.Debugf("retrieveBlockByTxID() - txID = [%s]", txID)
	loc, err := mgr.index.getBlockLocByTxID(txID)
	if err == errNilValue {
		return nil, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	logger.Debugf("retrieveTxValidationCodeByTxID() - txID = [%s]", txID)
	validationCode, blkNum, err := mgr.index.getTxValidationCodeByTxID(txID)
	if err == errNilValue {
		return peer.TxValidationCode(-1), 0, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	return validationCode, blkNum, err
}

// retrieveBlockHeaderByNumber 根据块编号检索块头(区块号、数据哈希、上一个哈希)。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：
//   - blockNum：要检索的块的编号。
//
// 返回值：
//   - *common.BlockHeader：检索到的块头。
//   - error：如果检索块头时出错，则返回错误。
func (mgr *blockfileMgr) retrieveBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error) {
	logger.Debugf("retrieveBlockHeaderByNumber() - blockNum = [%d]", blockNum)
	// 如果块编号小于块文件中第一个可能的块编号(最后一个+1)，则返回错误
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"无法使用的块高度 [%d], 通道账本是从快照引导的, 第一个可用块高度 = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}

	// 根据块编号获取块的位置信息
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}

	// 根据位置信息获取块的下一个字节数据
	blockBytes, err := mgr.fetchBlockBytes(loc)
	if err != nil {
		return nil, err
	}

	// 从块的下一个字节数据中提取序列化的块信息。头信息(区块号、数据哈希、上一个哈希), 提取交易偏移量信息, 提取元数据信息
	info, err := extractSerializedBlockInfo(blockBytes)
	if err != nil {
		return nil, err
	}

	// 返回检索到的块头(区块号、数据哈希、上一个哈希)
	return info.blockHeader, nil
}

func (mgr *blockfileMgr) retrieveBlocks(startNum uint64) (*blocksItr, error) {
	if startNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			startNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	return newBlockItr(mgr, startNum), nil
}

func (mgr *blockfileMgr) txIDExists(txID string) (bool, error) {
	return mgr.index.txIDExists(txID)
}

func (mgr *blockfileMgr) retrieveTransactionByID(txID string) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByID() - txId = [%s]", txID)
	loc, err := mgr.index.getTxLoc(txID)
	if err == errNilValue {
		return nil, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

func (mgr *blockfileMgr) retrieveTransactionByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByBlockNumTranNum() - blockNum = [%d], tranNum = [%d]", blockNum, tranNum)
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	loc, err := mgr.index.getTXLocByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

// fetchBlock 根据给定的文件位置指针获取下一个区块数据。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：
//   - lp：文件位置指针，指向要获取的区块的位置。
//
// 返回值：
//   - *common.Block：获取到的区块数据。
//   - error：如果获取区块数据时出错，则返回错误。
func (mgr *blockfileMgr) fetchBlock(lp *fileLocPointer) (*common.Block, error) {
	// 根据文件位置指针获取区块的下一个字节数据
	blockBytes, err := mgr.fetchBlockBytes(lp)
	if err != nil {
		return nil, err
	}

	// 反序列化区块字节数据为区块对象(头部、数据、元数据)
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (mgr *blockfileMgr) fetchTransactionEnvelope(lp *fileLocPointer) (*common.Envelope, error) {
	logger.Debugf("Entering fetchTransactionEnvelope() %v\n", lp)
	var err error
	var txEnvelopeBytes []byte
	if txEnvelopeBytes, err = mgr.fetchRawBytes(lp); err != nil {
		return nil, err
	}
	_, n := proto.DecodeVarint(txEnvelopeBytes)
	return protoutil.GetEnvelopeFromBlock(txEnvelopeBytes[n:])
}

// fetchBlockBytes 获取块的字节数据。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：
//   - lp：块的位置信息。
//
// 返回值：
//   - []byte：块的字节数据。
//   - error：如果获取块字节数据时出错，则返回错误。
func (mgr *blockfileMgr) fetchBlockBytes(lp *fileLocPointer) ([]byte, error) {
	// 创建一个新的块文件流
	stream, err := newBlockfileStream(mgr.rootDir, lp.fileSuffixNum, int64(lp.offset))
	if err != nil {
		return nil, err
	}
	// 在函数返回前关闭块文件流
	defer stream.close()

	// 读取下一个块的字节数据
	b, err := stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	// 返回块的字节数据
	return b, nil
}

func (mgr *blockfileMgr) fetchRawBytes(lp *fileLocPointer) ([]byte, error) {
	filePath := deriveBlockfilePath(mgr.rootDir, lp.fileSuffixNum)
	reader, err := newBlockfileReader(filePath)
	if err != nil {
		return nil, err
	}
	defer reader.close()
	b, err := reader.read(lp.offset, lp.bytesLength)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// loadBlkfilesInfo 从数据库中获取当前存储的块文件信息。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：无
// 返回值：
//   - *blockfilesInfo：当前存储的块文件信息。
//   - error：如果获取块文件信息时出错，则返回错误；否则返回nil。
func (mgr *blockfileMgr) loadBlkfilesInfo() (*blockfilesInfo, error) {
	var b []byte
	var err error
	// 从数据库中获取块文件信息
	if b, err = mgr.db.Get(blkMgrInfoKey); b == nil || err != nil {
		return nil, err
	}

	// 反序列化块文件信息,  维护有关区块文件的摘要信息
	i := &blockfilesInfo{}
	if err = i.unmarshal(b); err != nil {
		return nil, err
	}

	logger.Debugf("已加载当前存储的块文件信息 blockfilesInfo:%s", i)
	return i, nil
}

// saveBlkfilesInfo 将块文件信息保存到数据库中。
// 方法接收者：mgr（blockfileMgr类型的指针）
// 输入参数：
//   - i：要保存的块文件信息。
//   - sync：是否同步写入数据库。
//
// 返回值：
//   - error：如果保存块文件信息时出错，则返回错误；否则返回nil。
func (mgr *blockfileMgr) saveBlkfilesInfo(i *blockfilesInfo, sync bool) error {
	// 将块文件信息序列化为字节数据
	b, err := i.marshal()
	if err != nil {
		return err
	}

	// 将序列化后的字节数据保存到数据库中
	if err = mgr.db.Put(blkMgrInfoKey, b, sync); err != nil {
		return err
	}
	return nil
}

func (mgr *blockfileMgr) firstPossibleBlockNumberInBlockFiles() uint64 {
	if mgr.bootstrappingSnapshotInfo == nil {
		return 0
	}
	return mgr.bootstrappingSnapshotInfo.LastBlockNum + 1
}

func (mgr *blockfileMgr) bootstrappedFromSnapshot() bool {
	return mgr.firstPossibleBlockNumberInBlockFiles() > 0
}

// scanForLastCompleteBlock scan a given block file and detects the last offset in the file
// after which there may lie a block partially written (towards the end of the file in a crash scenario).
func scanForLastCompleteBlock(rootDir string, fileNum int, startingOffset int64) ([]byte, int64, int, error) {
	// scan the passed file number suffix starting from the passed offset to find the last completed block
	numBlocks := 0
	var lastBlockBytes []byte
	blockStream, errOpen := newBlockfileStream(rootDir, fileNum, startingOffset)
	if errOpen != nil {
		return nil, 0, 0, errOpen
	}
	defer blockStream.close()
	var errRead error
	var blockBytes []byte
	for {
		blockBytes, errRead = blockStream.nextBlockBytes()
		if blockBytes == nil || errRead != nil {
			break
		}
		lastBlockBytes = blockBytes
		numBlocks++
	}
	if errRead == ErrUnexpectedEndOfBlockfile {
		logger.Debugf(`Error:%s
		The error may happen if a crash has happened during block appending.
		Resetting error to nil and returning current offset as a last complete block's end offset`, errRead)
		errRead = nil
	}
	logger.Debugf("scanForLastCompleteBlock(): last complete block ends at offset=[%d]", blockStream.currentOffset)
	return lastBlockBytes, blockStream.currentOffset, numBlocks, errRead
}

// blockfilesInfo 维护有关区块文件的摘要信息。
type blockfilesInfo struct {
	latestFileNumber   int    // 最新的文件编号
	latestFileSize     int    // 最新文件的大小
	noBlockFiles       bool   // 是否没有区块文件
	lastPersistedBlock uint64 // 最后持久化的区块
}

func (i *blockfilesInfo) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	var err error
	if err = buffer.EncodeVarint(uint64(i.latestFileNumber)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileNumber [%d]", i.latestFileNumber)
	}
	if err = buffer.EncodeVarint(uint64(i.latestFileSize)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileSize [%d]", i.latestFileSize)
	}
	if err = buffer.EncodeVarint(i.lastPersistedBlock); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastPersistedBlock [%d]", i.lastPersistedBlock)
	}
	var noBlockFilesMarker uint64
	if i.noBlockFiles {
		noBlockFilesMarker = 1
	}
	if err = buffer.EncodeVarint(noBlockFilesMarker); err != nil {
		return nil, errors.Wrapf(err, "error encoding noBlockFiles [%d]", noBlockFilesMarker)
	}
	return buffer.Bytes(), nil
}

func (i *blockfilesInfo) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	var val uint64
	var noBlockFilesMarker uint64
	var err error

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileNumber = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileSize = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.lastPersistedBlock = val
	if noBlockFilesMarker, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.noBlockFiles = noBlockFilesMarker == 1
	return nil
}

func (i *blockfilesInfo) String() string {
	return fmt.Sprintf("latestFileNumber=[%d], latestFileSize=[%d], noBlockFiles=[%t], lastPersistedBlock=[%d]",
		i.latestFileNumber, i.latestFileSize, i.noBlockFiles, i.lastPersistedBlock)
}
