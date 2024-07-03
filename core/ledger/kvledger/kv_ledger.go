/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	common2 "github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/common/flogging"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validation"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("kvledger")

var (
	rwsetHashOpts    = &bccsp.SHA256Opts{}
	snapshotHashOpts = &bccsp.SHA256Opts{}
)

// kvLedger 提供了 `ledger.PeerLedger` 的实现。
// 此实现提供了基于键值的数据模型。
type kvLedger struct {
	ledgerID               string                            // 账本ID
	bootSnapshotMetadata   *SnapshotMetadata                 // 启动快照元数据
	blockStore             *blkstorage.BlockStore            // 区块存储
	pvtdataStore           *pvtdatastorage.Store             // 私有数据存储
	txmgr                  *txmgr.LockBasedTxMgr             // 事务管理器
	historyDB              *history.DB                       // 历史数据库
	configHistoryRetriever *collectionConfigHistoryRetriever // 集合配置历史检索器
	snapshotMgr            *snapshotMgr                      // 快照管理器
	blockAPIsRWLock        *sync.RWMutex                     // 区块API读写锁
	stats                  *ledgerStats                      // 账本统计信息
	commitHash             []byte                            // 提交哈希
	hashProvider           ledger.HashProvider               // 哈希提供者
	config                 *ledger.Config                    // 账本配置

	// isPvtDataStoreAheadOfBlockStore 在缺失私有数据协调期间进行读取，
	// 并且在常规区块提交期间可能会更新。
	// 因此，我们使用原子值来确保一致的读取。
	isPvtstoreAheadOfBlkstore atomic.Value

	commitNotifierLock sync.Mutex
	commitNotifier     *commitNotifier // 提交通知器
}

// lgrInitializer 是一个初始化账本的结构体。
type lgrInitializer struct {
	ledgerID                 string                                         // 账本ID
	initializingFromSnapshot bool                                           // 是否从快照初始化
	bootSnapshotMetadata     *SnapshotMetadata                              // 启动快照的元数据
	blockStore               *blkstorage.BlockStore                         // 区块存储
	pvtdataStore             *pvtdatastorage.Store                          // 私有数据存储
	stateDB                  *privacyenabledstate.DB                        // 状态数据库
	historyDB                *history.DB                                    // 历史数据库
	configHistoryMgr         *confighistory.Mgr                             // 配置历史管理器
	stateListeners           []ledger.StateListener                         // 状态监听器
	bookkeeperProvider       *bookkeeping.Provider                          // 记账提供程序
	ccInfoProvider           ledger.DeployedChaincodeInfoProvider           // 链码信息提供程序
	ccLifecycleEventProvider ledger.ChaincodeLifecycleEventProvider         // 链码生命周期事件提供程序
	stats                    *ledgerStats                                   // 账本统计信息
	customTxProcessors       map[common.HeaderType]ledger.CustomTxProcessor // 自定义交易处理器
	hashProvider             ledger.HashProvider                            // 哈希提供程序
	config                   *ledger.Config                                 // 账本配置
}

// 新 KV 账本
func newKVLedger(initializer *lgrInitializer) (*kvLedger, error) {
	ledgerID := initializer.ledgerID
	logger.Debugf("创建 KVLedger ledgerID=%s: ", ledgerID)
	l := &kvLedger{
		ledgerID:             ledgerID,                         // 账本ID
		bootSnapshotMetadata: initializer.bootSnapshotMetadata, // 启动快照元数据
		blockStore:           initializer.blockStore,           // 区块存储
		pvtdataStore:         initializer.pvtdataStore,         // 私有数据存储
		historyDB:            initializer.historyDB,            // 历史数据库
		hashProvider:         initializer.hashProvider,         // 哈希提供者
		config:               initializer.config,               // 账本配置
		blockAPIsRWLock:      &sync.RWMutex{},                  // 区块API读写锁
	}

	// 构造的BTLPolicy实例
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(&collectionInfoRetriever{ledgerID, l, initializer.ccInfoProvider})

	rwsetHashFunc := func(data []byte) ([]byte, error) {
		// todo luode 进行国密sm3的改造
		hash, err := initializer.hashProvider.GetHash(common2.Hash())
		if err != nil {
			return nil, err
		}
		if _, err = hash.Write(data); err != nil {
			return nil, err
		}
		return hash.Sum(nil), nil
	}

	// 捕获了事务管理器的依赖项
	txmgrInitializer := &txmgr.Initializer{
		LedgerID:            ledgerID,                       // 账本ID
		DB:                  initializer.stateDB,            // 数据库
		StateListeners:      initializer.stateListeners,     // 状态监听器
		BtlPolicy:           btlPolicy,                      // BTL策略
		BookkeepingProvider: initializer.bookkeeperProvider, // 记账提供者
		CCInfoProvider:      initializer.ccInfoProvider,     // 部署的链码信息提供者
		CustomTxProcessors:  initializer.customTxProcessors, // 自定义事务处理器
		HashFunc:            rwsetHashFunc,                  // 哈希函数
	}
	// 初始化事务管理器
	if err := l.initTxMgr(txmgrInitializer); err != nil {
		return nil, err
	}

	// btlPolicy在内部使用queryexecuter，并间接使用txmgr。
	// 因此，一旦txmgr启动，我们需要初始化pvtdataStore。
	l.pvtdataStore.Init(btlPolicy)

	var err error
	// 返回最后一个持久化的提交哈希
	l.commitHash, err = l.lastPersistedCommitHash()
	if err != nil {
		return nil, err
	}

	// 检查私有数据存储是否领先于区块存储
	isAhead, err := l.isPvtDataStoreAheadOfBlockStore()
	if err != nil {
		return nil, err
	}

	l.isPvtstoreAheadOfBlkstore.Store(isAhead)

	// 获取链码事件侦听器, 使账本组件（主要是状态数据库）能够监听链码生命周期事件。
	statedbIndexCreator := initializer.stateDB.GetChaincodeEventListener()
	if statedbIndexCreator != nil {
		logger.Debugf("为链码生命周期事件注册状态数据库")
		// 为链码生命周期事件注册状态数据库索引创建器
		err := l.registerStateDBIndexCreatorForChaincodeLifecycleEvents(
			statedbIndexCreator,                  // 状态数据库索引创建器
			initializer.ccInfoProvider,           // 部署的链码信息提供者
			initializer.ccLifecycleEventProvider, //链码生命周期事件提供者
			cceventmgmt.GetMgr(),                 // 旧版链码生命周期事件提供者
			initializer.initializingFromSnapshot, // 是否从快照初始化
		)
		if err != nil {
			return nil, err
		}
	}

	// 如果状态DB和历史DB与块存储不同步，则将其恢复
	if err := l.recoverDBs(); err != nil {
		return nil, err
	}
	// 用于检索集合配置历史记录
	l.configHistoryRetriever = &collectionConfigHistoryRetriever{
		Retriever:                     initializer.configHistoryMgr.GetRetriever(ledgerID), // 帮助消费者检索集合配置历史记录(账本id, 数据库处理函数)
		DeployedChaincodeInfoProvider: txmgrInitializer.CCInfoProvider,                     // 由账本用于构建集合配置历史记录
		ledger:                        l,
	}

	// 用于初始化快照管理器
	if err := l.initSnapshotMgr(initializer); err != nil {
		return nil, err
	}

	l.stats = initializer.stats
	return l, nil
}

// registerStateDBIndexCreatorForChaincodeLifecycleEvents 为链码生命周期事件注册状态数据库索引创建器。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：
//   - stateDBIndexCreator：状态数据库索引创建器
//   - deployedChaincodesInfoExtractor：部署的链码信息提供者
//   - chaincodesLifecycleEventsProvider：链码生命周期事件提供者
//   - legacyChaincodesLifecycleEventsProvider：旧版链码生命周期事件提供者
//   - bootstrappingFromSnapshot: 是否从快照初始化
func (l *kvLedger) registerStateDBIndexCreatorForChaincodeLifecycleEvents(
	stateDBIndexCreator cceventmgmt.ChaincodeLifecycleEventListener,
	deployedChaincodesInfoExtractor ledger.DeployedChaincodeInfoProvider,
	chaincodesLifecycleEventsProvider ledger.ChaincodeLifecycleEventProvider,
	legacyChaincodesLifecycleEventsProvider *cceventmgmt.Mgr,
	bootstrappingFromSnapshot bool,
) error {
	if !bootstrappingFromSnapshot {
		// 分类账的定期期初
		if err := chaincodesLifecycleEventsProvider.RegisterListener(
			l.ledgerID, &ccEventListenerAdaptor{stateDBIndexCreator}, false); err != nil {
			return err
		}
		legacyChaincodesLifecycleEventsProvider.Register(l.ledgerID, stateDBIndexCreator)
		return nil
	}

	// opening of ledger after creating from a snapshot -
	// it would have been better if we could explicitly retrieve the list of invocable chaincodes instead of
	// passing the flag initializer.initializingFromSnapshot to the ccLifecycleEventProvider (which is essentially
	// the _lifecycle cache) for directing ccLifecycleEventProvider to call us back. However, the lock that ensures
	// the synchronization with the chaincode installer is maintained in the lifecycle cache and by design the lifecycle
	// cache takes the responsibility of calling any listener under the lock
	if err := chaincodesLifecycleEventsProvider.RegisterListener(
		l.ledgerID, &ccEventListenerAdaptor{stateDBIndexCreator}, true); err != nil {
		return errors.WithMessage(err, "error while creating statdb indexes after bootstrapping from snapshot")
	}

	legacyChaincodes, err := l.listLegacyChaincodesDefined(deployedChaincodesInfoExtractor)
	if err != nil {
		return errors.WithMessage(err, "error while creating statdb indexes after bootstrapping from snapshot")
	}

	if err := legacyChaincodesLifecycleEventsProvider.RegisterAndInvokeFor(
		legacyChaincodes, l.ledgerID, stateDBIndexCreator); err != nil {
		return errors.WithMessage(err, "error while creating statdb indexes after bootstrapping from snapshot")
	}
	return nil
}

func (l *kvLedger) listLegacyChaincodesDefined(
	deployedChaincodesInfoExtractor ledger.DeployedChaincodeInfoProvider) (
	[]*cceventmgmt.ChaincodeDefinition, error) {
	qe, err := l.txmgr.NewQueryExecutor("")
	if err != nil {
		return nil, err
	}
	defer qe.Done()

	definedChaincodes, err := deployedChaincodesInfoExtractor.AllChaincodesInfo(l.ledgerID, qe)
	if err != nil {
		return nil, err
	}

	legacyChaincodes := []*cceventmgmt.ChaincodeDefinition{}
	for _, chaincodeInfo := range definedChaincodes {
		if !chaincodeInfo.IsLegacy {
			continue
		}
		legacyChaincodes = append(legacyChaincodes,
			&cceventmgmt.ChaincodeDefinition{
				Name:              chaincodeInfo.Name,
				Version:           chaincodeInfo.Version,
				Hash:              chaincodeInfo.Hash,
				CollectionConfigs: chaincodeInfo.ExplicitCollectionConfigPkg,
			},
		)
	}
	return legacyChaincodes, nil
}

// initTxMgr 初始化事务管理器。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：
//   - initializer：txmgr.Initializer类型的指针，用于初始化事务管理器。
//
// 返回值：
//   - error：如果初始化事务管理器时出错，则返回错误。
func (l *kvLedger) initTxMgr(initializer *txmgr.Initializer) error {
	var err error
	// 创建一个LockBasedTxMgr实例
	txmgr, err := txmgr.NewLockBasedTxMgr(initializer)
	if err != nil {
		return err
	}
	// 将txmgr赋值给kvLedger的txmgr字段
	l.txmgr = txmgr
	// 这是一个用于填充生命周期缓存的解决方法。
	// 有关详细信息，请参阅此函数的注释
	qe, err := txmgr.NewQueryExecutorNoCollChecks()
	if err != nil {
		return err
	}
	defer qe.Done()
	// 遍历initializer中的StateListeners，并调用其Initialize方法进行初始化
	for _, sl := range initializer.StateListeners {
		if err := sl.Initialize(l.ledgerID, qe); err != nil {
			return err
		}
	}
	return err
}

// initSnapshotMgr 用于初始化快照管理器。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：
//   - initializer：lgrInitializer类型的指针，包含初始化所需的信息。
//
// 返回值：
//   - error：如果初始化快照管理器过程中出错，则返回错误。
func (l *kvLedger) initSnapshotMgr(initializer *lgrInitializer) error {
	// 获取快照请求的数据库句柄
	dbHandle := initializer.bookkeeperProvider.GetDBHandle(l.ledgerID, bookkeeping.SnapshotRequest)
	// 创建快照请求的bookkeeper, 维护待处理快照请求的最小区块号(账本id, 数据库处理函数, 待处理快照请求的最小区块号)
	bookkeeper, err := newSnapshotRequestBookkeeper(l.ledgerID, dbHandle)
	if err != nil {
		return err
	}

	// 初始化快照管理器
	l.snapshotMgr = &snapshotMgr{
		snapshotRequestBookkeeper: bookkeeper,
		events:                    make(chan *event),
		commitProceed:             make(chan struct{}),
		requestResponses:          make(chan *requestResponse),
	}

	// 获取区块链信息(高度、当前hash、上一个hsash)
	bcInfo, err := l.blockStore.GetBlockchainInfo()
	if err != nil {
		return err
	}
	lastCommittedBlock := bcInfo.Height - 1

	// 启动一个goroutine来同步提交、快照生成和快照提交/取消
	go l.processSnapshotMgmtEvents(lastCommittedBlock)

	// 如果区块链高度不为0，则重新生成丢失的快照
	if bcInfo.Height != 0 {
		// 用于重新生成丢失的快照
		return l.regenrateMissedSnapshot(lastCommittedBlock)
	}
	return nil
}

// lastPersistedCommitHash 返回最后一个持久化的提交哈希。
// 方法接收者：l（kvLedger类型的指针）
// 返回值：
//   - []byte：最后一个持久化的提交哈希。
//   - error：如果获取最后一个持久化的提交哈希时出错，则返回错误。
func (l *kvLedger) lastPersistedCommitHash() ([]byte, error) {
	// 获取区块链信息(高度、当前hash、上一个hsash)
	bcInfo, err := l.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	// 如果区块链高度为0，则表示链为空，返回nil
	if bcInfo.Height == 0 {
		logger.Debugf("区块链高度为0, 链是空的")
		return nil, nil
	}

	// 如果存在引导快照元数据，并且引导快照元数据的最后一个区块号等于bcInfo.Height-1
	// 则表示在从快照创建后第一次启动账本，从引导快照元数据中检索最后一个提交哈希
	if l.bootSnapshotMetadata != nil && l.bootSnapshotMetadata.LastBlockNumber == bcInfo.Height-1 {
		logger.Debugw(
			"从快照创建后首次启动分类帐, 从启动快照元数据中检索上次提交的哈希",
			"ledger", l.ledgerID,
		)
		// LastBlockCommitHashInHex = 最后一个区块的提交哈希（十六进制表示）
		return hex.DecodeString(l.bootSnapshotMetadata.LastBlockCommitHashInHex)
	}

	logger.Debugf("获取块 [%d] 以检索 currentCommitHash", bcInfo.Height-1)
	// 获取区块高度为bcInfo.Height-1的区块, 根据给定的高度返回区块
	block, err := l.GetBlockByNumber(bcInfo.Height - 1)
	if err != nil {
		return nil, err
	}

	// 检查最后一个区块的元数据是否包含提交哈希
	if len(block.Metadata.Metadata) < int(common.BlockMetadataIndex_COMMIT_HASH+1) {
		logger.Debugf("最后一个块元数据不包含提交哈希")
		return nil, nil
	}

	// 反序列化最后一个区块的元数据中的提交哈希, 元数据是用于对块元数据进行编码的通用结构
	commitHash := &common.Metadata{}
	err = proto.Unmarshal(block.Metadata.Metadata[common.BlockMetadataIndex_COMMIT_HASH], commitHash)
	if err != nil {
		return nil, errors.Wrap(err, "反序列化最后一个区块的元数据中的提交哈希")
	}
	return commitHash.Value, nil
}

// isPvtDataStoreAheadOfBlockStore 检查私有数据存储是否领先于区块存储。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：无
// 返回值：
//   - bool：如果私有数据存储领先于区块存储，则返回true；否则返回false。
//   - error：如果获取区块链信息或最后提交的区块高度时出错，则返回错误。
func (l *kvLedger) isPvtDataStoreAheadOfBlockStore() (bool, error) {
	// 获取区块存储的区块链信息
	blockStoreInfo, err := l.blockStore.GetBlockchainInfo()
	if err != nil {
		return false, err
	}

	// 获取私有数据存储的最后提交的区块高度
	pvtstoreHeight, err := l.pvtdataStore.LastCommittedBlockHeight()
	if err != nil {
		return false, err
	}

	// 比较私有数据存储的最后提交的区块高度和区块存储的区块链高度
	return pvtstoreHeight > blockStoreInfo.Height, nil
}

// recoverDBs 用于恢复数据库。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：无
// 返回值：
//   - error：如果在恢复数据过程中出错，则返回错误。
func (l *kvLedger) recoverDBs() error {
	logger.Debugf("进入恢复数据阶段 recoverDB()")
	// 同步状态数据库和历史数据库与区块存储
	if err := l.syncStateAndHistoryDBWithBlockstore(); err != nil {
		return err
	}

	// 同步状态数据库与旧版块私有数据
	return l.syncStateDBWithOldBlkPvtdata()
}

func (l *kvLedger) syncStateAndHistoryDBWithBlockstore() error {
	// If there is no block in blockstorage, nothing to recover.
	info, _ := l.blockStore.GetBlockchainInfo()
	if info.Height == 0 {
		logger.Debug("Block storage is empty.")
		return nil
	}
	lastBlockInBlockStore := info.Height - 1
	recoverables := []recoverable{l.txmgr}
	if l.historyDB != nil {
		recoverables = append(recoverables, l.historyDB)
	}
	recoverers := []*recoverer{}
	for _, recoverable := range recoverables {
		// nextRequiredBlock is nothing but the nextBlockNum expected by the state DB.
		// In other words, the nextRequiredBlock is nothing but the height of stateDB.
		recoverFlag, nextRequiredBlock, err := recoverable.ShouldRecover(lastBlockInBlockStore)
		if err != nil {
			return err
		}

		if l.bootSnapshotMetadata != nil {
			lastBlockInSnapshot := l.bootSnapshotMetadata.LastBlockNumber
			if nextRequiredBlock <= lastBlockInSnapshot {
				return errors.Errorf(
					"recovery for DB [%s] not possible. Ledger [%s] is created from a snapshot. Last block in snapshot = [%d], DB needs block [%d] onward",
					recoverable.Name(),
					l.ledgerID,
					lastBlockInSnapshot,
					nextRequiredBlock,
				)
			}
		}

		if nextRequiredBlock > lastBlockInBlockStore+1 {
			dbName := recoverable.Name()
			return fmt.Errorf("the %s database [height=%d] is ahead of the block store [height=%d]. "+
				"This is possible when the %s database is not dropped after a ledger reset/rollback. "+
				"The %s database can safely be dropped and will be rebuilt up to block store height upon the next peer start",
				dbName, nextRequiredBlock, lastBlockInBlockStore+1, dbName, dbName)
		}
		if recoverFlag {
			recoverers = append(recoverers, &recoverer{nextRequiredBlock, recoverable})
		}
	}
	if len(recoverers) == 0 {
		return nil
	}
	if len(recoverers) == 1 {
		return l.recommitLostBlocks(recoverers[0].nextRequiredBlock, lastBlockInBlockStore, recoverers[0].recoverable)
	}

	// both dbs need to be recovered
	if recoverers[0].nextRequiredBlock > recoverers[1].nextRequiredBlock {
		// swap (put the lagger db at 0 index)
		recoverers[0], recoverers[1] = recoverers[1], recoverers[0]
	}
	if recoverers[0].nextRequiredBlock != recoverers[1].nextRequiredBlock {
		// bring the lagger db equal to the other db
		if err := l.recommitLostBlocks(recoverers[0].nextRequiredBlock, recoverers[1].nextRequiredBlock-1,
			recoverers[0].recoverable); err != nil {
			return err
		}
	}
	// get both the db upto block storage
	return l.recommitLostBlocks(recoverers[1].nextRequiredBlock, lastBlockInBlockStore,
		recoverers[0].recoverable, recoverers[1].recoverable)
}

func (l *kvLedger) syncStateDBWithOldBlkPvtdata() error {
	// TODO: syncStateDBWithOldBlkPvtdata, GetLastUpdatedOldBlocksPvtData(),
	// and ResetLastUpdatedOldBlocksList() can be removed in > v2 LTS.
	// From v2.0 onwards, we do not store the last updatedBlksList.
	// Only to support the rolling upgrade from v14 LTS to v2 LTS, we
	// retain these three functions in v2.0 - FAB-16294.

	blocksPvtData, err := l.pvtdataStore.GetLastUpdatedOldBlocksPvtData()
	if err != nil {
		return err
	}

	// Assume that the peer has restarted after a rollback or a reset.
	// As the pvtdataStore can contain pvtData of yet to be committed blocks,
	// we need to filter them before passing it to the transaction manager
	// for stateDB updates.
	if err := l.filterYetToCommitBlocks(blocksPvtData); err != nil {
		return err
	}

	if err = l.applyValidTxPvtDataOfOldBlocks(blocksPvtData); err != nil {
		return err
	}

	return l.pvtdataStore.ResetLastUpdatedOldBlocksList()
}

func (l *kvLedger) filterYetToCommitBlocks(blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	info, err := l.blockStore.GetBlockchainInfo()
	if err != nil {
		return err
	}
	for blkNum := range blocksPvtData {
		if blkNum > info.Height-1 {
			logger.Infof("found pvtdata associated with yet to be committed block [%d]", blkNum)
			delete(blocksPvtData, blkNum)
		}
	}
	return nil
}

// recommitLostBlocks retrieves blocks in specified range and commit the write set to either
// state DB or history DB or both
func (l *kvLedger) recommitLostBlocks(firstBlockNum uint64, lastBlockNum uint64, recoverables ...recoverable) error {
	logger.Infof("Recommitting lost blocks - firstBlockNum=%d, lastBlockNum=%d, recoverables=%#v", firstBlockNum, lastBlockNum, recoverables)
	var err error
	var blockAndPvtdata *ledger.BlockAndPvtData
	for blockNumber := firstBlockNum; blockNumber <= lastBlockNum; blockNumber++ {
		if blockAndPvtdata, err = l.GetPvtDataAndBlockByNum(blockNumber, nil); err != nil {
			return err
		}
		for _, r := range recoverables {
			if err := r.CommitLostBlock(blockAndPvtdata); err != nil {
				return err
			}
		}
	}
	logger.Infof("Recommitted lost blocks - firstBlockNum=%d, lastBlockNum=%d, recoverables=%#v", firstBlockNum, lastBlockNum, recoverables)
	return nil
}

// TxIDExists returns true if the specified txID is already present in one of the already committed blocks
func (l *kvLedger) TxIDExists(txID string) (bool, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	return l.blockStore.TxIDExists(txID)
}

// GetTransactionByID retrieves a transaction by id
func (l *kvLedger) GetTransactionByID(txID string) (*peer.ProcessedTransaction, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	tranEnv, err := l.blockStore.RetrieveTxByID(txID)
	if err != nil {
		return nil, err
	}
	txVResult, _, err := l.blockStore.RetrieveTxValidationCodeByTxID(txID)
	if err != nil {
		return nil, err
	}
	processedTran := &peer.ProcessedTransaction{TransactionEnvelope: tranEnv, ValidationCode: int32(txVResult)}
	return processedTran, nil
}

// GetBlockchainInfo 返回区块链的基本信息(高度、当前hash、上一个hsash)。
// 方法接收者：l（kvLedger类型的指针）
// 返回值：
//   - *common.BlockchainInfo：区块链的基本信息。
//   - error：如果获取区块链信息时出错，则返回错误。
func (l *kvLedger) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	// 获取读锁
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	// 调用blockStore的GetBlockchainInfo方法获取区块链信息(高度、当前hash、上一个hsash)
	bcInfo, err := l.blockStore.GetBlockchainInfo()
	return bcInfo, err
}

// GetBlockByNumber 根据给定的高度返回区块。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：
//   - blockNumber：要获取的区块的高度。使用math.MaxUint64将返回最后一个区块。
//
// 返回值：
//   - *common.Block：指定高度的区块。
//   - error：如果获取区块时出错，则返回错误。
func (l *kvLedger) GetBlockByNumber(blockNumber uint64) (*common.Block, error) {
	// 获取读锁
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	// 调用blockStore的RetrieveBlockByNumber方法获取指定高度的区块
	block, err := l.blockStore.RetrieveBlockByNumber(blockNumber)
	return block, err
}

// GetBlocksIterator returns an iterator that starts from `startBlockNumber`(inclusive).
// The iterator is a blocking iterator i.e., it blocks till the next block gets available in the ledger
// ResultsIterator contains type BlockHolder
func (l *kvLedger) GetBlocksIterator(startBlockNumber uint64) (commonledger.ResultsIterator, error) {
	blkItr, err := l.blockStore.RetrieveBlocks(startBlockNumber)
	if err != nil {
		return nil, err
	}
	return &blocksItr{l.blockAPIsRWLock, blkItr}, nil
}

// GetBlockByHash returns a block given it's hash
func (l *kvLedger) GetBlockByHash(blockHash []byte) (*common.Block, error) {
	block, err := l.blockStore.RetrieveBlockByHash(blockHash)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock() //lint:ignore SA2001 syncpoint
	return block, err
}

// GetBlockByTxID returns a block which contains a transaction
func (l *kvLedger) GetBlockByTxID(txID string) (*common.Block, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	block, err := l.blockStore.RetrieveBlockByTxID(txID)
	return block, err
}

// GetTxValidationCodeByTxID returns transaction validation code and block number in which the transaction was committed
func (l *kvLedger) GetTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	txValidationCode, blkNum, err := l.blockStore.RetrieveTxValidationCodeByTxID(txID)
	return txValidationCode, blkNum, err
}

// NewTxSimulator returns new `ledger.TxSimulator`
func (l *kvLedger) NewTxSimulator(txid string) (ledger.TxSimulator, error) {
	return l.txmgr.NewTxSimulator(txid)
}

// NewQueryExecutor gives handle to a query executor.
// A client can obtain more than one 'QueryExecutor's for parallel execution.
// Any synchronization should be performed at the implementation level if required
func (l *kvLedger) NewQueryExecutor() (ledger.QueryExecutor, error) {
	return l.txmgr.NewQueryExecutor(util.GenerateUUID())
}

// NewHistoryQueryExecutor gives handle to a history query executor.
// A client can obtain more than one 'HistoryQueryExecutor's for parallel execution.
// Any synchronization should be performed at the implementation level if required
// Pass the ledger blockstore so that historical values can be looked up from the chain
func (l *kvLedger) NewHistoryQueryExecutor() (ledger.HistoryQueryExecutor, error) {
	if l.historyDB != nil {
		return l.historyDB.NewQueryExecutor(l.blockStore)
	}
	return nil, nil
}

// CommitLegacy 方法在一个原子操作中提交区块及其对应的私有数据。
// 它通过事件和 commitProceed 通道同步提交操作、快照生成和快照请求。
// 在提交区块之前，它发送一个 commitStart 事件，并等待来自 commitProceed 通道的消息。
// 区块提交后，它发送一个 commitDone 事件。
// 参考 processEvents 函数了解通道和事件如何协同工作以处理同步。
func (l *kvLedger) CommitLegacy(pvtdataAndBlock *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	// 获取区块编号
	blockNumber := pvtdataAndBlock.Block.Header.Number

	// 向事件通道发送 commitStart 事件, 保证原子性
	l.snapshotMgr.events <- &event{commitStart, blockNumber}
	// 等待 commitProceed 通道的消息
	<-l.snapshotMgr.commitProceed

	// 执行提交操作
	if err := l.commit(pvtdataAndBlock, commitOpts); err != nil {
		// 如果提交过程中发生错误，返回错误
		return err
	}

	// 提交完成后，向事件通道发送 commitDone 事件
	l.snapshotMgr.events <- &event{commitDone, blockNumber}
	// 成功提交，返回 nil 表示无错误
	return nil
}

// commit 方法在一个原子操作中提交区块和相应的私有数据。
func (l *kvLedger) commit(pvtdataAndBlock *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	var err error
	block := pvtdataAndBlock.Block
	blockNo := pvtdataAndBlock.Block.Header.Number

	// 记录开始处理区块的时间点
	startBlockProcessing := time.Now()
	// 如果配置允许从账本中获取私有数据
	if commitOpts.FetchPvtDataFromLedger {
		// 当我们到达这里时，意味着私有数据存储已包含与该区块相关的私有数据，
		// 但状态数据库可能还没有这些数据。在提交这个区块时，私有数据存储中不会有任何更新，
		// 因为它已经包含了所需的全部数据。

		// 如果存在缺失的私有数据，reconciler 将会获取并更新私有数据存储和状态数据库。
		// 因此，我们可以从私有数据存储中获取可用的数据。如果与区块关联的任何或全部私有数据
		// 已过期且不再存在于私有数据存储中，最终这些私有数据也会在状态数据库中过期（尽管在此之前会缺失私有数据）。
		txPvtData, err := l.pvtdataStore.GetPvtDataByBlockNum(blockNo, nil)
		if err != nil {
			return err
		}
		pvtdataAndBlock.PvtData = convertTxPvtDataArrayToMap(txPvtData)
	}

	// 输出调试日志：验证区块的状态
	logger.Debugf("[%s] 验证区块 [%d] 的状态", l.ledgerID, blockNo)
	// 验证并准备区块，用于提交
	txstatsInfo, updateBatchBytes, err := l.txmgr.ValidateAndPrepare(pvtdataAndBlock, true)
	if err != nil {
		return err
	}
	// 计算处理区块所花费的时间
	elapsedBlockProcessing := time.Since(startBlockProcessing)

	// 记录开始提交到区块存储和私有数据存储的时间点
	startBlockstorageAndPvtdataCommit := time.Now()
	logger.Debugf("[%s] 将 CommitHash 添加到块 [%d]", l.ledgerID, blockNo)

	// 添加提交哈希值到区块
	// 确保只有在创世区块之后才计算并添加commitHash。
	// 换句话说，在加入新通道或节点重置后，commitHash才会被添加到区块中。
	if block.Header.Number == 1 || len(l.commitHash) != 0 {
		l.addBlockCommitHash(pvtdataAndBlock.Block, updateBatchBytes)
	}

	// 输出调试日志：提交区块和私有数据至存储
	logger.Infof("[%s] 向存储提交区块 [%d] 和私有数据", l.ledgerID, blockNo)
	// 加锁以保证线程安全
	l.blockAPIsRWLock.Lock()
	defer l.blockAPIsRWLock.Unlock()
	// 实际提交到区块存储和私有数据存储
	if err = l.commitToPvtAndBlockStore(pvtdataAndBlock); err != nil {
		return err
	}
	// 计算提交到区块存储和私有数据存储所花费的时间
	elapsedBlockstorageAndPvtdataCommit := time.Since(startBlockstorageAndPvtdataCommit)

	// 记录开始提交到状态数据库的时间点
	startCommitState := time.Now()
	// 输出调试日志：提交区块事务至状态数据库
	logger.Infof("[%s] 向状态数据库提交区块 [%d] 的事务", l.ledgerID, blockNo)
	// 提交事务到状态数据库
	if err = l.txmgr.Commit(); err != nil {
		panic(errors.WithMessage(err, "在提交事务到txmgr期间出错"))
	}
	// 计算提交到状态数据库所花费的时间
	elapsedCommitState := time.Since(startCommitState)

	// 历史数据库的写入可以并行于状态数据库和/或异步执行作为未来的优化，
	// 虽然目前它不是一个瓶颈……无需在日志中记录耗时详情。
	if l.historyDB != nil {
		// 输出调试日志：提交区块事务至历史数据库
		logger.Infof("[%s] 向历史数据库提交区块 [%d] 的事务", l.ledgerID, blockNo)
		// 提交事务到历史数据库
		if err := l.historyDB.Commit(block); err != nil {
			panic(errors.WithMessage(err, "在提交到历史数据库期间出错"))
		}
	}

	// 输出信息日志：完成区块提交
	logger.Infof("通道[%s] 已提交区块 [%d] 的 %d 笔交易记录 耗时 %dms (状态验证=%dms; 区块和私有数据提交=%dms; 状态数据库提交=%dms), commitHash=[%x]",
		l.ledgerID, block.Header.Number, len(block.Data.Data),
		time.Since(startBlockProcessing)/time.Millisecond,
		elapsedBlockProcessing/time.Millisecond,
		elapsedBlockstorageAndPvtdataCommit/time.Millisecond,
		elapsedCommitState/time.Millisecond,
		l.commitHash,
	)

	// 更新区块统计信息
	l.updateBlockStats(
		elapsedBlockProcessing,
		elapsedBlockstorageAndPvtdataCommit,
		elapsedCommitState,
		txstatsInfo,
	)

	// 发送区块提交通知
	l.sendCommitNotification(blockNo, txstatsInfo)
	return nil
}

func (l *kvLedger) commitToPvtAndBlockStore(blockAndPvtdata *ledger.BlockAndPvtData) error {
	pvtdataStoreHt, err := l.pvtdataStore.LastCommittedBlockHeight()
	if err != nil {
		return err
	}
	blockNum := blockAndPvtdata.Block.Header.Number

	if !l.isPvtstoreAheadOfBlkstore.Load().(bool) {
		logger.Debugf("Writing block [%d] to pvt data store", blockNum)
		// If a state fork occurs during a regular block commit,
		// we have a mechanism to drop all blocks followed by refetching of blocks
		// and re-processing them. In the current way of doing this, we only drop
		// the block files (and related artifacts) but we do not drop/overwrite the
		// pvtdatastorage as it might leads to data loss.
		// During block reprocessing, as there is a possibility of an invalid pvtdata
		// transaction to become valid, we store the pvtdata of invalid transactions
		// too in the pvtdataStore as we do for the publicdata in the case of blockStore.
		// Hence, we pass all pvtData present in the block to the pvtdataStore committer.
		pvtData, missingPvtData := constructPvtDataAndMissingData(blockAndPvtdata)
		if err := l.pvtdataStore.Commit(blockNum, pvtData, missingPvtData); err != nil {
			return err
		}
	} else {
		logger.Debugf("Skipping writing pvtData to pvt block store as it ahead of the block store")
	}

	if err := l.blockStore.AddBlock(blockAndPvtdata.Block); err != nil {
		return err
	}

	if pvtdataStoreHt == blockNum+1 {
		// Only when the pvtdataStore was ahead of blockStore
		// during the ledger initialization time, we reach here.
		// The pvtdataStore would be ahead of blockstore when
		// the peer restarts after a reset of rollback.
		l.isPvtstoreAheadOfBlkstore.Store(false)
	}

	return nil
}

func convertTxPvtDataArrayToMap(txPvtData []*ledger.TxPvtData) ledger.TxPvtDataMap {
	txPvtDataMap := make(ledger.TxPvtDataMap)
	for _, pvtData := range txPvtData {
		txPvtDataMap[pvtData.SeqInBlock] = pvtData
	}
	return txPvtDataMap
}

func (l *kvLedger) updateBlockStats(
	blockProcessingTime time.Duration,
	blockstorageAndPvtdataCommitTime time.Duration,
	statedbCommitTime time.Duration,
	txstatsInfo []*validation.TxStatInfo,
) {
	l.stats.updateBlockProcessingTime(blockProcessingTime)
	l.stats.updateBlockstorageAndPvtdataCommitTime(blockstorageAndPvtdataCommitTime)
	l.stats.updateStatedbCommitTime(statedbCommitTime)
	l.stats.updateTransactionsStats(txstatsInfo)
}

// GetMissingPvtDataInfoForMostRecentBlocks returns the missing private data information for the
// most recent `maxBlock` blocks which miss at least a private data of a eligible collection.
func (l *kvLedger) GetMissingPvtDataInfoForMostRecentBlocks(maxBlock int) (ledger.MissingPvtDataInfo, error) {
	// the missing pvtData info in the pvtdataStore could belong to a block which is yet
	// to be processed and committed to the blockStore and stateDB (such a scenario is possible
	// after a peer rollback). In such cases, we cannot return missing pvtData info. Otherwise,
	// we would end up in an inconsistent state database.
	if l.isPvtstoreAheadOfBlkstore.Load().(bool) {
		return nil, nil
	}
	// it is safe to not acquire a read lock on l.blockAPIsRWLock. Without a lock, the value of
	// lastCommittedBlock can change due to a new block commit. As a result, we may not
	// be able to fetch the missing data info of truly the most recent blocks. This
	// decision was made to ensure that the regular block commit rate is not affected.
	return l.pvtdataStore.GetMissingPvtDataInfoForMostRecentBlocks(maxBlock)
}

func (l *kvLedger) addBlockCommitHash(block *common.Block, updateBatchBytes []byte) {
	var valueBytes []byte

	txValidationCode := block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
	valueBytes = append(valueBytes, proto.EncodeVarint(uint64(len(txValidationCode)))...)
	valueBytes = append(valueBytes, txValidationCode...)
	valueBytes = append(valueBytes, updateBatchBytes...)
	valueBytes = append(valueBytes, l.commitHash...)

	l.commitHash = util.ComputeSHA256(valueBytes)
	block.Metadata.Metadata[common.BlockMetadataIndex_COMMIT_HASH] = protoutil.MarshalOrPanic(&common.Metadata{Value: l.commitHash})
}

// GetPvtDataAndBlockByNum returns the block and the corresponding pvt data.
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataAndBlockByNum(blockNum uint64, filter ledger.PvtNsCollFilter) (*ledger.BlockAndPvtData, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()

	var block *common.Block
	var pvtdata []*ledger.TxPvtData
	var err error

	if block, err = l.blockStore.RetrieveBlockByNumber(blockNum); err != nil {
		return nil, err
	}

	if pvtdata, err = l.pvtdataStore.GetPvtDataByBlockNum(blockNum, filter); err != nil {
		return nil, err
	}

	return &ledger.BlockAndPvtData{Block: block, PvtData: constructPvtdataMap(pvtdata)}, nil
}

// GetPvtDataByNum returns only the pvt data  corresponding to the given block number
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataByNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	var pvtdata []*ledger.TxPvtData
	var err error
	if pvtdata, err = l.pvtdataStore.GetPvtDataByBlockNum(blockNum, filter); err != nil {
		return nil, err
	}
	return pvtdata, nil
}

// DoesPvtDataInfoExist 方法用于检查给定区块号的私有数据信息是否已存在于分类账中。
// 方法返回true的情况包括：
// （1）分类账中已经有关联于给定区块号的私有数据；
// （2）虽然部分或全部私有数据丢失，但丢失的信息已被记录在分类账中；
// （3）区块已经被提交，但它不包含任何带有私有数据的交易。
func (l *kvLedger) DoesPvtDataInfoExist(blockNum uint64) (bool, error) {
	// 获取私有数据存储中最后提交的区块高度
	pvtStoreHt, err := l.pvtdataStore.LastCommittedBlockHeight()
	if err != nil {
		// 如果获取最后提交的区块高度失败，直接返回错误
		return false, err
	}

	// 判断给定的区块号加一是否小于等于私有数据存储中最后提交的区块高度
	// 这里加一是因为区块号从零开始，而高度是从一开始
	return blockNum+1 <= pvtStoreHt, nil
}

func (l *kvLedger) GetConfigHistoryRetriever() (ledger.ConfigHistoryRetriever, error) {
	return l.configHistoryRetriever, nil
}

func (l *kvLedger) CommitPvtDataOfOldBlocks(reconciledPvtdata []*ledger.ReconciledPvtdata, unreconciled ledger.MissingPvtDataInfo) ([]*ledger.PvtdataHashMismatch, error) {
	logger.Debugf("[%s:] Comparing pvtData of [%d] old blocks against the hashes in transaction's rwset to find valid and invalid data",
		l.ledgerID, len(reconciledPvtdata))

	lastBlockInBootstrapSnapshot := uint64(0)
	if l.bootSnapshotMetadata != nil {
		lastBlockInBootstrapSnapshot = l.bootSnapshotMetadata.LastBlockNumber
	}

	hashVerifiedPvtData, hashMismatches, err := constructValidAndInvalidPvtData(
		reconciledPvtdata, l.blockStore, l.pvtdataStore, lastBlockInBootstrapSnapshot,
	)
	if err != nil {
		return nil, err
	}

	err = l.applyValidTxPvtDataOfOldBlocks(hashVerifiedPvtData)
	if err != nil {
		return nil, err
	}

	logger.Debugf("[%s:] Committing pvtData of [%d] old blocks to the pvtdatastore", l.ledgerID, len(reconciledPvtdata))

	err = l.pvtdataStore.CommitPvtDataOfOldBlocks(hashVerifiedPvtData, unreconciled)
	if err != nil {
		return nil, err
	}

	return hashMismatches, nil
}

func (l *kvLedger) applyValidTxPvtDataOfOldBlocks(hashVerifiedPvtData map[uint64][]*ledger.TxPvtData) error {
	logger.Debugf("[%s:] Filtering pvtData of invalidation transactions", l.ledgerID)

	lastBlockInBootstrapSnapshot := uint64(0)
	if l.bootSnapshotMetadata != nil {
		lastBlockInBootstrapSnapshot = l.bootSnapshotMetadata.LastBlockNumber
	}
	committedPvtData, err := filterPvtDataOfInvalidTx(hashVerifiedPvtData, l.blockStore, lastBlockInBootstrapSnapshot)
	if err != nil {
		return err
	}

	// Assume the peer fails after storing the pvtData of old block in the stateDB but before
	// storing it in block store. When the peer starts again, the reconciler finds that the
	// pvtData is missing in the ledger store and hence, it would fetch those data again. As
	// a result, RemoveStaleAndCommitPvtDataOfOldBlocks gets already existing data. In this
	// scenario, RemoveStaleAndCommitPvtDataOfOldBlocks just replaces the old entry as we
	// always makes the comparison between hashed version and this pvtData. There is no
	// problem in terms of data consistency. However, if the reconciler is disabled before
	// the peer restart, then the pvtData in stateDB may not be in sync with the pvtData in
	// ledger store till the reconciler is enabled.
	logger.Debugf("[%s:] Committing pvtData of [%d] old blocks to the stateDB", l.ledgerID, len(hashVerifiedPvtData))
	return l.txmgr.RemoveStaleAndCommitPvtDataOfOldBlocks(committedPvtData)
}

func (l *kvLedger) GetMissingPvtDataTracker() (ledger.MissingPvtDataTracker, error) {
	return l, nil
}

type commitNotifier struct {
	dataChannel chan *ledger.CommitNotification
	doneChannel <-chan struct{}
}

// CommitNotificationsChannel returns a read-only channel on which ledger sends a `CommitNotification`
// when a block is committed. The CommitNotification contains entries for the transactions from the committed block,
// which are not malformed, carry a legitimate TxID, and in addition, are not marked as a duplicate transaction.
// The consumer can close the 'done' channel to signal that the notifications are no longer needed. This will cause the
// CommitNotifications channel to close. There is expected to be only one consumer at a time. The function returns error
// if already a CommitNotification channel is active.
func (l *kvLedger) CommitNotificationsChannel(done <-chan struct{}) (<-chan *ledger.CommitNotification, error) {
	l.commitNotifierLock.Lock()
	defer l.commitNotifierLock.Unlock()

	if l.commitNotifier != nil {
		return nil, errors.New("only one commit notifications channel is allowed at a time")
	}

	l.commitNotifier = &commitNotifier{
		dataChannel: make(chan *ledger.CommitNotification, 10),
		doneChannel: done,
	}

	return l.commitNotifier.dataChannel, nil
}

func (l *kvLedger) sendCommitNotification(blockNum uint64, txStatsInfo []*validation.TxStatInfo) {
	l.commitNotifierLock.Lock()
	defer l.commitNotifierLock.Unlock()

	if l.commitNotifier == nil {
		return
	}

	select {
	case <-l.commitNotifier.doneChannel:
		close(l.commitNotifier.dataChannel)
		l.commitNotifier = nil
	default:
		txsByID := map[string]struct{}{}
		txs := []*ledger.CommitNotificationTxInfo{}
		for _, t := range txStatsInfo {
			txID := t.TxIDFromChannelHeader
			_, ok := txsByID[txID]

			if txID == "" || ok {
				continue
			}
			txsByID[txID] = struct{}{}

			txs = append(txs, &ledger.CommitNotificationTxInfo{
				TxType:             t.TxType,
				TxID:               t.TxIDFromChannelHeader,
				ValidationCode:     t.ValidationCode,
				ChaincodeID:        t.ChaincodeID,
				ChaincodeEventData: t.ChaincodeEventData,
			})
		}

		l.commitNotifier.dataChannel <- &ledger.CommitNotification{
			BlockNumber: blockNum,
			TxsInfo:     txs,
		}
	}
}

// Close closes `KVLedger`.
// Currently this function is only used by test code. The caller should make sure no in-progress commit
// or snapshot generation before calling this function. Otherwise, the ledger may have unknown behavior
// and cause panic.
func (l *kvLedger) Close() {
	l.blockStore.Shutdown()
	l.txmgr.Shutdown()
	l.snapshotMgr.shutdown()
}

type blocksItr struct {
	blockAPIsRWLock *sync.RWMutex
	blocksItr       commonledger.ResultsIterator
}

func (itr *blocksItr) Next() (commonledger.QueryResult, error) {
	block, err := itr.blocksItr.Next()
	if err != nil {
		return nil, err
	}
	itr.blockAPIsRWLock.RLock()
	itr.blockAPIsRWLock.RUnlock() //lint:ignore SA2001 syncpoint
	return block, nil
}

func (itr *blocksItr) Close() {
	itr.blocksItr.Close()
}

type collectionInfoRetriever struct {
	ledgerID     string
	ledger       ledger.PeerLedger
	infoProvider ledger.DeployedChaincodeInfoProvider
}

func (r *collectionInfoRetriever) CollectionInfo(chaincodeName, collectionName string) (*peer.StaticCollectionConfig, error) {
	qe, err := r.ledger.NewQueryExecutor()
	if err != nil {
		return nil, err
	}
	defer qe.Done()
	return r.infoProvider.CollectionInfo(r.ledgerID, chaincodeName, collectionName, qe)
}

// collectionConfigHistoryRetriever 是一个结构体，用于检索集合配置历史记录。
type collectionConfigHistoryRetriever struct {
	*confighistory.Retriever             // 帮助消费者检索集合配置历史记录(账本id, 数据库处理函数)
	ledger.DeployedChaincodeInfoProvider // 部署的链码信息提供者，由账本用于构建集合配置历史记录

	ledger *kvLedger // 提供了基于键值的数据模型
}

func (r *collectionConfigHistoryRetriever) MostRecentCollectionConfigBelow(
	blockNum uint64,
	chaincodeName string,
) (*ledger.CollectionConfigInfo, error) {
	explicitCollections, err := r.Retriever.MostRecentCollectionConfigBelow(blockNum, chaincodeName)
	if err != nil {
		return nil, errors.WithMessage(err, "error while retrieving explicit collections")
	}
	qe, err := r.ledger.NewQueryExecutor()
	if err != nil {
		return nil, err
	}
	defer qe.Done()
	implicitCollections, err := r.ImplicitCollections(r.ledger.ledgerID, chaincodeName, qe)
	if err != nil {
		return nil, errors.WithMessage(err, "error while retrieving implicit collections")
	}

	combinedCollections := explicitCollections
	if combinedCollections == nil {
		if implicitCollections == nil {
			return nil, nil
		}
		combinedCollections = &ledger.CollectionConfigInfo{
			CollectionConfig: &peer.CollectionConfigPackage{},
		}
	}

	for _, c := range implicitCollections {
		cc := &peer.CollectionConfig{}
		cc.Payload = &peer.CollectionConfig_StaticCollectionConfig{StaticCollectionConfig: c}
		combinedCollections.CollectionConfig.Config = append(
			combinedCollections.CollectionConfig.Config,
			cc,
		)
	}
	return combinedCollections, nil
}

type ccEventListenerAdaptor struct {
	legacyEventListener cceventmgmt.ChaincodeLifecycleEventListener
}

func (a *ccEventListenerAdaptor) HandleChaincodeDeploy(chaincodeDefinition *ledger.ChaincodeDefinition, dbArtifactsTar []byte) error {
	return a.legacyEventListener.HandleChaincodeDeploy(&cceventmgmt.ChaincodeDefinition{
		Name:              chaincodeDefinition.Name,
		Hash:              chaincodeDefinition.Hash,
		Version:           chaincodeDefinition.Version,
		CollectionConfigs: chaincodeDefinition.CollectionConfigs,
	},
		dbArtifactsTar,
	)
}

func (a *ccEventListenerAdaptor) ChaincodeDeployDone(succeeded bool) {
	a.legacyEventListener.ChaincodeDeployDone(succeeded)
}

func filterPvtDataOfInvalidTx(
	hashVerifiedPvtData map[uint64][]*ledger.TxPvtData,
	blockStore *blkstorage.BlockStore,
	lastBlockInBootstrapSnapshot uint64,
) (map[uint64][]*ledger.TxPvtData, error) {
	committedPvtData := make(map[uint64][]*ledger.TxPvtData)
	for blkNum, txsPvtData := range hashVerifiedPvtData {
		if blkNum <= lastBlockInBootstrapSnapshot {
			committedPvtData[blkNum] = txsPvtData
			continue
		}
		// TODO: Instead of retrieving the whole block, we need to retrieve only
		// the TxValidationFlags from the block metadata. For that, we would need
		// to add a new index for the block metadata - FAB-15808
		block, err := blockStore.RetrieveBlockByNumber(blkNum)
		if err != nil {
			return nil, err
		}
		blockValidationFlags := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

		var blksPvtData []*ledger.TxPvtData
		for _, pvtData := range txsPvtData {
			if blockValidationFlags.IsValid(int(pvtData.SeqInBlock)) {
				blksPvtData = append(blksPvtData, pvtData)
			}
		}
		committedPvtData[blkNum] = blksPvtData
	}
	return committedPvtData, nil
}

func constructPvtdataMap(pvtdata []*ledger.TxPvtData) ledger.TxPvtDataMap {
	if pvtdata == nil {
		return nil
	}
	m := make(map[uint64]*ledger.TxPvtData)
	for _, pvtdatum := range pvtdata {
		m[pvtdatum.SeqInBlock] = pvtdatum
	}
	return m
}

func constructPvtDataAndMissingData(blockAndPvtData *ledger.BlockAndPvtData) ([]*ledger.TxPvtData,
	ledger.TxMissingPvtData) {
	var pvtData []*ledger.TxPvtData
	missingPvtData := make(ledger.TxMissingPvtData)

	numTxs := uint64(len(blockAndPvtData.Block.Data.Data))

	for txNum := uint64(0); txNum < numTxs; txNum++ {
		if pvtdata, ok := blockAndPvtData.PvtData[txNum]; ok {
			pvtData = append(pvtData, pvtdata)
		}

		if missingData, ok := blockAndPvtData.MissingPvtData[txNum]; ok {
			for _, missing := range missingData {
				missingPvtData.Add(txNum, missing.Namespace,
					missing.Collection, missing.IsEligible)
			}
		}
	}
	return pvtData, missingPvtData
}
