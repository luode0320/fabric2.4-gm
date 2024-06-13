/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history"
	"github.com/hyperledger/fabric/core/ledger/kvledger/msgs"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	// genesisBlkKeyPrefix is the prefix for each ledger key in idStore db
	genesisBlkKeyPrefix = []byte{'l'}
	// genesisBlkKeyStop is the end key when querying idStore db by ledger key
	genesisBlkKeyStop = []byte{'l' + 1}
	// metadataKeyPrefix is the prefix for each metadata key in idStore db
	metadataKeyPrefix = []byte{'s'}
	// metadataKeyStop is the end key when querying idStore db by metadata key
	metadataKeyStop = []byte{'s' + 1}

	// formatKey
	formatKey = []byte("f")

	attrsToIndex = []blkstorage.IndexableAttr{
		blkstorage.IndexableAttrBlockHash,
		blkstorage.IndexableAttrBlockNum,
		blkstorage.IndexableAttrTxID,
		blkstorage.IndexableAttrBlockNumTranNum,
	}
)

const maxBlockFileSize = 64 * 1024 * 1024

// Provider 实现了 ledger.PeerLedgerProvider 接口
type Provider struct {
	idStore              *idStore                        // ID 存储
	blkStoreProvider     *blkstorage.BlockStoreProvider  // 区块存储提供者
	pvtdataStoreProvider *pvtdatastorage.Provider        // 私有数据存储提供者
	dbProvider           *privacyenabledstate.DBProvider // 数据库提供者
	historydbProvider    *history.DBProvider             // 历史数据库提供者
	configHistoryMgr     *confighistory.Mgr              // 配置历史管理器
	stateListeners       []ledger.StateListener          // 状态监听器列表
	bookkeepingProvider  *bookkeeping.Provider           // 记账提供者
	initializer          *ledger.Initializer             // 初始化器
	collElgNotifier      *collElgNotifier                // 集合资格通知器
	stats                *stats                          // 统计信息
	fileLock             *leveldbhelper.FileLock         // 文件锁
}

// NewProvider 实例化一个新的 Provider。
// 注意：该方法不是线程安全的，调用者需要确保同步。
//
// 参数：
//   - initializer：账本初始化器，封装了账本的外部依赖。
//
// 返回值：
//   - *Provider：Provider 实例。
//   - error：如果实例化过程中发生错误，则返回错误。
func NewProvider(initializer *ledger.Initializer) (pr *Provider, e error) {
	p := &Provider{
		initializer: initializer,
	}

	defer func() {
		if e != nil {
			p.Close()
			if errFormatMismatch, ok := e.(*dataformat.ErrFormatMismatch); ok {
				if errFormatMismatch.Format == dataformat.PreviousFormat && errFormatMismatch.ExpectedFormat == dataformat.CurrentFormat {
					logger.Errorf("请执行 “peer node upgrade-dbs” 命令以升级数据库格式: %s", errFormatMismatch)
				} else {
					logger.Errorf("请检查Fabric版本是否与分类账本数据格式匹配: %s", errFormatMismatch)
				}
			}
		}
	}()

	// 创建文件锁
	fileLockPath := fileLockPath(initializer.Config.RootFSPath)
	fileLock := leveldbhelper.NewFileLock(fileLockPath)
	if err := fileLock.Lock(); err != nil {
		return nil, errors.Wrap(err, "当另一个对等节点命令正在执行时,"+
			" 等待该命令完成其执行或在重试之前终止它")
	}

	p.fileLock = fileLock

	// 初始化账本 ID 存储。
	if err := p.initLedgerIDInventory(); err != nil {
		return nil, err
	}

	// 初始化区块存储提供者。
	if err := p.initBlockStoreProvider(); err != nil {
		return nil, err
	}

	// 初始化私有数据存储提供者。
	if err := p.initPvtDataStoreProvider(); err != nil {
		return nil, err
	}

	// 初始化历史数据库提供者。
	if err := p.initHistoryDBProvider(); err != nil {
		return nil, err
	}

	// 初始化配置历史管理器。
	if err := p.initConfigHistoryManager(); err != nil {
		return nil, err
	}

	// 初始化集合资格通知器。
	p.initCollElgNotifier()
	// 初始化状态监听器。
	p.initStateListeners()

	// 初始化状态数据库提供者。
	if err := p.initStateDBProvider(); err != nil {
		return nil, err
	}

	// 初始化分类账本统计信息
	p.initLedgerStatistics()

	// 删除部分错误的分类账本数据
	if err := p.deletePartialLedgers(); err != nil {
		return nil, err
	}

	// 初始化快照目录。
	if err := p.initSnapshotDir(); err != nil {
		return nil, err
	}
	return p, nil
}

// initLedgerIDInventory 初始化账本 ID 存储。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initLedgerIDInventory() error {
	// 打开账本 ID 存储
	// LedgerProviderPath 返回 ledgerprovider 的绝对路径
	idStore, err := openIDStore(LedgerProviderPath(p.initializer.Config.RootFSPath))
	if err != nil {
		return err
	}

	// 将打开的账本 ID 存储赋值给提供者
	p.idStore = idStore

	return nil
}

// initBlockStoreProvider 初始化区块存储提供者。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initBlockStoreProvider() error {
	// 创建区块索引配置, 包含应编制索引的属性列表的配置
	indexConfig := &blkstorage.IndexConfig{AttrsToIndex: attrsToIndex}

	// 创建区块存储提供者, 构造基于文件系统的块存储提供程序
	blkStoreProvider, err := blkstorage.NewProvider(
		// 封装 “blockstore” 的所有配置
		blkstorage.NewConf(
			BlockStorePath(p.initializer.Config.RootFSPath), // “blockstore” 管理其数据的顶级文件夹, 块存储的绝对路径
			maxBlockFileSize, // 最大块文件大小
		),
		indexConfig,                   // 创建区块索引配置, 包含应编制索引的属性列表的配置
		p.initializer.MetricsProvider, // 指标提供者，用于收集和暴露指标数据
	)
	if err != nil {
		return err
	}

	// 将创建的区块存储提供者赋值给提供者
	p.blkStoreProvider = blkStoreProvider

	return nil
}

// initPvtDataStoreProvider 初始化私有数据存储提供者。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initPvtDataStoreProvider() error {
	// 创建私有数据配置, 封装了账本上私有数据存储的配置
	privateDataConfig := &pvtdatastorage.PrivateDataConfig{
		PrivateDataConfig: p.initializer.Config.PrivateDataConfig,            // 私有数据配置
		StorePath:         PvtDataStorePath(p.initializer.Config.RootFSPath), // 私有数据存储的文件系统路径, 返回pvtdata存储的绝对路径pvtdataStore
	}

	// 创建私有数据存储提供者
	pvtdataStoreProvider, err := pvtdatastorage.NewProvider(privateDataConfig)
	if err != nil {
		return err
	}

	// 将创建的私有数据存储提供者赋值给提供者
	p.pvtdataStoreProvider = pvtdataStoreProvider

	return nil
}

// initHistoryDBProvider 初始化历史数据库提供者。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initHistoryDBProvider() error {
	// 检查是否启用历史数据库
	if !p.initializer.Config.HistoryDBConfig.Enabled {
		return nil
	}

	// 初始化历史数据库（用于按键的历史值建立索引）
	historydbProvider, err := history.NewDBProvider(
		HistoryDBPath(p.initializer.Config.RootFSPath), // 返回历史数据库的绝对路径 historyLeveldb
	)
	if err != nil {
		return err
	}

	// 将创建的历史数据库提供者赋值给提供者
	p.historydbProvider = historydbProvider

	return nil
}

// initConfigHistoryManager 初始化配置历史管理器。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initConfigHistoryManager() error {
	var err error

	// 创建配置历史管理器
	configHistoryMgr, err := confighistory.NewMgr(
		ConfigHistoryDBPath(p.initializer.Config.RootFSPath), // 历史数据库绝对路径configHistory
		p.initializer.DeployedChaincodeInfoProvider,          // 部署链码信息提供者，用于获取已部署链码的信息
	)
	if err != nil {
		return err
	}

	// 将创建的配置历史管理器赋值给提供者
	p.configHistoryMgr = configHistoryMgr

	return nil
}

// initCollElgNotifier 初始化集合资格通知器。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
func (p *Provider) initCollElgNotifier() {
	// 创建集合资格通知器, 监听链码事件，并确定对于一个或多个现有的私有数据集合，对等节点是否有资格，并通知已注册的监听器
	collElgNotifier := &collElgNotifier{
		p.initializer.DeployedChaincodeInfoProvider, // 链码信息提供者
		p.initializer.MembershipInfoProvider,        // 成员信息提供者
		make(map[string]collElgListener),            // 注册的监听器映射，使用字符串作为键
	}

	// 将创建的集合资格通知器赋值给提供者
	p.collElgNotifier = collElgNotifier
}

// initStateListeners 初始化状态监听器。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
func (p *Provider) initStateListeners() {
	// 获取初始化器中的状态监听器列表
	stateListeners := p.initializer.StateListeners

	// 将集合资格通知器添加到状态监听器列表中
	stateListeners = append(stateListeners, p.collElgNotifier)

	// 将配置历史管理器添加到状态监听器列表中
	stateListeners = append(stateListeners, p.configHistoryMgr)

	// 将更新后的状态监听器列表赋值给提供者
	p.stateListeners = stateListeners
}

// initStateDBProvider 初始化状态数据库提供者。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initStateDBProvider() error {
	var err error

	// 创建记账提供者
	p.bookkeepingProvider, err = bookkeeping.NewProvider(
		BookkeeperDBPath(p.initializer.Config.RootFSPath), // 返回记账DB的绝对路径 bookkeeper
	)
	if err != nil {
		return err
	}

	// 创建状态数据库配置, 封装了账本上的 stateDB 的配置信息。
	stateDBConfig := &privacyenabledstate.StateDBConfig{
		StateDBConfig: p.initializer.Config.StateDBConfig,           // 配置账本上的 stateDB。保存状态数据库的配置参数。
		LevelDBPath:   StateDBPath(p.initializer.Config.RootFSPath), //是当 statedb 类型为 "goleveldb" 时的文件系统路径。stateLeveldb
	}

	// 获取系统命名空间列表, 库列表
	sysNamespaces := p.initializer.DeployedChaincodeInfoProvider.Namespaces()

	// 创建状态数据库提供者
	p.dbProvider, err = privacyenabledstate.NewDBProvider(
		p.bookkeepingProvider,             // 表示记账提供者
		p.initializer.MetricsProvider,     // 表示度量提供者
		p.initializer.HealthCheckRegistry, // 表示健康检查注册表
		stateDBConfig,                     // 表示状态数据库配置
		sysNamespaces,                     // 表示系统命名空间列表
	)

	return err
}

func (p *Provider) initLedgerStatistics() {
	p.stats = newStats(p.initializer.MetricsProvider)
}

// initSnapshotDir 初始化快照目录。
// 方法接收者：
//   - p：Provider 结构体的指针，表示提供者
//
// 返回值：
//   - error：表示初始化过程中可能出现的错误
func (p *Provider) initSnapshotDir() error {
	// 获取快照根目录路径
	snapshotsRootDir := p.initializer.Config.SnapshotsConfig.RootDir

	// 检查快照根目录路径是否为绝对路径
	if !filepath.IsAbs(snapshotsRootDir) {
		return errors.Errorf("无效路径: %s. 快照目录的路径应为绝对路径", snapshotsRootDir)
	}

	// 获取进行中快照的路径
	inProgressSnapshotsPath := SnapshotsTempDirPath(snapshotsRootDir)

	// 获取已完成快照的路径
	completedSnapshotsPath := CompletedSnapshotsPath(snapshotsRootDir)

	// 删除进行中快照目录
	if err := os.RemoveAll(inProgressSnapshotsPath); err != nil {
		return errors.Wrapf(err, "删除进行中快照目录时出错：%s", inProgressSnapshotsPath)
	}

	// 创建进行中快照目录
	if err := os.MkdirAll(inProgressSnapshotsPath, 0o755); err != nil {
		return errors.Wrapf(err, "创建进行中快照目录时出错：%s，请确保对配置的 ledger.snapshots.rootDir 目录具有写入权限", inProgressSnapshotsPath)
	}

	// 创建已完成快照目录
	if err := os.MkdirAll(completedSnapshotsPath, 0o755); err != nil {
		return errors.Wrapf(err, "创建已完成快照目录时出错：%s，请确保对配置的 ledger.snapshots.rootDir 目录具有写入权限", completedSnapshotsPath)
	}

	// 同步快照根目录
	return fileutil.SyncDir(snapshotsRootDir)
}

// CreateFromGenesisBlock 实现了接口ledger.PeerLedgerProvider中的相应方法。
// 此函数创建一个新的账本并提交创世块。如果在此过程中发生故障，则会删除部分创建的账本。
// 方法接收者：p（Provider类型的指针）
// 输入参数：
//   - genesisBlock：创世块。
//
// 返回值：
//   - ledger.PeerLedger：创建的账本。
//   - error：如果创建账本失败，则返回错误；否则返回nil。
func (p *Provider) CreateFromGenesisBlock(genesisBlock *common.Block) (ledger.PeerLedger, error) {
	// 从创世块中获取通道ID作为账本ID
	ledgerID, err := protoutil.GetChannelIDFromBlock(genesisBlock)
	if err != nil {
		return nil, err
	}

	// 创建账本ID并设置状态为 UNDER_CONSTRUCTION 在建
	// 创建一个账本ID，并将其与元数据关联存储在数据库中。
	if err = p.idStore.createLedgerID(
		ledgerID,
		&msgs.LedgerMetadata{
			Status: msgs.Status_UNDER_CONSTRUCTION,
		},
	); err != nil {
		return nil, err
	}

	// 打开账本(包含恢复数据、区块监听等功能)
	lgr, err := p.open(ledgerID, nil, false)
	if err != nil {
		return nil, p.deleteUnderConstructionLedger(lgr, ledgerID, err)
	}

	// 提交创世块
	if err = lgr.CommitLegacy(&ledger.BlockAndPvtData{Block: genesisBlock}, &ledger.CommitOptions{}); err != nil {
		return nil, p.deleteUnderConstructionLedger(lgr, ledgerID, err)
	}

	// 更新账本状态为ACTIVE
	if err = p.idStore.updateLedgerStatus(ledgerID, msgs.Status_ACTIVE); err != nil {
		return nil, p.deleteUnderConstructionLedger(lgr, ledgerID, err)
	}

	return lgr, nil
}

func (p *Provider) deleteUnderConstructionLedger(ledger ledger.PeerLedger, ledgerID string, creationErr error) error {
	if creationErr == nil {
		return nil
	} else {
		logger.Errorf("ledger creation error = %+v", creationErr)
	}

	if ledger != nil {
		ledger.Close()
	}
	cleanupErr := p.runCleanup(ledgerID)
	if cleanupErr == nil {
		return creationErr
	}
	return errors.WithMessagef(cleanupErr, creationErr.Error())
}

// Open implements the corresponding method from interface ledger.PeerLedgerProvider
func (p *Provider) Open(ledgerID string) (ledger.PeerLedger, error) {
	logger.Debugf("Open() opening kvledger: %s", ledgerID)
	// Check the ID store to ensure that the chainId/ledgerId exists
	ledgerMetadata, err := p.idStore.getLedgerMetadata(ledgerID)
	if err != nil {
		return nil, err
	}
	if ledgerMetadata == nil {
		return nil, errors.Errorf("cannot open ledger [%s], ledger does not exist", ledgerID)
	}
	if ledgerMetadata.Status != msgs.Status_ACTIVE {
		return nil, errors.Errorf("cannot open ledger [%s], ledger status is [%s]", ledgerID, ledgerMetadata.Status)
	}

	bootSnapshotMetadata, err := snapshotMetadataFromProto(ledgerMetadata.BootSnapshotMetadata)
	if err != nil {
		return nil, err
	}
	return p.open(ledgerID, bootSnapshotMetadata, false)
}

// open 打开一个账本。
// 方法接收者：p（Provider类型的指针）
// 输入参数：
//   - ledgerID：要打开的账本ID。
//   - bootSnapshotMetadata：引导快照的元数据。
//   - initializingFromSnapshot：是否从快照初始化。
//
// 返回值：
//   - ledger.PeerLedger：打开的账本。
//   - error：如果打开账本失败，则返回错误；否则返回nil。
func (p *Provider) open(ledgerID string, bootSnapshotMetadata *SnapshotMetadata, initializingFromSnapshot bool) (ledger.PeerLedger, error) {
	// 获取链/账本的块存储
	// 如果块存储不存在，则此方法会创建一个。
	// 此方法应该只在特定账本ID上调用一次。
	blockStore, err := p.blkStoreProvider.Open(ledgerID)
	if err != nil {
		return nil, err
	}

	// 获取链/账本的私有数据存储
	pvtdataStore, err := p.pvtdataStoreProvider.OpenStore(ledgerID)
	if err != nil {
		return nil, err
	}

	// 注册私有数据存储的监听器
	p.collElgNotifier.registerListener(ledgerID, pvtdataStore)

	// 获取链/账本的版本化数据库 (状态数据库), (通道名称 区块存储 部署的链码信息提供者)
	channelInfoProvider := &channelInfoProvider{ledgerID, blockStore, p.collElgNotifier.deployedChaincodeInfoProvider}
	// 获取给定id（即通道）的数据库句柄
	db, err := p.dbProvider.GetDBHandle(ledgerID, channelInfoProvider)
	if err != nil {
		return nil, err
	}

	// 获取链/账本的历史数据库 (按键值的历史索引)
	var historyDB *history.DB
	if p.historydbProvider != nil {
		historyDB = p.historydbProvider.GetDBHandle(ledgerID)
	}

	// 是一个初始化账本的结构体。
	initializer := &lgrInitializer{
		ledgerID:                 ledgerID,                                      // 账本ID
		blockStore:               blockStore,                                    // 区块存储
		pvtdataStore:             pvtdataStore,                                  // 私有数据存储
		stateDB:                  db,                                            // 状态数据库
		historyDB:                historyDB,                                     // 历史数据库
		configHistoryMgr:         p.configHistoryMgr,                            // 配置历史管理器
		stateListeners:           p.stateListeners,                              // 状态监听器
		bookkeeperProvider:       p.bookkeepingProvider,                         // 记账提供程序
		ccInfoProvider:           p.initializer.DeployedChaincodeInfoProvider,   // 链码信息提供程序
		ccLifecycleEventProvider: p.initializer.ChaincodeLifecycleEventProvider, // 链码生命周期事件提供程序
		stats:                    p.stats.ledgerStats(ledgerID),                 // 账本统计信息
		customTxProcessors:       p.initializer.CustomTxProcessors,              // 自定义交易处理器
		hashProvider:             p.initializer.HashProvider,                    // 哈希提供程序
		config:                   p.initializer.Config,                          // 账本配置
		bootSnapshotMetadata:     bootSnapshotMetadata,                          // 启动快照的元数据
		initializingFromSnapshot: initializingFromSnapshot,                      // 是否从快照初始化
	}

	// 新 KV 账本(包含恢复数据、区块监听等功能)
	l, err := newKVLedger(initializer)
	if err != nil {
		return nil, err
	}

	return l, nil
}

// Exists implements the corresponding method from interface ledger.PeerLedgerProvider
func (p *Provider) Exists(ledgerID string) (bool, error) {
	return p.idStore.ledgerIDExists(ledgerID)
}

// List 从接口ledger.PeerLedgerProvider实现相应的方法
func (p *Provider) List() ([]string, error) {
	return p.idStore.getActiveLedgerIDs()
}

// Close implements the corresponding method from interface ledger.PeerLedgerProvider
func (p *Provider) Close() {
	if p.idStore != nil {
		p.idStore.close()
	}
	if p.blkStoreProvider != nil {
		p.blkStoreProvider.Close()
	}
	if p.pvtdataStoreProvider != nil {
		p.pvtdataStoreProvider.Close()
	}
	if p.dbProvider != nil {
		p.dbProvider.Close()
	}
	if p.bookkeepingProvider != nil {
		p.bookkeepingProvider.Close()
	}
	if p.configHistoryMgr != nil {
		p.configHistoryMgr.Close()
	}
	if p.historydbProvider != nil {
		p.historydbProvider.Close()
	}
	if p.fileLock != nil {
		p.fileLock.Unlock()
	}
}

// deletePartialLedgers 扫描并删除状态为UNDER_CONSTRUCTION或UNDER_DELETION的任何分类帐。
// UNDER_CONSTRUCTION分类帐表示在分类帐创建过程中作为崩溃的副作用而创建的剩余结构。
// UNDER_DELETION分类帐表示在对等通道取消联接期间作为崩溃的副作用创建的剩余结构。
func (p *Provider) deletePartialLedgers() error {
	logger.Debug("删除分类账本状态为: UNDER_CONSTRUCTION or UNDER_DELETION 的数据")
	itr := p.idStore.db.GetIterator(metadataKeyPrefix, metadataKeyStop)
	defer itr.Release()
	if err := itr.Error(); err != nil {
		return errors.WithMessage(err, "获取不完整分类账本扫描的迭代器时出错")
	}
	for {
		hasMore := itr.Next()
		err := itr.Error()
		if err != nil {
			return errors.WithMessage(err, "扫描不完整分类账本时迭代分类帐列表时出错")
		}
		if !hasMore {
			return nil
		}
		ledgerID := ledgerIDFromMetadataKey(itr.Key())
		metadata := &msgs.LedgerMetadata{}
		if err := proto.Unmarshal(itr.Value(), metadata); err != nil {
			return errors.Wrapf(err, "解析账本元数据时错误 [%s]", ledgerID)
		}
		if metadata.Status == msgs.Status_UNDER_CONSTRUCTION || metadata.Status == msgs.Status_UNDER_DELETION {
			logger.Infow(
				"确定了部分分类账本在peer启动时, 指示在创建期间peer对等体停止/崩溃或失败的通道取消加入. 部分分类账本将被删除.",
				"ledgerID", ledgerID,
				"Status", metadata.Status,
			)
			if err := p.runCleanup(ledgerID); err != nil {
				logger.Errorw(
					"在开始时删除部分错误创建的分类账本时出错",
					"ledgerID", ledgerID,
					"Status", metadata.Status,
					"error", err,
				)
				return errors.WithMessagef(err, "删除状态为的部分错误构建的分类账本时出错 [%s] 分类账本开始时 = [%s]", metadata.Status, ledgerID)
			}
		}
	}
}

// runCleanup cleans up blockstorage, statedb, and historydb for what
// may have got created during in-complete ledger creation
func (p *Provider) runCleanup(ledgerID string) error {
	ledgerDataRemover := &ledgerDataRemover{
		blkStoreProvider:     p.blkStoreProvider,
		statedbProvider:      p.dbProvider,
		bookkeepingProvider:  p.bookkeepingProvider,
		configHistoryMgr:     p.configHistoryMgr,
		historydbProvider:    p.historydbProvider,
		pvtdataStoreProvider: p.pvtdataStoreProvider,
	}
	if err := ledgerDataRemover.Drop(ledgerID); err != nil {
		return errors.WithMessagef(err, "error while deleting data from ledger [%s]", ledgerID)
	}

	return p.idStore.deleteLedgerID(ledgerID)
}

func snapshotMetadataFromProto(p *msgs.BootSnapshotMetadata) (*SnapshotMetadata, error) {
	if p == nil {
		return nil, nil
	}

	m := &SnapshotMetadataJSONs{
		signableMetadata:   p.SingableMetadata,
		additionalMetadata: p.AdditionalMetadata,
	}

	return m.ToMetadata()
}

// ////////////////////////////////////////////////////////////////////
// Ledger id 持久性相关代码
// /////////////////////////////////////////////////////////////////////
type idStore struct {
	db     *leveldbhelper.DB
	dbPath string
}

// openIDStore 打开账本 ID 存储。
// 输入参数：
//   - path：string 类型，表示存储路径
//
// 返回值：
//   - *idStore：表示账本 ID 存储的指针
//   - error：表示打开过程中可能出现的错误
func openIDStore(path string) (s *idStore, e error) {
	// 构造一个 “数据库” 结构
	db := leveldbhelper.CreateDB(&leveldbhelper.Conf{DBPath: path})
	// 打开 LevelDB 数据库
	db.Open()
	defer func() {
		if e != nil {
			db.Close()
		}
	}()

	// 检查数据库是否为空
	emptyDB, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}

	expectedFormatBytes := []byte(dataformat.CurrentFormat)
	if emptyDB {
		// 如果数据库为空，则在新数据库中添加格式键
		err := db.Put(formatKey, expectedFormatBytes, true)
		if err != nil {
			return nil, err
		}
		return &idStore{db, path}, nil
	}

	// 验证现有数据库的格式是否正确
	format, err := db.Get(formatKey)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(format, expectedFormatBytes) {
		logger.Errorf("数据库的目录 [%s] 包含意外格式的数据. 预期的数据格式 = [%s] (%#v), 现有数据格式 = [%s] (%#v).",
			path, dataformat.CurrentFormat, expectedFormatBytes, format, format)
		return nil, &dataformat.ErrFormatMismatch{
			ExpectedFormat: dataformat.CurrentFormat,
			Format:         string(format),
			DBInfo:         fmt.Sprintf("leveldb 用于 channel-IDs 在 [%s]", path),
		}
	}

	return &idStore{db, path}, nil
}

// checkUpgradeEligibility checks if the format is eligible to upgrade.
// It returns true if the format is eligible to upgrade to the current format.
// It returns false if either the format is the current format or the db is empty.
// Otherwise, an ErrFormatMismatch is returned.
func (s *idStore) checkUpgradeEligibility() (bool, error) {
	emptydb, err := s.db.IsEmpty()
	if err != nil {
		return false, err
	}
	if emptydb {
		logger.Warnf("Ledger database %s is empty, nothing to upgrade", s.dbPath)
		return false, nil
	}
	format, err := s.db.Get(formatKey)
	if err != nil {
		return false, err
	}
	if bytes.Equal(format, []byte(dataformat.CurrentFormat)) {
		logger.Debugf("Ledger database %s has current data format, nothing to upgrade", s.dbPath)
		return false, nil
	}
	if !bytes.Equal(format, []byte(dataformat.PreviousFormat)) {
		err = &dataformat.ErrFormatMismatch{
			ExpectedFormat: dataformat.PreviousFormat,
			Format:         string(format),
			DBInfo:         fmt.Sprintf("leveldb for channel-IDs at [%s]", s.dbPath),
		}
		return false, err
	}
	return true, nil
}

func (s *idStore) upgradeFormat() error {
	eligible, err := s.checkUpgradeEligibility()
	if err != nil {
		return err
	}
	if !eligible {
		return nil
	}

	logger.Infof("Upgrading ledgerProvider database to the new format %s", dataformat.CurrentFormat)

	batch := &leveldb.Batch{}
	batch.Put(formatKey, []byte(dataformat.CurrentFormat))

	// add new metadata key for each ledger (channel)
	metadata, err := protoutil.Marshal(&msgs.LedgerMetadata{Status: msgs.Status_ACTIVE})
	if err != nil {
		logger.Errorf("Error marshalling ledger metadata: %s", err)
		return errors.Wrapf(err, "error marshalling ledger metadata")
	}
	itr := s.db.GetIterator(genesisBlkKeyPrefix, genesisBlkKeyStop)
	defer itr.Release()
	for itr.Error() == nil && itr.Next() {
		id := ledgerIDFromGenesisBlockKey(itr.Key())
		batch.Put(metadataKey(id), metadata)
	}
	if err = itr.Error(); err != nil {
		logger.Errorf("Error while upgrading idStore format: %s", err)
		return errors.Wrapf(err, "error while upgrading idStore format")
	}

	return s.db.WriteBatch(batch, true)
}

// createLedgerID 创建一个账本ID，并将其与元数据关联存储在数据库中。
// 方法接收者：s（idStore类型的指针）
// 输入参数：
//   - ledgerID：要创建的账本ID。
//   - metadata：账本的元数据。
//
// 返回值：
//   - error：如果创建账本ID失败或者账本ID已经存在，则返回错误；否则返回nil。
func (s *idStore) createLedgerID(ledgerID string, metadata *msgs.LedgerMetadata) error {
	// 获取账本ID的元数据
	m, err := s.getLedgerMetadata(ledgerID)
	if err != nil {
		return err
	}

	// 如果元数据不为空，则表示账本ID已经存在
	if m != nil {
		return errors.Errorf("通道账本 [%s] 已存在，状态为 [%s]", ledgerID, m.GetStatus())
	}

	// 序列化元数据
	metadataBytes, err := protoutil.Marshal(metadata)
	if err != nil {
		return err
	}

	// 将元数据存储在数据库中
	return s.db.Put(metadataKey(ledgerID), metadataBytes, true)
}

func (s *idStore) deleteLedgerID(ledgerID string) error {
	return s.db.Delete(metadataKey(ledgerID), true)
}

// updateLedgerStatus 用于更新账本的状态。
// 方法接收者：s（idStore类型的指针）
// 输入参数：
//   - ledgerID：要更新状态的账本ID。
//   - newStatus：新的账本状态。
//
// 返回值：
//   - error：如果在更新账本状态的过程中出错，则返回错误。
func (s *idStore) updateLedgerStatus(ledgerID string, newStatus msgs.Status) error {
	// 获取账本的元数据
	metadata, err := s.getLedgerMetadata(ledgerID)
	if err != nil {
		return err
	}

	// 如果元数据为空，则表示账本不存在
	if metadata == nil {
		logger.Errorf("LedgerID [%s] 不存在", ledgerID)
		return errors.Errorf("无法更新通道账本状态，通道账本 [%s] 不存在", ledgerID)
	}

	if metadata.Status == newStatus {
		logger.Infof("通道账本 [%s] 已处于 [%s] 状态了, 无事可做", ledgerID, newStatus)
		return nil
	}

	metadata.Status = newStatus
	metadataBytes, err := proto.Marshal(metadata)
	if err != nil {
		logger.Errorf("编码通道账本元数据时出错: %s", err)
		return errors.Wrapf(err, "编码通道账本元数据时出错")
	}
	logger.Infof("正在将通道账本 [%s] 状态更新为 [%s]", ledgerID, newStatus)
	key := metadataKey(ledgerID)

	// 更新数据库
	return s.db.Put(key, metadataBytes, true)
}

// getLedgerMetadata 获取指定账本ID的元数据。
// 方法接收者：s（idStore类型的指针）
// 输入参数：
//   - ledgerID：要获取元数据的账本ID。
//
// 返回值：
//   - *msgs.LedgerMetadata：指定账本ID的元数据。
//   - error：如果获取元数据失败，则返回错误；否则返回nil。
func (s *idStore) getLedgerMetadata(ledgerID string) (*msgs.LedgerMetadata, error) {
	// 从数据库中获取指定账本ID的元数据
	val, err := s.db.Get(metadataKey(ledgerID))
	if val == nil || err != nil {
		return nil, err
	}
	// 反序列化元数据
	metadata := &msgs.LedgerMetadata{}
	if err := proto.Unmarshal(val, metadata); err != nil {
		logger.Errorf("反序列化账本元数据时出错: %s", err)
		return nil, errors.Wrapf(err, "反序列化账本元数据时出错")
	}
	return metadata, nil
}

func (s *idStore) ledgerIDExists(ledgerID string) (bool, error) {
	key := metadataKey(ledgerID)
	val, err := s.db.Get(key)
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// getActiveLedgerIDs 返回当前存储中所有活跃账本的 ID 列表。
// 方法接收者：
//   - s：idStore 结构体的指针，表示 ID 存储
//
// 返回值：
//   - []string：表示活跃账本的 ID 列表
//   - error：表示获取账本 ID 列表过程中可能出现的错误
func (s *idStore) getActiveLedgerIDs() ([]string, error) {
	var ids []string

	// 获取存储中所有元数据键的迭代器
	itr := s.db.GetIterator(metadataKeyPrefix, metadataKeyStop)
	defer itr.Release()

	// 遍历迭代器，获取每个元数据并判断其状态是否为活跃
	for itr.Error() == nil && itr.Next() {
		// 反序列化元数据
		metadata := &msgs.LedgerMetadata{}
		if err := proto.Unmarshal(itr.Value(), metadata); err != nil {
			logger.Errorf("错误解析 ledger 账本元数据: %s", err)
			return nil, errors.Wrapf(err, "错误解析 ledger 账本元数据")
		}

		// 如果元数据状态为活跃，则将其对应的账本 ID 添加到列表中
		if metadata.Status == msgs.Status_ACTIVE {
			id := ledgerIDFromMetadataKey(itr.Key())
			ids = append(ids, id)
		}
	}

	// 检查迭代器是否出错
	if err := itr.Error(); err != nil {
		logger.Errorf("从本地持久性获取分类账本时出错: %s", err)
		return nil, errors.Wrapf(err, "从本地持久性获取分类账本时出错")
	}

	return ids, nil
}

func (s *idStore) close() {
	s.db.Close()
}

func ledgerIDFromGenesisBlockKey(key []byte) string {
	return string(key[len(genesisBlkKeyPrefix):])
}

func metadataKey(ledgerID string) []byte {
	return append(metadataKeyPrefix, []byte(ledgerID)...)
}

func ledgerIDFromMetadataKey(key []byte) string {
	return string(key[len(metadataKeyPrefix):])
}
