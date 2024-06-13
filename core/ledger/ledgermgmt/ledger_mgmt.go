/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledgermgmt

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("ledgermgmt")

// ErrLedgerAlreadyOpened is thrown by a CreateLedger call if a ledger with the given id is already opened
var ErrLedgerAlreadyOpened = errors.New("ledger already opened")

// ErrLedgerMgmtNotInitialized is thrown when ledger mgmt is used before initializing this
var ErrLedgerMgmtNotInitialized = errors.New("ledger mgmt should be initialized before using")

// LedgerMgr 管理所有通道的账本。
type LedgerMgr struct {
	creationLock         sync.Mutex                   // 创建锁，用于保护账本的创建过程
	joinBySnapshotStatus *pb.JoinBySnapshotStatus     // 加入快照状态，用于跟踪加入快照的状态
	lock                 sync.Mutex                   // 锁，用于保护共享资源的访问
	openedLedgers        map[string]ledger.PeerLedger // 已打开的账本映射，存储已打开的账本
	ledgerProvider       ledger.PeerLedgerProvider    // 账本提供者，用于创建和打开账本
	ebMetadataProvider   MetadataProvider             // 外部构建器的元数据提供者
}

type MetadataProvider interface {
	PackageMetadata(ccid string) ([]byte, error)
}

// Initializer 封装了账本模块的所有外部依赖。
type Initializer struct {
	CustomTxProcessors              map[common.HeaderType]ledger.CustomTxProcessor // 自定义交易处理器映射，存储不同类型交易的处理器
	StateListeners                  []ledger.StateListener                         // 状态监听器列表，存储状态变更的监听器
	DeployedChaincodeInfoProvider   ledger.DeployedChaincodeInfoProvider           // 部署链码信息提供者，用于获取已部署链码的信息
	MembershipInfoProvider          ledger.MembershipInfoProvider                  // 成员信息提供者，用于获取网络成员的信息
	ChaincodeLifecycleEventProvider ledger.ChaincodeLifecycleEventProvider         // 链码生命周期事件提供者，用于获取链码生命周期事件
	MetricsProvider                 metrics.Provider                               // 指标提供者，用于收集和暴露指标数据
	HealthCheckRegistry             ledger.HealthCheckRegistry                     // 健康检查注册表，用于注册和管理健康检查
	Config                          *ledger.Config                                 // 账本配置
	HashProvider                    ledger.HashProvider                            // 哈希提供者，用于计算哈希值
	EbMetadataProvider              MetadataProvider                               // 外部构建器的元数据提供者
}

// NewLedgerMgr 创建一个新的账本管理器。
//
// 参数：
//   - initializer：账本模块的初始化器，封装了账本模块的外部依赖。
//
// 返回值：
//   - *LedgerMgr：账本管理器实例。
func NewLedgerMgr(initializer *Initializer) *LedgerMgr {
	logger.Info("正在初始化 账本管理器")

	// 添加链码事件处理器的监听器
	finalStateListeners := addListenerForCCEventsHandler(
		initializer.DeployedChaincodeInfoProvider, // 部署链码信息提供者，用于获取已部署链码的信息
		initializer.StateListeners,                // 状态监听器列表，存储状态变更的监听器
	)

	// 实例化账本提供者, 并生成账本目录
	provider, err := kvledger.NewProvider(
		&ledger.Initializer{
			StateListeners:                  finalStateListeners,                         // 状态监听器列表，存储状态变更的监听器
			DeployedChaincodeInfoProvider:   initializer.DeployedChaincodeInfoProvider,   // 部署链码信息提供者，用于获取已部署链码的信息
			MembershipInfoProvider:          initializer.MembershipInfoProvider,          // 成员信息提供者，用于获取网络成员的信息
			ChaincodeLifecycleEventProvider: initializer.ChaincodeLifecycleEventProvider, // 链码生命周期事件提供者，用于获取链码生命周期事件
			MetricsProvider:                 initializer.MetricsProvider,                 // 指标提供者，用于收集和暴露指标数据
			HealthCheckRegistry:             initializer.HealthCheckRegistry,             // 健康检查注册表，用于注册和管理健康检查
			Config:                          initializer.Config,                          // 配置，用于配置 PeerLedgerProvider 账本
			CustomTxProcessors:              initializer.CustomTxProcessors,              // 自定义交易处理器映射，存储不同类型交易的处理器
			HashProvider:                    initializer.HashProvider,                    // 哈希提供者bccsp，用于计算哈希值
		},
	)
	if err != nil {
		panic(fmt.Sprintf("实例化 分类帐本 提供程序时出错: %+v", err))
	}

	// 创建账本管理器实例, 管理所有通道的账本。
	ledgerMgr := &LedgerMgr{
		joinBySnapshotStatus: &pb.JoinBySnapshotStatus{},         // 加入快照状态，用于跟踪加入快照的状态
		openedLedgers:        make(map[string]ledger.PeerLedger), // 已打开的账本映射，存储已打开的账本
		ledgerProvider:       provider,                           // 账本提供者，用于创建和打开账本
		ebMetadataProvider:   initializer.EbMetadataProvider,     // 外部构建器的元数据提供者
	}

	// 初始化链码事件管理器
	cceventmgmt.Initialize(&chaincodeInfoProviderImpl{
		ledgerMgr, // 创建账本管理器实例, 管理所有通道的账本。
		initializer.DeployedChaincodeInfoProvider, // 部署链码信息提供者，用于获取已部署链码的信息
	})

	logger.Info("已初始化 账本管理器")
	return ledgerMgr
}

// CreateLedger 使用给定的创世块创建一个新的账本。
// 此函数保证创建账本和提交创世块是一个原子操作。
// 从创世块中检索的通道ID被视为账本ID。
// 如果正在从快照创建另一个账本，则返回错误。
// 方法接收者：m（LedgerMgr类型的指针）
// 输入参数：
//   - id：账本ID。
//   - genesisBlock：创世块。
//
// 返回值：
//   - error：如果新的账本失败，则返回错误；否则返回nil。
func (m *LedgerMgr) CreateLedger(id string, genesisBlock *common.Block) (ledger.PeerLedger, error) {
	m.creationLock.Lock()
	defer m.creationLock.Unlock()
	// 加入快照状态，用于跟踪加入快照的状态
	if m.joinBySnapshotStatus.InProgress {
		return nil, errors.Errorf("正在从 %s 的快照创建通道账本. 完成后再次调用通道账本创建.", m.joinBySnapshotStatus.BootstrappingSnapshotDir)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	logger.Infof("正在使用创世区块创建通道账本 [%s] ...", id)

	// 使用给定的创世区块创建一个新的账本。创建一个账本ID，并将其与元数据关联存储在数据库中。
	l, err := m.ledgerProvider.CreateFromGenesisBlock(genesisBlock)
	if err != nil {
		return nil, err
	}

	// 已打开的账本映射，存储已打开的账本
	m.openedLedgers[id] = l
	logger.Infof("使用创世块创建的通道账本 [%s] 完成", id)
	return &closableLedger{
		ledgerMgr:  m,
		id:         id,
		PeerLedger: l,
	}, nil
}

// CreateLedgerFromSnapshot creates a new ledger with the given snapshot and executes the callback function
// after the ledger is created. This function launches to goroutine to create the ledger and call the callback func.
// All ledger dbs would be created in an atomic action. The channel id retrieved from the snapshot metadata
// is treated as a ledger id. It returns an error if another ledger is being created from a snapshot.
func (m *LedgerMgr) CreateLedgerFromSnapshot(snapshotDir string, channelCallback func(ledger.PeerLedger, string)) error {
	// verify snapshotDir exists and is not empty
	empty, err := fileutil.DirEmpty(snapshotDir)
	if err != nil {
		return err
	}
	if empty {
		return errors.Errorf("snapshot dir %s is empty", snapshotDir)
	}

	if err := m.setJoinBySnapshotStatus(snapshotDir); err != nil {
		return err
	}

	go func() {
		defer m.resetJoinBySnapshotStatus()

		ledger, cid, err := m.createFromSnapshot(snapshotDir)
		if err != nil {
			logger.Errorw("Error creating ledger from snapshot", "snapshotDir", snapshotDir, "error", err)
			return
		}

		channelCallback(ledger, cid)
	}()

	return nil
}

func (m *LedgerMgr) createFromSnapshot(snapshotDir string) (ledger.PeerLedger, string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	logger.Infof("Creating ledger from snapshot at %s", snapshotDir)
	l, cid, err := m.ledgerProvider.CreateFromSnapshot(snapshotDir)
	if err != nil {
		return nil, "", err
	}
	m.openedLedgers[cid] = l
	logger.Infof("Created ledger [%s] from snapshot", cid)
	return &closableLedger{
		ledgerMgr:  m,
		id:         cid,
		PeerLedger: l,
	}, cid, nil
}

// setJoinBySnapshotStatus sets joinBySnapshotStatus to indicate a CreateLedgerFromSnapshot is in-progress
// so that other CreateLedger or CreateLedgerFromSnapshot calls will not be allowed.
func (m *LedgerMgr) setJoinBySnapshotStatus(snapshotDir string) error {
	m.creationLock.Lock()
	defer m.creationLock.Unlock()
	if m.joinBySnapshotStatus.InProgress {
		return errors.Errorf("a ledger is being created from a snapshot at %s. Call ledger creation again after it is done.", m.joinBySnapshotStatus.BootstrappingSnapshotDir)
	}
	m.joinBySnapshotStatus.InProgress = true
	m.joinBySnapshotStatus.BootstrappingSnapshotDir = snapshotDir
	return nil
}

// resetJoinBySnapshotStatus resets joinBySnapshotStatus to indicate no CreateLedgerFromSnapshot is in-progress
// so that other CreateLedger or CreateLedgerFromSnapshot calls will be allowed.
func (m *LedgerMgr) resetJoinBySnapshotStatus() {
	m.creationLock.Lock()
	defer m.creationLock.Unlock()
	m.joinBySnapshotStatus.InProgress = false
	m.joinBySnapshotStatus.BootstrappingSnapshotDir = ""
}

// OpenLedger returns a ledger for the given id
func (m *LedgerMgr) OpenLedger(id string) (ledger.PeerLedger, error) {
	logger.Infof("打开账本在 id = %s", id)
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.openedLedgers[id]
	if ok {
		return nil, ErrLedgerAlreadyOpened
	}
	l, err := m.ledgerProvider.Open(id)
	if err != nil {
		return nil, err
	}
	m.openedLedgers[id] = l
	logger.Infof("已打开账本在 id = %s", id)
	return &closableLedger{
		ledgerMgr:  m,
		id:         id,
		PeerLedger: l,
	}, nil
}

// GetLedgerIDs 返回已创建的分类账本的id
func (m *LedgerMgr) GetLedgerIDs() ([]string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	// 列出现有账本的 ID。
	return m.ledgerProvider.List()
}

// JoinBySnapshotStatus returns the status of joinbysnapshot which includes
// ledger creation and channel callback.
func (m *LedgerMgr) JoinBySnapshotStatus() *pb.JoinBySnapshotStatus {
	m.creationLock.Lock()
	defer m.creationLock.Unlock()
	// return a copy of joinBySnapshotStatus to the caller
	return &pb.JoinBySnapshotStatus{
		InProgress:               m.joinBySnapshotStatus.InProgress,
		BootstrappingSnapshotDir: m.joinBySnapshotStatus.BootstrappingSnapshotDir,
	}
}

// Close closes all the opened ledgers and any resources held for ledger management
func (m *LedgerMgr) Close() {
	logger.Infof("Closing ledger mgmt")
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, l := range m.openedLedgers {
		l.Close()
	}
	m.ledgerProvider.Close()
	m.openedLedgers = nil
	logger.Infof("ledger mgmt closed")
}

func (m *LedgerMgr) getOpenedLedger(ledgerID string) (ledger.PeerLedger, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	l, ok := m.openedLedgers[ledgerID]
	if !ok {
		return nil, errors.Errorf("Ledger not opened [%s]", ledgerID)
	}
	return l, nil
}

func (m *LedgerMgr) closeLedger(ledgerID string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	l, ok := m.openedLedgers[ledgerID]
	if ok {
		l.Close()
		delete(m.openedLedgers, ledgerID)
	}
}

// closableLedger extends from actual validated ledger and overwrites the Close method
type closableLedger struct {
	ledgerMgr *LedgerMgr
	id        string
	ledger.PeerLedger
}

// Close closes the actual ledger and removes the entries from opened ledgers map
func (l *closableLedger) Close() {
	l.ledgerMgr.closeLedger(l.id)
}

// lscc namespace listener for chaincode instantiate transactions (which manipulates data in 'lscc' namespace)
// this code should be later moved to peer and passed via `Initialize` function of ledgermgmt
func addListenerForCCEventsHandler(
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	stateListeners []ledger.StateListener) []ledger.StateListener {
	return append(stateListeners, &cceventmgmt.KVLedgerLSCCStateListener{DeployedChaincodeInfoProvider: deployedCCInfoProvider})
}

// chaincodeInfoProviderImpl 实现接口cceventmgmt.Chaincodeinfodrovider
type chaincodeInfoProviderImpl struct {
	ledgerMgr              *LedgerMgr                           // 创建账本管理器实例, 管理所有通道的账本。
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider // 部署链码信息提供者，用于获取已部署链码的信息
}

// GetDeployedChaincodeInfo implements function in the interface cceventmgmt.ChaincodeInfoProvider
func (p *chaincodeInfoProviderImpl) GetDeployedChaincodeInfo(chainid string,
	chaincodeDefinition *cceventmgmt.ChaincodeDefinition) (*ledger.DeployedChaincodeInfo, error) {
	ledger, err := p.ledgerMgr.getOpenedLedger(chainid)
	if err != nil {
		return nil, err
	}
	qe, err := ledger.NewQueryExecutor()
	if err != nil {
		return nil, err
	}
	defer qe.Done()
	deployedChaincodeInfo, err := p.deployedCCInfoProvider.ChaincodeInfo(chainid, chaincodeDefinition.Name, qe)
	if err != nil || deployedChaincodeInfo == nil {
		return nil, err
	}
	if deployedChaincodeInfo.Version != chaincodeDefinition.Version ||
		!bytes.Equal(deployedChaincodeInfo.Hash, chaincodeDefinition.Hash) {
		// if the deployed chaincode with the given name has different version or different hash, return nil
		return nil, nil
	}
	return deployedChaincodeInfo, nil
}

// RetrieveChaincodeArtifacts implements function in the interface cceventmgmt.ChaincodeInfoProvider
func (p *chaincodeInfoProviderImpl) RetrieveChaincodeArtifacts(chaincodeDefinition *cceventmgmt.ChaincodeDefinition) (installed bool, dbArtifactsTar []byte, err error) {
	ccid := chaincodeDefinition.Name + ":" + chaincodeDefinition.Version
	md, err := p.ledgerMgr.ebMetadataProvider.PackageMetadata(ccid)
	if err != nil {
		return false, nil, err
	}
	if md != nil {
		return true, md, nil
	}
	return ccprovider.ExtractStatedbArtifactsForChaincode(ccid)
}
