/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"fmt"
	"hash"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/metrics"
)

const (
	GoLevelDB = "goleveldb"
	CouchDB   = "CouchDB"
)

// Initializer 封装了 PeerLedgerProvider 的依赖项。
type Initializer struct {
	StateListeners                  []StateListener                         // 状态监听器列表，存储状态变更的监听器
	DeployedChaincodeInfoProvider   DeployedChaincodeInfoProvider           // 部署链码信息提供者，用于获取已部署链码的信息
	MembershipInfoProvider          MembershipInfoProvider                  // 成员信息提供者，用于获取网络成员的信息
	ChaincodeLifecycleEventProvider ChaincodeLifecycleEventProvider         // 链码生命周期事件提供者，用于获取链码生命周期事件
	MetricsProvider                 metrics.Provider                        // 指标提供者，用于收集和暴露指标数据
	HealthCheckRegistry             HealthCheckRegistry                     // 健康检查注册表，用于注册和管理健康检查
	Config                          *Config                                 // 配置，用于配置 PeerLedgerProvider
	CustomTxProcessors              map[common.HeaderType]CustomTxProcessor // 自定义交易处理器映射，存储不同类型交易的处理器
	HashProvider                    HashProvider                            // 哈希提供者，用于计算哈希值
}

// Config 是用于配置分类账本提供程序的结构。
type Config struct {
	// RootFSPath 是存储分类账本文件的顶级目录。
	RootFSPath string
	// StateDBConfig 保存状态数据库的配置参数。
	StateDBConfig *StateDBConfig
	// PrivateDataConfig holds the configuration parameters for the private data store.
	PrivateDataConfig *PrivateDataConfig
	// HistoryDBConfig holds the configuration parameters for the transaction history database.
	HistoryDBConfig *HistoryDBConfig
	// SnapshotsConfig holds the configuration parameters for the snapshots.
	SnapshotsConfig *SnapshotsConfig
}

// StateDBConfig 是用于配置分类账本的状态参数的结构。
type StateDBConfig struct {
	// StateDatabase 用于存储上次已知状态的数据库。
	// 支持的两个选项是 “goleveldb” 和 “CouchDB” (分别在常量GoLevelDB和CouchDB中捕获)。
	StateDatabase string
	// CouchDB 是CouchDB的配置。当StateDatabase设置为 “CouchDB” 时使用。
	CouchDB *CouchDBConfig
}

// CouchDBConfig 是用于配置 Couchdb Instance 的结构体。
type CouchDBConfig struct {
	Address               string        // CouchDB 数据库实例的主机名和端口号
	Username              string        // 用于与 CouchDB 进行身份验证的用户名，该用户名必须具有读写权限
	Password              string        // 用于用户名的密码
	MaxRetries            int           // 在失败时重试 CouchDB 操作的最大次数
	MaxRetriesOnStartup   int           // 在初始化账本时失败时重试 CouchDB 操作的最大次数
	RequestTimeout        time.Duration // CouchDB 操作的超时时间
	InternalQueryLimit    int           // 在查询 CouchDB 时内部返回的最大记录数
	MaxBatchUpdateSize    int           // CouchDB 批量更新操作中包含的最大记录数
	CreateGlobalChangesDB bool          // 是否创建 "_global_changes" 系统数据库
	RedoLogPath           string        // 存储 CouchDB 重做日志文件的目录
	UserCacheSizeMBs      int           // 用户状态缓存的最大兆字节数（MB），用于存储用户部署的所有链码。注意，UserCacheSizeMBs 需要是 32 MB 的倍数，如果不是 32 MB 的倍数，对等节点会将大小舍入到下一个 32 MB 的倍数。
}

// PrivateDataConfig 是用于配置私有数据存储提供程序的结构。
type PrivateDataConfig struct {
	// BatchesInterval 将不符合条件的缺失数据条目转换为符合条件的条目的批次之间的最小持续时间 (毫秒)。
	BatchesInterval int
	// MaxBatchSize 是将不符合条件的缺失数据条目转换为符合条件的条目时的最大批大小。
	MaxBatchSize int
	// PurgeInterval 清除过期的专用数据条目之前要等待的块数。
	PurgeInterval int
	// 缺失的数据条目分为三类:
	// (1) 符合条件的优先级别
	// (2) 符合条件的取消优先级
	// (3) 无法选择
	// 调和器将从其他对等端获取符合条件的优先化缺失数据。
	// 在每次降低优先级后，将给出符合条件的降低优先级的丢失数据的机会
	DeprioritizedDataReconcilerInterval time.Duration
}

// HistoryDBConfig 是用于配置事务历史数据库的结构。
type HistoryDBConfig struct {
	// 指示是否应存储密钥更新的历史记录。
	// 所有历史 “索引” 将存储在goleveldb中，无论使用CouchDB还是备用数据库的状态。
	Enabled bool
}

// SnapshotsConfig 是用于配置快照功能的结构
type SnapshotsConfig struct {
	// RootDir 是快照的顶级目录。
	RootDir string
}

// PeerLedgerProvider 提供对账本实例的句柄
type PeerLedgerProvider interface {
	// CreateFromGenesisBlock 使用给定的创世区块创建一个新的账本。
	// 此函数保证创建账本和提交创世区块是原子操作。
	// 从创世区块中检索到的通道 ID 被视为账本 ID。
	CreateFromGenesisBlock(genesisBlock *common.Block) (PeerLedger, error)
	// CreateFromSnapshot 从快照创建一个新的账本，并返回账本和通道 ID。
	// 从快照元数据中检索到的通道 ID 被视为账本 ID。
	CreateFromSnapshot(snapshotDir string) (PeerLedger, string, error)
	// Open 打开一个已创建的账本。
	Open(ledgerID string) (PeerLedger, error)
	// Exists 告诉是否存在具有给定 ID 的账本。
	Exists(ledgerID string) (bool, error)
	// List 列出现有账本的 ID。
	List() ([]string, error)
	// Close 关闭 PeerLedgerProvider。
	Close()
}

// PeerLedger 是与 OrdererLedger 不同的，因为 PeerLedger 本地维护了一个位掩码，用于区分有效的交易和无效的交易。
// 包含方法 判断txid是否已经提交完成、根据txid获取交易、根据hash返回块、txid返回块、定块号的块和相应的私有数据、
type PeerLedger interface {
	commonledger.Ledger // 继承自 commonledger.Ledger 接口

	// TxIDExists 如果指定的 txID 已经存在于已提交的块中，则返回 true。
	// 如果存在阻止检查 txID 的底层条件（例如 I/O 错误），则返回错误。
	TxIDExists(txID string) (bool, error)

	// GetTransactionByID 根据交易 ID 获取交易。
	GetTransactionByID(txID string) (*peer.ProcessedTransaction, error)

	// GetBlockByHash 根据块哈希返回块。
	GetBlockByHash(blockHash []byte) (*common.Block, error)

	// GetBlockByTxID 返回包含交易的块。
	GetBlockByTxID(txID string) (*common.Block, error)

	// GetTxValidationCodeByTxID 返回交易的验证代码和提交该交易的块号。
	GetTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error)

	// NewTxSimulator 提供一个事务模拟器的句柄。
	// 客户端可以获取多个 'TxSimulator' 以进行并行执行。
	// 如果需要，任何快照/同步应在实现级别执行。
	NewTxSimulator(txid string) (TxSimulator, error)

	// NewQueryExecutor 提供一个查询执行器的句柄。
	// 客户端可以获取多个 'QueryExecutor' 以进行并行执行。
	// 如果需要，任何同步应在实现级别执行。
	NewQueryExecutor() (QueryExecutor, error)

	// NewHistoryQueryExecutor 提供一个历史查询执行器的句柄。
	// 客户端可以获取多个 'HistoryQueryExecutor' 以进行并行执行。
	// 如果需要，任何同步应在实现级别执行。
	NewHistoryQueryExecutor() (HistoryQueryExecutor, error)

	// GetPvtDataAndBlockByNum 返回指定块号的块和相应的私有数据。
	// 私有数据由提供的 'ns/collections' 列表进行过滤。
	// 空的过滤器不会过滤任何结果，并导致检索给定块号的所有私有数据。
	GetPvtDataAndBlockByNum(blockNum uint64, filter PvtNsCollFilter) (*BlockAndPvtData, error)

	// GetPvtDataByNum 仅返回与给定块号对应的私有数据。
	// 私有数据由提供的 'ns/collections' 进行过滤。
	// 空的过滤器不会过滤任何结果，并导致检索给定块号的所有私有数据。
	GetPvtDataByNum(blockNum uint64, filter PvtNsCollFilter) ([]*TxPvtData, error)

	// CommitLegacy 在遵循 v14 验证/提交路径的原子操作中提交块和相应的私有数据。
	// TODO: 添加一个新的 Commit() 路径，用于替换 CommitLegacy()，用于描述 FAB-12221 中的验证重构
	CommitLegacy(blockAndPvtdata *BlockAndPvtData, commitOpts *CommitOptions) error

	// GetConfigHistoryRetriever 返回 ConfigHistoryRetriever。
	GetConfigHistoryRetriever() (ConfigHistoryRetriever, error)

	// CommitPvtDataOfOldBlocks 提交已经提交的块对应的私有数据。
	// 如果提供的私有数据的哈希与块中的哈希不匹配，则不会提交不匹配的私有数据，并返回不匹配的信息。
	CommitPvtDataOfOldBlocks(reconciledPvtdata []*ReconciledPvtdata, unreconciled MissingPvtDataInfo) ([]*PvtdataHashMismatch, error)

	// GetMissingPvtDataTracker 返回 MissingPvtDataTracker。
	GetMissingPvtDataTracker() (MissingPvtDataTracker, error)

	// DoesPvtDataInfoExist 在以下情况下返回 true：
	// (1) 账本与给定块号关联的私有数据存在（或）
	// (2) 与给定块号关联的一些或所有私有数据丢失，但丢失的信息记录在账本中（或）
	// (3) 块已提交且不包含任何私有数据。
	DoesPvtDataInfoExist(blockNum uint64) (bool, error)

	// SubmitSnapshotRequest 提交指定高度的快照请求。
	// 请求将存储在账本中，直到账本的块高度等于指定的高度并且快照生成完成。
	// 当高度为0时，它将在当前块高度生成快照。
	// 如果指定的高度小于账本的块高度，则返回错误。
	SubmitSnapshotRequest(height uint64) error

	// CancelSnapshotRequest 取消先前提交的请求。
	// 如果不存在或正在处理此类请求，则返回错误。
	CancelSnapshotRequest(height uint64) error

	// PendingSnapshotRequests 返回挂起（或正在处理）快照请求的高度列表。
	PendingSnapshotRequests() ([]uint64, error)

	// CommitNotificationsChannel 返回一个只读通道，在其中账本在提交块时发送一个 `CommitNotification`。
	// CommitNotification 包含从已提交块中的交易条目，这些交易条目不是格式错误的，携带合法的 TxID，并且不被标记为重复交易。
	// 消费者可以关闭 'done' 通道来表示不再需要通知。这将导致 CommitNotifications 通道关闭。
	// 预计一次只有一个消费者。如果已经存在 CommitNotification 通道，则返回错误。
	CommitNotificationsChannel(done <-chan struct{}) (<-chan *CommitNotification, error)
}

// SimpleQueryExecutor 封装基本功能
type SimpleQueryExecutor interface {
	// GetState 获取给定命名空间和键的值。对于链码，命名空间对应于chaincodeId
	GetState(namespace string, key string) ([]byte, error)
	// GetStateRangeScanIterator 返回一个迭代器，其中包含给定键范围之间的所有键值。
	// 结果中包含startKey，排除endKey。空的startKey是指第一个可用的键，空的endKey是指最后一个可用的键。
	// 为了扫描所有键，startKey和endKey都可以作为空字符串提供。
	// 但是，出于性能原因，应明智地使用完整扫描。
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果。
	GetStateRangeScanIterator(namespace string, startKey string, endKey string) (commonledger.ResultsIterator, error)
	// GetPrivateDataHash 获取由tuple <namespace，collection，key> 标识的私有数据项的值的哈希
	// 函数 'GetPrivateData' 只有在被授权拥有集合 <namespace，collection> 的私有数据的对等方上调用它时才有意义。
	// 但是，可以在任何对等节点上调用函数 'GetPrivateDataHash' 以获取当前值的哈希
	GetPrivateDataHash(namespace, collection, key string) ([]byte, error)
}

// QueryExecutor executes the queries
// Get* methods are for supporting KV-based data model. ExecuteQuery method is for supporting a rich datamodel and query support
//
// ExecuteQuery 在丰富数据模型的情况下，方法有望支持对最新状态的查询，
// 历史状态以及状态和事务的交集
type QueryExecutor interface {
	SimpleQueryExecutor
	// GetStateMetadata 返回给定名称空间和键的元数据
	GetStateMetadata(namespace, key string) (map[string][]byte, error)
	// GetStateMultipleKeys 在单个调用中获取多个键的值
	GetStateMultipleKeys(namespace string, keys []string) ([][]byte, error)
	// GetStateRangeScanIteratorWithPagination 返回一个迭代器，其中包含给定键范围之间的所有键值。
	// 结果中包含startKey，排除endKey。空的startKey是指第一个可用的键，空的endKey是指最后一个可用的键。
	// 为了扫描所有键，startKey和endKey都可以作为空字符串提供。
	// 但是，出于性能原因，应明智地使用完整扫描。
	// page size参数限制返回结果的数量
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果。
	GetStateRangeScanIteratorWithPagination(namespace string, startKey, endKey string, pageSize int32) (QueryResultsIterator, error)
	// ExecuteQuery 执行给定的查询并返回一个迭代器，该迭代器包含特定于基础数据存储区的类型的结果。
	// 仅用于支持查询链码的状态数据库，
	// namespace对应chaincodeId
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果.
	ExecuteQuery(namespace, query string) (commonledger.ResultsIterator, error)
	// ExecuteQueryWithPagination 执行给定的查询并返回一个迭代器，该迭代器包含特定于基础数据存储区的类型的结果。
	// bookmark 和 pageSize 参数与分页相关联。 仅用于支持查询链码的状态数据库，
	// namespace对应chaincodeId
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果。
	ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (QueryResultsIterator, error)
	// GetPrivateData 获取由tuple <namespace，collection，key> 标识的私有数据项的值
	GetPrivateData(namespace, collection, key string) ([]byte, error)
	// GetPrivateDataMetadata 获取由tuple <namespace，collection，key> 标识的私有数据项的元数据
	GetPrivateDataMetadata(namespace, collection, key string) (map[string][]byte, error)
	// GetPrivateDataMetadataByHash 获取由tuple <namespace，collection，keyhash> 标识的私有数据项的元数据
	GetPrivateDataMetadataByHash(namespace, collection string, keyhash []byte) (map[string][]byte, error)
	// GetPrivateDataMultipleKeys 在单个调用中获取多个私有数据项的值
	GetPrivateDataMultipleKeys(namespace, collection string, keys []string) ([][]byte, error)
	// GetPrivateDataRangeScanIterator 返回一个迭代器，其中包含给定键范围之间的所有键值。
	// 结果中包含startKey，排除endKey。空的startKey是指第一个可用的键，空的endKey是指最后一个可用的键。
	// 为了扫描所有键，startKey和endKey都可以作为空字符串提供。
	// 但是，出于性能原因，应明智地使用完整扫描。
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果。
	GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error)
	// ExecuteQueryOnPrivateData 执行给定的查询并返回一个迭代器，该迭代器包含特定于基础数据存储区的类型的结果。
	// 仅用于支持查询链码的状态数据库，
	// namespace对应chaincodeId
	// 返回的ResultsIterator包含在fabric-protos/ledger/queryresult中定义的 * KV类型的结果。
	ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error)
	// Done 释放QueryExecutor占用的资源
	Done()
}

// HistoryQueryExecutor executes the history queries
type HistoryQueryExecutor interface {
	// GetHistoryForKey retrieves the history of values for a key.
	// The returned ResultsIterator contains results of type *KeyModification which is defined in fabric-protos/ledger/queryresult.
	GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error)
}

// TxSimulator 在 “尽可能最近的状态” 的一致快照上模拟事务
// Set * 方法用于支持基于KV的数据模型。ExecuteUpdate方法用于支持丰富的数据模型和查询支持
type TxSimulator interface {
	QueryExecutor
	// SetState 设置给定命名空间和键的给定值。对于链码，命名空间对应于chaincodeId
	SetState(namespace string, key string, value []byte) error
	// DeleteState 删除给定的命名空间和键
	DeleteState(namespace string, key string) error
	// SetStateMultipleKeys 在单个调用中设置多个键的值
	SetStateMultipleKeys(namespace string, kvs map[string][]byte) error
	// SetStateMetadata 设置与现有key-tuple <namespace，key> 关联的元数据
	SetStateMetadata(namespace, key string, metadata map[string][]byte) error
	// DeleteStateMetadata 删除与现有key-tuple <namespace，key> 关联的元数据 (如果有)
	DeleteStateMetadata(namespace, key string) error
	// ExecuteUpdate 用于支持丰富的数据模型 (请参阅上面的QueryExecutor评论)
	ExecuteUpdate(query string) error
	// SetPrivateData 将给定值设置为元组 <namespace，collection，key> 表示的私有数据状态下的键
	SetPrivateData(namespace, collection, key string, value []byte) error
	// SetPrivateDataMultipleKeys 在一次调用中设置私有数据空间中的多个键的值
	SetPrivateDataMultipleKeys(namespace, collection string, kvs map[string][]byte) error
	// DeletePrivateData 从私有数据中删除给定的元组 <namespace，collection，key>
	DeletePrivateData(namespace, collection, key string) error
	// SetPrivateDataMetadata 设置与现有key-tuple <namespace，collection，key> 关联的元数据
	SetPrivateDataMetadata(namespace, collection, key string, metadata map[string][]byte) error
	// DeletePrivateDataMetadata 删除与现有key-tuple <namespace，collection，key> 关联的元数据
	DeletePrivateDataMetadata(namespace, collection, key string) error
	// GetTxSimulationResults 封装事务模拟的结果。
	// 这应该包含足够的细节
	// - 如果要提交事务将导致的状态更新
	// - 执行交易的环境，以便能够决定环境的有效性
	//   (稍后在不同的对等体上) 在提交事务期间
	// 不同的账本实现 (或单个实现的配置) 可能希望表示上述两部分
	// 以不同的方式信息，以支持不同的数据模型或优化信息表示。
	// 返回的类型 “txsimulationresults” 包含公共数据和私有数据的模拟结果。
	// 公共数据模拟结果预期与V1一样，私有数据模拟结果预期
	// 被gossip用来将其传播给其他代言人 (在sidedb的第2阶段)
	GetTxSimulationResults() (*TxSimulationResults, error)
}

// QueryResultsIterator - an iterator for query result set
type QueryResultsIterator interface {
	commonledger.ResultsIterator
	// GetBookmarkAndClose returns a paging bookmark and releases resources occupied by the iterator
	GetBookmarkAndClose() string
}

// TxPvtData encapsulates the transaction number and pvt write-set for a transaction
type TxPvtData struct {
	SeqInBlock uint64
	WriteSet   *rwset.TxPvtReadWriteSet
}

// TxPvtDataMap is a map from txNum to the pvtData
type TxPvtDataMap map[uint64]*TxPvtData

// MissingPvtData contains a namespace and collection for
// which the pvtData is not present. It also denotes
// whether the missing pvtData is eligible (i.e., whether
// the peer is member of the [namespace, collection]
type MissingPvtData struct {
	Namespace  string
	Collection string
	IsEligible bool
}

// TxMissingPvtData is a map from txNum to the list of
// missing pvtData
type TxMissingPvtData map[uint64][]*MissingPvtData

// BlockAndPvtData encapsulates the block and a map that contains the tuples <seqInBlock, *TxPvtData>
// The map is expected to contain the entries only for the transactions that has associated pvt data
type BlockAndPvtData struct {
	Block          *common.Block
	PvtData        TxPvtDataMap
	MissingPvtData TxMissingPvtData
}

// ReconciledPvtdata contains the private data for a block for reconciliation
type ReconciledPvtdata struct {
	BlockNum  uint64
	WriteSets TxPvtDataMap
}

// Add adds a given missing private data in the MissingPrivateDataList
func (txMissingPvtData TxMissingPvtData) Add(txNum uint64, ns, coll string, isEligible bool) {
	txMissingPvtData[txNum] = append(txMissingPvtData[txNum], &MissingPvtData{ns, coll, isEligible})
}

// RetrievedPvtdata is a dependency that is implemented by coordinator/gossip for ledger
// to be able to purge the transactions from the block after retrieving private data
type RetrievedPvtdata interface {
	GetBlockPvtdata() *BlockPvtdata
	Purge()
}

// TxPvtdataInfo captures information about the requested private data to be retrieved
type TxPvtdataInfo struct {
	TxID                  string
	Invalid               bool
	SeqInBlock            uint64
	CollectionPvtdataInfo []*CollectionPvtdataInfo
}

// CollectionPvtdataInfo contains information about the private data for a given collection
type CollectionPvtdataInfo struct {
	Namespace, Collection string
	ExpectedHash          []byte
	CollectionConfig      *peer.StaticCollectionConfig
	Endorsers             []*peer.Endorsement
}

// BlockPvtdata contains the retrieved private data as well as missing and ineligible
// private data for use at commit time
type BlockPvtdata struct {
	PvtData        TxPvtDataMap
	MissingPvtData TxMissingPvtData
}

// CommitOptions 封装与块提交关联的选项。
type CommitOptions struct {
	FetchPvtDataFromLedger bool
}

// PvtCollFilter represents the set of the collection names (as keys of the map with value 'true')
type PvtCollFilter map[string]bool

// PvtNsCollFilter specifies the tuple <namespace, PvtCollFilter>
type PvtNsCollFilter map[string]PvtCollFilter

// NewPvtNsCollFilter constructs an empty PvtNsCollFilter
func NewPvtNsCollFilter() PvtNsCollFilter {
	return make(map[string]PvtCollFilter)
}

// Has returns true if the pvtdata includes the data for collection <ns,coll>
func (pvtdata *TxPvtData) Has(ns string, coll string) bool {
	if pvtdata.WriteSet == nil {
		return false
	}
	for _, nsdata := range pvtdata.WriteSet.NsPvtRwset {
		if nsdata.Namespace == ns {
			for _, colldata := range nsdata.CollectionPvtRwset {
				if colldata.CollectionName == coll {
					return true
				}
			}
		}
	}
	return false
}

// Add adds a namespace-collection tuple to the filter
func (filter PvtNsCollFilter) Add(ns string, coll string) {
	collFilter, ok := filter[ns]
	if !ok {
		collFilter = make(map[string]bool)
		filter[ns] = collFilter
	}
	collFilter[coll] = true
}

// Has returns true if the filter has the entry for tuple namespace-collection
func (filter PvtNsCollFilter) Has(ns string, coll string) bool {
	collFilter, ok := filter[ns]
	if !ok {
		return false
	}
	return collFilter[coll]
}

// PrivateReads captures which private data collections are read during TX simulation.
type PrivateReads map[string]map[string]struct{}

// Add a collection to the set of private data collections that are read by the chaincode.
func (pr PrivateReads) Add(ns, coll string) {
	if _, ok := pr[ns]; !ok {
		pr[ns] = map[string]struct{}{}
	}
	pr[ns][coll] = struct{}{}
}

// Clone returns a copy of this struct.
func (pr PrivateReads) Clone() PrivateReads {
	clone := PrivateReads{}
	for ns, v := range pr {
		for coll := range v {
			clone.Add(ns, coll)
		}
	}
	return clone
}

// Exists returns whether a collection has been read
func (pr PrivateReads) Exists(ns, coll string) bool {
	if c, ok := pr[ns]; ok {
		if _, ok2 := c[coll]; ok2 {
			return true
		}
	}
	return false
}

// WritesetMetadata represents the content of the state metadata for each state (key) that gets written to during transaction simulation.
type WritesetMetadata map[string]map[string]map[string]map[string][]byte

// Add metadata to the structure.
func (wm WritesetMetadata) Add(ns, coll, key string, metadata map[string][]byte) {
	if _, ok := wm[ns]; !ok {
		wm[ns] = map[string]map[string]map[string][]byte{}
	}
	if _, ok := wm[ns][coll]; !ok {
		wm[ns][coll] = map[string]map[string][]byte{}
	}
	if metadata == nil {
		metadata = map[string][]byte{}
	}
	wm[ns][coll][key] = metadata
}

// Clone returns a copy of this struct.
func (wm WritesetMetadata) Clone() WritesetMetadata {
	clone := WritesetMetadata{}
	for ns, cm := range wm {
		for coll, km := range cm {
			for key, metadata := range km {
				clone.Add(ns, coll, key, metadata)
			}
		}
	}
	return clone
}

// TxSimulationResults captures the details of the simulation results
type TxSimulationResults struct {
	PubSimulationResults *rwset.TxReadWriteSet
	PvtSimulationResults *rwset.TxPvtReadWriteSet
	PrivateReads         PrivateReads
	WritesetMetadata     WritesetMetadata
}

// GetPubSimulationBytes returns the serialized bytes of public readwrite set
func (txSim *TxSimulationResults) GetPubSimulationBytes() ([]byte, error) {
	return proto.Marshal(txSim.PubSimulationResults)
}

// GetPvtSimulationBytes returns the serialized bytes of private readwrite set
func (txSim *TxSimulationResults) GetPvtSimulationBytes() ([]byte, error) {
	if !txSim.ContainsPvtWrites() {
		return nil, nil
	}
	return proto.Marshal(txSim.PvtSimulationResults)
}

// ContainsPvtWrites returns true if the simulation results include the private writes
func (txSim *TxSimulationResults) ContainsPvtWrites() bool {
	return txSim.PvtSimulationResults != nil
}

// StateListener allows a custom code for performing additional stuff upon state change
// for a particular namespace against which the listener is registered.
// This helps to perform custom tasks other than the state updates.
// A ledger implementation is expected to invoke Function `HandleStateUpdates` once per block and
// the `stateUpdates` parameter passed to the function captures the state changes caused by the block
// for the namespace. The actual data type of stateUpdates depends on the data model enabled.
// For instance, for KV data model, the actual type would be proto message
// `github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset.KVWrite`
// Function `HandleStateUpdates` is expected to be invoked before block is committed and if this
// function returns an error, the ledger implementation is expected to halt block commit operation
// and result in a panic.
// The function Initialize is invoked only once at the time of opening the ledger.
type StateListener interface {
	Name() string
	Initialize(ledgerID string, qe SimpleQueryExecutor) error
	InterestedInNamespaces() []string
	HandleStateUpdates(trigger *StateUpdateTrigger) error
	StateCommitDone(channelID string)
}

// StateUpdateTrigger encapsulates the information and helper tools that may be used by a StateListener
type StateUpdateTrigger struct {
	LedgerID                    string
	StateUpdates                StateUpdates
	CommittingBlockNum          uint64
	CommittedStateQueryExecutor SimpleQueryExecutor
	PostCommitQueryExecutor     SimpleQueryExecutor
}

// StateUpdates encapsulates the state updates
type StateUpdates map[string]*KVStateUpdates

// KVStateUpdates captures the state updates for a namespace for KV datamodel
type KVStateUpdates struct {
	PublicUpdates   []*kvrwset.KVWrite
	CollHashUpdates map[string][]*kvrwset.KVWriteHash
}

// ConfigHistoryRetriever allow retrieving history of collection configs
type ConfigHistoryRetriever interface {
	MostRecentCollectionConfigBelow(blockNum uint64, chaincodeName string) (*CollectionConfigInfo, error)
}

// MissingPvtDataTracker allows getting information about the private data that is not missing on the peer
type MissingPvtDataTracker interface {
	GetMissingPvtDataInfoForMostRecentBlocks(maxBlocks int) (MissingPvtDataInfo, error)
}

// MissingPvtDataInfo is a map of block number to MissingBlockPvtdataInfo
type MissingPvtDataInfo map[uint64]MissingBlockPvtdataInfo

// MissingBlockPvtdataInfo is a map of transaction number (within the block) to MissingCollectionPvtDataInfo
type MissingBlockPvtdataInfo map[uint64][]*MissingCollectionPvtDataInfo

// MissingCollectionPvtDataInfo includes the name of the chaincode and collection for which private data is missing
type MissingCollectionPvtDataInfo struct {
	Namespace, Collection string
}

// CollectionConfigInfo encapsulates a collection config for a chaincode and its committing block number
type CollectionConfigInfo struct {
	CollectionConfig   *peer.CollectionConfigPackage
	CommittingBlockNum uint64
}

// Add adds a missing data entry to the MissingPvtDataInfo Map
func (missingPvtDataInfo MissingPvtDataInfo) Add(blkNum, txNum uint64, ns, coll string) {
	missingBlockPvtDataInfo, ok := missingPvtDataInfo[blkNum]
	if !ok {
		missingBlockPvtDataInfo = make(MissingBlockPvtdataInfo)
		missingPvtDataInfo[blkNum] = missingBlockPvtDataInfo
	}

	if _, ok := missingBlockPvtDataInfo[txNum]; !ok {
		missingBlockPvtDataInfo[txNum] = []*MissingCollectionPvtDataInfo{}
	}

	missingBlockPvtDataInfo[txNum] = append(missingBlockPvtDataInfo[txNum],
		&MissingCollectionPvtDataInfo{
			Namespace:  ns,
			Collection: coll,
		})
}

// CollConfigNotDefinedError is returned whenever an operation
// is requested on a collection whose config has not been defined
type CollConfigNotDefinedError struct {
	Ns string
}

func (e *CollConfigNotDefinedError) Error() string {
	return fmt.Sprintf("collection config not defined for chaincode [%s], pass the collection configuration upon chaincode definition/instantiation", e.Ns)
}

// InvalidCollNameError is returned whenever an operation
// is requested on a collection whose name is invalid
type InvalidCollNameError struct {
	Ns, Coll string
}

func (e *InvalidCollNameError) Error() string {
	return fmt.Sprintf("collection [%s] not defined in the collection config for chaincode [%s]", e.Coll, e.Ns)
}

// PvtdataHashMismatch is used when the hash of private write-set
// does not match the corresponding hash present in the block
// or there is a mismatch with the boot-KV-hashes present in the
// private block store if the legder is created from a snapshot
type PvtdataHashMismatch struct {
	BlockNum, TxNum       uint64
	Namespace, Collection string
}

// DeployedChaincodeInfoProvider 是一个依赖项，由账本用于构建集合配置历史记录。
// LSCC 模块应该为这个依赖项提供一个实现。
type DeployedChaincodeInfoProvider interface {
	// Namespaces 返回用于维护链码生命周期数据的命名空间切片。
	Namespaces() []string
	// UpdatedChaincodes 返回由提供的 'stateUpdates' 更新的链码。
	UpdatedChaincodes(stateUpdates map[string][]*kvrwset.KVWrite) ([]*ChaincodeLifecycleInfo, error)
	// ChaincodeInfo 返回部署链码的信息。
	ChaincodeInfo(channelName, chaincodeName string, qe SimpleQueryExecutor) (*DeployedChaincodeInfo, error)
	// AllChaincodesInfo 返回所有已部署链码的链码名称到 DeployedChaincodeInfo 的映射。
	AllChaincodesInfo(channelName string, qe SimpleQueryExecutor) (map[string]*DeployedChaincodeInfo, error)
	// CollectionInfo 返回定义了指定集合的 proto 消息。此函数可用于显式和隐式集合。
	CollectionInfo(channelName, chaincodeName, collectionName string, qe SimpleQueryExecutor) (*peer.StaticCollectionConfig, error)
	// ImplicitCollections 返回包含每个隐式集合的一个 proto 消息的切片。
	ImplicitCollections(channelName, chaincodeName string, qe SimpleQueryExecutor) ([]*peer.StaticCollectionConfig, error)
	// GenerateImplicitCollectionForOrg 为组织生成隐式集合。
	GenerateImplicitCollectionForOrg(mspid string) *peer.StaticCollectionConfig
	// AllCollectionsConfigPkg 返回一个包含显式和隐式集合的组合集合配置包。
	AllCollectionsConfigPkg(channelName, chaincodeName string, qe SimpleQueryExecutor) (*peer.CollectionConfigPackage, error)
}

// DeployedChaincodeInfo encapsulates chaincode information from the deployed chaincodes
type DeployedChaincodeInfo struct {
	Name                        string
	Hash                        []byte
	Version                     string
	ExplicitCollectionConfigPkg *peer.CollectionConfigPackage
	IsLegacy                    bool
}

// ChaincodeLifecycleInfo captures the update info of a chaincode
type ChaincodeLifecycleInfo struct {
	Name    string
	Deleted bool
	Details *ChaincodeLifecycleDetails // Can contain finer details about lifecycle event that can be used for certain optimization
}

// ChaincodeLifecycleDetails captures the finer details of chaincode lifecycle event
type ChaincodeLifecycleDetails struct {
	Updated bool // true, if an existing chaincode is updated (false for newly deployed chaincodes).
	// Following attributes are meaningful only if 'Updated' is true
	HashChanged        bool     // true, if the chaincode code package is changed
	CollectionsUpdated []string // names of the explicit collections that are either added or updated
	CollectionsRemoved []string // names of the explicit collections that are removed
}

// MembershipInfoProvider is a dependency that is used by ledger to determine whether the current peer is
// a member of a collection. Gossip module is expected to provide the dependency to ledger
type MembershipInfoProvider interface {
	// AmMemberOf checks whether the current peer is a member of the given collection
	AmMemberOf(channelName string, collectionPolicyConfig *peer.CollectionPolicyConfig) (bool, error)
	// MyImplicitCollectionName returns the name of the implicit collection for the current peer
	MyImplicitCollectionName() string
}

type HealthCheckRegistry interface {
	RegisterChecker(string, healthz.HealthChecker) error
}

// ChaincodeLifecycleEventListener interface enables ledger components (mainly, intended for statedb)
// to be able to listen to chaincode lifecycle events. 'dbArtifactsTar' represents db specific artifacts
// (such as index specs) packaged in a tar. Note that this interface is redefined here (in addition to
// the one defined in ledger/cceventmgmt package). Using the same interface for the new lifecycle path causes
// a cyclic import dependency. Moreover, eventually the whole package ledger/cceventmgmt is intended to
// be removed when migration to new lifecycle is mandated.
type ChaincodeLifecycleEventListener interface {
	// HandleChaincodeDeploy is invoked when chaincode installed + defined becomes true.
	// The expected usage are to creates all the necessary statedb structures (such as indexes) and update
	// service discovery info. This function is invoked immediately before the committing the state changes
	// that contain chaincode definition or when a chaincode install happens
	HandleChaincodeDeploy(chaincodeDefinition *ChaincodeDefinition, dbArtifactsTar []byte) error
	// ChaincodeDeployDone is invoked after the chaincode deployment is finished - `succeeded` indicates
	// whether the deploy finished successfully
	ChaincodeDeployDone(succeeded bool)
}

// ChaincodeDefinition captures the info about chaincode
type ChaincodeDefinition struct {
	Name              string
	Hash              []byte
	Version           string
	CollectionConfigs *peer.CollectionConfigPackage
}

func (cdef *ChaincodeDefinition) String() string {
	return fmt.Sprintf("Name=%s, Version=%s, Hash=%#v", cdef.Name, cdef.Version, cdef.Hash)
}

// ChaincodeLifecycleEventProvider enables ledger to create indexes in the statedb
type ChaincodeLifecycleEventProvider interface {
	// RegisterListener is used by ledger to receive a callback alongwith dbArtifacts when a chaincode becomes invocable on the peer
	// In addition, if needsExistingChaincodesDefinitions is true, the provider calls back the listener with existing invocable chaincodes
	// This parameter is used when we create a ledger from a snapshot so that we can create indexes for the existing invocable chaincodes
	// already defined in the imported ledger data
	RegisterListener(channelID string, listener ChaincodeLifecycleEventListener, needsExistingChaincodesDefinitions bool) error
}

// CustomTxProcessor 允许在自定义事务的提交时间内生成模拟结果。
// 自定义处理器可以以适当的方式表示信息，并且可以使用该过程将信息转换为 “txsimulationresults” 的形式。
// 因为，原始信息是在自定义表示中签名的，
// “处理器” 的实现应谨慎，自定义表示以确定性方式用于仿真，并应注意跨结构版本的兼容性。
// 'initializingLedger' true表示正在处理的事务来自创世区块，或者分类账正在同步状态 (如果发现statedb落后于区块链，则可能在对等启动期间发生)。
// 在前一种情况下，所处理的事务预期是有效的，而在后一种情况下，仅重新处理有效的事务，因此可以跳过任何验证。
type CustomTxProcessor interface {
	GenerateSimulationResults(txEnvelop *common.Envelope, simulator TxSimulator, initializingLedger bool) error
}

// InvalidTxError is expected to be thrown by a custom transaction processor
// if it wants the ledger to record a particular transaction as invalid
type InvalidTxError struct {
	Msg string
}

func (e *InvalidTxError) Error() string {
	return e.Msg
}

// HashProvider provides access to a hash.Hash for ledger components.
// Currently works at a stepping stone to decrease surface area of bccsp
type HashProvider interface {
	GetHash(opts bccsp.HashOpts) (hash.Hash, error)
}

// CommitNotification is sent on each block commit to the channel returned by PeerLedger.CommitNotificationsChannel().
// TxsInfo field contains the info about individual transactions in the block in the order the transactions appear in the block
// The transactions with a unique and non-empty txID are included in the notification
type CommitNotification struct {
	BlockNumber uint64
	TxsInfo     []*CommitNotificationTxInfo
}

// CommitNotificationTxInfo contains the details of a transaction that is included in the CommitNotification
// ChaincodeID will be nil if the transaction is not an endorser transaction. This may or may not be nil if the tranasction is invalid.
// Specifically, it will be nil if the transaction is marked invalid by the validator (e.g., bad payload or insufficient endorements) and it will be non-nil if the transaction is marked invalid for concurrency conflicts.
// However, it is guaranteed be non-nil if the transaction is a valid endorser transaction.
type CommitNotificationTxInfo struct {
	TxType             common.HeaderType
	TxID               string
	ValidationCode     peer.TxValidationCode
	ChaincodeID        *peer.ChaincodeID
	ChaincodeEventData []byte
}

//go:generate counterfeiter -o mock/state_listener.go -fake-name StateListener . StateListener
//go:generate counterfeiter -o mock/query_executor.go -fake-name QueryExecutor . QueryExecutor
//go:generate counterfeiter -o mock/tx_simulator.go -fake-name TxSimulator . TxSimulator
//go:generate counterfeiter -o mock/deployed_ccinfo_provider.go -fake-name DeployedChaincodeInfoProvider . DeployedChaincodeInfoProvider
//go:generate counterfeiter -o mock/membership_info_provider.go -fake-name MembershipInfoProvider . MembershipInfoProvider
//go:generate counterfeiter -o mock/health_check_registry.go -fake-name HealthCheckRegistry . HealthCheckRegistry
//go:generate counterfeiter -o mock/cc_event_listener.go -fake-name ChaincodeLifecycleEventListener . ChaincodeLifecycleEventListener
//go:generate counterfeiter -o mock/custom_tx_processor.go -fake-name CustomTxProcessor . CustomTxProcessor
//go:generate counterfeiter -o mock/cc_event_provider.go -fake-name ChaincodeLifecycleEventProvider . ChaincodeLifecycleEventProvider
