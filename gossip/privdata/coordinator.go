/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	protostransientstore "github.com/hyperledger/fabric-protos-go/transientstore"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/gossip/metrics"
	privdatacommon "github.com/hyperledger/fabric/gossip/privdata/common"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const pullRetrySleepInterval = time.Second

var logger = util.GetLogger(util.PrivateDataLogger, "")

//go:generate mockery -dir . -name CollectionStore -case underscore -output mocks/

// CollectionStore is the local interface used to generate mocks for foreign interface.
type CollectionStore interface {
	privdata.CollectionStore
}

//go:generate mockery -dir . -name Committer -case underscore -output mocks/

// Committer is the local interface used to generate mocks for foreign interface.
type Committer interface {
	committer.Committer
}

// Coordinator orchestrates the flow of the new
// blocks arrival and in flight transient data, responsible
// to complete missing parts of transient data for given block.
type Coordinator interface {
	// StoreBlock 方法负责将包含私有数据的区块存储到分类账中，涉及验证、私有数据检索与存储等关键步骤。
	StoreBlock(block *common.Block, data util.PvtDataCollections) error

	// StorePvtData used to persist private data into transient store
	StorePvtData(txid string, privData *protostransientstore.TxPvtReadWriteSetWithConfigInfo, blckHeight uint64) error

	// GetPvtDataAndBlockByNum gets block by number and also returns all related private data
	// that requesting peer is eligible for.
	// The order of private data in slice of PvtDataCollections doesn't imply the order of
	// transactions in the block related to these private data, to get the correct placement
	// need to read TxPvtData.SeqInBlock field
	GetPvtDataAndBlockByNum(seqNum uint64, peerAuth protoutil.SignedData) (*common.Block, util.PvtDataCollections, error)

	// Get recent block sequence number
	LedgerHeight() (uint64, error)

	// Close coordinator, shuts down coordinator service
	Close()
}

type dig2sources map[privdatacommon.DigKey][]*peer.Endorsement

func (d2s dig2sources) keys() []privdatacommon.DigKey {
	res := make([]privdatacommon.DigKey, 0, len(d2s))
	for dig := range d2s {
		res = append(res, dig)
	}
	return res
}

// Fetcher interface which defines API to fetch missing
// private data elements
type Fetcher interface {
	fetch(dig2src dig2sources) (*privdatacommon.FetchedPvtDataContainer, error)
}

//go:generate mockery -dir ./ -name CapabilityProvider -case underscore -output mocks/

// CapabilityProvider contains functions to retrieve capability information for a channel
type CapabilityProvider interface {
	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

// Support encapsulates set of interfaces to
// aggregate required functionality by single struct
type Support struct {
	ChainID string
	privdata.CollectionStore
	txvalidator.Validator
	committer.Committer
	Fetcher
	CapabilityProvider
}

// CoordinatorConfig encapsulates the config that is passed to a new coordinator
type CoordinatorConfig struct {
	// TransientBlockRetention indicates the number of blocks to retain in the transient store
	// when purging below height on committing every TransientBlockRetention-th block
	TransientBlockRetention uint64
	// PullRetryThreshold indicates the max duration an attempted fetch from a remote peer will retry
	// for before giving up and leaving the private data as missing
	PullRetryThreshold time.Duration
	// SkipPullingInvalidTransactions if true will skip the fetch from remote peer step for transactions
	// marked as invalid
	SkipPullingInvalidTransactions bool
}

type coordinator struct {
	mspID          string
	selfSignedData protoutil.SignedData
	Support
	store                          *transientstore.Store
	transientBlockRetention        uint64
	logger                         util.Logger
	metrics                        *metrics.PrivdataMetrics
	pullRetryThreshold             time.Duration
	skipPullingInvalidTransactions bool
	idDeserializerFactory          IdentityDeserializerFactory
}

// NewCoordinator creates a new instance of coordinator
func NewCoordinator(mspID string, support Support, store *transientstore.Store, selfSignedData protoutil.SignedData, metrics *metrics.PrivdataMetrics,
	config CoordinatorConfig, idDeserializerFactory IdentityDeserializerFactory) Coordinator {
	return &coordinator{
		Support:                        support,
		mspID:                          mspID,
		store:                          store,
		selfSignedData:                 selfSignedData,
		transientBlockRetention:        config.TransientBlockRetention,
		logger:                         logger.With("channel", support.ChainID),
		metrics:                        metrics,
		pullRetryThreshold:             config.PullRetryThreshold,
		skipPullingInvalidTransactions: config.SkipPullingInvalidTransactions,
		idDeserializerFactory:          idDeserializerFactory,
	}
}

// StoreBlock 方法负责将包含私有数据的区块存储到分类账中，涉及验证、私有数据检索与存储等关键步骤。
func (c *coordinator) StoreBlock(block *common.Block, privateDataSets util.PvtDataCollections) error {
	// 验证区块数据是否存在
	if block.Data == nil {
		return errors.New("区块数据为空")
	}
	// 验证区块头是否存在
	if block.Header == nil {
		return errors.New("区块头为nil")
	}

	// 日志记录接收到的区块信息
	c.logger.Infof("从缓冲区收到块 [%d]", block.Header.Number)

	// 开始验证区块
	c.logger.Debugf("正在验证块 [%d]", block.Header.Number)

	// 标记验证开始时间
	validationStart := time.Now()
	// 执行区块验证
	err := c.Validator.Validate(block)
	// 记录验证耗时
	c.reportValidationDuration(time.Since(validationStart))
	if err != nil {
		// 如果验证失败，日志记录错误并返回
		c.logger.Errorf("验证失败: %+v", err)
		return err
	}

	// 创建区块与私有数据结构
	blockAndPvtData := &ledger.BlockAndPvtData{
		Block:          block,
		PvtData:        make(ledger.TxPvtDataMap),
		MissingPvtData: make(ledger.TxMissingPvtData),
	}

	// 检查私有数据信息是否已存在于分类账中
	exist, err := c.DoesPvtDataInfoExistInLedger(block.Header.Number)
	if err != nil {
		return err
	}
	if exist {
		// 如果私有数据已存在，使用特定选项提交区块
		commitOpts := &ledger.CommitOptions{FetchPvtDataFromLedger: true}
		return c.CommitLegacy(blockAndPvtData, commitOpts)
	}

	// 初始化度量指标
	listMissingPrivateDataDurationHistogram := c.metrics.ListMissingPrivateDataDuration.With("channel", c.ChainID)
	fetchDurationHistogram := c.metrics.FetchDuration.With("channel", c.ChainID)
	purgeDurationHistogram := c.metrics.PurgeDuration.With("channel", c.ChainID)

	// 创建私有数据提供者
	pdp := &PvtdataProvider{
		mspID:                                   c.mspID,
		selfSignedData:                          c.selfSignedData,
		logger:                                  logger.With("channel", c.ChainID),
		listMissingPrivateDataDurationHistogram: listMissingPrivateDataDurationHistogram,
		fetchDurationHistogram:                  fetchDurationHistogram,
		purgeDurationHistogram:                  purgeDurationHistogram,
		transientStore:                          c.store,
		pullRetryThreshold:                      c.pullRetryThreshold,
		prefetchedPvtdata:                       privateDataSets,
		transientBlockRetention:                 c.transientBlockRetention,
		channelID:                               c.ChainID,
		blockNum:                                block.Header.Number,
		storePvtdataOfInvalidTx:                 c.Support.CapabilityProvider.Capabilities().StorePvtDataOfInvalidTx(),
		skipPullingInvalidTransactions:          c.skipPullingInvalidTransactions,
		fetcher:                                 c.Fetcher,
		idDeserializerFactory:                   c.idDeserializerFactory,
	}
	// 从区块中获取私有数据信息
	pvtdataToRetrieve, err := c.getTxPvtdataInfoFromBlock(block)
	if err != nil {
		c.logger.Warningf("从区块获取私有数据信息失败: %s", err)
		return err
	}

	// 检索私有数据
	// RetrievePvtdata 方法首先检查此节点的资格，然后从缓存、临时存储或远程节点检索私有数据。
	retrievedPvtdata, err := pdp.RetrievePvtdata(pvtdataToRetrieve)
	if err != nil {
		c.logger.Warningf("检索私有数据失败: %s", err)
		return err
	}

	// 更新区块与私有数据结构中的私有数据
	blockAndPvtData.PvtData = retrievedPvtdata.blockPvtdata.PvtData
	blockAndPvtData.MissingPvtData = retrievedPvtdata.blockPvtdata.MissingPvtData

	// 开始提交区块和私有数据
	commitStart := time.Now()
	// 执行提交操作
	err = c.CommitLegacy(blockAndPvtData, &ledger.CommitOptions{})
	// 记录提交耗时
	c.reportCommitDuration(time.Since(commitStart))
	if err != nil {
		// 如果提交失败，日志记录错误并返回
		return errors.Wrap(err, "提交失败")
	}

	// 异步清理事务
	go retrievedPvtdata.Purge()

	return nil
}

// StorePvtData used to persist private data into transient store
func (c *coordinator) StorePvtData(txID string, privData *protostransientstore.TxPvtReadWriteSetWithConfigInfo, blkHeight uint64) error {
	return c.store.Persist(txID, blkHeight, privData)
}

// GetPvtDataAndBlockByNum gets block by number and also returns all related private data
// that requesting peer is eligible for.
// The order of private data in slice of PvtDataCollections doesn't imply the order of
// transactions in the block related to these private data, to get the correct placement
// need to read TxPvtData.SeqInBlock field
func (c *coordinator) GetPvtDataAndBlockByNum(seqNum uint64, peerAuthInfo protoutil.SignedData) (*common.Block, util.PvtDataCollections, error) {
	blockAndPvtData, err := c.Committer.GetPvtDataAndBlockByNum(seqNum)
	if err != nil {
		return nil, nil, err
	}

	seqs2Namespaces := aggregatedCollections{}
	for seqInBlock := range blockAndPvtData.Block.Data.Data {
		txPvtDataItem, exists := blockAndPvtData.PvtData[uint64(seqInBlock)]
		if !exists {
			continue
		}

		// Iterate through the private write sets and include them in response if requesting peer is eligible for it
		for _, ns := range txPvtDataItem.WriteSet.NsPvtRwset {
			for _, col := range ns.CollectionPvtRwset {
				cc := privdata.CollectionCriteria{
					Channel:    c.ChainID,
					Namespace:  ns.Namespace,
					Collection: col.CollectionName,
				}
				sp, err := c.CollectionStore.RetrieveCollectionAccessPolicy(cc)
				if err != nil {
					c.logger.Warningf("Failed obtaining policy for collection criteria [%#v]: %s", cc, err)
					continue
				}
				isAuthorized := sp.AccessFilter()
				if isAuthorized == nil {
					c.logger.Warningf("Failed obtaining filter for collection criteria [%#v]", cc)
					continue
				}
				if !isAuthorized(peerAuthInfo) {
					c.logger.Debugf("Skipping collection criteria [%#v] because peer isn't authorized", cc)
					continue
				}
				seqs2Namespaces.addCollection(uint64(seqInBlock), txPvtDataItem.WriteSet.DataModel, ns.Namespace, col)
			}
		}
	}

	return blockAndPvtData.Block, seqs2Namespaces.asPrivateData(), nil
}

// getTxPvtdataInfoFromBlock parses the block transactions and returns the list of private data items in the block.
// Note that this peer's eligibility for the private data is not checked here.
func (c *coordinator) getTxPvtdataInfoFromBlock(block *common.Block) ([]*ledger.TxPvtdataInfo, error) {
	txPvtdataItemsFromBlock := []*ledger.TxPvtdataInfo{}

	if block.Metadata == nil || len(block.Metadata.Metadata) <= int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		return nil, errors.New("Block.Metadata is nil or Block.Metadata lacks a Tx filter bitmap")
	}
	txsFilter := txValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	data := block.Data.Data
	if len(txsFilter) != len(block.Data.Data) {
		return nil, errors.Errorf("block data size(%d) is different from Tx filter size(%d)", len(block.Data.Data), len(txsFilter))
	}

	for seqInBlock, txEnvBytes := range data {
		invalid := txsFilter[seqInBlock] != uint8(peer.TxValidationCode_VALID)
		txInfo, err := getTxInfoFromTransactionBytes(txEnvBytes)
		if err != nil {
			continue
		}

		colPvtdataInfo := []*ledger.CollectionPvtdataInfo{}
		for _, ns := range txInfo.txRWSet.NsRwSets {
			for _, hashedCollection := range ns.CollHashedRwSets {
				// skip if no writes
				if !containsWrites(txInfo.txID, ns.NameSpace, hashedCollection) {
					continue
				}
				cc := privdata.CollectionCriteria{
					Channel:    txInfo.channelID,
					Namespace:  ns.NameSpace,
					Collection: hashedCollection.CollectionName,
				}

				colConfig, err := c.CollectionStore.RetrieveCollectionConfig(cc)
				if err != nil {
					c.logger.Warningf("Failed to retrieve collection config for collection criteria [%#v]: %s", cc, err)
					return nil, err
				}
				col := &ledger.CollectionPvtdataInfo{
					Namespace:        ns.NameSpace,
					Collection:       hashedCollection.CollectionName,
					ExpectedHash:     hashedCollection.PvtRwSetHash,
					CollectionConfig: colConfig,
					Endorsers:        txInfo.endorsements,
				}
				colPvtdataInfo = append(colPvtdataInfo, col)
			}
		}
		txPvtdataToRetrieve := &ledger.TxPvtdataInfo{
			TxID:                  txInfo.txID,
			Invalid:               invalid,
			SeqInBlock:            uint64(seqInBlock),
			CollectionPvtdataInfo: colPvtdataInfo,
		}
		txPvtdataItemsFromBlock = append(txPvtdataItemsFromBlock, txPvtdataToRetrieve)
	}

	return txPvtdataItemsFromBlock, nil
}

func (c *coordinator) reportValidationDuration(time time.Duration) {
	c.metrics.ValidationDuration.With("channel", c.ChainID).Observe(time.Seconds())
}

func (c *coordinator) reportCommitDuration(time time.Duration) {
	c.metrics.CommitPrivateDataDuration.With("channel", c.ChainID).Observe(time.Seconds())
}

type seqAndDataModel struct {
	seq       uint64
	dataModel rwset.TxReadWriteSet_DataModel
}

// map from seqAndDataModel to:
//
//	map from namespace to []*rwset.CollectionPvtReadWriteSet
type aggregatedCollections map[seqAndDataModel]map[string][]*rwset.CollectionPvtReadWriteSet

func (ac aggregatedCollections) addCollection(seqInBlock uint64, dm rwset.TxReadWriteSet_DataModel, namespace string, col *rwset.CollectionPvtReadWriteSet) {
	seq := seqAndDataModel{
		dataModel: dm,
		seq:       seqInBlock,
	}
	if _, exists := ac[seq]; !exists {
		ac[seq] = make(map[string][]*rwset.CollectionPvtReadWriteSet)
	}
	ac[seq][namespace] = append(ac[seq][namespace], col)
}

func (ac aggregatedCollections) asPrivateData() []*ledger.TxPvtData {
	var data []*ledger.TxPvtData
	for seq, ns := range ac {
		txPrivateData := &ledger.TxPvtData{
			SeqInBlock: seq.seq,
			WriteSet: &rwset.TxPvtReadWriteSet{
				DataModel: seq.dataModel,
			},
		}
		for namespaceName, cols := range ns {
			txPrivateData.WriteSet.NsPvtRwset = append(txPrivateData.WriteSet.NsPvtRwset, &rwset.NsPvtReadWriteSet{
				Namespace:          namespaceName,
				CollectionPvtRwset: cols,
			})
		}
		data = append(data, txPrivateData)
	}
	return data
}

type txInfo struct {
	channelID    string
	txID         string
	endorsements []*peer.Endorsement
	txRWSet      *rwsetutil.TxRwSet
}

// getTxInfoFromTransactionBytes parses a transaction and returns info required for private data retrieval
func getTxInfoFromTransactionBytes(envBytes []byte) (*txInfo, error) {
	txInfo := &txInfo{}
	env, err := protoutil.GetEnvelopeFromBlock(envBytes)
	if err != nil {
		logger.Warningf("Invalid envelope: %s", err)
		return nil, err
	}

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		logger.Warningf("Invalid payload: %s", err)
		return nil, err
	}
	if payload.Header == nil {
		err := errors.New("payload header is nil")
		logger.Warningf("Invalid tx: %s", err)
		return nil, err
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Warningf("Invalid channel header: %s", err)
		return nil, err
	}
	txInfo.channelID = chdr.ChannelId
	txInfo.txID = chdr.TxId

	if chdr.Type != int32(common.HeaderType_ENDORSER_TRANSACTION) {
		err := errors.New("header type is not an endorser transaction")
		logger.Debugf("Invalid transaction type: %s", err)
		return nil, err
	}

	respPayload, err := protoutil.GetActionFromEnvelope(envBytes)
	if err != nil {
		logger.Warningf("Failed obtaining action from envelope: %s", err)
		return nil, err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		logger.Warningf("Invalid transaction in payload data for tx [%s]: %s", chdr.TxId, err)
		return nil, err
	}

	ccActionPayload, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		logger.Warningf("Invalid chaincode action in payload for tx [%s]: %s", chdr.TxId, err)
		return nil, err
	}

	if ccActionPayload.Action == nil {
		logger.Warningf("Action in ChaincodeActionPayload for tx [%s] is nil", chdr.TxId)
		return nil, err
	}
	txInfo.endorsements = ccActionPayload.Action.Endorsements

	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		logger.Warningf("Failed obtaining TxRwSet from ChaincodeAction's results: %s", err)
		return nil, err
	}
	txInfo.txRWSet = txRWSet

	return txInfo, nil
}

// containsWrites checks whether the given CollHashedRwSet contains writes
func containsWrites(txID string, namespace string, colHashedRWSet *rwsetutil.CollHashedRwSet) bool {
	if colHashedRWSet.HashedRwSet == nil {
		logger.Warningf("HashedRWSet of tx [%s], namespace [%s], collection [%s] is nil", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	if len(colHashedRWSet.HashedRwSet.HashedWrites) == 0 && len(colHashedRWSet.HashedRwSet.MetadataWrites) == 0 {
		logger.Debugf("HashedRWSet of tx [%s], namespace [%s], collection [%s] doesn't contain writes", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	return true
}
