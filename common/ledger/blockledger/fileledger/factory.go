/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fileledger

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/orderer/common/filerepo"
)

//go:generate counterfeiter -o mock/block_store_provider.go --fake-name BlockStoreProvider . blockStoreProvider
type blockStoreProvider interface {
	Open(ledgerid string) (*blkstorage.BlockStore, error)
	Drop(ledgerid string) error
	List() ([]string, error)
	Close()
}

type fileLedgerFactory struct {
	blkstorageProvider blockStoreProvider
	ledgers            map[string]*FileLedger
	mutex              sync.Mutex
	removeFileRepo     *filerepo.Repo
}

// GetOrCreate gets an existing ledger (if it exists) or creates it
// if it does not.
func (f *fileLedgerFactory) GetOrCreate(channelID string) (blockledger.ReadWriter, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// check cache
	ledger, ok := f.ledgers[channelID]
	if ok {
		return ledger, nil
	}
	// open fresh
	blockStore, err := f.blkstorageProvider.Open(channelID)
	if err != nil {
		return nil, err
	}
	ledger = NewFileLedger(blockStore)
	f.ledgers[channelID] = ledger
	return ledger, nil
}

// Remove removes an existing ledger and its indexes. This operation
// is blocking.
func (f *fileLedgerFactory) Remove(channelID string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if err := f.removeFileRepo.Save(channelID, []byte{}); err != nil && err != os.ErrExist {
		return err
	}

	// check cache for open blockstore and, if one exists,
	// shut it down in order to avoid resource contention
	ledger, ok := f.ledgers[channelID]
	if ok {
		ledger.blockStore.Shutdown()
	}

	err := f.blkstorageProvider.Drop(channelID)
	if err != nil {
		return err
	}

	delete(f.ledgers, channelID)

	if err := f.removeFileRepo.Remove(channelID); err != nil {
		return err
	}

	return nil
}

// ChannelIDs returns the channel IDs the factory is aware of.
func (f *fileLedgerFactory) ChannelIDs() []string {
	channelIDs, err := f.blkstorageProvider.List()
	if err != nil {
		logger.Panic(err)
	}
	return channelIDs
}

// Close releases all resources acquired by the factory.
func (f *fileLedgerFactory) Close() {
	f.blkstorageProvider.Close()
}

// New creates a new ledger factory
// 创建和初始化账本工厂
// 账本工厂的主要内容
// 创建和初始化账本设置（Ledger Config）：账本工厂可能需要根据配置文件中的参数创建和初始化账本的设置，如区块存储路径、索引配置等。
// 创建和初始化区块存储（Block Store）：账本工厂可以创建并连接到用于存储区块的后端存储系统，如文件系统、数据库等。
// 创建和初始化状态数据库（State Database）：账本工厂可以创建并连接到用于存储和管理状态的后端数据库，如LevelDB、CouchDB等。
// 创建和初始化区块链的订阅和事件处理器：账本工厂可能需要订阅和处理与账本相关的事件，例如新区块的接收、状态的更新等。
func New(directory string, metricsProvider metrics.Provider) (blockledger.Factory, error) {
	p, err := blkstorage.NewProvider(
		blkstorage.NewConf(directory, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		},
		metricsProvider,
	)
	if err != nil {
		return nil, err
	}

	// 创建新的文件存储库实例, 该实例实现了save,remove,read等方法
	fileRepo, err := filerepo.New(filepath.Join(directory, "pendingops"), "remove")
	if err != nil {
		return nil, err
	}

	// 创建一个fileLedgerFactory 账本工厂实例
	factory := &fileLedgerFactory{
		blkstorageProvider: p,
		ledgers:            map[string]*FileLedger{},
		removeFileRepo:     fileRepo,
	}

	files, err := factory.removeFileRepo.List()
	if err != nil {
		return nil, err
	}
	for _, fileName := range files {
		channelID := factory.removeFileRepo.FileToBaseName(fileName)
		err = factory.Remove(channelID)
		if err != nil {
			logger.Errorf("Failed to remove channel %s: %s", channelID, err.Error())
			return nil, err
		}
		logger.Infof("Removed channel: %s", channelID)
	}

	return factory, nil
}
