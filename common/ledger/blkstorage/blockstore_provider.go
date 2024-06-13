/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("blkstorage")

// IndexableAttr 表示可索引的属性
type IndexableAttr string

// 可索引属性的常量
const (
	IndexableAttrBlockNum        = IndexableAttr("BlockNum")
	IndexableAttrBlockHash       = IndexableAttr("BlockHash")
	IndexableAttrTxID            = IndexableAttr("TxID")
	IndexableAttrBlockNumTranNum = IndexableAttr("BlockNumTranNum")
)

// IndexConfig - 包含应编制索引的属性列表的配置
type IndexConfig struct {
	AttrsToIndex []IndexableAttr
}

// SnapshotInfo captures some of the details about the snapshot
type SnapshotInfo struct {
	LastBlockNum      uint64
	LastBlockHash     []byte
	PreviousBlockHash []byte
}

// Contains returns true iff the supplied parameter is present in the IndexConfig.AttrsToIndex
func (c *IndexConfig) Contains(indexableAttr IndexableAttr) bool {
	for _, a := range c.AttrsToIndex {
		if a == indexableAttr {
			return true
		}
	}
	return false
}

// BlockStoreProvider 提供对区块存储的句柄 - 这不是线程安全的
type BlockStoreProvider struct {
	conf            *Conf                   // 配置
	indexConfig     *IndexConfig            // 索引配置
	leveldbProvider *leveldbhelper.Provider // LevelDB 提供者
	stats           *stats                  // 统计信息
}

// NewProvider 构造基于文件系统的块存储提供程序。
// 输入参数：
//   - conf：Conf 封装 “blockstore” 的所有配置
//   - indexConfig： 创建区块索引配置, 包含应编制索引的属性列表的配置
//   - metricsProvider：指标提供者，用于收集和暴露指标数据
//
// 返回值：
//   - *BlockStoreProvider：表示块存储提供程序的指针
//   - error：表示构造过程中可能出现的错误
func NewProvider(conf *Conf, indexConfig *IndexConfig, metricsProvider metrics.Provider) (*BlockStoreProvider, error) {
	// 创建 LevelDB 配置
	dbConf := &leveldbhelper.Conf{
		DBPath:         conf.getIndexDir(),             // 索引目录
		ExpectedFormat: dataFormatVersion(indexConfig), // 索引字段配置
	}

	// 创建 LevelDB 提供者
	p, err := leveldbhelper.NewProvider(dbConf)
	if err != nil {
		return nil, err
	}

	// 获取链目录路径
	dirPath := conf.getChainsDir()

	// 检查链目录是否存在，如果不存在则创建
	if _, err := os.Stat(dirPath); err != nil {
		if !os.IsNotExist(err) { // NotExist是唯一允许的错误类型
			return nil, errors.Wrapf(err, "读取分类账本目录失败 %s", dirPath)
		}

		logger.Info("在以下位置创建新的文件分类账本目录", dirPath)
		if err = os.MkdirAll(dirPath, 0o755); err != nil {
			return nil, errors.Wrapf(err, "无法创建分类账本目录: %s", dirPath)
		}
	}

	// 创建度量指标统计
	stats := newStats(metricsProvider)

	return &BlockStoreProvider{conf, indexConfig, p, stats}, nil
}

// Open 打开给定账本ID的块存储。
// 如果块存储不存在，则此方法会创建一个。
// 此方法应该只在特定账本ID上调用一次。
// 方法接收者：p（BlockStoreProvider类型的指针）
// 输入参数：
//   - ledgerid：要打开块存储的账本ID。
//
// 返回值：
//   - *BlockStore：打开的块存储。
//   - error：如果打开块存储失败，则返回错误；否则返回nil。
func (p *BlockStoreProvider) Open(ledgerid string) (*BlockStore, error) {
	// 一个指向命名数据库的关闭处理函数
	indexStoreHandle := p.leveldbProvider.GetDBHandle(ledgerid)
	// 创建新的块存储
	return newBlockStore(ledgerid, p.conf, p.indexConfig, indexStoreHandle, p.stats)
}

// ImportFromSnapshot initializes blockstore from a previously generated snapshot
// Any failure during bootstrapping the blockstore may leave the partial loaded data
// on disk. The consumer, such as peer is expected to keep track of failures and cleanup the
// data explicitly.
func (p *BlockStoreProvider) ImportFromSnapshot(
	ledgerID string,
	snapshotDir string,
	snapshotInfo *SnapshotInfo,
) error {
	indexStoreHandle := p.leveldbProvider.GetDBHandle(ledgerID)
	if err := bootstrapFromSnapshottedTxIDs(ledgerID, snapshotDir, snapshotInfo, p.conf, indexStoreHandle); err != nil {
		return err
	}
	return nil
}

// Exists tells whether the BlockStore with given id exists
func (p *BlockStoreProvider) Exists(ledgerid string) (bool, error) {
	exists, err := fileutil.DirExists(p.conf.getLedgerBlockDir(ledgerid))
	return exists, err
}

// Drop drops blockstore data (block index and blocks directory) for the given ledgerid (channelID).
// It is not an error if the channel does not exist.
// This function is not error safe. If this function returns an error or a crash takes place, it is highly likely
// that the data for this ledger is left in an inconsistent state. Opening the ledger again or reusing the previously
// opened ledger can show unknown behavior.
func (p *BlockStoreProvider) Drop(ledgerid string) error {
	exists, err := p.Exists(ledgerid)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := p.leveldbProvider.Drop(ledgerid); err != nil {
		return err
	}
	if err := os.RemoveAll(p.conf.getLedgerBlockDir(ledgerid)); err != nil {
		return err
	}
	return fileutil.SyncDir(p.conf.getChainsDir())
}

// List lists the ids of the existing ledgers
func (p *BlockStoreProvider) List() ([]string, error) {
	return fileutil.ListSubdirs(p.conf.getChainsDir())
}

// Close closes the BlockStoreProvider
func (p *BlockStoreProvider) Close() {
	p.leveldbProvider.Close()
}

func dataFormatVersion(indexConfig *IndexConfig) string {
	// in version 2.0 we merged three indexable into one `IndexableAttrTxID`
	if indexConfig.Contains(IndexableAttrTxID) {
		return dataformat.CurrentFormat
	}
	return dataformat.PreviousFormat
}
