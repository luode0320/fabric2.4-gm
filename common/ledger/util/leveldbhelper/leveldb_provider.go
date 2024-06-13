/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	// internalDBName 用于跟踪与内部相关的数据，如数据格式使用_作为名称，因为不允许将其作为channelname
	internalDBName = "_"
	// maxBatchSize 限制批处理的内存使用 (1MB)。它是通过字节总数来衡量的批处理中的所有键。
	maxBatchSize = 1000000
)

var (
	dbNameKeySep     = []byte{0x00}
	lastKeyIndicator = byte(0x01)
	formatVersionKey = []byte{'f'} // a single key in db whose value indicates the version of the data format
)

// closeFunc closes the db handle
type closeFunc func()

// Conf 是 `Provider` 的配置信息。
//
// `ExpectedFormat` 是内部数据库中格式键的预期值。
// 在打开数据库时，会进行检查，要么数据库为空（即首次打开），要么格式版本键的值等于 `ExpectedFormat`。
// 否则，将返回错误。
// 对于 ExpectedFormat 的空值表示格式从未设置，因此没有此记录。
type Conf struct {
	DBPath         string // 数据库存储目录
	ExpectedFormat string // 内部数据库中格式键的预期值。
}

// Provider 允许将单个 LevelDB 用作多个逻辑 LevelDB
type Provider struct {
	db        *DB                  // 单个 LevelDB 实例
	mux       sync.Mutex           // 互斥锁，用于保护 dbHandles 的并发访问
	dbHandles map[string]*DBHandle // 逻辑 LevelDB 句柄的映射，使用字符串作为键
}

// NewProvider 函数构造一个 Provider, 创建leveldb, 并生成存储目录。
//
// 参数：
//   - conf: *Conf 类型，表示配置信息。
//
// 返回值：
//   - *Provider: 表示 Provider 实例。
//   - error: 表示构造过程中的错误。
func NewProvider(conf *Conf) (*Provider, error) {
	db, err := openDBAndCheckFormat(conf)
	if err != nil {
		return nil, err
	}
	return &Provider{
		db:        db,
		dbHandles: make(map[string]*DBHandle),
	}, nil
}

// 创建leveldb配置, 生成存储目录, 并启动leveldb
func openDBAndCheckFormat(conf *Conf) (d *DB, e error) {
	// 创建启动配置
	db := CreateDB(conf)
	// 启动leveldb
	db.Open()

	defer func() {
		if e != nil {
			// 如果有错误, 则关闭leveldb数据库
			db.Close()
		}
	}()

	internalDB := &DBHandle{
		db:     db,
		dbName: internalDBName,
	}

	dbEmpty, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}

	if dbEmpty && conf.ExpectedFormat != "" {
		logger.Infof("leveldb是空的，将db格式设置为 %s", conf.ExpectedFormat)
		if err := internalDB.Put(formatVersionKey, []byte(conf.ExpectedFormat), true); err != nil {
			return nil, err
		}
		return db, nil
	}

	formatVersion, err := internalDB.Get(formatVersionKey)
	if err != nil {
		return nil, err
	}
	logger.Debugf("在路径处检查db格式 [%s]", conf.DBPath)

	if !bytes.Equal(formatVersion, []byte(conf.ExpectedFormat)) {
		logger.Errorf("路径处的数据库 [%s] 包含意外格式的数据. 预期的数据格式 = [%s] (%#v), data format = [%s] (%#v).",
			conf.DBPath, conf.ExpectedFormat, []byte(conf.ExpectedFormat), formatVersion, formatVersion)
		return nil, &dataformat.ErrFormatMismatch{
			ExpectedFormat: conf.ExpectedFormat,
			Format:         string(formatVersion),
			DBInfo:         fmt.Sprintf("leveldb at [%s]", conf.DBPath),
		}
	}
	logger.Debug("格式是最新的，无事可做")
	return db, nil
}

// GetDataFormat returns the format of the data
func (p *Provider) GetDataFormat() (string, error) {
	f, err := p.GetDBHandle(internalDBName).Get(formatVersionKey)
	return string(f), err
}

// GetDBHandle 返回一个指向命名数据库的处理函数
// 方法接收者：p（Provider类型的指针）
// 输入参数：
//   - dbName：要获取句柄的数据库名称。
//
// 返回值：
//   - *DBHandle：一个指向命名数据库的关闭处理函数
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	// 获取互斥锁
	p.mux.Lock()
	defer p.mux.Unlock()

	// 检查是否已经存在数据库处理函数
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		// 如果不存在，则创建一个新的数据库句柄
		closeFunc := func() {
			// 关闭数据库句柄时，删除对应的数据库句柄映射
			p.mux.Lock()
			defer p.mux.Unlock()
			delete(p.dbHandles, dbName)
		}
		dbHandle = &DBHandle{dbName, p.db, closeFunc}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying leveldb
func (p *Provider) Close() {
	p.db.Close()
}

// Drop drops all the data for the given dbName
func (p *Provider) Drop(dbName string) error {
	dbHandle := p.GetDBHandle(dbName)
	defer dbHandle.Close()
	return dbHandle.deleteAll()
}

// DBHandle 是一个指向命名数据库的处理函数
type DBHandle struct {
	dbName    string
	db        *DB
	closeFunc closeFunc
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructLevelKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte, sync bool) error {
	return h.db.Put(constructLevelKey(h.dbName, key), value, sync)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte, sync bool) error {
	return h.db.Delete(constructLevelKey(h.dbName, key), sync)
}

// DeleteAll deletes all the keys that belong to the channel (dbName).
func (h *DBHandle) deleteAll() error {
	iter, err := h.GetIterator(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Release()

	// use leveldb iterator directly to be more efficient
	dbIter := iter.Iterator

	// This is common code shared by all the leveldb instances. Because each leveldb has its own key size pattern,
	// each batch is limited by memory usage instead of number of keys. Once the batch memory usage reaches maxBatchSize,
	// the batch will be committed.
	numKeys := 0
	batchSize := 0
	batch := &leveldb.Batch{}
	for dbIter.Next() {
		if err := dbIter.Error(); err != nil {
			return errors.Wrap(err, "internal leveldb error while retrieving data from db iterator")
		}
		key := dbIter.Key()
		numKeys++
		batchSize = batchSize + len(key)
		batch.Delete(key)
		if batchSize >= maxBatchSize {
			if err := h.db.WriteBatch(batch, true); err != nil {
				return err
			}
			logger.Infof("Have removed %d entries for channel %s in leveldb %s", numKeys, h.dbName, h.db.conf.DBPath)
			batchSize = 0
			batch.Reset()
		}
	}
	if batch.Len() > 0 {
		return h.db.WriteBatch(batch, true)
	}
	return nil
}

// IsEmpty returns true if no data exists for the DBHandle
func (h *DBHandle) IsEmpty() (bool, error) {
	itr, err := h.GetIterator(nil, nil)
	if err != nil {
		return false, err
	}
	defer itr.Release()

	if err := itr.Error(); err != nil {
		return false, errors.WithMessagef(itr.Error(), "internal leveldb error while obtaining next entry from iterator")
	}

	return !itr.Next(), nil
}

// NewUpdateBatch returns a new UpdateBatch that can be used to update the db
func (h *DBHandle) NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{
		dbName: h.dbName,
		Batch:  &leveldb.Batch{},
	}
}

// WriteBatch writes a batch in an atomic way
func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	if err := h.db.WriteBatch(batch.Batch, sync); err != nil {
		return err
	}
	return nil
}

// GetIterator 获取一个迭代器的句柄。在使用完毕后，应该释放迭代器。
// 结果集包含在startKey（包含）和endKey（不包含）之间存在的所有键。
// 一个空的startKey表示第一个可用的键，一个空的endKey表示最后一个可用键之后的逻辑键。
// 方法接收者：h（DBHandle类型的指针）
// 输入参数：
//   - startKey：起始键，作为迭代器的起始位置。
//   - endKey：结束键，作为迭代器的结束位置。
//
// 返回值：
//   - *Iterator：迭代器的句柄。
//   - error：如果在获取迭代器时出错，则返回错误。
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) (*Iterator, error) {
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil {
		// 将最后一个字节'dbNameKeySep'替换为'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	logger.Debugf("正在获取范围的迭代器 [%#v] - [%#v]", sKey, eKey)
	// 获取迭代器
	itr := h.db.GetIterator(sKey, eKey)
	if err := itr.Error(); err != nil {
		itr.Release()
		return nil, errors.Wrapf(err, "获取db迭代器时出现内部leveldb错误")
	}
	return &Iterator{h.dbName, itr}, nil
}

// Close closes the DBHandle after its db data have been deleted
func (h *DBHandle) Close() {
	if h.closeFunc != nil {
		h.closeFunc()
	}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	*leveldb.Batch
	dbName string
}

// Put adds a KV
func (b *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	b.Batch.Put(constructLevelKey(b.dbName, key), value)
}

// Delete deletes a Key and associated value
func (b *UpdateBatch) Delete(key []byte) {
	b.Batch.Delete(constructLevelKey(b.dbName, key))
}

// Iterator extends actual leveldb iterator
type Iterator struct {
	dbName string
	iterator.Iterator
}

// Key wraps actual leveldb iterator method
func (itr *Iterator) Key() []byte {
	return retrieveAppKey(itr.Iterator.Key())
}

// Seek moves the iterator to the first key/value pair
// whose key is greater than or equal to the given key.
// It returns whether such pair exist.
func (itr *Iterator) Seek(key []byte) bool {
	levelKey := constructLevelKey(itr.dbName, key)
	return itr.Iterator.Seek(levelKey)
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}
