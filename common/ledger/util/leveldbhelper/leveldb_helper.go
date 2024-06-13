/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"fmt"
	"sync"
	"syscall"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var logger = flogging.MustGetLogger("leveldbhelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

// DB 是实际存储的包装器
type DB struct {
	conf            *Conf             // 配置信息
	db              *leveldb.DB       // LevelDB 实例
	dbState         dbState           // 数据库状态
	mutex           sync.RWMutex      // 读写锁
	readOpts        *opt.ReadOptions  // 读取选项
	writeOptsNoSync *opt.WriteOptions // 无同步写入选项
	writeOptsSync   *opt.WriteOptions // 同步写入选项
}

// CreateDB 构造一个 “数据库” 结构
func CreateDB(conf *Conf) *DB {
	readOpts := &opt.ReadOptions{}
	writeOptsNoSync := &opt.WriteOptions{}
	writeOptsSync := &opt.WriteOptions{}
	writeOptsSync.Sync = true

	return &DB{
		conf:            conf,            // 配置信息
		dbState:         closed,          // 数据库状态
		readOpts:        readOpts,        // 读取选项
		writeOptsNoSync: writeOptsNoSync, // 无同步写入选项
		writeOptsSync:   writeOptsSync,   // 同步写入选项
	}
}

// Open 打开底层数据库。
// 方法接收者：
//   - dbInst：DB 结构体的指针，表示数据库实例
func (dbInst *DB) Open() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()

	// 如果数据库已经打开，则直接返回
	if dbInst.dbState == opened {
		return
	}

	// 创建 leveldb 存储目录
	dbOpts := &opt.Options{}
	dbPath := dbInst.conf.DBPath
	var err error
	var dirEmpty bool
	if dirEmpty, err = fileutil.CreateDirIfMissing(dbPath); err != nil {
		panic(fmt.Sprintf("leveldb存储目录缺少, 创建目录时出错: %s", err))
	}
	dbOpts.ErrorIfMissing = !dirEmpty

	// 开启 leveldb
	if dbInst.db, err = leveldb.OpenFile(dbPath, dbOpts); err != nil {
		panic(fmt.Sprintf("leveldb启动错误: %s", err))
	}

	dbInst.dbState = opened
}

// IsEmpty 返回数据库是否为空。
// 方法接收者：
//   - dbInst：DB 结构体的指针，表示数据库实例
//
// 返回值：
//   - bool：表示数据库是否为空
//   - error：表示检查过程中可能出现的错误
func (dbInst *DB) IsEmpty() (bool, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// 创建迭代器，遍历数据库中的所有键值对
	itr := dbInst.db.NewIterator(&goleveldbutil.Range{}, dbInst.readOpts)
	defer itr.Release()

	// 检查迭代器是否有下一个键值对
	hasItems := itr.Next()

	// 返回数据库是否为空以及可能的错误
	return !hasItems, errors.Wrapf(itr.Error(), "尝试查看路径处的leveldb时出错 [%s] 为空", dbInst.conf.DBPath)
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == closed {
		return
	}
	if err := dbInst.db.Close(); err != nil {
		logger.Errorf("Error closing leveldb: %s", err)
	}
	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	value, err := dbInst.db.Get(key, dbInst.readOpts)
	if err == leveldb.ErrNotFound {
		value = nil
		err = nil
	}
	if err != nil {
		logger.Errorf("Error retrieving leveldb key [%#v]: %s", key, err)
		return nil, errors.Wrapf(err, "error retrieving leveldb key [%#v]", key)
	}
	return value, nil
}

// Put 保存键值对。
// 方法接收者：dbInst（DB类型的指针）
// 输入参数：
//   - key：要保存的键。
//   - value：要保存的值。
//   - sync：是否同步写入。
//
// 返回值：
//   - error：如果保存键值对失败，则返回错误；否则返回nil。
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	// 获取读锁
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// 根据sync参数选择写入选项, 默认异步写入
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}

	// 将键值对保存到数据库中
	err := dbInst.db.Put(key, value, wo)
	if err != nil {
		logger.Errorf("数据写入 leveldb 时出错 [%#v]", key)
		return errors.Wrapf(err, "数据写入 leveldb 时出错 [%#v]", key)
	}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}
	err := dbInst.db.Delete(key, wo)
	if err != nil {
		logger.Errorf("Error deleting leveldb key [%#v]", key)
		return errors.Wrapf(err, "error deleting leveldb key [%#v]", key)
	}
	return nil
}

// GetIterator 返回一个键值存储的迭代器。在使用后，应该释放迭代器。
// 结果集包含在 startKey（包含）和 endKey（不包含）之间存在的所有键。
// 一个空的 startKey 表示第一个可用的键，一个空的 endKey 表示最后一个可用键之后的逻辑键。
// 方法接收者：
//   - dbInst：DB 结构体的指针，表示数据库实例
//
// 输入参数：
//   - startKey：[]byte 类型，表示起始键
//   - endKey：[]byte 类型，表示结束键
//
// 返回值：
//   - iterator.Iterator：表示一个迭代器，用于遍历键值存储
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) iterator.Iterator {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	// 返回底层DB的最新快照的迭代器。
	return dbInst.db.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}
	if err := dbInst.db.Write(batch, wo); err != nil {
		return errors.Wrap(err, "error writing batch to leveldb")
	}
	return nil
}

// FileLock encapsulate the DB that holds the file lock.
// As the FileLock to be used by a single process/goroutine,
// there is no need for the semaphore to synchronize the
// FileLock usage.
type FileLock struct {
	db       *leveldb.DB
	filePath string
}

// NewFileLock 函数返回一个基于文件的锁管理器。
//
// 参数：
//   - filePath: string 类型，表示文件路径。
//
// 返回值：
//   - *FileLock: 表示 FileLock 实例。
func NewFileLock(filePath string) *FileLock {
	return &FileLock{
		filePath: filePath,
	}
}

// Lock acquire a file lock. We achieve this by opening
// a db for the given filePath. Internally, leveldb acquires a
// file lock while opening a db. If the db is opened again by the same or
// another process, error would be returned. When the db is closed
// or the owner process dies, the lock would be released and hence
// the other process can open the db. We exploit this leveldb
// functionality to acquire and release file lock as the leveldb
// supports this for Windows, Solaris, and Unix.
func (f *FileLock) Lock() error {
	dbOpts := &opt.Options{}
	var err error
	var dirEmpty bool
	if dirEmpty, err = fileutil.CreateDirIfMissing(f.filePath); err != nil {
		panic(fmt.Sprintf("Error creating dir if missing: %s", err))
	}
	dbOpts.ErrorIfMissing = !dirEmpty
	db, err := leveldb.OpenFile(f.filePath, dbOpts)
	if err != nil && err == syscall.EAGAIN {
		return errors.Errorf("lock is already acquired on file %s", f.filePath)
	}
	if err != nil {
		panic(fmt.Sprintf("Error acquiring lock on file %s: %s", f.filePath, err))
	}

	// only mutate the lock db reference AFTER validating that the lock was held.
	f.db = db

	return nil
}

// Determine if the lock is currently held open.
func (f *FileLock) IsLocked() bool {
	return f.db != nil
}

// Unlock releases a previously acquired lock. We achieve this by closing
// the previously opened db. FileUnlock can be called multiple times.
func (f *FileLock) Unlock() {
	if f.db == nil {
		return
	}
	if err := f.db.Close(); err != nil {
		logger.Warningf("unable to release the lock on file %s: %s", f.filePath, err)
		return
	}
	f.db = nil
}
