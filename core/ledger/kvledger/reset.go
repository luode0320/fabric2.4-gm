/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/pkg/errors"
)

// ResetAllKVLedgers resets all ledger to the genesis block.
func ResetAllKVLedgers(rootFSPath string) error {
	fileLockPath := fileLockPath(rootFSPath)
	fileLock := leveldbhelper.NewFileLock(fileLockPath)
	if err := fileLock.Lock(); err != nil {
		return errors.Wrap(err, "as another peer node command is executing,"+
			" wait for that command to complete its execution or terminate it before retrying")
	}
	defer fileLock.Unlock()

	blockstorePath := BlockStorePath(rootFSPath)
	ledgerIDs, err := blkstorage.GetLedgersBootstrappedFromSnapshot(blockstorePath)
	if err != nil {
		return err
	}
	if len(ledgerIDs) > 0 {
		return errors.Errorf("cannot reset channels because the peer contains channel(s) %s that were bootstrapped from snapshot", ledgerIDs)
	}

	logger.Info("Resetting all channel ledgers to genesis block")
	logger.Infof("Ledger data folder from config = [%s]", rootFSPath)
	if err := dropDBs(rootFSPath); err != nil {
		return err
	}
	if err := resetBlockStorage(rootFSPath); err != nil {
		return err
	}
	logger.Info("All channel ledgers have been successfully reset to the genesis block")
	return nil
}

// LoadPreResetHeight 返回指定账本的预重置高度。
// 输入参数：
//   - rootFSPath：string，表示根文件系统路径
//   - ledgerIDs：[]string，表示账本的标识符列表
//
// 返回值：
//   - map[string]uint64：表示账本标识符和预重置高度的映射
//   - error：表示加载预重置高度时可能发生的错误
func LoadPreResetHeight(rootFSPath string, ledgerIDs []string) (map[string]uint64, error) {
	// 获取块存储路径
	blockstorePath := BlockStorePath(rootFSPath)
	logger.Infof("加载最后记录的块高度从目录 [%s]", blockstorePath)

	// 搜索指定账本的预重置高度文件，并返回一个通道名称到最后记录的块高度的映射。
	return blkstorage.LoadPreResetHeight(blockstorePath, ledgerIDs)
}

// ClearPreResetHeight removes the prereset height recorded in the file system for the specified ledgers.
func ClearPreResetHeight(rootFSPath string, ledgerIDs []string) error {
	blockstorePath := BlockStorePath(rootFSPath)
	logger.Infof("Clearing off prereset height files from path [%s] for ledgerIDs [%#v]", blockstorePath, ledgerIDs)
	return blkstorage.ClearPreResetHeight(blockstorePath, ledgerIDs)
}

func resetBlockStorage(rootFSPath string) error {
	blockstorePath := BlockStorePath(rootFSPath)
	logger.Infof("Resetting BlockStore to genesis block at location [%s]", blockstorePath)
	return blkstorage.ResetBlockStore(blockstorePath)
}
