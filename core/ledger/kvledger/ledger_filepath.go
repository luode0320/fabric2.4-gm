/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"path/filepath"
	"strconv"
)

func fileLockPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "fileLock")
}

// LedgerProviderPath 返回 ledgerprovider 的绝对路径
func LedgerProviderPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "ledgerProvider")
}

// BlockStorePath 返回块存储的绝对路径
func BlockStorePath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "chains")
}

// PvtDataStorePath 返回pvtdata存储的绝对路径
func PvtDataStorePath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "pvtdataStore")
}

// StateDBPath 返回状态级别DB的绝对路径
func StateDBPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "stateLeveldb")
}

// HistoryDBPath 返回历史数据库的绝对路径
func HistoryDBPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "historyLeveldb")
}

// ConfigHistoryDBPath 返回configHistory DB的绝对路径
func ConfigHistoryDBPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "configHistory")
}

// BookkeeperDBPath 返回bookkeeper DB的绝对路径
func BookkeeperDBPath(rootFSPath string) string {
	return filepath.Join(rootFSPath, "bookkeeper")
}

// SnapshotsTempDirPath returns the dir path that is used temporarily during the genration or import of the snapshots for a ledger
func SnapshotsTempDirPath(snapshotRootDir string) string {
	return filepath.Join(snapshotRootDir, "temp")
}

// CompletedSnapshotsPath returns the absolute path that is used for persisting the snapshots
func CompletedSnapshotsPath(snapshotRootDir string) string {
	return filepath.Join(snapshotRootDir, "completed")
}

// SnapshotsDirForLedger returns the absolute path of the dir for the snapshots for a specified ledger
func SnapshotsDirForLedger(snapshotRootDir, ledgerID string) string {
	return filepath.Join(CompletedSnapshotsPath(snapshotRootDir), ledgerID)
}

// SnapshotDirForLedgerBlockNum returns the absolute path for a particular snapshot for a ledger
func SnapshotDirForLedgerBlockNum(snapshotRootDir, ledgerID string, blockNumber uint64) string {
	return filepath.Join(SnapshotsDirForLedger(snapshotRootDir, ledgerID), strconv.FormatUint(blockNumber, 10))
}
