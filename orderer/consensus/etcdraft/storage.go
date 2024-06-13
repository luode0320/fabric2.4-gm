/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
)

// MaxSnapshotFiles 定义在文件系统上保留的etcd/raft快照文件的最大数量。从最新的到最旧的读取快照文件，直到第一个
// 找到完整的文件。我们保存的快照文件越多，我们就越能减轻快照损坏的影响。这是为了测试目的而导出的。
// 这个必须大于等于1。
var MaxSnapshotFiles = 4

// MemoryStorage 目前由etcd/raft.MemoryStorage支持。这个接口是
// 定义为公开FSM的依赖关系，以便将来可以交换。
// TODO(jay) Add other necessary methods to this interface once we need
// them in implementation, e.g. ApplySnapshot.
type MemoryStorage interface {
	raft.Storage
	Append(entries []raftpb.Entry) error
	SetHardState(st raftpb.HardState) error
	CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(compactIndex uint64) error
	ApplySnapshot(snap raftpb.Snapshot) error
}

// RaftStorage 封装etcd/raft数据所需的存储空间，即内存、日志
type RaftStorage struct {
	SnapshotCatchUpEntries uint64

	walDir  string
	snapDir string

	lg *flogging.FabricLogger

	ram  MemoryStorage
	wal  *wal.WAL
	snap *snap.Snapshotter

	// 跟踪磁盘上快照索引的队列
	snapshotIndex []uint64
}

// CreateStorage 尝试创建存储来持久化etcd/raft数据。
// 如果数据存在于指定的磁盘中，则加载数据重构存储状态。
func CreateStorage(
	lg *flogging.FabricLogger,
	walDir string,
	snapDir string,
	ram MemoryStorage,
) (*RaftStorage, error) {
	sn, err := createSnapshotter(lg, snapDir)
	if err != nil {
		return nil, err
	}

	snapshot, err := sn.Load()
	if err != nil {
		if err == snap.ErrNoSnapshot {
			lg.Debugf("No snapshot found at %s", snapDir)
		} else {
			return nil, errors.Errorf("failed to load snapshot: %s", err)
		}
	} else {
		// snapshot found
		lg.Debugf("Loaded snapshot at Term %d and Index %d, Nodes: %+v",
			snapshot.Metadata.Term, snapshot.Metadata.Index, snapshot.Metadata.ConfState.Nodes)
	}

	w, st, ents, err := createOrReadWAL(lg, walDir, snapshot)
	if err != nil {
		return nil, errors.Errorf("failed to create or read WAL: %s", err)
	}

	if snapshot != nil {
		lg.Debugf("Applying snapshot to raft MemoryStorage")
		if err := ram.ApplySnapshot(*snapshot); err != nil {
			return nil, errors.Errorf("Failed to apply snapshot to memory: %s", err)
		}
	}

	lg.Debugf("Setting HardState to {Term: %d, Commit: %d}", st.Term, st.Commit)
	ram.SetHardState(st) // MemoryStorage.SetHardState always returns nil

	lg.Debugf("Appending %d entries to memory storage", len(ents))
	ram.Append(ents) // MemoryStorage.Append always return nil

	return &RaftStorage{
		lg:            lg,
		ram:           ram,
		wal:           w,
		snap:          sn,
		walDir:        walDir,
		snapDir:       snapDir,
		snapshotIndex: ListSnapshots(lg, snapDir),
	}, nil
}

// ListSnapshots 返回存储在磁盘上的快照的RaftIndex列表。
// 如果文件已损坏，请重命名该文件。
func ListSnapshots(logger *flogging.FabricLogger, snapDir string) []uint64 {
	dir, err := os.Open(snapDir)
	if err != nil {
		logger.Errorf("Failed to open snapshot directory %s: %s", snapDir, err)
		return nil
	}
	defer dir.Close()

	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		logger.Errorf("Failed to read snapshot files: %s", err)
		return nil
	}

	snapfiles := []string{}
	for i := range filenames {
		if strings.HasSuffix(filenames[i], ".snap") {
			snapfiles = append(snapfiles, filenames[i])
		}
	}
	sort.Strings(snapfiles)

	var snapshots []uint64
	for _, snapfile := range snapfiles {
		fpath := filepath.Join(snapDir, snapfile)
		s, err := snap.Read(logger.Zap(), fpath)
		if err != nil {
			logger.Errorf("Snapshot file %s is corrupted: %s", fpath, err)

			broken := fpath + ".broken"
			if err = os.Rename(fpath, broken); err != nil {
				logger.Errorf("Failed to rename corrupted snapshot file %s to %s: %s", fpath, broken, err)
			} else {
				logger.Debugf("Renaming corrupted snapshot file %s to %s", fpath, broken)
			}

			continue
		}

		snapshots = append(snapshots, s.Metadata.Index)
	}

	return snapshots
}

func createSnapshotter(logger *flogging.FabricLogger, snapDir string) (*snap.Snapshotter, error) {
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		return nil, errors.Errorf("failed to mkdir '%s' for snapshot: %s", snapDir, err)
	}

	return snap.New(logger.Zap(), snapDir), nil
}

func createOrReadWAL(lg *flogging.FabricLogger, walDir string, snapshot *raftpb.Snapshot) (w *wal.WAL, st raftpb.HardState, ents []raftpb.Entry, err error) {
	if !wal.Exist(walDir) {
		lg.Infof("No WAL data found, creating new WAL at path '%s'", walDir)
		// TODO(jay_guo) add metadata to be persisted with wal once we need it.
		// 用例可以是在新节点上转储和恢复数据。
		w, err := wal.Create(lg.Zap(), walDir, nil)
		if err == os.ErrExist {
			lg.Fatalf("programming error, we've just checked that WAL does not exist")
		}

		if err != nil {
			return nil, st, nil, errors.Errorf("failed to initialize WAL: %s", err)
		}

		if err = w.Close(); err != nil {
			return nil, st, nil, errors.Errorf("failed to close the WAL just created: %s", err)
		}
	} else {
		lg.Infof("在路径处找到 WAL 数据 '%s', 重播它", walDir)
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	lg.Debugf("Loading WAL at Term %d and Index %d", walsnap.Term, walsnap.Index)

	var repaired bool
	for {
		if w, err = wal.Open(lg.Zap(), walDir, walsnap); err != nil {
			return nil, st, nil, errors.Errorf("failed to open WAL: %s", err)
		}

		if _, st, ents, err = w.ReadAll(); err != nil {
			lg.Warnf("Failed to read WAL: %s", err)

			if errc := w.Close(); errc != nil {
				return nil, st, nil, errors.Errorf("failed to close erroneous WAL: %s", errc)
			}

			// 只修复 UnexpectedEOF 的错误，并且只修复一次
			if repaired || err != io.ErrUnexpectedEOF {
				return nil, st, nil, errors.Errorf("failed to read WAL and cannot repair: %s", err)
			}

			if !wal.Repair(lg.Zap(), walDir) {
				return nil, st, nil, errors.Errorf("failed to repair WAL: %s", err)
			}

			repaired = true
			// 下一次循环应该能够打开WAL并返回
			continue
		}

		// 成功打开 WAL 并且读取所有的条目，终止循环
		break
	}

	return w, st, ents, nil
}

// Snapshot 返回存储在内存中的最新快照
func (rs *RaftStorage) Snapshot() raftpb.Snapshot {
	sn, _ := rs.ram.Snapshot() // Snapshot always returns nil error
	return sn
}

// Store 持久化etcd/raft数据
func (rs *RaftStorage) Store(entries []raftpb.Entry, hardstate raftpb.HardState, snapshot raftpb.Snapshot) error {
	if err := rs.wal.Save(hardstate, entries); err != nil {
		return err
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := rs.saveSnap(snapshot); err != nil {
			return err
		}

		if err := rs.ram.ApplySnapshot(snapshot); err != nil {
			if err == raft.ErrSnapOutOfDate {
				rs.lg.Warnf("Attempted to apply out-of-date snapshot at Term %d and Index %d",
					snapshot.Metadata.Term, snapshot.Metadata.Index)
			} else {
				rs.lg.Fatalf("Unexpected programming error: %s", err)
			}
		}
	}

	if err := rs.ram.Append(entries); err != nil {
		return err
	}

	return nil
}

func (rs *RaftStorage) saveSnap(snap raftpb.Snapshot) error {
	rs.lg.Infof("Persisting snapshot (term: %d, index: %d) to WAL and disk", snap.Metadata.Term, snap.Metadata.Index)

	//在保存快照索引之前，必须将快照索引保存到WAL
	//快照来维持我们只打开的不变性
	//查看以前保存的快照索引。
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}

	if err := rs.wal.SaveSnapshot(walsnap); err != nil {
		return errors.Errorf("failed to save snapshot to WAL: %s", err)
	}

	if err := rs.snap.SaveSnap(snap); err != nil {
		return errors.Errorf("failed to save snapshot to disk: %s", err)
	}

	rs.lg.Debugf("Releasing lock to wal files prior to %d", snap.Metadata.Index)
	if err := rs.wal.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}

	return nil
}

// TakeSnapshot 从MemoryStorage获取索引i处的快照，并将其持久化到磁盘和磁盘。
func (rs *RaftStorage) TakeSnapshot(i uint64, cs raftpb.ConfState, data []byte) error {
	rs.lg.Debugf("Creating snapshot at index %d from MemoryStorage", i)
	snap, err := rs.ram.CreateSnapshot(i, &cs, data)
	if err != nil {
		return errors.Errorf("failed to create snapshot from MemoryStorage: %s", err)
	}

	if err = rs.saveSnap(snap); err != nil {
		return err
	}

	rs.snapshotIndex = append(rs.snapshotIndex, snap.Metadata.Index)

	// 将一些条目存储到缓存中，以便缓慢的追随者追赶
	if i > rs.SnapshotCatchUpEntries {
		compacti := i - rs.SnapshotCatchUpEntries
		rs.lg.Debugf("Purging in-memory raft entries prior to %d", compacti)
		if err = rs.ram.Compact(compacti); err != nil {
			if err == raft.ErrCompacted {
				rs.lg.Warnf("Raft entries prior to %d are already purged", compacti)
			} else {
				rs.lg.Fatalf("Failed to purge raft entries: %s", err)
			}
		}
	}

	rs.lg.Infof("Snapshot is taken at index %d", i)

	rs.gc()
	return nil
}

// gc 收集etcd/raft垃圾文件，即wal和snapshot文件
func (rs *RaftStorage) gc() {
	if len(rs.snapshotIndex) < MaxSnapshotFiles {
		rs.lg.Debugf("Snapshots on disk (%d) < limit (%d), no need to purge wal/snapshot",
			len(rs.snapshotIndex), MaxSnapshotFiles)
		return
	}

	rs.snapshotIndex = rs.snapshotIndex[len(rs.snapshotIndex)-MaxSnapshotFiles:]

	rs.purgeWAL()
	rs.purgeSnap()
}

func (rs *RaftStorage) purgeWAL() {
	retain := rs.snapshotIndex[0]

	var files []string
	err := filepath.Walk(rs.walDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, ".wal") {
			return nil
		}

		var seq, index uint64
		_, f := filepath.Split(path)
		fmt.Sscanf(f, "%016x-%016x.wal", &seq, &index)

		//只清除索引低于最老快照文件路径的WAL。SkipDir捕获Walk而不返回错误。
		if index >= retain {
			return filepath.SkipDir
		}

		files = append(files, path)
		return nil
	})
	if err != nil {
		rs.lg.Errorf("Failed to read WAL directory %s: %s", rs.walDir, err)
	}

	if len(files) <= 1 {
		// 我们需要保留一个索引小于快照的墙段。
		//见注释。ReleaseLockTo获取更多细节。
		return
	}

	rs.purge(files[:len(files)-1])
}

func (rs *RaftStorage) purgeSnap() {
	var files []string
	err := filepath.Walk(rs.snapDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".snap") {
			files = append(files, path)
		} else if strings.HasSuffix(path, ".broken") {
			rs.lg.Warnf("Found broken snapshot file %s, it can be removed manually", path)
		}

		return nil
	})
	if err != nil {
		rs.lg.Errorf("Failed to read Snapshot directory %s: %s", rs.snapDir, err)
		return
	}

	l := len(files)
	if l <= MaxSnapshotFiles {
		return
	}

	rs.purge(files[:l-MaxSnapshotFiles]) // 保留最后的MaxSnapshotFiles快照文件
}

func (rs *RaftStorage) purge(files []string) {
	for _, file := range files {
		l, err := fileutil.TryLockFile(file, os.O_WRONLY, fileutil.PrivateFileMode)
		if err != nil {
			rs.lg.Debugf("Failed to lock %s, abort purging", file)
			break
		}

		if err = os.Remove(file); err != nil {
			rs.lg.Errorf("Failed to remove %s: %s", file, err)
		} else {
			rs.lg.Debugf("Purged file %s", file)
		}

		if err = l.Close(); err != nil {
			rs.lg.Errorf("Failed to close file lock %s: %s", l.Name(), err)
		}
	}
}

// ApplySnapshot 将快照应用于本地内存存储
func (rs *RaftStorage) ApplySnapshot(snap raftpb.Snapshot) {
	if err := rs.ram.ApplySnapshot(snap); err != nil {
		if err == raft.ErrSnapOutOfDate {
			rs.lg.Warnf("Attempted to apply out-of-date snapshot at Term %d and Index %d",
				snap.Metadata.Term, snap.Metadata.Index)
		} else {
			rs.lg.Fatalf("Unexpected programming error: %s", err)
		}
	}
}

// Close 关闭存储
func (rs *RaftStorage) Close() error {
	if err := rs.wal.Close(); err != nil {
		return err
	}

	return nil
}
