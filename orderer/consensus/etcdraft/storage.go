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

// MemoryStorage 当前由 etcd/raft.MemoryStorage 实现支持。此接口设计
// 旨在公开FSM（有限状态机）的依赖项，以便未来可以替换或扩展存储实现。
// 目标是使共识层能更灵活地适应不同的存储需求或优化策略。
// 注意：随着实现的发展，可能需要向此接口添加更多方法，例如 ApplySnapshot，
// 当在具体实现中需要用到它们时。

// MemoryStorage 接口定义了用于存储和管理 Raft 共识算法状态的内存存储行为。
// 这些方法允许共识层在内存中持久化和恢复其状态，包括日志条目和硬状态，
// 以及创建和应用快照，从而支持 Raft 算法的正常运作和性能优化。
type MemoryStorage interface {
	// raft.Storage 是 raft 包定义的存储接口，MemoryStorage 必须实现它。
	// 这意味着 MemoryStorage 需要支持从磁盘读取和写入 Raft 的状态。
	raft.Storage

	// Append 方法用于将一系列 Raft 日志条目追加到存储中。
	// 这些条目将被持久化，以便在系统重启后可以从中恢复状态。
	Append(entries []raftpb.Entry) error

	// SetHardState 方法用于设置 Raft 的硬状态。
	// 硬状态包括最新任期、投票给谁的ID和已知已提交的最后条目的索引和任期。
	SetHardState(st raftpb.HardState) error

	// CreateSnapshot 方法用于创建一个快照，它捕获了当前状态机的一个快照。
	// 快照包括状态机的当前配置状态（ConfState）和任意的数据。
	CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)

	// Compact 方法用于压缩存储，删除不再需要的日志条目。
	// 这有助于减少存储空间的占用，同时保持状态机的完整性。
	Compact(compactIndex uint64) error

	// ApplySnapshot 方法用于应用一个快照，这将覆盖当前的状态机状态。
	// 快照的应用可以显著提高状态机的性能，尤其是在日志条目变得非常长时。
	ApplySnapshot(snap raftpb.Snapshot) error
}

// RaftStorage 结构体封装了 etcd/raft 数据所需的存储空间，包括内存、WAL（Write-Ahead Log）日志和快照。
// 这些存储组件共同构成了 raft 状态机的持久化和恢复机制。
type RaftStorage struct {
	// SnapshotCatchUpEntries 用于配置在应用快照后需要从 WAL 中追加的额外条目数量。
	SnapshotCatchUpEntries uint64

	// walDir 和 snapDir 分别存储 WAL 日志文件和快照文件的目录路径。
	walDir  string
	snapDir string

	// lg 是日志记录器，用于记录 RaftStorage 实例的操作日志。
	lg *flogging.FabricLogger

	// ram 是内存存储，用于暂存最新的 raft 状态和条目，提供快速访问和应用快照的能力。
	ram  MemoryStorage
	wal  *wal.WAL          // wal 是 WAL 日志实例，用于持久化 raft 状态机的更改。
	snap *snap.Snapshotter // snap 是快照处理器，用于创建和恢复快照。

	// snapshotIndex 是一个队列，用于跟踪磁盘上快照的索引，帮助管理快照的生命周期。
	snapshotIndex []uint64
}

// CreateStorage 尝试创建一个存储实例，用于持久化 etcd/raft 的数据。
// 如果在指定的磁盘位置存在数据，则会加载这些数据以重构存储的状态。
func CreateStorage(
	// lg 是日志记录器，用于记录调试和错误信息。
	lg *flogging.FabricLogger,
	// walDir 是 Write-Ahead Log (WAL) 文件所在的目录路径。
	walDir string,
	// snapDir 是快照文件所在的目录路径。
	snapDir string,
	// ram 是内存中的存储实例，用于保存临时状态。
	ram MemoryStorage,
) (*RaftStorage, error) {
	// 创建快照处理器，并尝试加载快照数据。
	sn, err := createSnapshotter(lg, snapDir)
	if err != nil {
		return nil, err
	}

	// 尝试加载快照。
	snapshot, err := sn.Load()
	if err != nil {
		if err == snap.ErrNoSnapshot {
			lg.Debugf("在 %s 中未找到快照。", snapDir)
		} else {
			return nil, errors.Errorf("加载快照失败: %s", err)
		}
	} else {
		// 如果找到了快照，打印快照的详细信息。
		lg.Debugf("在 Term %d 和 Index %d 加载了快照，节点列表: %+v",
			snapshot.Metadata.Term, snapshot.Metadata.Index, snapshot.Metadata.ConfState.Nodes)
	}

	// 创建或读取 WAL 文件。
	w, st, ents, err := createOrReadWAL(lg, walDir, snapshot)
	if err != nil {
		return nil, errors.Errorf("创建或读取 WAL 文件失败: %s", err)
	}

	// 如果有快照数据，将其应用到内存存储中。
	if snapshot != nil {
		lg.Debugf("将快照应用到 raft 内存存储中")
		if err := ram.ApplySnapshot(*snapshot); err != nil {
			return nil, errors.Errorf("将快照应用到内存中失败: %s", err)
		}
	}

	// 设置硬状态（hard state）。
	lg.Debugf("设置 HardState 为 {Term: %d, Commit: %d}", st.Term, st.Commit)
	ram.SetHardState(st) // MemoryStorage.SetHardState 总是返回 nil

	// 将 WAL 日志条目追加到内存存储中。
	lg.Debugf("向内存存储追加 %d 条日志条目", len(ents))
	ram.Append(ents) // MemoryStorage.Append 总是返回 nil

	// 创建并返回 RaftStorage 实例。
	return &RaftStorage{
		lg:            lg,                         // lg 是日志记录器，用于记录 RaftStorage 实例的操作日志。
		ram:           ram,                        // ram 是内存存储，用于暂存最新的 raft 状态和条目，提供快速访问和应用快照的能力。
		wal:           w,                          // wal 是 WAL 日志实例，用于持久化 raft 状态机的更改。
		snap:          sn,                         // snap 是快照处理器，用于创建和恢复快照。
		walDir:        walDir,                     // walDir 和 snapDir 分别存储 WAL 日志文件和快照文件的目录路径。
		snapDir:       snapDir,                    // walDir 和 snapDir 分别存储 WAL 日志文件和快照文件的目录路径。
		snapshotIndex: ListSnapshots(lg, snapDir), // snapshotIndex 是一个队列，用于跟踪磁盘上快照的索引，帮助管理快照的生命周期。
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

// createSnapshotter 函数用于创建一个 Snapshotter 实例，该实例用于处理和管理快照文件。
// 此函数首先确保用于存放快照文件的目录存在，如果目录不存在则创建之。
// 成功创建目录后，使用提供的日志记录器和快照目录路径来初始化 Snapshotter 实例。
func createSnapshotter(logger *flogging.FabricLogger, snapDir string) (*snap.Snapshotter, error) {
	// 尝试创建快照目录，如果目录不存在的话。
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		// 如果创建目录失败，返回错误信息。
		return nil, errors.Errorf("为快照创建目录 '%s' 失败: %s", snapDir, err)
	}

	// 成功创建目录后，使用 logger 和 snapDir 初始化 Snapshotter 实例。
	return snap.New(logger.Zap(), snapDir), nil
}

// createOrReadWAL 函数用于创建或读取 Write-Ahead Log (WAL)，WAL 是一种用于持久化 raft 状态的日志文件。
// 当没有快照数据时，该函数会创建一个新的 WAL；当有快照数据时，它会打开现有的 WAL 并从中读取状态和条目。
func createOrReadWAL(lg *flogging.FabricLogger, walDir string, snapshot *raftpb.Snapshot) (w *wal.WAL, st raftpb.HardState, ents []raftpb.Entry, err error) {
	// 检查 WAL 目录是否存在
	if !wal.Exist(walDir) {
		// 如果 WAL 目录不存在，创建新的 WAL
		lg.Infof("未找到 WAL 数据，在路径 '%s' 创建新的 WAL", walDir)
		w, err := wal.Create(lg.Zap(), walDir, nil)
		if err == os.ErrExist {
			// 这不应该发生，因为我们刚刚检查过 WAL 不存在
			lg.Fatalf("编程错误，我们刚刚检查过 WAL 不存在")
		}

		if err != nil {
			// 如果创建 WAL 失败，返回错误
			return nil, st, nil, errors.Errorf("初始化 WAL 失败: %s", err)
		}

		// 创建完成后关闭 WAL
		if err = w.Close(); err != nil {
			return nil, st, nil, errors.Errorf("关闭刚刚创建的 WAL 失败: %s", err)
		}
	} else {
		// 如果 WAL 目录存在，读取并重播 WAL 数据
		lg.Infof("在路径 '%s' 找到 WAL 数据，重播它", walDir)
	}

	// 初始化 WAL 快照结构体
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		// 如果存在快照，更新 WAL 快照结构体中的 Index 和 Term
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	// 打印日志信息，显示正在加载的 WAL 的 Term 和 Index
	lg.Debugf("在 Term %d 和 Index %d 加载 WAL", walsnap.Term, walsnap.Index)

	// 循环尝试打开 WAL，直到成功
	var repaired bool
	for {
		w, err = wal.Open(lg.Zap(), walDir, walsnap)
		if err != nil {
			// 如果打开 WAL 失败，返回错误
			return nil, st, nil, errors.Errorf("打开 WAL 失败: %s", err)
		}

		// 读取 WAL 中的所有条目和硬状态
		_, st, ents, err = w.ReadAll()
		if err != nil {
			// 如果读取 WAL 失败，打印警告信息
			lg.Warnf("读取 WAL 失败: %s", err)

			// 关闭错误的 WAL
			if errc := w.Close(); errc != nil {
				return nil, st, nil, errors.Errorf("关闭错误的 WAL 失败: %s", errc)
			}

			// 只修复 UnexpectedEOF 错误，并且只修复一次
			if repaired || err != io.ErrUnexpectedEOF {
				// 如果不是 UnexpectedEOF 或已经修复过，返回错误
				return nil, st, nil, errors.Errorf("读取 WAL 失败且无法修复: %s", err)
			}

			// 尝试修复 WAL
			if !wal.Repair(lg.Zap(), walDir) {
				// 如果修复 WAL 失败，返回错误
				return nil, st, nil, errors.Errorf("修复 WAL 失败: %s", err)
			}

			// 设置 repaired 为 true，表示已经尝试过修复
			repaired = true
			// 继续循环，尝试再次打开 WAL
			continue
		}

		// 如果成功打开 WAL 并读取所有条目，跳出循环
		break
	}

	// 返回打开的 WAL、硬状态和条目列表
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
