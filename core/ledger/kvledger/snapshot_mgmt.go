/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/pkg/errors"
)

type eventType string

const (
	commitStart         eventType = "commitStart"
	commitDone          eventType = "commitDone"
	requestAdd          eventType = "requestAdd"
	requestCancel       eventType = "requestCancel"
	snapshotDone        eventType = "snapshotDone"
	snapshotMgrShutdown eventType = "snapshotMgrShutdown"
)

type nextBlockCommitStatus int

const (
	idle nextBlockCommitStatus = iota
	blocked
	inProcess
)

type snapshotMgr struct {
	snapshotRequestBookkeeper *snapshotRequestBookkeeper
	events                    chan *event
	commitProceed             chan struct{}
	requestResponses          chan *requestResponse
	stopped                   bool
	shutdownLock              sync.Mutex
}

type event struct {
	typ         eventType
	blockNumber uint64
}

func (e *event) String() string {
	return fmt.Sprintf("{type=%s, blockNumber=%d}", e.typ, e.blockNumber)
}

type requestResponse struct {
	err error
}

// SubmitSnapshotRequest submits a snapshot request for the specified block number.
// The request will be stored in the ledger until the ledger commits the given block number
// and the snapshot generation is completed.
// When block number is 0, it will generate a snapshot at the last committed block.
// It returns an error if the specified block number is smaller than the last committed block number
// or the requested block number already exists.
func (l *kvLedger) SubmitSnapshotRequest(blockNumber uint64) error {
	l.snapshotMgr.events <- &event{requestAdd, blockNumber}
	response := <-l.snapshotMgr.requestResponses
	return response.err
}

// CancelSnapshotRequest cancels the previously submitted request.
// It returns an error if such a request does not exist or is under processing.
func (l *kvLedger) CancelSnapshotRequest(blockNumber uint64) error {
	l.snapshotMgr.events <- &event{requestCancel, blockNumber}
	response := <-l.snapshotMgr.requestResponses
	return response.err
}

// PendingSnapshotRequests returns a list of block numbers for the pending (or under processing) snapshot requests.
func (l *kvLedger) PendingSnapshotRequests() ([]uint64, error) {
	return l.snapshotMgr.snapshotRequestBookkeeper.list()
}

// processSnapshotMgmtEvents handles each event in the events channel and performs synchronization acorss
// block commits, snapshot generation, and snapshot request submission/cancellation.
// It should be started in a separate goroutine when the ledger is created/opened.
// There are 3 unbuffered channels and 5 events working together to process events one by one
// and perform synchronization.
// - events: a channel receiving all the events
// - commitProceed: a channel indicating if commit can be proceeded. Commit is blocked if a snapshot generation is in progress.
// - requestResponses: a channel returning the response for snapshot request submission/cancellation.
// The 5 events are:
// - commitStart: sent before committing a block
// - commitDone: sent after a block is committed
// - snapshotDone: sent when a snapshot generation is finished, regardless of success or failure
// - requestAdd: sent when a snapshot request is submitted
// - requestCancel: sent when a snapshot request is cancelled
// In addition, the snapshotMgrShutdown event is sent when snapshotMgr shutdown is called. Upon receiving this event,
// this function will return immediately.
func (l *kvLedger) processSnapshotMgmtEvents(lastCommittedBlockNumber uint64) {
	committerStatus := idle
	// snapshotInProgress is set to true before a generateSnapshot is called when processing commitDone or requestAdd event
	// and set to false when snapshotDone event is received
	snapshotInProgress := false

	events := l.snapshotMgr.events
	commitProceed := l.snapshotMgr.commitProceed
	requestResponses := l.snapshotMgr.requestResponses

	for {
		e := <-events
		logger.Debugw("Event received",
			"channelID", l.ledgerID, "event", e, "snapshotInProgress", snapshotInProgress,
			"lastCommittedBlockNumber", lastCommittedBlockNumber, "committerStatus", committerStatus,
		)

		switch e.typ {
		case commitStart:
			committerStatus = blocked
			if snapshotInProgress {
				logger.Infow("Blocking the commit till snapshot generation completes", "channelID", l.ledgerID, "blockNumber", e.blockNumber)
				continue
			}
			// no in-progress snapshot, let commit proceed
			committerStatus = inProcess
			commitProceed <- struct{}{}

		case commitDone:
			lastCommittedBlockNumber = e.blockNumber
			committerStatus = idle
			if lastCommittedBlockNumber != l.snapshotMgr.snapshotRequestBookkeeper.smallestRequestBlockNum {
				continue
			}
			snapshotInProgress = true
			go func() {
				logger.Infow("Generating snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber)
				if err := l.generateSnapshot(); err != nil {
					logger.Errorw("Failed to generate snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber, "error", err)
				} else {
					logger.Infow("Generated snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber)
				}
				events <- &event{snapshotDone, lastCommittedBlockNumber}
			}()

		case snapshotDone:
			requestedBlockNum := e.blockNumber
			if err := l.snapshotMgr.snapshotRequestBookkeeper.delete(e.blockNumber); err != nil {
				logger.Errorw("Failed to delete snapshot request, the pending snapshot requests (if any) may not be processed", "channelID", l.ledgerID, "requestedBlockNum", requestedBlockNum, "error", err)
			}
			if committerStatus == blocked {
				logger.Infow("Unblocking the commit", "channelID", l.ledgerID)
				committerStatus = inProcess
				commitProceed <- struct{}{}
			}
			snapshotInProgress = false

		case requestAdd:
			leastAcceptableBlockNum := lastCommittedBlockNumber
			if committerStatus != idle {
				leastAcceptableBlockNum++
			}

			requestedBlockNum := e.blockNumber
			if requestedBlockNum == 0 {
				requestedBlockNum = leastAcceptableBlockNum
				logger.Infow("Converting the snapshot generation request from block number 0 to the latest committed block number",
					"channelID", l.ledgerID, "convertedRequestBlockNumber", leastAcceptableBlockNum)
			}

			if requestedBlockNum < leastAcceptableBlockNum {
				requestResponses <- &requestResponse{errors.Errorf("requested snapshot for block number %d cannot be less than the last committed block number %d", requestedBlockNum, leastAcceptableBlockNum)}
				continue
			}

			if requestedBlockNum == lastCommittedBlockNumber {
				// this is a corner case where no block has been committed since last snapshot was generated.
				exists, err := l.snapshotExists(requestedBlockNum)
				if err != nil {
					requestResponses <- &requestResponse{err}
					continue
				}
				if exists {
					requestResponses <- &requestResponse{errors.Errorf("snapshot already generated for block number %d", requestedBlockNum)}
					continue
				}
			}

			if err := l.snapshotMgr.snapshotRequestBookkeeper.add(requestedBlockNum); err != nil {
				requestResponses <- &requestResponse{err}
				continue
			}

			if committerStatus == idle && requestedBlockNum == lastCommittedBlockNumber {
				snapshotInProgress = true
				go func() {
					logger.Infow("Generating snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber)
					if err := l.generateSnapshot(); err != nil {
						logger.Errorw("Failed to generate snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber, "error", err)
					} else {
						logger.Infow("Generated snapshot", "channelID", l.ledgerID, "lastCommittedBlockNumber", lastCommittedBlockNumber)
					}
					events <- &event{snapshotDone, requestedBlockNum}
				}()
			}
			requestResponses <- &requestResponse{}

		case requestCancel:
			requestedBlockNum := e.blockNumber
			if snapshotInProgress && requestedBlockNum == lastCommittedBlockNumber {
				requestResponses <- &requestResponse{errors.Errorf("cannot cancel the snapshot request because it is under processing")}
				continue
			}
			requestResponses <- &requestResponse{l.snapshotMgr.snapshotRequestBookkeeper.delete(requestedBlockNum)}

		case snapshotMgrShutdown:
			return
		}
	}
}

// regenrateMissedSnapshot 用于重新生成丢失的快照。
// 方法接收者：l（kvLedger类型的指针）
// 输入参数：
//   - blockNumber：要重新生成快照的块号。
//
// 返回值：
//   - error：如果在重新生成快照过程中出错，则返回错误。
func (l *kvLedger) regenrateMissedSnapshot(blockNumber uint64) error {
	// 如果块号不等于最小的快照请求块号，则直接返回
	if blockNumber != l.snapshotMgr.snapshotRequestBookkeeper.smallestRequestBlockNum {
		return nil
	}
	// 检查快照是否存在
	exists, err := l.snapshotExists(blockNumber)
	if err != nil {
		return err
	}
	// 如果快照不存在，则发送commitDone事件以生成丢失的快照
	if !exists {
		l.snapshotMgr.events <- &event{typ: commitDone, blockNumber: blockNumber}
	}
	return nil
}

// snapshotExists checks if the snapshot for the given block number exists
func (l *kvLedger) snapshotExists(blockNum uint64) (bool, error) {
	snapshotDir := SnapshotDirForLedgerBlockNum(l.config.SnapshotsConfig.RootDir, l.ledgerID, blockNum)
	stat, err := os.Stat(snapshotDir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stat != nil, nil
}

// shutdown sends a snapshotMgrShutdown event and close all the channels, which is called
// when the ledger is closed. For simplicity, this function does not consider in-progress commit
// or snapshot generation. The caller should make sure there is no in-progress commit or
// snapshot generation. Otherwise, it may cause panic because the channels have been closed.
func (m *snapshotMgr) shutdown() {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()

	if m.stopped {
		return
	}

	m.stopped = true
	m.events <- &event{typ: snapshotMgrShutdown}
	close(m.events)
	close(m.commitProceed)
	close(m.requestResponses)
}

// snapshotRequestBookkeeper 在 leveldb 中管理快照请求，并维护待处理快照请求的最小区块号。
type snapshotRequestBookkeeper struct {
	ledgerID                string                  // 账本ID
	dbHandle                *leveldbhelper.DBHandle // 数据库句柄
	smallestRequestBlockNum uint64                  // 待处理快照请求的最小区块号
}

// 维护待处理快照请求的最小区块号(账本id, 数据库处理函数, 待处理快照请求的最小区块号)
func newSnapshotRequestBookkeeper(ledgerID string, dbHandle *leveldbhelper.DBHandle) (*snapshotRequestBookkeeper, error) {
	// 维护待处理快照请求的最小区块号(账本id, 数据库处理函数, 待处理快照请求的最小区块号)
	bk := &snapshotRequestBookkeeper{
		ledgerID: ledgerID, // 账本ID
		dbHandle: dbHandle, // 数据库句柄
	}

	var err error
	// 待处理快照请求的最小区块号, 返回最小的快照请求块号
	if bk.smallestRequestBlockNum, err = bk.smallestRequest(); err != nil {
		return nil, err
	}

	return bk, nil
}

// add adds the given block number to the bookkeeper db and returns an error if the block number already exists
func (k *snapshotRequestBookkeeper) add(blockNumber uint64) error {
	logger.Infow("Adding new request for snapshot", "channelID", k.ledgerID, "blockNumber", blockNumber)
	key := encodeSnapshotRequestKey(blockNumber)

	exists, err := k.exist(blockNumber)
	if err != nil {
		return err
	}
	if exists {
		return errors.Errorf("duplicate snapshot request for block number %d", blockNumber)
	}

	if err := k.dbHandle.Put(key, []byte{}, true); err != nil {
		return err
	}

	if blockNumber < k.smallestRequestBlockNum {
		k.smallestRequestBlockNum = blockNumber
	}
	logger.Infow("Added new request for snapshot", "channelID", k.ledgerID, "blockNumber", blockNumber, "next snapshot blockNumber", k.smallestRequestBlockNum)
	return nil
}

// delete deletes the given block number from the bookkeeper db and returns an error if the block number does not exist
func (k *snapshotRequestBookkeeper) delete(blockNumber uint64) error {
	logger.Infow("Deleting pending request for snapshot", "channelID", k.ledgerID, "blockNumber", blockNumber)
	exists, err := k.exist(blockNumber)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Errorf("no snapshot request exists for block number %d", blockNumber)
	}

	if err = k.dbHandle.Delete(encodeSnapshotRequestKey(blockNumber), true); err != nil {
		return err
	}

	if k.smallestRequestBlockNum != blockNumber {
		return nil
	}

	if k.smallestRequestBlockNum, err = k.smallestRequest(); err != nil {
		return err
	}
	logger.Infow("Deleted pending request for snapshot", "channelID", k.ledgerID, "blockNumber", blockNumber, "next snapshot blockNumber", k.smallestRequestBlockNum)
	return nil
}

func (k *snapshotRequestBookkeeper) list() ([]uint64, error) {
	requestedBlockNumbers := []uint64{}
	itr, err := k.dbHandle.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}
	defer itr.Release()

	for {
		hasMore := itr.Next()
		if err = itr.Error(); err != nil {
			return nil, errors.Wrapf(err, "internal leveldb error while iterating for snapshot requests")
		}
		if !hasMore {
			break
		}
		blockNumber, _, err := decodeSnapshotRequestKey(itr.Key())
		if err != nil {
			return nil, err
		}
		requestedBlockNumbers = append(requestedBlockNumbers, blockNumber)
	}

	return requestedBlockNumbers, nil
}

func (k *snapshotRequestBookkeeper) exist(blockNumber uint64) (bool, error) {
	val, err := k.dbHandle.Get(encodeSnapshotRequestKey(blockNumber))
	if err != nil {
		return false, err
	}
	exists := val != nil
	return exists, nil
}

const defaultSmallestBlockNumber uint64 = math.MaxUint64

// smallestRequest 返回最小的快照请求块号。
// 方法接收者：k（snapshotRequestBookkeeper类型的指针）
// 输入参数：无
// 返回值：
//   - uint64：最小的快照请求块号。
//   - error：如果在获取迭代器或解码快照请求键时出错，则返回错误。
func (k *snapshotRequestBookkeeper) smallestRequest() (uint64, error) {
	// 获取数据库迭代器
	itr, err := k.dbHandle.GetIterator(nil, nil)
	if err != nil {
		return 0, err
	}
	defer itr.Release()

	// 检查是否还有更多的键值对
	hasMore := itr.Next()
	if err = itr.Error(); err != nil {
		return 0, errors.Wrapf(err, "internal leveldb error while iterating for snapshot requests")
	}
	if !hasMore {
		return defaultSmallestBlockNumber, nil
	}

	// 解码快照请求键，获取最小的快照请求块号
	smallestBlockNumber, _, err := decodeSnapshotRequestKey(itr.Key())
	if err != nil {
		return 0, err
	}
	return smallestBlockNumber, nil
}

var snapshotRequestKeyPrefix = []byte("s")

func encodeSnapshotRequestKey(blockNumber uint64) []byte {
	return append(snapshotRequestKeyPrefix, util.EncodeOrderPreservingVarUint64(blockNumber)...)
}

func decodeSnapshotRequestKey(key []byte) (uint64, int, error) {
	return util.DecodeOrderPreservingVarUint64(key[len(snapshotRequestKeyPrefix):])
}
