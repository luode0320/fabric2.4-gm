/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockledger

import (
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
)

var logger = flogging.MustGetLogger("common.ledger.blockledger.util")

var closedChan chan struct{}

func init() {
	closedChan = make(chan struct{})
	close(closedChan)
}

// NotFoundErrorIterator simply always returns an error of cb.Status_NOT_FOUND,
// and is generally useful for implementations of the Reader interface
type NotFoundErrorIterator struct{}

// Next returns nil, cb.Status_NOT_FOUND
func (nfei *NotFoundErrorIterator) Next() (*cb.Block, cb.Status) {
	return nil, cb.Status_NOT_FOUND
}

// ReadyChan returns a closed channel
func (nfei *NotFoundErrorIterator) ReadyChan() <-chan struct{} {
	return closedChan
}

// Close does nothing
func (nfei *NotFoundErrorIterator) Close() {}

// CreateNextBlock 提供了一种实用的方法，根据给定账本的内容和元数据构造下一个区块。
// 注意：此功能未来需要修改以接受已序列化的信封，
// 以便适应非确定性的序列化过程。
func CreateNextBlock(rl Reader, messages []*cb.Envelope) *cb.Block {
	var nextBlockNumber uint64   // 下一个区块的编号
	var previousBlockHash []byte // 前一个区块的哈希值
	var err error                // 错误变量

	// 如果账本中已有区块（高度大于0）
	if rl.Height() > 0 {
		// 获取账本的最新迭代器
		it, _ := rl.Iterator(&ab.SeekPosition{
			Type: &ab.SeekPosition_Newest{ // 查找最新的区块
				Newest: &ab.SeekNewest{},
			},
		})
		// 获取最新区块
		block, status := it.Next()
		// 检查获取状态，若非成功则恐慌
		if status != cb.Status_SUCCESS {
			panic("尝试获取非零高度链的最新区块时出错")
		}
		// 设置下一个区块编号为当前最新区块编号加一
		nextBlockNumber = block.Header.Number + 1
		// 设置前一个区块哈希值
		previousBlockHash = protoutil.BlockHeaderHash(block.Header)
	}

	// 初始化新的区块数据结构
	data := &cb.BlockData{
		Data: make([][]byte, len(messages)), // 为消息数组预分配空间
	}

	// 遍历消息数组，将每条消息序列化后存入区块数据中
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg) // 序列化消息
		if err != nil {
			panic(err) // 序列化失败则恐慌
		}
	}

	// 使用上述信息创建新的区块
	block := protoutil.NewBlock(nextBlockNumber, previousBlockHash)
	// 计算并设置区块数据的哈希值
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data // 设置区块数据

	// 返回构造好的新区块
	return block
}

// GetBlock 是一个实用方法，用于从区块链中检索单个区块。
// 参数：
// rl - Reader 接口，用于读取区块链数据。
// index - 要检索的区块的索引号（区块高度）。
//
// 返回值：
// *cb.Block - 返回指定索引号的区块数据。如果区块不存在或读取失败，则返回nil。
//
// 功能说明：
// 本方法通过传入的Reader接口实例rl来定位并读取区块链中指定索引号index的区块。
// 它首先使用rl的Iterator方法创建一个迭代器，该迭代器指向区块链中索引为index的区块位置。
// 如果迭代器创建成功，将调用Next方法来获取下一个区块数据，这里下一个区块实际上就是目标区块，
// 因为迭代器已经定位到了正确的区块位置。如果区块数据获取成功，返回该区块数据；
// 如果获取失败（例如，区块不存在或读取异常），则返回nil。
func GetBlock(rl Reader, index uint64) *cb.Block {
	iterator, _ := rl.Iterator(&ab.SeekPosition{
		Type: &ab.SeekPosition_Specified{
			Specified: &ab.SeekSpecified{Number: index},
		},
	})
	if iterator == nil {
		return nil
	}
	defer iterator.Close()
	block, status := iterator.Next()
	if status != cb.Status_SUCCESS {
		return nil
	}
	return block
}

func GetBlockByNumber(rl Reader, blockNum uint64) (*cb.Block, error) {
	logger.Debugw("Retrieving block", "blockNum", blockNum)
	return rl.RetrieveBlockByNumber(blockNum)
}
