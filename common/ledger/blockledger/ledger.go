/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockledger

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
)

// Factory retrieves or creates new ledgers by channelID
type Factory interface {
	// GetOrCreate gets an existing ledger (if it exists)
	// or creates it if it does not
	GetOrCreate(channelID string) (ReadWriter, error)

	// Remove removes an existing ledger
	Remove(channelID string) error

	// ChannelIDs returns the channel IDs the Factory is aware of
	ChannelIDs() []string

	// Close releases all resources acquired by the factory
	Close()
}

// Iterator is useful for a chain Reader to stream blocks as they are created
type Iterator interface {
	// Next blocks until there is a new block available, or returns an error if
	// the next block is no longer retrievable
	Next() (*cb.Block, cb.Status)
	// Close releases resources acquired by the Iterator
	Close()
}

// Reader 接口允许调用者检查账本内容。
type Reader interface {
	// Iterator 根据 ab.SeekInfo 消息中指定的条件返回一个迭代器，以及迭代器的起始区块编号。
	Iterator(startType *ab.SeekPosition) (Iterator, uint64)
	// Height 返回账本上区块的数量，即账本高度。
	Height() uint64
	// RetrieveBlockByNumber 根据区块编号检索账本中的区块。
	RetrieveBlockByNumber(blockNumber uint64) (*cb.Block, error)
}

// Writer 接口允许调用者修改账本。
type Writer interface {
	// Append 向账本追加一个新的区块。
	// 参数block为待追加的区块指针。
	Append(block *cb.Block) error
}

// ReadWriter encapsulates the read/write functions of the ledger
type ReadWriter interface {
	Reader
	Writer
}
