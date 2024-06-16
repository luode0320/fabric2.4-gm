/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ledger

import (
	"github.com/hyperledger/fabric-protos-go/common"
)

// Ledger 接口捕获了“PeerLedger”、“OrdererLedger”和“ValidatedLedger”之间共有的方法，
// 这些方法用于访问和操作区块链账本的关键信息。
type Ledger interface {
	// GetBlockchainInfo 返回关于区块链的基本信息，如当前高度和当前哈希。
	GetBlockchainInfo() (*common.BlockchainInfo, error)

	// GetBlockByNumber 根据给定的高度返回区块。如果blockNumber为math.MaxUint64，则返回最后一个区块。
	GetBlockByNumber(blockNumber uint64) (*common.Block, error)

	// GetBlocksIterator 从指定的起始区块号（包含）开始返回一个区块迭代器。
	// 这是一个阻塞迭代器，意味着它会等待直到账本中有新的区块可用。
	// ResultsIterator 中包含的数据类型为 BlockHolder。
	GetBlocksIterator(startBlockNumber uint64) (ResultsIterator, error)

	// Close 关闭账本，释放关联资源。
	Close()
}

// ResultsIterator - an iterator for query result set
type ResultsIterator interface {
	// Next returns the next item in the result set. The `QueryResult` is expected to be nil when
	// the iterator gets exhausted
	Next() (QueryResult, error)
	// Close releases resources occupied by the iterator
	Close()
}

// QueryResultsIterator - an iterator for query result set
type QueryResultsIterator interface {
	ResultsIterator
	GetBookmarkAndClose() string
}

// QueryResult - a general interface for supporting different types of query results. Actual types differ for different queries
type QueryResult interface{}

// PrunePolicy - a general interface for supporting different pruning policies
type PrunePolicy interface{}
