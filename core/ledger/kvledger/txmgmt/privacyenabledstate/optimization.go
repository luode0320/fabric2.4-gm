/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privacyenabledstate

import (
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

// metadataHint 是一个元数据提示的结构体。
type metadataHint struct {
	cache      map[string]bool         // 缓存
	bookkeeper *leveldbhelper.DBHandle // bookkeeper 数据库句柄
}

// newMetadataHint 创建一个metadataHint实例。
// 方法接收者：无（函数）
// 输入参数：
//   - bookkeeper：leveldbhelper.DBHandle类型的指针，用于获取元数据信息。
//
// 返回值：
//   - *metadataHint：创建的metadataHint实例。
//   - error：如果创建metadataHint实例时出错，则返回错误。
func newMetadataHint(bookkeeper *leveldbhelper.DBHandle) (*metadataHint, error) {
	// 创建一个缓存，用于存储已经存在的命名空间
	cache := map[string]bool{}
	// 获取bookkeeper的迭代器
	itr, err := bookkeeper.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}
	defer itr.Release()
	// 遍历迭代器，将命名空间添加到缓存中
	for itr.Next() {
		namespace := string(itr.Key())
		cache[namespace] = true
	}
	// 创建并返回metadataHint实例
	return &metadataHint{cache, bookkeeper}, nil
}

func (h *metadataHint) metadataEverUsedFor(namespace string) bool {
	return h.cache[namespace]
}

func (h *metadataHint) setMetadataUsedFlag(updates *UpdateBatch) error {
	batch := h.bookkeeper.NewUpdateBatch()
	for ns := range filterNamespacesThatHasMetadata(updates) {
		if h.cache[ns] {
			continue
		}
		h.cache[ns] = true
		batch.Put([]byte(ns), []byte{})
	}
	return h.bookkeeper.WriteBatch(batch, true)
}

func (h *metadataHint) importNamespacesThatUseMetadata(namespaces map[string]struct{}) error {
	batch := h.bookkeeper.NewUpdateBatch()
	for ns := range namespaces {
		batch.Put([]byte(ns), []byte{})
	}
	return h.bookkeeper.WriteBatch(batch, true)
}

func filterNamespacesThatHasMetadata(updates *UpdateBatch) map[string]bool {
	namespaces := map[string]bool{}
	pubUpdates, hashUpdates := updates.PubUpdates, updates.HashUpdates
	// add ns for public data
	for _, ns := range pubUpdates.GetUpdatedNamespaces() {
		for _, vv := range updates.PubUpdates.GetUpdates(ns) {
			if vv.Metadata == nil {
				continue
			}
			namespaces[ns] = true
		}
	}
	// add ns for private hashes
	for ns, nsBatch := range hashUpdates.UpdateMap {
		for _, coll := range nsBatch.GetCollectionNames() {
			for _, vv := range nsBatch.GetUpdates(coll) {
				if vv.Metadata == nil {
					continue
				}
				namespaces[ns] = true
			}
		}
	}
	return namespaces
}
