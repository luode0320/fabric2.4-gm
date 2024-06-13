/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cache

import (
	"sync"
	"sync/atomic"
)

// This package implements Second-Chance Algorithm, an approximate LRU algorithms.
// https://www.cs.jhu.edu/~yairamir/cs418/os6/tsld023.htm

// secondChanceCache 保存具有有限大小的键值项。
// 当缓存的项数超过限制时，受害者是基于第二次机会算法选择并清除的
type secondChanceCache struct {
	// 管理键和项之间的映射
	table map[string]*cacheItem

	// 保存缓存项的列表。
	items []*cacheItem

	// 指示项目列表中受害者的下一个候选者
	position int

	// get的读锁和add的写锁
	rwlock sync.RWMutex
}

type cacheItem struct {
	key   string
	value interface{}
	// 当调用get() 时设置为1。当受害者扫描时设置为0
	referenced int32
}

// newSecondChanceCache 函数用于创建一个 第二次机会缓存 实例。
//
// 参数：
//   - cacheSize int：缓存的大小。
//
// 返回值：
//   - *secondChanceCache：创建的 Second Chance Cache 实例的指针。
func newSecondChanceCache(cacheSize int) *secondChanceCache {
	var cache secondChanceCache

	// 初始化缓存
	cache.position = 0
	cache.items = make([]*cacheItem, cacheSize)
	cache.table = make(map[string]*cacheItem)

	return &cache
}

func (cache *secondChanceCache) len() int {
	cache.rwlock.RLock()
	defer cache.rwlock.RUnlock()

	return len(cache.table)
}

func (cache *secondChanceCache) get(key string) (interface{}, bool) {
	cache.rwlock.RLock()
	defer cache.rwlock.RUnlock()

	item, ok := cache.table[key]
	if !ok {
		return nil, false
	}

	// 被引用的位被设置为true以指示最近访问了此项目。
	atomic.StoreInt32(&item.referenced, 1)

	return item.value, true
}

func (cache *secondChanceCache) add(key string, value interface{}) {
	cache.rwlock.Lock()
	defer cache.rwlock.Unlock()

	if old, ok := cache.table[key]; ok {
		old.value = value
		atomic.StoreInt32(&old.referenced, 1)
		return
	}

	var item cacheItem
	item.key = key
	item.value = value

	size := len(cache.items)
	num := len(cache.table)
	if num < size {
		// cache is not full, so just store the new item at the end of the list
		cache.table[key] = &item
		cache.items[num] = &item
		return
	}

	// starts victim scan since cache is full
	for {
		// checks whether this item is recently accessed or not
		victim := cache.items[cache.position]
		if atomic.LoadInt32(&victim.referenced) == 0 {
			// a victim is found. delete it, and store the new item here.
			delete(cache.table, victim.key)
			cache.table[key] = &item
			cache.items[cache.position] = &item
			cache.position = (cache.position + 1) % size
			return
		}

		// referenced bit is set to false so that this item will be get purged
		// unless it is accessed until a next victim scan
		atomic.StoreInt32(&victim.referenced, 0)
		cache.position = (cache.position + 1) % size
	}
}
