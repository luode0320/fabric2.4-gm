/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import "path/filepath"

const (
	// ChainsDir 是包含链分类账本的目录的名称。
	ChainsDir = "chains"
	// IndexDir 是包含跨分类账本的所有块索引的目录的名称。
	IndexDir                = "index"
	defaultMaxBlockfileSize = 64 * 1024 * 1024 // bytes
)

// Conf 封装 “blockstore” 的所有配置
type Conf struct {
	blockStorageDir  string
	maxBlockfileSize int
}

// NewConf 构造新的 “conf”。
// blockStorageDir是 “blockstore” 管理其数据的顶级文件夹
func NewConf(blockStorageDir string, maxBlockfileSize int) *Conf {
	if maxBlockfileSize <= 0 {
		maxBlockfileSize = defaultMaxBlockfileSize
	}
	return &Conf{blockStorageDir, maxBlockfileSize}
}

func (conf *Conf) getIndexDir() string {
	return filepath.Join(conf.blockStorageDir, IndexDir)
}

func (conf *Conf) getChainsDir() string {
	return filepath.Join(conf.blockStorageDir, ChainsDir)
}

// getLedgerBlockDir 返回给定账本ID的账本块目录路径。
// 方法接收者：conf（Conf类型的指针）
// 输入参数：
//   - ledgerid：要获取账本块目录路径的账本ID。
//
// 返回值：
//   - string：给定账本ID的账本块目录路径。
func (conf *Conf) getLedgerBlockDir(ledgerid string) string {
	// 使用filepath.Join函数将链目录路径和账本ID拼接成账本块目录路径
	return filepath.Join(conf.getChainsDir(), ledgerid)
}
