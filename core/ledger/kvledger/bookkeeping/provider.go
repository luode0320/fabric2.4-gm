/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bookkeeping

import (
	"fmt"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

// Category is an enum type for representing the bookkeeping of different type
type Category int

const (
	// PvtdataExpiry represents the bookkeeping related to expiry of pvtdata because of BTL policy
	PvtdataExpiry Category = iota
	// MetadataPresenceIndicator 维护有关是否为命名空间设置元数据的簿记
	MetadataPresenceIndicator
	// SnapshotRequest 维护快照请求的信息
	SnapshotRequest
)

// Provider 为不同的簿记员提供db句柄
type Provider struct {
	dbProvider *leveldbhelper.Provider // 允许将单个 LevelDB 用作多个逻辑 LevelDB
}

// NewProvider 实例化一个新的提供者。
// 输入参数：
//   - dbPath：字符串，表示数据库路径
//
// 返回值：
//   - *Provider：表示提供者的指针
//   - error：表示实例化过程中可能出现的错误
func NewProvider(dbPath string) (*Provider, error) {
	// 创建 LevelDB 提供者
	dbProvider, err := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	if err != nil {
		return nil, err
	}

	return &Provider{dbProvider: dbProvider}, nil
}

// GetDBHandle 数据库处理函数
func (p *Provider) GetDBHandle(ledgerID string, cat Category) *leveldbhelper.DBHandle {
	return p.dbProvider.GetDBHandle(dbName(ledgerID, cat))
}

// Close implements the function in the interface 'BookKeeperProvider'
func (p *Provider) Close() {
	p.dbProvider.Close()
}

// Drop drops channel-specific data from the config history db
func (p *Provider) Drop(ledgerID string) error {
	for _, cat := range []Category{PvtdataExpiry, MetadataPresenceIndicator, SnapshotRequest} {
		if err := p.dbProvider.Drop(dbName(ledgerID, cat)); err != nil {
			return err
		}
	}
	return nil
}

func dbName(ledgerID string, cat Category) string {
	return fmt.Sprintf(ledgerID+"/%d", cat)
}
