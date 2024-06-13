/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"path/filepath"
	"time"

	coreconfig "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/spf13/viper"
)

func ledgerConfig() *ledger.Config {
	// 在内部，链码可以执行多个CouchDB查询，查询CouchDB时内部返回的最大记录数。
	internalQueryLimit := 1000
	if viper.IsSet("ledger.state.couchDBConfig.internalQueryLimit") {
		internalQueryLimit = viper.GetInt("ledger.state.couchDBConfig.internalQueryLimit")
	}

	// CouchDB批量更新操作中包含的最大记录数。
	maxBatchUpdateSize := 500
	if viper.IsSet("ledger.state.couchDBConfig.maxBatchUpdateSize") {
		maxBatchUpdateSize = viper.GetInt("ledger.state.couchDBConfig.maxBatchUpdateSize")
	}

	// 是将不符合条件的缺失数据条目转换为符合条件的条目时的最大批大小。
	collElgProcMaxDbBatchSize := 5000
	if viper.IsSet("ledger.pvtdataStore.collElgProcMaxDbBatchSize") {
		collElgProcMaxDbBatchSize = viper.GetInt("ledger.pvtdataStore.collElgProcMaxDbBatchSize")
	}

	// 将不符合条件的缺失数据条目转换为符合条件的条目的批次之间的最小持续时间 (毫秒)。
	collElgProcDbBatchesInterval := 1000
	if viper.IsSet("ledger.pvtdataStore.collElgProcDbBatchesInterval") {
		collElgProcDbBatchesInterval = viper.GetInt("ledger.pvtdataStore.collElgProcDbBatchesInterval")
	}

	// 清除过期的专用数据条目之前要等待的块数。
	purgeInterval := 100
	if viper.IsSet("ledger.pvtdataStore.purgeInterval") {
		purgeInterval = viper.GetInt("ledger.pvtdataStore.purgeInterval")
	}

	// 缺失的数据条目分为三类:
	// (1) 符合条件的优先级别
	// (2) 符合条件的取消优先级
	// (3) 无法选择
	// 调和器将从其他对等端获取符合条件的优先化缺失数据。
	// 在每次降低优先级后，将给出符合条件的降低优先级的丢失数据的机会
	deprioritizedDataReconcilerInterval := 60 * time.Minute
	if viper.IsSet("ledger.pvtdataStore.deprioritizedDataReconcilerInterval") {
		deprioritizedDataReconcilerInterval = viper.GetDuration("ledger.pvtdataStore.deprioritizedDataReconcilerInterval")
	}

	// 文件存储目录
	fsPath := coreconfig.GetPath("peer.fileSystemPath")
	// 是存储分类账本文件的顶级目录。
	ledgersDataRootDir := filepath.Join(fsPath, "ledgersData")
	// 对等方将存储分类账本快照的文件系统上的路径, 默认 peer.fileSystemPath/snapshots
	snapshotsRootDir := viper.GetString("ledger.snapshots.rootDir")
	if snapshotsRootDir == "" {
		snapshotsRootDir = filepath.Join(fsPath, "snapshots")
	}

	// 状态数据库, 支持的两个选项是 “goleveldb” 和 “CouchDB” (分别在常量GoLevelDB和CouchDB中捕获)。
	stateDatabase := viper.GetString("ledger.state.stateDatabase")
	if stateDatabase == "" {
		stateDatabase = ledger.GoLevelDB
	}

	// 是用于配置分类账本提供程序的结构。
	conf := &ledger.Config{
		// 是存储分类账本文件的顶级目录。
		RootFSPath: ledgersDataRootDir,
		// 是用于配置分类账本的状态参数的结构。
		StateDBConfig: &ledger.StateDBConfig{
			StateDatabase: stateDatabase,           // 支持的两个选项是 “goleveldb” 和 “CouchDB”
			CouchDB:       &ledger.CouchDBConfig{}, // 是CouchDB的配置。当StateDatabase设置为 “CouchDB” 时使用。
		},
		// 是用于配置私有数据存储提供程序的结构。
		PrivateDataConfig: &ledger.PrivateDataConfig{
			MaxBatchSize:                        collElgProcMaxDbBatchSize,           // 是将不符合条件的缺失数据条目转换为符合条件的条目时的最大批大小。
			BatchesInterval:                     collElgProcDbBatchesInterval,        // 将不符合条件的缺失数据条目转换为符合条件的条目的批次之间的最小持续时间 (毫秒)。
			PurgeInterval:                       purgeInterval,                       // 清除过期的专用数据条目之前要等待的块数。
			DeprioritizedDataReconcilerInterval: deprioritizedDataReconcilerInterval, // 调和器将从其他对等端获取符合条件的优先化缺失数据。
		},
		// 是用于配置事务历史数据库的结构。
		HistoryDBConfig: &ledger.HistoryDBConfig{
			Enabled: viper.GetBool("ledger.history.enableHistoryDatabase"), // 指示是否应存储密钥更新的历史记录。
		},
		// 是用于配置快照功能的结构
		SnapshotsConfig: &ledger.SnapshotsConfig{
			RootDir: snapshotsRootDir, // 是快照的顶级目录。
		},
	}

	// 如果使用couchdb
	if conf.StateDBConfig.StateDatabase == ledger.CouchDB {
		// 是用于配置 Couchdb Instance 的结构体。
		conf.StateDBConfig.CouchDB = &ledger.CouchDBConfig{
			Address:               viper.GetString("ledger.state.couchDBConfig.couchDBAddress"),      // CouchDB 数据库实例的主机名和端口号
			Username:              viper.GetString("ledger.state.couchDBConfig.username"),            // 用于与 CouchDB 进行身份验证的用户名，该用户名必须具有读写权限
			Password:              viper.GetString("ledger.state.couchDBConfig.password"),            // 用于用户名的密码
			MaxRetries:            viper.GetInt("ledger.state.couchDBConfig.maxRetries"),             // 在失败时重试 CouchDB 操作的最大次数
			MaxRetriesOnStartup:   viper.GetInt("ledger.state.couchDBConfig.maxRetriesOnStartup"),    // 在初始化账本时失败时重试 CouchDB 操作的最大次数
			RequestTimeout:        viper.GetDuration("ledger.state.couchDBConfig.requestTimeout"),    // CouchDB 操作的超时时间
			InternalQueryLimit:    internalQueryLimit,                                                // 在查询 CouchDB 时内部返回的最大记录数
			MaxBatchUpdateSize:    maxBatchUpdateSize,                                                // CouchDB 批量更新操作中包含的最大记录数
			CreateGlobalChangesDB: viper.GetBool("ledger.state.couchDBConfig.createGlobalChangesDB"), // 是否创建 "_global_changes" 系统数据库
			RedoLogPath:           filepath.Join(ledgersDataRootDir, "couchdbRedoLogs"),              // 存储 CouchDB 重做日志文件的目录
			UserCacheSizeMBs:      viper.GetInt("ledger.state.couchDBConfig.cacheSize"),              // 用户状态缓存的最大兆字节数（MB），用于存储用户部署的所有链码。
		}
	}
	return conf
}
