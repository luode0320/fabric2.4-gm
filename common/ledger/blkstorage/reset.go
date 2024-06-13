/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/hyperledger/fabric/internal/fileutil"
)

// ResetBlockStore drops the block storage index and truncates the blocks files for all channels/ledgers to genesis blocks
func ResetBlockStore(blockStorageDir string) error {
	if err := DeleteBlockStoreIndex(blockStorageDir); err != nil {
		return err
	}
	conf := &Conf{blockStorageDir: blockStorageDir}
	chainsDir := conf.getChainsDir()
	chainsDirExists, err := pathExists(chainsDir)
	if err != nil {
		return err
	}
	if !chainsDirExists {
		logger.Infof("Dir [%s] missing... exiting", chainsDir)
		return nil
	}
	ledgerIDs, err := fileutil.ListSubdirs(chainsDir)
	if err != nil {
		return err
	}
	if len(ledgerIDs) == 0 {
		logger.Info("No ledgers found.. exiting")
		return nil
	}
	logger.Infof("Found ledgers - %s", ledgerIDs)
	for _, ledgerID := range ledgerIDs {
		ledgerDir := conf.getLedgerBlockDir(ledgerID)
		if err := recordHeightIfGreaterThanPreviousRecording(ledgerDir); err != nil {
			return err
		}
		if err := resetToGenesisBlk(ledgerDir); err != nil {
			return err
		}
	}
	return nil
}

// DeleteBlockStoreIndex deletes block store index file
func DeleteBlockStoreIndex(blockStorageDir string) error {
	conf := &Conf{blockStorageDir: blockStorageDir}
	indexDir := conf.getIndexDir()
	logger.Infof("Dropping all contents under the index dir [%s]... if present", indexDir)
	return fileutil.RemoveContents(indexDir)
}

func resetToGenesisBlk(ledgerDir string) error {
	logger.Infof("Resetting ledger [%s] to genesis block", ledgerDir)
	lastFileNum, err := retrieveLastFileSuffix(ledgerDir)
	logger.Infof("lastFileNum = [%d]", lastFileNum)
	if err != nil {
		return err
	}
	if lastFileNum < 0 {
		return nil
	}
	zeroFilePath, genesisBlkEndOffset, err := retrieveGenesisBlkOffsetAndMakeACopy(ledgerDir)
	if err != nil {
		return err
	}
	for lastFileNum > 0 {
		filePath := deriveBlockfilePath(ledgerDir, lastFileNum)
		logger.Infof("Deleting file number = [%d]", lastFileNum)
		if err := os.Remove(filePath); err != nil {
			return err
		}
		lastFileNum--
	}
	logger.Infof("Truncating file [%s] to offset [%d]", zeroFilePath, genesisBlkEndOffset)
	return os.Truncate(zeroFilePath, genesisBlkEndOffset)
}

func retrieveGenesisBlkOffsetAndMakeACopy(ledgerDir string) (string, int64, error) {
	blockfilePath := deriveBlockfilePath(ledgerDir, 0)
	blockfileStream, err := newBlockfileStream(ledgerDir, 0, 0)
	if err != nil {
		return "", -1, err
	}
	genesisBlockBytes, _, err := blockfileStream.nextBlockBytesAndPlacementInfo()
	if err != nil {
		return "", -1, err
	}
	endOffsetGenesisBlock := blockfileStream.currentOffset
	blockfileStream.close()

	if err := assertIsGenesisBlock(genesisBlockBytes); err != nil {
		return "", -1, err
	}
	// just for an extra safety make a backup of genesis block
	if err := ioutil.WriteFile(path.Join(ledgerDir, "__backupGenesisBlockBytes"), genesisBlockBytes, 0o640); err != nil {
		return "", -1, err
	}
	logger.Infof("Genesis block backed up. Genesis block info file [%s], offset [%d]", blockfilePath, endOffsetGenesisBlock)
	return blockfilePath, endOffsetGenesisBlock, nil
}

func assertIsGenesisBlock(blockBytes []byte) error {
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return err
	}
	if block.Header.Number != 0 || block.Header.GetDataHash() == nil {
		return fmt.Errorf("The supplied bytes are not of genesis block. blockNum=%d, blockHash=%x", block.Header.Number, block.Header.GetDataHash())
	}
	return nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

const (
	fileNamePreRestHt = "__preResetHeight"
)

// recordHeightIfGreaterThanPreviousRecording creates a file "__preResetHeight" in the ledger's
// directory. This file contains human readable string for the current block height. This function
// only overwrites this information if the current block height is higher than the one recorded in
// the existing file (if present). This helps in achieving fail-safe behviour of reset utility
func recordHeightIfGreaterThanPreviousRecording(ledgerDir string) error {
	logger.Infof("Preparing to record current height for ledger at [%s]", ledgerDir)
	blkfilesInfo, err := constructBlockfilesInfo(ledgerDir)
	if err != nil {
		return err
	}
	logger.Infof("Loaded current info from blockfiles %#v", blkfilesInfo)
	preResetHtFile := path.Join(ledgerDir, fileNamePreRestHt)
	exists, err := pathExists(preResetHtFile)
	logger.Infof("preResetHtFile already exists? = %t", exists)
	if err != nil {
		return err
	}
	previuoslyRecordedHt := uint64(0)
	if exists {
		htBytes, err := ioutil.ReadFile(preResetHtFile)
		if err != nil {
			return err
		}
		if previuoslyRecordedHt, err = strconv.ParseUint(string(htBytes), 10, 64); err != nil {
			return err
		}
		logger.Infof("preResetHtFile contains height = %d", previuoslyRecordedHt)
	}
	currentHt := blkfilesInfo.lastPersistedBlock + 1
	if currentHt > previuoslyRecordedHt {
		logger.Infof("Recording current height [%d]", currentHt)
		return ioutil.WriteFile(preResetHtFile,
			[]byte(strconv.FormatUint(currentHt, 10)),
			0o640,
		)
	}
	logger.Infof("Not recording current height [%d] since this is less than previously recorded height [%d]",
		currentHt, previuoslyRecordedHt)

	return nil
}

// LoadPreResetHeight 搜索指定账本的预重置高度文件，并返回一个通道名称到最后记录的块高度的映射。
// 输入参数：
//   - blockStorageDir：string，表示块存储目录
//   - ledgerIDs：[]string，表示账本的标识符列表
//
// 返回值：
//   - map[string]uint64：表示通道名称到最后记录的块高度的映射
//   - error：表示加载预重置高度时可能发生的错误
func LoadPreResetHeight(blockStorageDir string, ledgerIDs []string) (map[string]uint64, error) {
	logger.Debug("正在加载最后记录的块高度")

	// 获取预重置高度文件的映射
	preResetFilesMap, err := preResetHtFiles(blockStorageDir, ledgerIDs)
	if err != nil {
		return nil, err
	}

	// 创建一个映射，用于存储通道名称到最后记录的块高度
	m := map[string]uint64{}

	// 遍历预重置高度文件的映射
	for ledgerID, filePath := range preResetFilesMap {
		// 读取文件内容
		bytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			return nil, err
		}

		// 将文件内容解析为 uint64 类型的块高度
		previuoslyRecordedHt, err := strconv.ParseUint(string(bytes), 10, 64)
		if err != nil {
			return nil, err
		}

		// 将通道名称和块高度添加到映射中
		m[ledgerID] = previuoslyRecordedHt
	}

	// 如果映射不为空，则输出日志信息
	if len(m) > 0 {
		logger.Infof("最后记录的块高度: %v", m)
	}

	return m, nil
}

// ClearPreResetHeight deletes the files that contain the last recorded reset heights for the specified ledgers
func ClearPreResetHeight(blockStorageDir string, ledgerIDs []string) error {
	logger.Info("Clearing Pre-reset heights")
	preResetFilesMap, err := preResetHtFiles(blockStorageDir, ledgerIDs)
	if err != nil {
		return err
	}
	for _, filePath := range preResetFilesMap {
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}
	logger.Info("Cleared off Pre-reset heights")
	return nil
}

// preResetHtFiles 返回指定账本的预重置高度文件的映射。
// 输入参数：
//   - blockStorageDir：string，表示块存储目录
//   - ledgerIDs：[]string，表示账本的标识符列表
//
// 返回值：
//   - map[string]string：表示账本标识符和预重置高度文件路径的映射
//   - error：表示获取预重置高度文件路径时可能发生的错误
func preResetHtFiles(blockStorageDir string, ledgerIDs []string) (map[string]string, error) {
	// 如果账本标识符列表为空，则输出日志信息并返回空映射
	if len(ledgerIDs) == 0 {
		logger.Info("没有可用的通道")
		return nil, nil
	}

	// 创建 Conf 实例
	conf := &Conf{blockStorageDir: blockStorageDir}

	// 获取链目录路径
	chainsDir := conf.getChainsDir()

	// 检查链目录是否存在
	chainsDirExists, err := pathExists(chainsDir)
	if err != nil {
		return nil, err
	}
	if !chainsDirExists {
		logger.Infof("Dir [%s] 缺失... 正在退出", chainsDir)
		return nil, err
	}

	// 创建一个映射，用于存储账本标识符和预重置高度文件路径的映射
	m := map[string]string{}

	// 遍历账本标识符列表
	for _, ledgerID := range ledgerIDs {
		// 获取账本块目录路径
		ledgerDir := conf.getLedgerBlockDir(ledgerID)

		// 构建预重置高度文件的路径
		file := path.Join(ledgerDir, fileNamePreRestHt)

		// 检查文件是否存在
		exists, err := pathExists(file)
		if err != nil {
			return nil, err
		}

		// 如果文件不存在，则继续下一个账本
		if !exists {
			continue
		}

		// 将账本标识符和预重置高度文件
		m[ledgerID] = file
	}
	return m, nil
}
