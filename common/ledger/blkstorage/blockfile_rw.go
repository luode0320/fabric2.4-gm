/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"

	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
)

// blockfileWriter 是一个用于写入区块文件的结构体。
type blockfileWriter struct {
	filePath string   // 文件路径
	file     *os.File // 文件句柄
}

// newBlockfileWriter 创建一个新的块文件写入器。
// 方法接收者：无（函数）
// 输入参数：
//   - filePath：要写入的块文件路径。
//
// 返回值：
//   - *blockfileWriter：新创建的块文件写入器。
//   - error：如果打开块文件时出错，则返回错误；否则返回nil。
func newBlockfileWriter(filePath string) (*blockfileWriter, error) {
	// 创建一个blockfileWriter实例，并设置filePath属性, 一个用于写入区块文件的结构体
	writer := &blockfileWriter{filePath: filePath}
	// 打开块文件以供写入
	return writer, writer.open()
}

// truncateFile 将文件截断为目标大小。
// 方法接收者：w（blockfileWriter类型的指针）
// 输入参数：
//   - targetSize：目标大小，以字节为单位。
//
// 返回值：
//   - error：如果截断文件时出错，则返回错误；否则返回nil。
func (w *blockfileWriter) truncateFile(targetSize int) error {
	// 获取文件的状态信息
	fileStat, err := w.file.Stat()
	if err != nil {
		return errors.Wrapf(err, "将文件 [%s] 截断为大小 [%d] 时出错", w.filePath, targetSize)
	}

	// 如果文件大小大于目标大小，则进行截断操作
	if fileStat.Size() > int64(targetSize) {
		w.file.Truncate(int64(targetSize))
	}
	return nil
}

func (w *blockfileWriter) append(b []byte, sync bool) error {
	_, err := w.file.Write(b)
	if err != nil {
		return err
	}
	if sync {
		return w.file.Sync()
	}
	return nil
}

// open 打开块文件以供写入。
// 方法接收者：w（blockfileWriter类型的指针）
// 输入参数：无
// 返回值：
//   - error：如果打开块文件时出错，则返回错误；否则返回nil。
func (w *blockfileWriter) open() error {
	// 使用 os.OpenFile 函数以读写、追加、创建模式打开块文件
	file, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o660)
	if err != nil {
		return errors.Wrapf(err, "打开文件的块文件写入器时出错 %s", w.filePath)
	}
	// 同步父目录
	if err := fileutil.SyncParentDir(w.filePath); err != nil {
		return err
	}

	// 将打开的文件设置为 blockfileWriter 的file属性
	w.file = file
	return nil
}

func (w *blockfileWriter) close() error {
	return errors.WithStack(w.file.Close())
}

// //  READER ////
type blockfileReader struct {
	file *os.File
}

func newBlockfileReader(filePath string) (*blockfileReader, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0o600)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening block file reader for file %s", filePath)
	}
	reader := &blockfileReader{file}
	return reader, nil
}

func (r *blockfileReader) read(offset int, length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := r.file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, errors.Wrapf(err, "error reading block file for offset %d and length %d", offset, length)
	}
	return b, nil
}

func (r *blockfileReader) close() error {
	return errors.WithStack(r.file.Close())
}
