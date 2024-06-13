/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fileutil

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// CreateAndSyncFileAtomically writes the content to the tmpFile, fsyncs the tmpFile,
// renames the tmpFile to the finalFile. In other words, in the event of a crash, either
// the final file will not be visible or it will have the full contents.
// The tmpFile should not be existing. The finalFile, if exists, will be overwritten (default rename behavior)
func CreateAndSyncFileAtomically(dir, tmpFile, finalFile string, content []byte, perm os.FileMode) error {
	tempFilePath := filepath.Join(dir, tmpFile)
	finalFilePath := filepath.Join(dir, finalFile)
	if err := CreateAndSyncFile(tempFilePath, content, perm); err != nil {
		return err
	}
	if err := os.Rename(tempFilePath, finalFilePath); err != nil {
		return err
	}
	return nil
}

// CreateAndSyncFile creates a file, writes the content and syncs the file
func CreateAndSyncFile(filePath string, content []byte, perm os.FileMode) error {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return errors.Wrapf(err, "error while creating file:%s", filePath)
	}
	_, err = file.Write(content)
	if err != nil {
		file.Close()
		return errors.Wrapf(err, "error while writing to file:%s", filePath)
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return errors.Wrapf(err, "error while synching the file:%s", filePath)
	}
	if err := file.Close(); err != nil {
		return errors.Wrapf(err, "error while closing the file:%s", filePath)
	}
	return nil
}

// SyncParentDir 同步父目录
func SyncParentDir(path string) error {
	return SyncDir(filepath.Dir(path))
}

// CreateDirIfMissing 确保目录存在，并创建目录返回目录是否为空。
// 方法接收者：无（函数）
// 输入参数：
//   - dirPath：要创建的目录路径。
//
// 返回值：
//   - bool：目录是否为空。
//   - error：如果创建目录或同步父目录时出错，则返回错误；否则返回nil。
func CreateDirIfMissing(dirPath string) (bool, error) {
	// 使用os.MkdirAll函数创建目录，权限为0o755（即rwxr-xr-x）
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return false, errors.Wrapf(err, "创建目录时出错: %s", dirPath)
	}
	// 同步父目录
	if err := SyncParentDir(dirPath); err != nil {
		return false, err
	}
	// 检查目录是否为空
	return DirEmpty(dirPath)
}

// DirEmpty 如果dirPath目录为空，则返回true。
// 方法接收者：无（函数）
// 输入参数：
//   - dirPath：要检查的目录路径。
//
// 返回值：
//   - bool：如果目录为空，则为true。
//   - error：如果打开目录或检查目录是否为空时出错，则返回错误；否则返回nil。
func DirEmpty(dirPath string) (bool, error) {
	// 打开目录
	f, err := os.Open(dirPath)
	if err != nil {
		return false, errors.Wrapf(err, "打开目录时出错 [%s]", dirPath)
	}
	defer f.Close()

	// 读取目录的一个条目
	_, err = f.Readdir(1)
	if err == io.EOF {
		// 如果读取到了EOF，则目录为空
		return true, nil
	}
	// 如果读取目录条目时出错，则返回错误
	err = errors.Wrapf(err, "检查目录 [%s] 是否为空时出错", dirPath)
	return false, err
}

// FileExists checks whether the given file exists.
// If the file exists, this method also returns the size of the file.
func FileExists(path string) (bool, int64, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, errors.Wrapf(err, "error checking if file [%s] exists", path)
	}
	if info.IsDir() {
		return false, 0, errors.Errorf("the supplied path [%s] is a dir", path)
	}
	return true, info.Size(), nil
}

// DirExists returns true if the dir already exists
func DirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrapf(err, "error checking if file [%s] exists", path)
	}
	if !info.IsDir() {
		return false, errors.Errorf("the supplied path [%s] exists but is not a dir", path)
	}
	return true, nil
}

// ListSubdirs returns the subdirectories
func ListSubdirs(dirPath string) ([]string, error) {
	subdirs := []string{}
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading dir %s", dirPath)
	}
	for _, f := range files {
		if f.IsDir() {
			subdirs = append(subdirs, f.Name())
		}
	}
	return subdirs, nil
}

// RemoveContents removes all the files and subdirs under the specified directory.
// It returns nil if the specified directory does not exist.
func RemoveContents(dir string) error {
	contents, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "error reading directory %s", dir)
	}

	for _, c := range contents {
		if err = os.RemoveAll(filepath.Join(dir, c.Name())); err != nil {
			return errors.Wrapf(err, "error removing %s under directory %s", c.Name(), dir)
		}
	}
	return SyncDir(dir)
}
