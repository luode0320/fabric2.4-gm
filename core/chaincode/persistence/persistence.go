/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package persistence

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"

	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("chaincode.persistence")

// IOReadWriter defines the interface needed for reading, writing, removing, and
// checking for existence of a specified file
type IOReadWriter interface {
	ReadDir(string) ([]os.FileInfo, error)
	ReadFile(string) ([]byte, error)
	Remove(name string) error
	WriteFile(string, string, []byte) error
	MakeDir(string, os.FileMode) error
	Exists(path string) (bool, error)
}

// FilesystemIO 是IOWriter接口的生产实现
type FilesystemIO struct{}

// WriteFile writes a file to the filesystem; it does so atomically
// by first writing to a temp file and then renaming the file so that
// if the operation crashes midway we're not stuck with a bad package
func (f *FilesystemIO) WriteFile(path, name string, data []byte) error {
	if path == "" {
		return errors.New("empty path not allowed")
	}
	tmpFile, err := ioutil.TempFile(path, ".ccpackage.")
	if err != nil {
		return errors.Wrapf(err, "error creating temp file in directory '%s'", path)
	}
	defer os.Remove(tmpFile.Name())

	if n, err := tmpFile.Write(data); err != nil || n != len(data) {
		if err == nil {
			err = errors.Errorf(
				"failed to write the entire content of the file, expected %d, wrote %d",
				len(data), n)
		}
		return errors.Wrapf(err, "error writing to temp file '%s'", tmpFile.Name())
	}

	if err := tmpFile.Close(); err != nil {
		return errors.Wrapf(err, "error closing temp file '%s'", tmpFile.Name())
	}

	if err := os.Rename(tmpFile.Name(), filepath.Join(path, name)); err != nil {
		return errors.Wrapf(err, "error renaming temp file '%s'", tmpFile.Name())
	}

	return nil
}

// Remove removes a file from the filesystem - used for rolling back an in-flight
// Save operation upon a failure
func (f *FilesystemIO) Remove(name string) error {
	return os.Remove(name)
}

// ReadFile reads a file from the filesystem
func (f *FilesystemIO) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

// ReadDir reads a directory from the filesystem
func (f *FilesystemIO) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

// MakeDir makes a directory on the filesystem (and any
// necessary parent directories).
func (f *FilesystemIO) MakeDir(dirname string, mode os.FileMode) error {
	return os.MkdirAll(dirname, mode)
}

// Exists checks whether a file exists
func (*FilesystemIO) Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "could not determine whether file '%s' exists", path)
}

// Store 保存链码安装包所需的信息
type Store struct {
	Path       string
	ReadWriter IOReadWriter
}

// NewStore 函数使用提供的文件系统路径创建一个新的链码持久化存储。
//
// 参数：
//   - path: 字符串类型，表示文件系统路径。
//
// 返回值：
//   - *Store: 表示链码持久化存储的*Store实例。
func NewStore(path string) *Store {
	store := &Store{
		Path:       path,            // 设置存储路径
		ReadWriter: &FilesystemIO{}, // 使用FilesystemIO作为读写器
	}

	// 初始化存储
	store.Initialize()
	return store
}

// Initialize 方法检查 _lifecycle 链码目录是否存在，如果不存在则创建它。
func (s *Store) Initialize() {
	var (
		exists bool
		err    error
	)

	// 检查目录是否存在
	if exists, err = s.ReadWriter.Exists(s.Path); exists {
		return
	}
	if err != nil {
		panic(fmt.Sprintf("链码存储目录初始化失败: %s", err))
	}
	if err = s.ReadWriter.MakeDir(s.Path, 0o750); err != nil {
		panic(fmt.Sprintf("无法创建 _lifecycle 链码安装路径: %s", err))
	}
}

// Save persists chaincode install package bytes. It returns
// the hash of the chaincode install package
func (s *Store) Save(label string, ccInstallPkg []byte) (string, error) {
	packageID := PackageID(label, ccInstallPkg)

	ccInstallPkgFileName := CCFileName(packageID)
	ccInstallPkgFilePath := filepath.Join(s.Path, ccInstallPkgFileName)

	if exists, _ := s.ReadWriter.Exists(ccInstallPkgFilePath); exists {
		// chaincode install package was already installed
		return packageID, nil
	}

	if err := s.ReadWriter.WriteFile(s.Path, ccInstallPkgFileName, ccInstallPkg); err != nil {
		err = errors.Wrapf(err, "error writing chaincode install package to %s", ccInstallPkgFilePath)
		logger.Error(err.Error())
		return "", err
	}

	return packageID, nil
}

// Load loads a persisted chaincode install package bytes with
// the given packageID.
func (s *Store) Load(packageID string) ([]byte, error) {
	ccInstallPkgPath := filepath.Join(s.Path, CCFileName(packageID))

	exists, err := s.ReadWriter.Exists(ccInstallPkgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "无法确定链码安装包 %s 是否存在", packageID)
	}
	if !exists {
		return nil, &CodePackageNotFoundErr{
			PackageID: packageID,
		}
	}

	ccInstallPkg, err := s.ReadWriter.ReadFile(ccInstallPkgPath)
	if err != nil {
		err = errors.Wrapf(err, "在 %s 处读取链码安装包时出错", ccInstallPkgPath)
		return nil, err
	}

	return ccInstallPkg, nil
}

// Delete deletes a persisted chaincode.  Note, there is no locking,
// so this should only be performed if the chaincode has already
// been marked built.
func (s *Store) Delete(packageID string) error {
	ccInstallPkgPath := filepath.Join(s.Path, CCFileName(packageID))
	return s.ReadWriter.Remove(ccInstallPkgPath)
}

// CodePackageNotFoundErr is the error returned when a code package cannot
// be found in the persistence store
type CodePackageNotFoundErr struct {
	PackageID string
}

func (e CodePackageNotFoundErr) Error() string {
	return fmt.Sprintf("chaincode install package '%s' not found", e.PackageID)
}

// ListInstalledChaincodes 返回一个数组，其中包含有关持久性存储中安装的链码的信息
func (s *Store) ListInstalledChaincodes() ([]chaincode.InstalledChaincode, error) {
	// 读取链码目录中的文件列表
	files, err := s.ReadWriter.ReadDir(s.Path)
	if err != nil {
		return nil, errors.Wrapf(err, "读取 chaincode 链码目录时出错 %s", s.Path)
	}

	// 存储安装的链码信息的数组
	installedChaincodes := []chaincode.InstalledChaincode{}
	for _, file := range files {
		// 检查文件名是否符合链码安装的命名规则, 从文件名中提取已安装链码的信息
		if instCC, isInstCC := installedChaincodeFromFilename(file.Name()); isInstCC {
			// 将符合条件的链码信息添加到数组中
			installedChaincodes = append(installedChaincodes, instCC)
		}
	}
	return installedChaincodes, nil
}

// GetChaincodeInstallPath returns the path where chaincodes
// are installed
func (s *Store) GetChaincodeInstallPath() string {
	return s.Path
}

// PackageID returns the package ID with the label and hash of the chaincode install package
func PackageID(label string, ccInstallPkg []byte) string {
	hash := util.ComputeSHA256(ccInstallPkg)
	return packageID(label, hash)
}

func packageID(label string, hash []byte) string {
	return fmt.Sprintf("%s:%x", label, hash)
}

func CCFileName(packageID string) string {
	return strings.Replace(packageID, ":", ".", 1) + ".tar.gz"
}

var packageFileMatcher = regexp.MustCompile("^(.+)[.]([0-9a-f]{64})[.]tar[.]gz$")

// installedChaincodeFromFilename 从文件名中提取已安装链码的信息
// 输入参数：
//   - fileName：文件名，用于提取已安装链码的信息。
//
// 返回值：
//   - chaincode.InstalledChaincode：已安装链码的信息。
//   - bool：如果文件名符合链码安装的命名规则，则返回true；否则返回false。
func installedChaincodeFromFilename(fileName string) (chaincode.InstalledChaincode, bool) {
	// 使用正则表达式匹配文件名
	matches := packageFileMatcher.FindStringSubmatch(fileName)
	if len(matches) == 3 {
		// 提取匹配结果中的标签和哈希值
		label := matches[1]
		hash, _ := hex.DecodeString(matches[2])
		packageID := packageID(label, hash)

		// 创建并返回已安装链码的信息
		return chaincode.InstalledChaincode{
			Label:     label,
			Hash:      hash,
			PackageID: packageID,
		}, true
	}

	// 如果文件名不符合链码安装的命名规则，则返回false
	return chaincode.InstalledChaincode{}, false
}
