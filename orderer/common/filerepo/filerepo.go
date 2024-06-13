/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package filerepo

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
)

const (
	repoFilePermPrivateRW      os.FileMode = 0o600
	defaultTransientFileMarker             = "~"
)

// Repo manages filesystem operations for saving files marked by the fileSuffix
// in order to support crash fault tolerance for components that need it by maintaining
// a file repo structure storing intermediate state.
type Repo struct {
	mu                  sync.Mutex
	fileRepoDir         string
	fileSuffix          string
	transientFileMarker string
}

// New initializes a new file repo at repoParentDir/fileSuffix.
// All file system operations on the returned file repo are thread safe.
func New(repoParentDir, fileSuffix string) (*Repo, error) {
	if err := validateFileSuffix(fileSuffix); err != nil {
		return nil, err
	}

	fileRepoDir := filepath.Join(repoParentDir, fileSuffix)

	if _, err := fileutil.CreateDirIfMissing(fileRepoDir); err != nil {
		return nil, err
	}

	if err := fileutil.SyncDir(repoParentDir); err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(fileRepoDir)
	if err != nil {
		return nil, err
	}

	// Remove existing transient files in the repo
	transientFilePattern := "*" + fileSuffix + defaultTransientFileMarker
	for _, f := range files {
		isTransientFile, err := filepath.Match(transientFilePattern, f.Name())
		if err != nil {
			return nil, err
		}
		if isTransientFile {
			if err := os.Remove(filepath.Join(fileRepoDir, f.Name())); err != nil {
				return nil, errors.Wrapf(err, "error cleaning up transient files")
			}
		}
	}

	if err := fileutil.SyncDir(fileRepoDir); err != nil {
		return nil, err
	}

	return &Repo{
		transientFileMarker: defaultTransientFileMarker,
		fileSuffix:          fileSuffix,
		fileRepoDir:         fileRepoDir,
	}, nil
}

// Save atomically persists the content to suffix/baseName+suffix file by first writing it
// to a tmp file marked by the transientFileMarker and then moves the file to the final
// destination indicated by the FileSuffix.
func (r *Repo) Save(baseName string, content []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fileName := r.baseToFileName(baseName)
	dest := r.baseToFilePath(baseName)

	if _, err := os.Stat(dest); err == nil {
		return os.ErrExist
	}

	tmpFileName := fileName + r.transientFileMarker
	if err := fileutil.CreateAndSyncFileAtomically(r.fileRepoDir, tmpFileName, fileName, content, repoFilePermPrivateRW); err != nil {
		return err
	}

	return fileutil.SyncDir(r.fileRepoDir)
}

// Remove removes the file associated with baseName from the file system.
func (r *Repo) Remove(baseName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	filePath := r.baseToFilePath(baseName)

	if err := os.RemoveAll(filePath); err != nil {
		return err
	}

	return fileutil.SyncDir(r.fileRepoDir)
}

// Read reads the file in the fileRepo associated with baseName's contents.
func (r *Repo) Read(baseName string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	filePath := r.baseToFilePath(baseName)
	return ioutil.ReadFile(filePath)
}

// List 解析目录并生成一个文件名列表，根据后缀进行过滤。
// 方法接收者：r（Repo类型的指针）
// 输入参数：无
// 返回值：
//   - []string：文件名列表，根据后缀进行过滤。
//   - error：如果在读取目录或匹配后缀时出错，则返回错误。
func (r *Repo) List() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var repoFiles []string

	// 读取目录中的文件列表
	files, err := ioutil.ReadDir(r.fileRepoDir)
	if err != nil {
		return nil, err
	}

	// 遍历文件列表，根据后缀进行过滤
	for _, f := range files {
		// 使用filepath.Match函数匹配文件名和后缀
		isFileSuffix, err := filepath.Match("*"+r.fileSuffix, f.Name())
		if err != nil {
			return nil, err
		}
		// 如果文件名匹配后缀，则将其添加到文件名列表中
		if isFileSuffix {
			repoFiles = append(repoFiles, f.Name())
		}
	}

	return repoFiles, nil
}

// FileToBaseName strips the suffix from the file name to get the associated channel name.
func (r *Repo) FileToBaseName(fileName string) string {
	baseFile := filepath.Base(fileName)

	return strings.TrimSuffix(baseFile, "."+r.fileSuffix)
}

func (r *Repo) baseToFilePath(baseName string) string {
	return filepath.Join(r.fileRepoDir, r.baseToFileName(baseName))
}

func (r *Repo) baseToFileName(baseName string) string {
	return baseName + "." + r.fileSuffix
}

func validateFileSuffix(fileSuffix string) error {
	if len(fileSuffix) == 0 {
		return errors.New("fileSuffix illegal, cannot be empty")
	}

	if strings.Contains(fileSuffix, string(os.PathSeparator)) {
		return errors.Errorf("fileSuffix [%s] illegal, cannot contain os path separator", fileSuffix)
	}

	return nil
}
