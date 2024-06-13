/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package golang

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const listTimeout = 3 * time.Minute

// PackageInfo is the subset of data from `go list -deps -json` that's
// necessary to calculate chaincode package dependencies.
type PackageInfo struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	Goroot         bool
	CFiles         []string
	CgoFiles       []string
	HFiles         []string
	SFiles         []string
	IgnoredGoFiles []string
	Incomplete     bool
}

func (p PackageInfo) Files() []string {
	var files []string
	files = append(files, p.GoFiles...)
	files = append(files, p.CFiles...)
	files = append(files, p.CgoFiles...)
	files = append(files, p.HFiles...)
	files = append(files, p.SFiles...)
	files = append(files, p.IgnoredGoFiles...)
	return files
}

// gopathDependencyPackageInfo 提取的依赖关系信息指定的包。
func gopathDependencyPackageInfo(goos, goarch, pkg string) ([]PackageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	fmt.Println("提取的依赖关系信息指定的包", pkg)

	// 创建一个执行 "go list -deps" 命令的命令对象
	cmd := exec.CommandContext(ctx, "go", "list", "-deps", "-json", pkg)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)

	fmt.Println("env: ", "GOOS="+goos, "GOARCH="+goarch)

	// 创建一个管道来读取命令的标准输出
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, wrapExitErr(err, "'go list -deps' 失败")
	}
	decoder := json.NewDecoder(stdout)

	// 启动命令
	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	var list []PackageInfo
	for {
		var packageInfo PackageInfo
		err := decoder.Decode(&packageInfo)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if packageInfo.Incomplete {
			return nil, fmt.Errorf("计算依赖项失败: 包不完整: %s", packageInfo.ImportPath)
		}
		if packageInfo.Goroot {
			continue
		}

		list = append(list, packageInfo)
	}

	// 等待命令执行完成
	err = cmd.Wait()
	if err != nil {
		return nil, errors.Wrapf(err, "列出软件包的依赖 %s 失败", pkg)
	}

	return list, nil
}

func wrapExitErr(err error, message string) error {
	if ee, ok := err.(*exec.ExitError); ok {
		return errors.Wrapf(err, message+" with: %s", strings.TrimRight(string(ee.Stderr), "\n\r\t"))
	}
	return errors.Wrap(err, message)
}

type ModuleInfo struct {
	Dir        string
	GoMod      string
	ImportPath string
	ModulePath string
}

// listModuleInfo 提取curent工作目录的模块信息。
func listModuleInfo(extraEnv ...string) (*ModuleInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "list", "-json", ".")
	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	cmd.Env = append(cmd.Env, extraEnv...)

	output, err := cmd.Output()
	if err != nil {
		return nil, wrapExitErr(err, "'go list' failed")
	}

	var moduleData struct {
		ImportPath string
		Module     struct {
			Dir   string
			Path  string
			GoMod string
		}
	}

	if err := json.Unmarshal(output, &moduleData); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal output from 'go list'")
	}

	return &ModuleInfo{
		Dir:        moduleData.Module.Dir,
		GoMod:      moduleData.Module.GoMod,
		ImportPath: moduleData.ImportPath,
		ModulePath: moduleData.Module.Path,
	}, nil
}
