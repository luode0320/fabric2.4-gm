/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package golang

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/chaincode/platforms/util"
	"github.com/hyperledger/fabric/internal/ccmetadata"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// Platform for chaincodes written in Go
type Platform struct{}

// Name returns the name of this platform.
func (p *Platform) Name() string {
	return pb.ChaincodeSpec_GOLANG.String()
}

// ValidatePath is used to ensure that path provided points to something that
// looks like go chainccode.
//
// NOTE: this is only used at the _client_ side by the peer CLI.
func (p *Platform) ValidatePath(rawPath string) error {
	_, err := DescribeCode(rawPath)
	if err != nil {
		return err
	}

	return nil
}

// NormalizePath is used to extract a relative module path from a module root.
// This should not impact legacy GOPATH chaincode.
//
// NOTE: this is only used at the _client_ side by the peer CLI.
func (p *Platform) NormalizePath(rawPath string) (string, error) {
	modInfo, err := moduleInfo(rawPath)
	if err != nil {
		return "", err
	}

	// not a module
	if modInfo == nil {
		return rawPath, nil
	}

	return modInfo.ImportPath, nil
}

// ValidateCodePackage examines the chaincode archive to ensure it is valid.
//
// NOTE: this code is used in some transaction validation paths but can be changed
// post 2.0.
func (p *Platform) ValidateCodePackage(code []byte) error {
	is := bytes.NewReader(code)
	gr, err := gzip.NewReader(is)
	if err != nil {
		return fmt.Errorf("failure opening codepackage gzip stream: %s", err)
	}

	re := regexp.MustCompile(`^(src|META-INF)/`)
	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// maintain check for conforming paths for validation
		if !re.MatchString(header.Name) {
			return fmt.Errorf("illegal file name in payload: %s", header.Name)
		}

		// only files and directories; no links or special files
		mode := header.FileInfo().Mode()
		if mode&^(os.ModeDir|0o777) != 0 {
			return fmt.Errorf("illegal file mode in payload: %s", header.Name)
		}
	}

	return nil
}

// Directory constant copied from tar package.
const c_ISDIR = 0o40000

// Default compression to use for production. Test packages disable compression.
var gzipCompressionLevel = gzip.DefaultCompression

// GetDeploymentPayload creates a gzip compressed tape archive that contains the
// required assets to build and run go chaincode.
//
// NOTE: this is only used at the _client_ side by the peer CLI.
func (p *Platform) GetDeploymentPayload(codepath string) ([]byte, error) {
	codeDescriptor, err := DescribeCode(codepath)
	if err != nil {
		return nil, err
	}

	fileMap, err := findSource(codeDescriptor)
	if err != nil {
		return nil, err
	}

	var dependencyPackageInfo []PackageInfo

	// 如果不处于模块模式，则获取依赖包的信息
	if !codeDescriptor.Module {
		for _, dist := range distributions() {
			pi, err := gopathDependencyPackageInfo(dist.goos, dist.goarch, codeDescriptor.Path)
			if err != nil {
				return nil, err
			}
			dependencyPackageInfo = append(dependencyPackageInfo, pi...)
		}
	}

	// 遍历依赖包信息，将其源文件添加到文件映射中
	for _, pkg := range dependencyPackageInfo {
		for _, filename := range pkg.Files() {
			sd := SourceDescriptor{
				Name: path.Join("src", pkg.ImportPath, filename),
				Path: filepath.Join(pkg.Dir, filename),
			}
			fileMap[sd.Name] = sd
		}
	}

	payload := bytes.NewBuffer(nil)
	gw, err := gzip.NewWriterLevel(payload, gzipCompressionLevel)
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(gw)

	// Create directories so they get sane ownership and permissions
	for _, dirname := range fileMap.Directories() {
		err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeDir,
			Name:     dirname + "/",
			Mode:     c_ISDIR | 0o755,
			Uid:      500,
			Gid:      500,
		})
		if err != nil {
			return nil, err
		}
	}

	for _, file := range fileMap.Sources() {
		err = util.WriteFileToPackage(file.Path, file.Name, tw)
		if err != nil {
			return nil, fmt.Errorf("Error writing %s to tar: %s", file.Name, err)
		}
	}

	err = tw.Close()
	if err == nil {
		err = gw.Close()
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create tar for chaincode")
	}

	return payload.Bytes(), nil
}

func (p *Platform) GenerateDockerfile() (string, error) {
	var buf []string
	buf = append(buf, "FROM "+util.GetDockerImageFromConfig("chaincode.golang.runtime"))
	buf = append(buf, "ADD binpackage.tar /usr/local/bin")

	return strings.Join(buf, "\n"), nil
}

const (
	staticLDFlagsOpts  = "-ldflags \"-linkmode external -extldflags '-static'\""
	dynamicLDFlagsOpts = ""
)

func getLDFlagsOpts() string {
	if viper.GetBool("chaincode.golang.dynamicLink") {
		return dynamicLDFlagsOpts
	}
	return staticLDFlagsOpts
}

var buildScript = `
set -e
if [ -f "/chaincode/input/src/go.mod" ] && [ -d "/chaincode/input/src/vendor" ]; then
    cd /chaincode/input/src
    GO111MODULE=on go build -v -mod=vendor %[1]s -o /chaincode/output/chaincode %[2]s
elif [ -f "/chaincode/input/src/go.mod" ]; then
    cd /chaincode/input/src
    GO111MODULE=on go build -v -mod=readonly %[1]s -o /chaincode/output/chaincode %[2]s
elif [ -f "/chaincode/input/src/%[2]s/go.mod" ] && [ -d "/chaincode/input/src/%[2]s/vendor" ]; then
    cd /chaincode/input/src/%[2]s
    GO111MODULE=on go build -v -mod=vendor %[1]s -o /chaincode/output/chaincode .
elif [ -f "/chaincode/input/src/%[2]s/go.mod" ]; then
    cd /chaincode/input/src/%[2]s
    GO111MODULE=on go build -v -mod=readonly %[1]s -o /chaincode/output/chaincode .
else
    GO111MODULE=off GOPATH=/chaincode/input:$GOPATH go build -v %[1]s -o /chaincode/output/chaincode %[2]s
fi
echo Done!
`

func (p *Platform) DockerBuildOptions(path string) (util.DockerBuildOptions, error) {
	env := []string{}
	for _, key := range []string{"GOPROXY", "GOSUMDB"} {
		if val, ok := os.LookupEnv(key); ok {
			env = append(env, fmt.Sprintf("%s=%s", key, val))
			continue
		}
		if key == "GOPROXY" {
			env = append(env, "GOPROXY=https://proxy.golang.org")
		}
	}
	ldFlagOpts := getLDFlagsOpts()
	return util.DockerBuildOptions{
		Cmd: fmt.Sprintf(buildScript, ldFlagOpts, path),
		Env: env,
	}, nil
}

// CodeDescriptor describes the code we're packaging.
type CodeDescriptor struct {
	Source       string // absolute path of the source to package
	MetadataRoot string // absolute path META-INF
	Path         string // import path of the package
	Module       bool   // does this represent a go module
}

func (cd CodeDescriptor) isMetadata(path string) bool {
	return strings.HasPrefix(
		filepath.Clean(path),
		filepath.Clean(cd.MetadataRoot),
	)
}

// DescribeCode 返回 GOPATH 和包信息。
func DescribeCode(path string) (*CodeDescriptor, error) {
	if path == "" {
		return nil, errors.New("无法从空链码路径收集文件")
	}

	// 使用模块根目录作为go模块的源路径
	modInfo, err := moduleInfo(path)
	if err != nil {
		return nil, err
	}

	if modInfo != nil {
		// 计算元数据相对于模块根目录的位置
		relImport, err := filepath.Rel(modInfo.ModulePath, modInfo.ImportPath)
		if err != nil {
			return nil, err
		}

		return &CodeDescriptor{
			Module:       true,
			MetadataRoot: filepath.Join(modInfo.Dir, relImport, "META-INF"),
			Path:         modInfo.ImportPath,
			Source:       modInfo.Dir,
		}, nil
	}

	return describeGopath(path)
}

func describeGopath(importPath string) (*CodeDescriptor, error) {
	output, err := exec.Command("go", "list", "-f", "{{.Dir}}", importPath).Output()
	if err != nil {
		return nil, wrapExitErr(err, "'go list' failed")
	}
	sourcePath := filepath.Clean(strings.TrimSpace(string(output)))

	return &CodeDescriptor{
		Path:         importPath,                            // 导入路径
		MetadataRoot: filepath.Join(sourcePath, "META-INF"), // 元数据根目录
		Source:       sourcePath,                            // 源代码路径
	}, nil
}

func regularFileExists(path string) (bool, error) {
	fi, err := os.Stat(path)
	switch {
	case os.IsNotExist(err):
		return false, nil
	case err != nil:
		return false, err
	default:
		return fi.Mode().IsRegular(), nil
	}
}

func moduleInfo(path string) (*ModuleInfo, error) {
	entryWD, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "无法获取工作目录")
	}

	// 目录不存在，所以不太可能是一个模块
	if err := os.Chdir(path); err != nil {
		return nil, nil
	}
	defer func() {
		if err := os.Chdir(entryWD); err != nil {
			panic(fmt.Sprintf("还原工作目录失败: %s", err))
		}
	}()

	// 使用 'go list-m-f '{{ if .Main }}{{.GoMod }}}{{ end }} 'all' 可能会尝试
	// 在使用供应商工具时生成go.Mod。为了避免这种行为
	// 我们使用 'go env GOMOD'，后跟一个存在性检查。
	cmd := exec.Command("go", "env", "GOMOD")
	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	output, err := cmd.Output()
	if err != nil {
		return nil, wrapExitErr(err, "无法确定模块根目录")
	}

	modExists, err := regularFileExists(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, err
	}
	if !modExists {
		return nil, nil
	}

	return listModuleInfo()
}

type SourceDescriptor struct {
	Name string
	Path string
}

type Sources []SourceDescriptor

func (s Sources) Len() int           { return len(s) }
func (s Sources) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Sources) Less(i, j int) bool { return s[i].Name < s[j].Name }

type SourceMap map[string]SourceDescriptor

func (s SourceMap) Sources() Sources {
	var sources Sources
	for _, src := range s {
		sources = append(sources, src)
	}

	sort.Sort(sources)
	return sources
}

func (s SourceMap) Directories() []string {
	dirMap := map[string]bool{}
	for entryName := range s {
		dir := path.Dir(entryName)
		for dir != "." && !dirMap[dir] {
			dirMap[dir] = true
			dir = path.Dir(dir)
		}
	}

	var dirs []string
	for dir := range dirMap {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	return dirs
}

func findSource(cd *CodeDescriptor) (SourceMap, error) {
	sources := SourceMap{} // 创建一个空的源文件映射

	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			// 如果是目录

			// 允许将顶级链码目录导入链码代码包
			if path == cd.Source {
				return nil
			}

			// 允许将 META-INF 元数据目录导入链码代码包 tar 文件
			// META-INF 目录包含链码元数据文件，如 statedb 索引定义
			if cd.isMetadata(path) {
				return nil
			}

			// 当不处于 vendor 模式时，包含除隐藏目录以外的所有内容
			if cd.Module && !strings.HasPrefix(info.Name(), ".") {
				return nil
			}

			// 不要将其他目录导入链码代码包
			return filepath.SkipDir
		}

		relativeRoot := cd.Source
		if cd.isMetadata(path) {
			relativeRoot = cd.MetadataRoot
		}

		name, err := filepath.Rel(relativeRoot, path)
		if err != nil {
			return errors.Wrapf(err, "failed to calculate relative path for %s", path)
		}

		switch {
		case cd.isMetadata(path):
			// 在元数据中跳过隐藏文件
			if strings.HasPrefix(info.Name(), ".") {
				return nil
			}
			name = filepath.Join("META-INF", name)
			err := validateMetadata(name, path)
			if err != nil {
				return err
			}
		case cd.Module:
			name = filepath.Join("src", name)
		default:
			// 当不处于模块模式时，跳过顶级的 go.mod 和 go.sum 文件
			if name == "go.mod" || name == "go.sum" {
				return nil
			}
			name = filepath.Join("src", cd.Path, name)
		}

		name = filepath.ToSlash(name)
		sources[name] = SourceDescriptor{Name: name, Path: path} // 将源文件添加到源文件映射中
		return nil
	}

	if err := filepath.Walk(cd.Source, walkFn); err != nil {
		return nil, errors.Wrap(err, "walk failed")
	}

	return sources, nil // 返回源文件映射
}

func validateMetadata(name, path string) error {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	// Validate metadata file for inclusion in tar
	// Validation is based on the passed filename with path
	err = ccmetadata.ValidateMetadataFile(filepath.ToSlash(name), contents)
	if err != nil {
		return err
	}

	return nil
}

// dist holds go "distribution" information. The full list of distributions can
// be obtained with `go tool dist list.
type dist struct{ goos, goarch string }

// distributions returns the list of OS and ARCH combinations that we calcluate
// deps for.
func distributions() []dist {
	// pre-populate linux architecutures
	dists := map[dist]bool{
		{goos: "linux", goarch: "amd64"}: true,
	}

	// add local OS and ARCH, linux and current ARCH
	dists[dist{goos: runtime.GOOS, goarch: runtime.GOARCH}] = true
	dists[dist{goos: "linux", goarch: runtime.GOARCH}] = true

	var list []dist
	for d := range dists {
		list = append(list, d)
	}

	return list
}
