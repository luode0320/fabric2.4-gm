/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platforms

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metadata"
	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	"github.com/hyperledger/fabric/core/chaincode/platforms/java"
	"github.com/hyperledger/fabric/core/chaincode/platforms/node"
	"github.com/hyperledger/fabric/core/chaincode/platforms/util"
	"github.com/pkg/errors"
)

// SupportedPlatforms 是Fabric支持的平台的规范列表
var SupportedPlatforms = []Platform{
	&java.Platform{},
	&golang.Platform{},
	&node.Platform{},
}

// Platform 用于验证规范和编写给定平台的包的接口
type Platform interface {
	Name() string
	GenerateDockerfile() (string, error)
	DockerBuildOptions(path string) (util.DockerBuildOptions, error)
}

type PackageWriter interface {
	Write(name string, payload []byte, tw *tar.Writer) error
}

type PackageWriterWrapper func(name string, payload []byte, tw *tar.Writer) error

func (pw PackageWriterWrapper) Write(name string, payload []byte, tw *tar.Writer) error {
	return pw(name, payload, tw)
}

type BuildFunc func(util.DockerBuildOptions, *docker.Client) error

type Registry struct {
	Platforms     map[string]Platform
	PackageWriter PackageWriter
	DockerBuild   BuildFunc
}

var logger = flogging.MustGetLogger("chaincode.platform")

// NewRegistry 函数创建一个新的注册表实例。
//
// 参数：
//   - platformTypes: Platform类型的可变参数，表示要注册的平台类型
//
// 返回值：
//   - *Registry: Registry结构体指针，表示注册表实例
func NewRegistry(platformTypes ...Platform) *Registry {
	platforms := make(map[string]Platform)
	for _, platform := range platformTypes {
		// 获取语言名称
		if _, ok := platforms[platform.Name()]; ok {
			logger.Panicf("指定了同名的多个平台: %s", platform.Name())
		}
		platforms[platform.Name()] = platform
	}
	return &Registry{
		Platforms:     platforms,                                 // 支持的语言java,go,node
		PackageWriter: PackageWriterWrapper(writeBytesToPackage), // 将字节写入tar包中的指定文件
		DockerBuild:   util.DockerBuild,                          // docker容器中 “传递” 构建链码
	}
}

func (r *Registry) GenerateDockerfile(ccType string) (string, error) {
	platform, ok := r.Platforms[ccType]
	if !ok {
		return "", fmt.Errorf("Unknown chaincodeType: %s", ccType)
	}

	var buf []string

	// ----------------------------------------------------------------------------------------------------
	// Let the platform define the base Dockerfile
	// ----------------------------------------------------------------------------------------------------
	base, err := platform.GenerateDockerfile()
	if err != nil {
		return "", fmt.Errorf("Failed to generate platform-specific Dockerfile: %s", err)
	}
	buf = append(buf, base)
	buf = append(buf, fmt.Sprintf(`LABEL %s.chaincode.type="%s" \`, metadata.BaseDockerLabel, ccType))
	buf = append(buf, fmt.Sprintf(`      %s.version="%s"`, metadata.BaseDockerLabel, metadata.Version))
	// ----------------------------------------------------------------------------------------------------
	// Then augment it with any general options
	// ----------------------------------------------------------------------------------------------------
	// append version so chaincode build version can be compared against peer build version
	buf = append(buf, fmt.Sprintf("ENV CORE_CHAINCODE_BUILDLEVEL=%s", metadata.Version))

	// ----------------------------------------------------------------------------------------------------
	// Finalize it
	// ----------------------------------------------------------------------------------------------------
	contents := strings.Join(buf, "\n")
	logger.Debugf("\n%s", contents)

	return contents, nil
}

func (r *Registry) StreamDockerBuild(ccType, path string, codePackage io.Reader, inputFiles map[string][]byte, tw *tar.Writer, client *docker.Client) error {
	var err error

	// ----------------------------------------------------------------------------------------------------
	// Determine our platform driver from the spec
	// ----------------------------------------------------------------------------------------------------
	platform, ok := r.Platforms[ccType]
	if !ok {
		return fmt.Errorf("could not find platform of type: %s", ccType)
	}

	// ----------------------------------------------------------------------------------------------------
	// First stream out our static inputFiles
	// ----------------------------------------------------------------------------------------------------
	for name, data := range inputFiles {
		err = r.PackageWriter.Write(name, data, tw)
		if err != nil {
			return fmt.Errorf(`Failed to inject "%s": %s`, name, err)
		}
	}

	buildOptions, err := platform.DockerBuildOptions(path)
	if err != nil {
		return errors.Wrap(err, "platform failed to create docker build options")
	}

	output := &bytes.Buffer{}
	buildOptions.InputStream = codePackage
	buildOptions.OutputStream = output

	err = r.DockerBuild(buildOptions, client)
	if err != nil {
		return errors.Wrap(err, "docker build failed")
	}

	return writeBytesToPackage("binpackage.tar", output.Bytes(), tw)
}

// writeBytesToPackage 函数将字节写入tar包中的指定文件。
//
// 参数：
//   - name: string，要写入的文件名
//   - payload: []byte，要写入的字节数据
//   - tw: *tar.Writer，tar.Writer实例，用于写入tar包
//
// 返回值：
//   - error: 如果写入过程中出现错误，则返回相应的错误信息
func writeBytesToPackage(name string, payload []byte, tw *tar.Writer) error {
	err := tw.WriteHeader(&tar.Header{
		Name: name,
		Size: int64(len(payload)),
		Mode: 0o100644,
	})
	if err != nil {
		return err
	}

	_, err = tw.Write(payload)
	if err != nil {
		return err
	}

	return nil
}

func (r *Registry) GenerateDockerBuild(ccType, path string, codePackage io.Reader, client *docker.Client) (io.Reader, error) {
	inputFiles := make(map[string][]byte)

	// ----------------------------------------------------------------------------------------------------
	// Generate the Dockerfile specific to our context
	// ----------------------------------------------------------------------------------------------------
	dockerFile, err := r.GenerateDockerfile(ccType)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate a Dockerfile: %s", err)
	}

	inputFiles["Dockerfile"] = []byte(dockerFile)

	// ----------------------------------------------------------------------------------------------------
	// Finally, launch an asynchronous process to stream all of the above into a docker build context
	// ----------------------------------------------------------------------------------------------------
	input, output := io.Pipe()

	go func() {
		gw := gzip.NewWriter(output)
		tw := tar.NewWriter(gw)
		err := r.StreamDockerBuild(ccType, path, codePackage, inputFiles, tw, client)
		if err != nil {
			logger.Error(err)
		}

		tw.Close()
		gw.Close()
		output.CloseWithError(err)
	}()

	return input, nil
}
