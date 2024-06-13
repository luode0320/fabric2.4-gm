/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metadata"
	"github.com/spf13/viper"
)

var logger = flogging.MustGetLogger("chaincode.platform.util")

type DockerBuildOptions struct {
	Image        string    // (可选) 要使用的构建器映像或 “chaincode.builder”
	Cmd          string    // 在容器内执行的命令。
	Env          []string  // 环境变量
	InputStream  io.Reader // 将被扩展为/chaincode/input的文件的tarball。
	OutputStream io.Writer // 将从/chaincode/output收集的文件的tarball
}

func (dbo DockerBuildOptions) String() string {
	return fmt.Sprintf("Image=%s Env=%s Cmd=%s)", dbo.Image, dbo.Env, dbo.Cmd)
}

// -------------------------------------------------------------------------------------------
// DockerBuild
// -------------------------------------------------------------------------------------------
// 此函数允许在docker容器中 “传递” 构建链码
// 替代使用标准的 “docker build” Dockerfile机制。普通的docker由于生成的图像是构建时环境和运行时环境。
// 这个超集在几个上可能是有问题的前端，例如膨胀的图像大小，以及与不需要的应用程序等。
//
// 因此，此机制创建了一个由临时docker组成的管道接受源代码作为输入的容器，运行一些函数 (例如 “go build”)，
// 并且输出结果。目的是将此输出作为一个精简的容器，通过将输出安装到下游的docker-build基于适当的最小图像。
//
// 输入参数相当简单:
// - Image :(可选) 要使用的构建器映像或 “chaincode.builder”
// - Cmd: 在容器内执行的命令。
// - InputStream: 将被扩展为/chaincode/input的文件的tarball。
// - OutputStream: 将从/chaincode/output收集的文件的tarball
// 成功执行Cmd后。
//
// -------------------------------------------------------------------------------------------
func DockerBuild(opts DockerBuildOptions, client *docker.Client) error {
	if opts.Image == "" {
		// 配置中替换变量并返回Docker镜像名称
		opts.Image = GetDockerImageFromConfig("chaincode.builder")
		if opts.Image == "" {
			return fmt.Errorf("没有提供 docker image 并且配置 \"chaincode.builder\" 不存在")
		}
	}

	logger.Debugf("尝试使用选项进行构建: %s", opts)

	//-----------------------------------------------------------------------------------
	// 确保映像存在于本地，或从注册表中提取它。按其名称或ID拉取
	//-----------------------------------------------------------------------------------
	_, err := client.InspectImage(opts.Image)
	if err != nil {
		logger.Debugf("尝试拉取镜像, Image %s 本地不存在", opts.Image)

		err = client.PullImage(docker.PullImageOptions{Repository: opts.Image}, docker.AuthConfiguration{})
		if err != nil {
			return fmt.Errorf("拉取镜像失败 %s: %s", opts.Image, err)
		}
	}

	//-----------------------------------------------------------------------------------
	// 创建一个临时容器，配备我们的 Image/Cmd。创建一个新容器，返回容器实例
	//-----------------------------------------------------------------------------------
	container, err := client.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			Image:        opts.Image,
			Cmd:          []string{"/bin/sh", "-c", opts.Cmd},
			Env:          opts.Env,
			AttachStdout: true,
			AttachStderr: true,
		},
	})
	if err != nil {
		return fmt.Errorf("创建docker容器时出错: %s", err)
	}
	defer client.RemoveContainer(docker.RemoveContainerOptions{ID: container.ID})

	//-----------------------------------------------------------------------------------
	// 上传我们的输入流
	//-----------------------------------------------------------------------------------
	err = client.UploadToContainer(container.ID, docker.UploadToContainerOptions{
		Path:        "/chaincode/input",
		InputStream: opts.InputStream,
	})
	if err != nil {
		return fmt.Errorf("上传到容器时出错: %s", err)
	}

	//-----------------------------------------------------------------------------------
	// 附加stdout缓冲区以捕获可能的编译错误
	//-----------------------------------------------------------------------------------
	stdout := bytes.NewBuffer(nil)
	cw, err := client.AttachToContainerNonBlocking(docker.AttachToContainerOptions{
		Container:    container.ID,
		OutputStream: stdout,
		ErrorStream:  stdout,
		Logs:         true,
		Stdout:       true,
		Stderr:       true,
		Stream:       true,
	})
	if err != nil {
		return fmt.Errorf("附加到容器时出错: %s", err)
	}

	//-----------------------------------------------------------------------------------
	// 启动实际构建，实现容器创建时指定的Env/Cmd。启动一个容器
	//-----------------------------------------------------------------------------------
	err = client.StartContainer(container.ID, nil)
	if err != nil {
		cw.Close()
		return fmt.Errorf("docker容器启动出错: %s \"%s\"", err, stdout.String())
	}

	//-----------------------------------------------------------------------------------
	// 等待构建完成并收集返回值。阻塞，直到给定的容器停止
	//-----------------------------------------------------------------------------------
	retval, err := client.WaitContainer(container.ID)
	if err != nil {
		cw.Close()
		return fmt.Errorf("等待docker容器完成时出错: %s", err)
	}

	// 在访问stdout之前，请等待流复制完成。
	cw.Close()
	if err := cw.Wait(); err != nil {
		logger.Errorf("附加等待失败: %s", err)
	}

	if retval > 0 {
		logger.Errorf("Docker构建使用选项opts失败: %s", opts)
		return fmt.Errorf("启动容器错误: %d \"%s\"", retval, stdout.String())
	}

	logger.Debugf("构建输出 is %s", stdout.String())

	//-----------------------------------------------------------------------------------
	// 最后，下载结果
	//-----------------------------------------------------------------------------------
	err = client.DownloadFromContainer(container.ID, docker.DownloadFromContainerOptions{
		Path:         "/chaincode/output/.",
		OutputStream: opts.OutputStream,
	})
	if err != nil {
		return fmt.Errorf("下载输出时出错: %s", err)
	}

	return nil
}

// GetDockerImageFromConfig 函数从配置中替换变量并返回Docker镜像名称。
//
// 参数：
//   - path: string，配置路径，表示要获取的配置值的路径
//
// 返回值：
//   - string: Docker镜像名称
func GetDockerImageFromConfig(path string) string {
	r := strings.NewReplacer(
		"$(ARCH)", runtime.GOARCH,
		"$(PROJECT_VERSION)", metadata.Version,
		"$(TWO_DIGIT_VERSION)", twoDigitVersion(metadata.Version),
		"$(DOCKER_NS)", metadata.DockerNamespace)

	return r.Replace(viper.GetString(path))
}

// twoDigitVersion 函数将一个三位数的版本号（例如2.0.0）截断为两位数的版本号（例如2.0）。
// 如果版本号不包含点（例如latest），则直接返回传入的版本号。
//
// 参数：
//   - version: string，要截断的版本号
//
// 返回值：
//   - string: 截断后的版本号
func twoDigitVersion(version string) string {
	if strings.LastIndex(version, ".") < 0 {
		return version
	}
	return version[0:strings.LastIndex(version, ".")]
}
