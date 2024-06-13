/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platforms

import (
	"io"

	docker "github.com/fsouza/go-dockerclient"
)

// Builder 用于构建镜像。
type Builder struct {
	Registry *Registry      // 注册表
	Client   *docker.Client // Docker 客户端
}

func (b *Builder) GenerateDockerBuild(ccType, path string, codePackage io.Reader) (io.Reader, error) {
	return b.Registry.GenerateDockerBuild(ccType, path, codePackage, b.Client)
}
