/*
Copyright London Stock Exchange 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package metadata

// 由 Makefile 定义并通过ldflags传入的变量
var (
	// Version 表示程序的版本号
	Version = "latest"

	// CommitSHA 表示程序的提交哈希值
	CommitSHA = "development build"

	// BaseDockerLabel 表示基本的Docker标签
	BaseDockerLabel = "org.hyperledger.fabric"

	// DockerNamespace 表示Docker的命名空间
	DockerNamespace = "hyperledger"
)
