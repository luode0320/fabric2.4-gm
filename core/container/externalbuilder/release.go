package externalbuilder

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/otiai10/copy"
)

var loggerRelease = log.New(os.Stderr, "", 0)

// Connection 结构体用于表示链码包中提供的 connection.json 文件。
type connection struct {
	Address     string `json:"address"`              // 连接地址
	DialTimeout string `json:"dial_timeout"`         // 连接超时时间
	TLS         bool   `json:"tls_required"`         // 是否需要 TLS
	ClientAuth  bool   `json:"client_auth_required"` // 是否需要客户端认证
	RootCert    string `json:"root_cert"`            // 根证书
	ClientKey   string `json:"client_key"`           // 客户端私钥
	ClientCert  string `json:"client_cert"`          // 客户端证书
}

// RunRelease 释放外部链码包
func RunRelease(arg ...string) error {
	loggerRelease.Println("::释放链码包")

	if len(arg) < 3 {
		return fmt.Errorf("参数数目不正确")
	}

	builderOutputDir, releaseDir := arg[1], arg[2]
	connectionSrcFile := filepath.Join(builderOutputDir, "/connection.json")

	connectionDir := filepath.Join(releaseDir, "chaincode/server/")
	connectionDestFile := filepath.Join(releaseDir, "chaincode/server/connection.json")

	metadataDir := filepath.Join(builderOutputDir, "META-INF/statedb")
	metadataDestDir := filepath.Join(releaseDir, "statedb")

	// 如果 builderOutput/META-INF/statedb 存在则复制目录到 release/statedb
	if _, err := os.Stat(metadataDir); !os.IsNotExist(err) {
		if err := copy.Copy(metadataDir, metadataDestDir); err != nil {
			return fmt.Errorf("复制 '%s' 目录到 '%s' 文件夹失败: %s", metadataDir, metadataDestDir, err)
		}
	}

	// 处理和更新连接文件
	_, err := os.Stat(connectionSrcFile)
	if err != nil {
		return fmt.Errorf("在源文件夹 %s 中找不到 connection.json: %s", builderOutputDir, err)
	}

	err = os.MkdirAll(connectionDir, 0750)
	if err != nil {
		return fmt.Errorf("无法为 connection.json 创建目标 '%s' 文件夹: %s", connectionDir, err)
	}

	// 复制 连接文件到 release/chaincode/server/connection.json
	if err = Copy(connectionSrcFile, connectionDestFile); err != nil {
		return err
	}

	loggerRelease.Printf("::发布阶段已完成")

	return nil
}

// Copy dst的src文件。任何现有的文件将被覆盖，不会
// 复制文件属性。
func Copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
