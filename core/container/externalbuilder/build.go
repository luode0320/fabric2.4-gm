package externalbuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"
)

var loggerBuild = log.New(os.Stderr, "", 0)

type Config struct {
	PeerName string
}

func RunBuild(arg ...string) error {
	loggerBuild.Println("::构建链码包")

	if len(arg) < 4 {
		return fmt.Errorf("参数数目不正确")
	}

	sourceDir, metadataDir, outputDir := arg[1], arg[2], arg[3]

	connectionSrcFile := filepath.Join(sourceDir, "/connection.json")
	metadataFile := filepath.Clean(filepath.Join(metadataDir, "metadata.json"))
	connectionDestFile := filepath.Join(outputDir, "/connection.json")
	metainfoSrcDir := filepath.Join(sourceDir, "META-INF")
	metainfoDestDir := filepath.Join(outputDir, "META-INF")

	// 处理并检查元数据文件，然后复制到输出位置
	if _, err := os.Stat(metadataFile); err != nil {
		return errors.WithMessagef(err, "%s 未找到 ", metadataFile)
	}

	metadataFileContents, cause := ioutil.ReadFile(metadataFile)
	if cause != nil {
		return errors.WithMessagef(cause, "%s 文件不可读 ", metadataFile)
	}

	var metadata chaincodeMetadata
	if err := json.Unmarshal(metadataFileContents, &metadata); err != nil {
		return errors.WithMessage(err, "无法解析JSON")
	}

	if strings.ToLower(metadata.Type) != "ccaas" {
		return fmt.Errorf("链码类型应为ccaas, 为 %s", metadata.Type)
	}

	// metadata复制到输出位置
	if err := copy.Copy(metadataDir, outputDir); err != nil {
		return fmt.Errorf("无法复制 '%s' 到生成元数据 '%s' 文件夹: %s", metadataDir, outputDir, err)
	}

	// 检测 sourceDir/META-INF 是否存在, 如果存在复制到输出目录的 outputDir/META-INF
	if _, err := os.Stat(metainfoSrcDir); !os.IsNotExist(err) {
		if err := copy.Copy(metainfoSrcDir, metainfoDestDir); err != nil {
			return fmt.Errorf("无法复制 '%s' 到 '%s' 文件夹: %s", metainfoSrcDir, metainfoDestDir, err)
		}
	}

	// 处理和更新连接文件
	fileInfo, err := os.Stat(connectionSrcFile)
	if err != nil {
		return errors.WithMessagef(err, "%s 未找到 ", connectionSrcFile)
	}

	connectionFileContents, err := ioutil.ReadFile(connectionSrcFile)
	if err != nil {
		return err
	}

	// 将connection.json文件读入结构以进行处理
	var connectionData connection
	if err := json.Unmarshal(connectionFileContents, &connectionData); err != nil {
		return err
	}

	// 将connection.json中的每个字符串字段视为 Go 模板字符串。
	// 它们可以是固定字符串，但如果它们是模板
	// 然后在 CHAINCODE_AS_A_SERVICE_BUILDER_CONFIG 中定义的JSON字符串
	// 用作 'context' 来解析字符串

	updatedConnection := connection{}
	var cfg map[string]interface{}

	cfgString := os.Getenv("CHAINCODE_AS_A_SERVICE_BUILDER_CONFIG")
	if cfgString != "" {
		if err := json.Unmarshal([]byte(cfgString), &cfg); err != nil {
			return fmt.Errorf("无法反序列化 CHAINCODE_AS_A_SERVICE_BUILDER_CONFIG json环境变量配置 %s", err)
		}
	}

	updatedConnection.Address, err = execTempl(cfg, connectionData.Address)
	if err != nil {
		return fmt.Errorf("无法分析地址 connection.json Address 地址字段模板: %s", err)
	}

	updatedConnection.DialTimeout, err = execTempl(cfg, connectionData.DialTimeout)
	if err != nil {
		return fmt.Errorf("无法分析 connection.json DialTimeout 连接超时字段模板: %s", err)
	}

	// 如果连接启用了TLS，则使用正确的信息进行更新
	// 对于无TLS情况，不需要其他信息，因此可以假定为默认值
	if connectionData.TLS {
		updatedConnection.TLS = true
		updatedConnection.ClientAuth = connectionData.ClientAuth

		updatedConnection.RootCert, err = execTempl(cfg, connectionData.RootCert)
		if err != nil {
			return fmt.Errorf("无法分析 connection.json RootCert 根证书字段模板: %s", err)
		}
		updatedConnection.ClientKey, err = execTempl(cfg, connectionData.ClientKey)
		if err != nil {
			return fmt.Errorf("无法分析 connection.json ClientKey 客户端私钥字段模板: %s", err)
		}
		updatedConnection.ClientCert, err = execTempl(cfg, connectionData.ClientCert)
		if err != nil {
			return fmt.Errorf("无法分析 connection.json ClientCert 客户端证书字段模板: %s", err)
		}
	}

	updatedConnectionBytes, err := json.Marshal(updatedConnection)
	if err != nil {
		return fmt.Errorf("无法序列化读取的 connection.json 文件: %s", err)
	}

	err = ioutil.WriteFile(connectionDestFile, updatedConnectionBytes, fileInfo.Mode())
	if err != nil {
		return err
	}

	loggerBuild.Printf("::构建阶段已完成")

	return nil

}

// execTempl 是一个辅助函数，用于根据一个字符串对模板进行处理，并返回一个字符串。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - cfg：map[string]interface{} 类型，表示模板中要使用的配置。
//   - inputStr：string 类型，表示要处理的模板字符串。
//
// 返回值：
//   - string：表示处理后的字符串。
//   - error：如果处理模板时出错，则返回错误。
func execTempl(cfg map[string]interface{}, inputStr string) (string, error) {
	// 解析模板字符串
	t, err := template.New("").Option("missingkey=error").Parse(inputStr)
	if err != nil {
		fmt.Printf("无法分析模板: %s", err)
		return "", err
	}

	// 创建一个缓冲区
	buf := &bytes.Buffer{}
	// 执行模板，并将结果写入缓冲区
	err = t.Execute(buf, cfg)
	if err != nil {
		return "", err
	}

	// 将缓冲区中的内容转换为字符串并返回
	return buf.String(), nil
}
