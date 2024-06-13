package externalbuilder

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

var loggerDetect = log.New(os.Stderr, "", 0)

type chaincodeMetadata struct {
	Type string `json:"type"`
}

func RunDetect(arg ...string) error {
	loggerDetect.Println("::检测链码包")

	// 函数检查传递给程序的命令行参数数量是否足够
	if len(arg) < 3 {
		return fmt.Errorf("参数太少")
	}

	// 使用第二个命令行参数作为链码元数据目录的路径，并构建元数据文件的完整路径
	chaincodeMetaData := arg[2]
	metadataFile := filepath.Join(chaincodeMetaData, "metadata.json")
	// 使用 os.Stat 函数检查元数据文件是否存在
	_, err := os.Stat(metadataFile)
	if err != nil {
		return fmt.Errorf("%s 未找到 ", metadataFile)
	}

	if _, err := os.Stat(metadataFile); err != nil {
		return errors.WithMessagef(err, "%s 未找到 ", metadataFile)
	}

	// 使用 ioutil.ReadFile 函数读取元数据文件的内容，并将其存储在 mdbytes 变量中
	mdbytes, cause := ioutil.ReadFile(metadataFile)
	if cause != nil {
		err := errors.WithMessagef(cause, "%s 不可读", metadataFile)
		return err
	}

	// 使用 json.Unmarshal 函数将元数据文件的内容解析为 chaincodeMetadata 结构体
	var metadata chaincodeMetadata
	cause = json.Unmarshal(mdbytes, &metadata)
	if cause != nil {
		return errors.WithMessage(cause, "无法解析metadata.json文件")
	}

	// 检查解析后的元数据中的链码类型是否为 "ccaas"
	if strings.ToLower(metadata.Type) != "ccaas" {
		return fmt.Errorf("不支持链码类型: %s", metadata.Type)
	}

	loggerDetect.Printf("::检测为 ccaas 的链码类型")
	// 返回nil向对等方指示成功检测
	return nil

}
