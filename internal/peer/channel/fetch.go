/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric-config/protolator"
	"github.com/hyperledger/fabric/core/config"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/spf13/cobra"
)

func fetchCmd(cf *ChannelCmdFactory) *cobra.Command {
	fetchCmd := &cobra.Command{
		Use:   "fetch <newest|oldest|config|(number)> [outputfile]",
		Short: "获取一个块信息",
		Long:  "获取指定的 block 块、json、pb, 将其写入文件.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fetch(cmd, args, cf)
		},
	}
	flagList := []string{
		"channelID",
		"bestEffort",
	}
	attachFlags(fetchCmd, flagList)

	return fetchCmd
}

// 获取指定的 block、json、pb, 将其写入文件.
func fetch(cmd *cobra.Command, args []string, cf *ChannelCmdFactory) error {
	if len(args) == 0 {
		return fmt.Errorf("fetch 必须有一个参数 required、lorest、newest、config 或一个数字")
	}
	if len(args) > 2 {
		return fmt.Errorf("fetch 最多只能包含2个参数")
	}
	// 命令行的解析完成，所以沉默cmd的使用
	cmd.SilenceUsage = true

	// 默认为从orderer提取
	ordererRequired := OrdererRequired
	peerDeliverRequired := PeerDeliverNotRequired
	if len(strings.Split(common.OrderingEndpoint, ":")) != 2 {
		// 如果未提供 orderer 节点，请连接到对等方的 deliver service
		ordererRequired = OrdererNotRequired
		peerDeliverRequired = PeerDeliverRequired
	}
	var err error
	if cf == nil {
		cf, err = InitCmdFactory(EndorserNotRequired, peerDeliverRequired, ordererRequired)
		if err != nil {
			return err
		}
	}

	var block *cb.Block

	switch args[0] {
	case "oldest":
		block, err = cf.DeliverClient.GetOldestBlock()
	case "newest":
		block, err = cf.DeliverClient.GetNewestBlock()
	case "config":
		// 方法从对等方/排序方的交付服务获取最新的块
		iBlock, err2 := cf.DeliverClient.GetNewestBlock()
		if err2 != nil {
			return err2
		}

		// 从块元数据中检索最后一个配置块的索引
		lc, err2 := protoutil.GetLastConfigIndexFromBlock(iBlock)
		if err2 != nil {
			return err2
		}

		logger.Infof("检索到最后一个区块: %d", lc)
		// 方法从对等方/排序方的交付服务获取指定的块
		block, err = cf.DeliverClient.GetSpecifiedBlock(lc)
	default:
		num, err2 := strconv.Atoi(args[0])
		if err2 != nil {
			return fmt.Errorf("fetch target illegal: %s", args[0])
		}
		block, err = cf.DeliverClient.GetSpecifiedBlock(uint64(num))
	}

	if err != nil {
		return err
	}

	b, err := proto.Marshal(block)
	if err != nil {
		return err
	}

	var file string
	if len(args) == 1 {
		file = channelID + "_" + args[0] + ".block"
	} else {
		file = args[1]
	}

	file = config.TranslatePath(filepath.Dir(viper.ConfigFileUsed()), file)
	logger.Infof("生成通道 block 文件: %s", file)
	if err = ioutil.WriteFile(file, b, 0o644); err != nil {
		return err
	}

	// 生成json
	err = doInspectBlock(file)
	if err != nil {
		return err
	}

	return nil
}

// Block 过滤json前缀
type Block struct {
	Data struct {
		Data []struct {
			Payload struct {
				Data struct {
					Config interface{} `json:"config"`
				} `json:"data"`
			} `json:"payload"`
		} `json:"data"`
	} `json:"data"`
}

// 生成json和pb文件
func doInspectBlock(inspectBlock string) error {
	logger.Info("检查 block 创世块")
	data, err := ioutil.ReadFile(inspectBlock)
	if err != nil {
		return fmt.Errorf("无法读取块 %s", inspectBlock)
	}

	// 构建文件路径
	// 将字符串中的".block"替换为".json"
	filePath := strings.Replace(inspectBlock, ".block", ".json", 1)

	logger.Info("解析 block 创世块")
	block, err := protoutil.UnmarshalBlock(data)
	if err != nil {
		return fmt.Errorf("反序列化到 block 块时出错: %s", err)
	}

	// 打开文件，如果文件不存在则创建，如果文件存在则覆盖写入
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s", err)
	}
	defer file.Close()

	// 将JSON写入文件
	err = protolator.DeepMarshalJSON(file, block)
	if err != nil {
		return fmt.Errorf("生成通道 json 文件失败: %s", err)
	}

	// 读取文件内容
	file, err = os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer file.Close()

	data, err = ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("读取文件失败: %s\n", err)
	}

	// 解析JSON数据
	var blockFilter Block
	err = json.Unmarshal(data, &blockFilter)
	if err != nil {
		return fmt.Errorf("解析JSON失败: %s\n", err)
	}

	// 提取需要的数据
	configData := blockFilter.Data.Data[0].Payload.Data.Config

	// 将数据转换为JSON格式
	filteredData, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return fmt.Errorf("转换为JSON失败: %s\n", err)
	}

	// 将过滤后的数据写入原始文件
	err = ioutil.WriteFile(filePath, filteredData, 0644)
	if err != nil {
		return fmt.Errorf("写入文件失败: %s\n", err)
	}

	logger.Infof("生成通道 json 文件: %s", filePath)

	// 再次读取文件内容
	file, err = os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer file.Close()

	// 构建文件路径
	// 将字符串中的".block"替换为".json"
	filePath = strings.Replace(inspectBlock, ".block", ".pb", 1)

	// 读取文件内容
	pbfile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("无法打开文件: %s\n", err)
	}
	defer pbfile.Close()

	err = EncodeProto(file, pbfile)
	if err != nil {
		return fmt.Errorf("生成通道 pb 文件失败: %s", filePath)
	}

	logger.Infof("生成通道 pb 文件: %s", filePath)
	return nil
}

// EncodeProto 函数将输入的 JSON 数据解码为指定消息类型的 Protocol Buffers 对象，并将其编码为二进制格式。
// 方法接收者：无。
// 输入参数：
//   - input *os.File，表示输入的 JSON 文件。
//   - output *os.File，表示输出的二进制文件。
//
// 返回值：
//   - error，表示可能发生的错误。
func EncodeProto(input, output *os.File) error {
	// 获取消息类型
	msgType := proto.MessageType("common.Config")
	if msgType == nil {
		return errors.Errorf("未知类型 %s 的消息", msgType)
	}

	// 创建消息对象
	msg := reflect.New(msgType.Elem()).Interface().(proto.Message)

	// 将输入的 JSON 数据解码为消息对象
	err := protolator.DeepUnmarshalJSON(input, msg)
	if err != nil {
		return errors.Wrapf(err, "错误解码输入")
	}

	// 将消息对象编码为二进制格式
	out, err := proto.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "反序列化出错")
	}

	// 将编码后的二进制数据写入输出文件
	_, err = output.Write(out)
	if err != nil {
		return errors.Wrapf(err, "写入输出时出错")
	}

	return nil
}
