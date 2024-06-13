/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-config/protolator"
	cb "github.com/hyperledger/fabric-protos-go/common"
	_ "github.com/hyperledger/fabric-protos-go/msp"
	_ "github.com/hyperledger/fabric-protos-go/orderer"
	_ "github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	_ "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/configtxlator/metadata"
	"github.com/hyperledger/fabric/internal/configtxlator/rest"
	"github.com/hyperledger/fabric/internal/configtxlator/update"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

// 命令行标志
var (
	app = kingpin.New("configtxlator", "用于生成Hyperledger Fabric通道配置的实用程序")

	start    = app.Command("start", "启动configtxlator REST服务器")
	hostname = start.Flag("hostname", "REST服务器将侦听的主机名或IP").Default("0.0.0.0").String()
	port     = start.Flag("port", "REST服务器将侦听的端口").Default("7059").Int()
	cors     = start.Flag("CORS", "允许的 CORS 跨域,例如 '*' 或 'www.Example.com' (可能会重复).").Strings()

	// json 编码为 protobuf 二进制
	protoEncode       = app.Command("proto_encode", "将 json 序列化为 protobuf.")
	protoEncodeType   = protoEncode.Flag("type", "要编码到的 protobuf 结构的类型. 例如, 'common.Config'.").Required().String()
	protoEncodeSource = protoEncode.Flag("input", "输入 json 文件.").Default(os.Stdin.Name()).File()
	protoEncodeDest   = protoEncode.Flag("output", "输出 pb 文件.").Default(os.Stdout.Name()).OpenFile(os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)

	// protobuf 二进制解码为 json
	protoDecode       = app.Command("proto_decode", "将 protobuf 反序列化为 json.")
	protoDecodeType   = protoDecode.Flag("type", "要解码的 protobuf 结构的类型. 例如, 'common.Config'.").Required().String()
	protoDecodeSource = protoDecode.Flag("input", "输入 protobuf 文件.").Default(os.Stdin.Name()).File()
	protoDecodeDest   = protoDecode.Flag("output", "输出 json 文件.").Default(os.Stdout.Name()).OpenFile(os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)

	// 比较差异
	computeUpdate          = app.Command("compute_update", "接受两个序列化的 common.Config 消息, 并计算在两个消息之间转换的配置差异.")
	computeUpdateOriginal  = computeUpdate.Flag("original", "原始的 config 消息.").File()
	computeUpdateUpdated   = computeUpdate.Flag("updated", "更新的 config 消息.").File()
	computeUpdateChannelID = computeUpdate.Flag("channel_id", "此更新的通道名称.").Required().String()
	computeUpdateDest      = computeUpdate.Flag("output", "输出差异 pb 文件, 并附加输出同名的差异 json 文件.").Default(os.Stdout.Name()).OpenFile(os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)

	version = app.Command("version", "显示版本信息")
)

var logger = flogging.MustGetLogger("configtxlator")

func main() {
	kingpin.Version("0.0.1")
	parse := kingpin.MustParse(app.Parse(os.Args[1:]))
	switch parse {
	// "start" command
	case start.FullCommand():
		startServer(fmt.Sprintf("%s:%d", *hostname, *port), *cors)
	// json 编码为 protobuf 二进制
	case protoEncode.FullCommand():
		defer (*protoEncodeSource).Close() // 输入 json 文件
		defer (*protoEncodeDest).Close()   // 输出 protobuf 文件

		logger.Infof("json 编码为 protobuf 二进制的 pb 文件 \n")
		// 函数将输入的 JSON 数据解码为指定消息类型的 Protocol Buffers 对象，并将其编码为二进制格式
		err := encodeProto(*protoEncodeType, *protoEncodeSource, *protoEncodeDest)
		if err != nil {
			app.Fatalf("错误解码: %s", err)
		}
		logger.Infof("由 json 编码的 protobuf 二进制的 pb 文件: %s \n", (*protoEncodeDest).Name())
	// protobuf 二进制解码为 json
	case protoDecode.FullCommand():
		defer (*protoDecodeSource).Close() // 输入 protobuf 文件
		defer (*protoDecodeDest).Close()   // 输出 json 文件

		logger.Infof("protobuf 二进制解码为 json \n")
		// 函数将输入的二进制数据解码为指定消息类型的 Protocol Buffers 对象，并将其编码为 JSON 格式
		err := decodeProto(*protoDecodeType, *protoDecodeSource, *protoDecodeDest)
		if err != nil {
			app.Fatalf("错误解码: %s", err)
		}
		logger.Infof("protobuf 二进制解码为 json 文件: %s \n", (*protoDecodeDest).Name())
	// 比较差异
	case computeUpdate.FullCommand():
		defer (*computeUpdateOriginal).Close() // 原始的 config 消息
		defer (*computeUpdateUpdated).Close()  // 更新的 config 消息
		defer (*computeUpdateDest).Close()     // 输出 pb 文件

		logger.Infof("计算给定的原始配置和更新配置之间的配置差异 \n")
		// 比较差异, 函数计算给定的原始配置和更新配置之间的配置更新，并将结果编码为二进制格式
		err := computeUpdt(*computeUpdateOriginal, *computeUpdateUpdated, *computeUpdateDest, *computeUpdateChannelID)
		if err != nil {
			app.Fatalf("计算配置差异时出错: %s", err)
		}

		// 将差异pb 转 提交通道更新的 json, 并将提交通道更新的 json 转 pb
		computeToJsonAndPb((*computeUpdateDest).Name(), *computeUpdateChannelID)
	// "version" command
	case version.FullCommand():
		printVersion()
	}
}

func startServer(address string, cors []string) {
	var err error

	listener, err := net.Listen("tcp", address)
	if err != nil {
		app.Fatalf("Could not bind to address '%s': %s", address, err)
	}

	if len(cors) > 0 {
		origins := handlers.AllowedOrigins(cors)
		// Note, configtxlator only exposes POST APIs for the time being, this
		// list will need to be expanded if new non-POST APIs are added
		methods := handlers.AllowedMethods([]string{http.MethodPost})
		headers := handlers.AllowedHeaders([]string{"Content-Type"})
		logger.Infof("Serving HTTP requests on %s with CORS %v", listener.Addr(), cors)
		err = http.Serve(listener, handlers.CORS(origins, methods, headers)(rest.NewRouter()))
	} else {
		logger.Infof("Serving HTTP requests on %s", listener.Addr())
		err = http.Serve(listener, rest.NewRouter())
	}

	app.Fatalf("Error starting server:[%s]\n", err)
}

func printVersion() {
	fmt.Println(metadata.GetVersionInfo())
}

// encodeProto 函数将输入的 JSON 数据解码为指定消息类型的 Protocol Buffers 对象，并将其编码为二进制格式。
// 方法接收者：无。
// 输入参数：
//   - msgName string，表示消息类型的名称。
//   - input *os.File，表示输入的 JSON 文件。
//   - output *os.File，表示输出的二进制文件。
//
// 返回值：
//   - error，表示可能发生的错误。
func encodeProto(msgName string, input, output *os.File) error {
	// 获取消息类型
	msgType := proto.MessageType(msgName)
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

// decodeProto 函数将输入的二进制数据解码为指定消息类型的 Protocol Buffers 对象，并将其编码为 JSON 格式。
// 方法接收者：无。
// 输入参数：
//   - msgName string，表示消息类型的名称。
//   - input *os.File，表示输入的二进制文件。
//   - output *os.File，表示输出的 JSON 文件。
//
// 返回值：
//   - error，表示可能发生的错误。
func decodeProto(msgName string, input, output *os.File) error {
	// 获取消息类型
	msgType := proto.MessageType(msgName)
	if msgType == nil {
		return errors.Errorf("未知类型 %s 的消息", msgType)
	}

	// 创建消息对象
	msg := reflect.New(msgType.Elem()).Interface().(proto.Message)

	// 读取输入文件的内容
	in, err := ioutil.ReadAll(input)
	if err != nil {
		return errors.Wrapf(err, "读取输入时出错")
	}

	// 将输入的二进制数据解码为消息对象
	err = proto.Unmarshal(in, msg)
	if err != nil {
		return errors.Wrapf(err, "反序列化时出错")
	}

	// 将消息对象编码为 JSON 格式，并写入输出文件
	err = protolator.DeepMarshalJSON(output, msg)
	if err != nil {
		return errors.Wrapf(err, "错误编码输出")
	}

	return nil
}

// computeUpdt 函数计算给定的原始配置和更新配置之间的配置更新，并将结果编码为二进制格式。
// 方法接收者：无。
// 输入参数：
//   - original *os.File，表示原始配置的文件。
//   - updated *os.File，表示更新配置的文件。
//   - output *os.File，表示输出的二进制文件。
//   - channelID string，表示通道的ID。
//
// 返回值：
//   - error，表示可能发生的错误。
func computeUpdt(original, updated, output *os.File, channelID string) error {
	// 读取原始配置文件的内容
	origIn, err := ioutil.ReadAll(original)
	if err != nil {
		return errors.Wrapf(err, "读取原始配置时出错")
	}

	// 创建原始配置对象
	origConf := &cb.Config{}
	err = proto.Unmarshal(origIn, origConf)
	if err != nil {
		return errors.Wrapf(err, "反序列化原始配置时出错")
	}

	// 读取更新配置文件的内容
	updtIn, err := ioutil.ReadAll(updated)
	if err != nil {
		return errors.Wrapf(err, "读取更新配置时出错")
	}

	// 创建更新配置对象
	updtConf := &cb.Config{}
	err = proto.Unmarshal(updtIn, updtConf)
	if err != nil {
		return errors.Wrapf(err, "反序列化更新的配置时出错")
	}

	// 计算配置更新
	cu, err := update.Compute(origConf, updtConf)
	if err != nil {
		return errors.Wrapf(err, "计算配置更新时出错")
	}

	// 设置配置更新的通道ID
	cu.ChannelId = channelID

	// 将配置更新对象编码为二进制格式
	outBytes, err := proto.Marshal(cu)
	if err != nil {
		return errors.Wrapf(err, "序列化计算的配置更新时出错")
	}

	// 将编码后的二进制数据写入输出文件
	_, err = output.Write(outBytes)
	if err != nil {
		return errors.Wrapf(err, "将配置更新写入输出时出错")
	}

	return nil
}

// peer配置文件目录
var peerLinuxConfig = "sampleconfig"
var peerWindowsConfig = "samplewindowsconfig"

/**
 * 初始化
 */
func init() {
	// 检查 FABRIC_CFG_PATH 环境变量是否已设置
	cfgPath := os.Getenv("FABRIC_CFG_PATH")

	if cfgPath == "" {
		// 获取当前工作目录
		currentDir, err := os.Getwd()
		if err != nil {
			fmt.Println("无法获取当前路径:", err)
			return
		}
		// 根据操作系统确定 peerConfig 的路径
		var peerConfig string
		switch runtime.GOOS {
		case "linux":
			peerConfig = peerLinuxConfig
		case "windows":
			peerConfig = peerWindowsConfig
		default:
			fmt.Println("不支持的操作系统")
			return
		}
		// 构建 peerConfigPath 的路径
		cfgPath = filepath.Join(currentDir, peerConfig)
		if _, err := os.Stat(cfgPath); err == nil {
			os.Setenv("FABRIC_CFG_PATH", cfgPath)
		} else {
			os.Setenv("FABRIC_CFG_PATH", currentDir)
		}
	}
}

// UpdateJSON 提交到通道更新的附加头部
type UpdateJSON struct {
	Payload `json:"payload"`
}

// Payload 提交到通道更新的附加头部的负载
type Payload struct {
	Header `json:"header"`
	Data   `json:"data"`
}

// Header 提交到通道更新的附加头部的负载头部
type Header struct {
	ChannelHeader `json:"channel_header"`
}

// ChannelHeader 提交到通道更新的附加头部的负载头部的通道头部
type ChannelHeader struct {
	ChannelID string `json:"channel_id"`
	Type      int    `json:"type"`
}

// Data 提交到通道更新的附加头部的负载数据
type Data struct {
	ConfigUpdate json.RawMessage `json:"config_update"`
}

// computeToJsonAndPb 函数将给定的差异配置更新（protobuf格式）转换为JSON格式, 并将提交通道更新的 json 转 pb
// 方法接收者：无。
// 输入参数：
//   - name string，表示差异配置更新的名称。
//   - channelID string，表示通道的ID。
//
// 返回值：无。
func computeToJsonAndPb(name string, channelID string) {
	// 将差异pb转json
	// 再次读取差异文件内容
	file, err := os.Open(name)
	if err != nil {
		app.Fatalf("无法打开差异 pb 文件: %s", err)
	}
	defer file.Close()

	// 将字符串中的".pb"替换为".json"
	filePath := strings.Replace(name, ".pb", ".json", 1)

	// 读取文件内容
	pbfile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		app.Fatalf("无法打开差异 pb 文件同名的 json 文件: %s", err)
	}
	defer pbfile.Close()

	// pb 转 json
	err = decodeProto("common.ConfigUpdate", file, pbfile)
	if err != nil {
		app.Fatalf("错误解码: %s", err)
	}

	// 函数用于在差异JSON文件的头部添加账本提交通道的更新头部，并替换原始的差异JSON文件
	addJsonHeader(filePath, channelID)
	// 函数用于将给定的JSON文件转换为对应的Protocol Buffers（protobuf）格式
	jsonToPb(name, filePath)
}

// jsonToPb 函数用于将给定的JSON文件转换为对应的Protocol Buffers（protobuf）格式。
// 方法接收者：无。
// 输入参数：
//   - name string，表示转换后的protobuf对象的名称。
//   - path string，表示输入的JSON文件的路径。
//
// 返回值：无。
func jsonToPb(name string, path string) {
	// 补充更新的头部 json 转 pb, 准备提交到通道更新
	// 再次读取差异文件内容
	file, err := os.Open(path)
	if err != nil {
		app.Fatalf("无法打开差异 pb 文件: %s", err)
	}
	defer file.Close()

	// 读取文件内容
	pbfile, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		app.Fatalf("无法打开差异 pb 文件同名的 json 文件: %s", err)
	}
	defer pbfile.Close()

	// json 转 pb
	err = encodeProto("common.Envelope", file, pbfile)
	if err != nil {
		app.Fatalf("json 转 pb 文件失败: %s", err)
	}

	logger.Infof("原始配置和更新配置的配置差异 pb 文件: %s \n", name)
}

// addJsonHeader 函数用于在差异JSON文件的头部添加账本提交通道的更新头部，并替换原始的差异JSON文件。
// 方法接收者：无。
// 输入参数：
//   - path string，表示差异JSON文件的路径。
//   - channelID string，表示账本提交通道的ID。
//
// 返回值：无。
func addJsonHeader(path string, channelID string) {
	// 读取原始 JSON 文件
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Errorf("无法读取差异 JSON 文件 [%s] : %s \n", path, err)
		return
	}

	// 创建 UpdateJSON 结构体并设置头部信息和数据
	updateJSON := UpdateJSON{
		Payload: Payload{
			Header: Header{
				ChannelHeader: ChannelHeader{
					ChannelID: channelID,
					Type:      2,
				},
			},
			Data: Data{
				ConfigUpdate: data,
			},
		},
	}

	// 将 UpdateJSON 结构体编码为 JSON 字符串
	filteredData, err := json.MarshalIndent(updateJSON, "", "  ")
	if err != nil {
		logger.Errorf("无法将更新头部序列化: %s \n", err)
		return
	}

	// 将 JSON 字符串写入文件
	err = ioutil.WriteFile(path, filteredData, 0644)
	if err != nil {
		logger.Errorf("无法写入更新头部后的 json 文件 [%s]: %s \n", path, err)
		return
	}

	logger.Infof("原始配置和更新配置的配置差异 json 文件: %s \n", path)
}
