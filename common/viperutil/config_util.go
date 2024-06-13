/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package viperutil

import (
	"encoding/pem"
	"fmt"
	"github.com/hyperledger/fabric/core/config"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	version "github.com/hashicorp/go-version"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var logger = flogging.MustGetLogger("viperutil")

// ConfigPaths 从环境和返回路径, 默认为 FABRIC_CFG_PATH 和/etc/hyperledger/fabric。
func ConfigPaths() []string {
	var paths []string
	if p := os.Getenv("FABRIC_CFG_PATH"); p != "" {
		paths = append(paths, p)
	}
	paths = append(paths, config.OfficialPath)
	paths = append(paths, config.Current)
	paths = append(paths, config.CurrentDefault)
	// 根据操作系统确定 peerConfig 的路径
	switch runtime.GOOS {
	case "linux":
		// 2. 自定义的 ./sampleconfig
		paths = append(paths, config.PeerLinuxConfig)
		paths = append(paths, config.PeerLinuxDefault)
	case "windows":
		// 3. 自定义的 ./samplewindowsconfig
		paths = append(paths, config.PeerWindowsConfig)
		paths = append(paths, config.PeerWindowsDefault)
	}
	return paths
}

// ConfigParser 包含配置文件的位置信息。
// 它保存配置文件的目录位置和环境变量。
// 从文件中解析并存储配置。
// 目前仅支持 "yaml" 格式。
type ConfigParser struct {
	configPaths []string // 配置文件路径列表
	configName  string   // 配置文件名
	configFile  string   // 配置文件绝对路径

	config map[string]interface{} // 解析后的配置
}

// New creates a ConfigParser instance
func New() *ConfigParser {
	return &ConfigParser{
		config: map[string]interface{}{},
	}
}

// AddConfigPaths keeps a list of path to search the relevant
// config file. Multiple paths can be provided.
func (c *ConfigParser) AddConfigPaths(cfgPaths ...string) {
	c.configPaths = append(c.configPaths, cfgPaths...)
}

// SetConfigName 提供配置文件名主干。
// 该值的上限版本也用作环境变量覆盖前缀。
func (c *ConfigParser) SetConfigName(in string) {
	c.configName = in
}

// ConfigFileUsed 返回使用的configFile。
func (c *ConfigParser) ConfigFileUsed() string {
	return c.configFile
}

// 搜索所有支持的扩展名是否存在文件名
func (c *ConfigParser) searchInPath(in string) (filename string) {
	supportedExts := []string{"yaml", "yml"}
	for _, ext := range supportedExts {
		fullPath := filepath.Join(in, c.configName+"."+ext)
		_, err := os.Stat(fullPath)
		if err == nil {
			return fullPath
		}
	}
	return ""
}

// 在所有configPaths中搜索configName
func (c *ConfigParser) findConfigFile() string {
	paths := c.configPaths
	if len(paths) == 0 {
		// 从环境和返回路径, 默认为 FABRIC_CFG_PATH 和/etc/hyperledger/fabric。
		paths = ConfigPaths()
	}
	for _, cp := range paths {
		file := c.searchInPath(cp)
		if file != "" {
			return file
		}
	}
	return ""
}

// 获取有效且当前的配置文件
func (c *ConfigParser) getConfigFile() string {
	// 如果显式设置，则使用它
	if c.configFile != "" {
		return c.configFile
	}

	c.configFile = c.findConfigFile()
	return c.configFile
}

// ReadInConfig 读取和取消配置文件。
func (c *ConfigParser) ReadInConfig() error {
	// 获取配置文件绝对路径
	cf := c.getConfigFile()
	logger.Debugf("试图打开配置文件: %s", cf)
	file, err := os.Open(cf)
	if err != nil {
		logger.Warnf("无法打开配置文件: %s", cf)
		return err
	}
	defer file.Close()

	logger.Debugf("配置文件: %s", file.Name())
	return c.ReadConfig(file)
}

// ReadConfig 解析缓冲区并初始化配置。
func (c *ConfigParser) ReadConfig(in io.Reader) error {
	return yaml.NewDecoder(in).Decode(c.config)
}

// Get value for the key by searching environment variables.
func (c *ConfigParser) getFromEnv(key string) string {
	envKey := key
	if c.configName != "" {
		envKey = c.configName + "_" + envKey
	}
	envKey = strings.ToUpper(envKey)
	envKey = strings.ReplaceAll(envKey, ".", "_")
	return os.Getenv(envKey)
}

// Prototype declaration for getFromEnv function.
type envGetter func(key string) string

func getKeysRecursively(base string, getenv envGetter, nodeKeys map[string]interface{}, oType reflect.Type) map[string]interface{} {
	subTypes := map[string]reflect.Type{}

	if oType != nil && oType.Kind() == reflect.Struct {
	outer:
		for i := 0; i < oType.NumField(); i++ {
			fieldName := oType.Field(i).Name
			fieldType := oType.Field(i).Type

			for key := range nodeKeys {
				if strings.EqualFold(fieldName, key) {
					subTypes[key] = fieldType
					continue outer
				}
			}

			subTypes[fieldName] = fieldType
			nodeKeys[fieldName] = nil
		}
	}

	result := make(map[string]interface{})
	for key, val := range nodeKeys {
		fqKey := base + key

		// overwrite val, if an environment is available
		if override := getenv(fqKey); override != "" {
			val = override
		}

		switch val := val.(type) {
		case map[string]interface{}:
			logger.Debugf("Found map[string]interface{} value for %s", fqKey)
			result[key] = getKeysRecursively(fqKey+".", getenv, val, subTypes[key])

		case map[interface{}]interface{}:
			logger.Debugf("Found map[interface{}]interface{} value for %s", fqKey)
			result[key] = getKeysRecursively(fqKey+".", getenv, toMapStringInterface(val), subTypes[key])

		case nil:
			if override := getenv(fqKey + ".File"); override != "" {
				result[key] = map[string]interface{}{"File": override}
			}

		default:
			result[key] = val
		}
	}
	return result
}

func toMapStringInterface(m map[interface{}]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for k, v := range m {
		k, ok := k.(string)
		if !ok {
			panic(fmt.Sprintf("Non string %v, %v: key-entry: %v", k, v, k))
		}
		result[k] = v
	}
	return result
}

// customDecodeHook parses strings of the format "[thing1, thing2, thing3]"
// into string slices. Note that whitespace around slice elements is removed.
func customDecodeHook(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String {
		return data, nil
	}

	raw := data.(string)
	l := len(raw)
	if l > 1 && raw[0] == '[' && raw[l-1] == ']' {
		slice := strings.Split(raw[1:l-1], ",")
		for i, v := range slice {
			slice[i] = strings.TrimSpace(v)
		}
		return slice, nil
	}

	return data, nil
}

func byteSizeDecodeHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	if f != reflect.String || t != reflect.Uint32 {
		return data, nil
	}
	raw := data.(string)
	if raw == "" {
		return data, nil
	}
	re := regexp.MustCompile(`^(?P<size>[0-9]+)\s*(?i)(?P<unit>(k|m|g))b?$`)
	if re.MatchString(raw) {
		size, err := strconv.ParseUint(re.ReplaceAllString(raw, "${size}"), 0, 64)
		if err != nil {
			return data, nil
		}
		unit := re.ReplaceAllString(raw, "${unit}")
		switch strings.ToLower(unit) {
		case "g":
			size = size << 10
			fallthrough
		case "m":
			size = size << 10
			fallthrough
		case "k":
			size = size << 10
		}
		if size > math.MaxUint32 {
			return size, fmt.Errorf("value '%s' overflows uint32", raw)
		}
		return size, nil
	}
	return data, nil
}

func stringFromFileDecodeHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	// "to" type should be string
	if t != reflect.String {
		return data, nil
	}
	// "from" type should be map
	if f != reflect.Map {
		return data, nil
	}
	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.String:
		return data, nil
	case reflect.Map:
		d := data.(map[string]interface{})
		fileName, ok := d["File"]
		if !ok {
			fileName, ok = d["file"]
		}
		switch {
		case ok && fileName != nil:
			bytes, err := ioutil.ReadFile(fileName.(string))
			if err != nil {
				return data, err
			}
			return string(bytes), nil
		case ok:
			// fileName was nil
			return nil, fmt.Errorf("Value of File: was nil")
		}
	}
	return data, nil
}

func pemBlocksFromFileDecodeHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	// "to" type should be string
	if t != reflect.Slice {
		return data, nil
	}
	// "from" type should be map
	if f != reflect.Map {
		return data, nil
	}
	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.String:
		return data, nil
	case reflect.Map:
		var fileName string
		var ok bool
		switch d := data.(type) {
		case map[string]string:
			fileName, ok = d["File"]
			if !ok {
				fileName, ok = d["file"]
			}
		case map[string]interface{}:
			var fileI interface{}
			fileI, ok = d["File"]
			if !ok {
				fileI = d["file"]
			}
			fileName, ok = fileI.(string)
		}

		switch {
		case ok && fileName != "":
			var result []string
			bytes, err := ioutil.ReadFile(fileName)
			if err != nil {
				return data, err
			}
			for len(bytes) > 0 {
				var block *pem.Block
				block, bytes = pem.Decode(bytes)
				if block == nil {
					break
				}
				if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
					continue
				}
				result = append(result, string(pem.EncodeToMemory(block)))
			}
			return result, nil
		case ok:
			// fileName was nil
			return nil, fmt.Errorf("Value of File: was nil")
		}
	}
	return data, nil
}

var kafkaVersionConstraints map[sarama.KafkaVersion]version.Constraints

func init() {
	kafkaVersionConstraints = make(map[sarama.KafkaVersion]version.Constraints)
	kafkaVersionConstraints[sarama.V0_8_2_0], _ = version.NewConstraint(">=0.8.2,<0.8.2.1")
	kafkaVersionConstraints[sarama.V0_8_2_1], _ = version.NewConstraint(">=0.8.2.1,<0.8.2.2")
	kafkaVersionConstraints[sarama.V0_8_2_2], _ = version.NewConstraint(">=0.8.2.2,<0.9.0.0")
	kafkaVersionConstraints[sarama.V0_9_0_0], _ = version.NewConstraint(">=0.9.0.0,<0.9.0.1")
	kafkaVersionConstraints[sarama.V0_9_0_1], _ = version.NewConstraint(">=0.9.0.1,<0.10.0.0")
	kafkaVersionConstraints[sarama.V0_10_0_0], _ = version.NewConstraint(">=0.10.0.0,<0.10.0.1")
	kafkaVersionConstraints[sarama.V0_10_0_1], _ = version.NewConstraint(">=0.10.0.1,<0.10.1.0")
	kafkaVersionConstraints[sarama.V0_10_1_0], _ = version.NewConstraint(">=0.10.1.0,<0.10.2.0")
	kafkaVersionConstraints[sarama.V0_10_2_0], _ = version.NewConstraint(">=0.10.2.0,<0.11.0.0")
	kafkaVersionConstraints[sarama.V0_11_0_0], _ = version.NewConstraint(">=0.11.0.0,<1.0.0")
	kafkaVersionConstraints[sarama.V1_0_0_0], _ = version.NewConstraint(">=1.0.0")
}

func kafkaVersionDecodeHook(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String || t != reflect.TypeOf(sarama.KafkaVersion{}) {
		return data, nil
	}

	v, err := version.NewVersion(data.(string))
	if err != nil {
		return nil, fmt.Errorf("Unable to parse Kafka version: %s", err)
	}

	for kafkaVersion, constraints := range kafkaVersionConstraints {
		if constraints.Check(v) {
			return kafkaVersion, nil
		}
	}

	return nil, fmt.Errorf("Unsupported Kafka version: '%s'", data)
}

func bccspHook(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if t != reflect.TypeOf(&factory.FactoryOpts{}) {
		return data, nil
	}

	config := factory.GetDefaultOpts()

	err := mapstructure.WeakDecode(data, config)
	if err != nil {
		return nil, errors.Wrap(err, "could not decode bccsp type")
	}

	return config, nil
}

// EnhancedExactUnmarshal 用于将配置文件解析为一个结构体，并在引入多余变量时产生错误，
// 同时支持 time.Duration 类型。
// 方法接收者：
//   - c：*ConfigParser，表示配置解析器的实例
//
// 输入参数：
//   - output：interface{}，表示解析后的输出结构体
//
// 返回值：
//   - error：表示解析过程中可能发生的错误
func (c *ConfigParser) EnhancedExactUnmarshal(output interface{}) error {
	oType := reflect.TypeOf(output)
	if oType.Kind() != reflect.Ptr {
		return errors.Errorf("提供的输出参数必须是指向结构的指针类型")
	}
	eType := oType.Elem()
	if eType.Kind() != reflect.Struct {
		return errors.Errorf("提供的输出参数必须是指向结构的指针类型, 但它是指向其他内容的指针类型")
	}

	// 获取配置文件的基本键值对
	baseKeys := c.config

	// 递归获取结构体中的所有字段的键值对
	leafKeys := getKeysRecursively("", c.getFromEnv, baseKeys, eType)

	// 打印 leafKeys，用于调试目的
	logger.Debugf("%+v", leafKeys)

	// 创建 mapstructure.DecoderConfig 实例
	config := &mapstructure.DecoderConfig{
		ErrorUnused:      false, // 一些不属于结构体的配置项不解析
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			bccspHook,
			mapstructure.StringToTimeDurationHookFunc(),
			customDecodeHook,
			byteSizeDecodeHook,
			stringFromFileDecodeHook,
			pemBlocksFromFileDecodeHook,
			kafkaVersionDecodeHook,
		),
	}

	// 创建 mapstructure.Decoder 实例
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	// 使用 decoder 对 leafKeys 进行解析
	return decoder.Decode(leafKeys)
}

// YamlStringToStructHook is a hook for viper(viper.Unmarshal(*,*, here)), it is able to parse a string of minified yaml into a slice of structs
func YamlStringToStructHook(m interface{}) func(rf reflect.Kind, rt reflect.Kind, data interface{}) (interface{}, error) {
	return func(rf reflect.Kind, rt reflect.Kind, data interface{}) (interface{}, error) {
		if rf != reflect.String || rt != reflect.Slice {
			return data, nil
		}

		raw := data.(string)
		if raw == "" {
			return m, nil
		}

		return m, yaml.UnmarshalStrict([]byte(raw), &m)
	}
}
