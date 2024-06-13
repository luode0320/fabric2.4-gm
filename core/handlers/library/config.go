/*
 Copyright IBM Corp All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package library

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// Config 配置了注册表的工厂方法和插件。
type Config struct {
	AuthFilters []*HandlerConfig `yaml:"authFilters"` // 认证过滤器配置
	Decorators  []*HandlerConfig `yaml:"decorators"`  // 装饰器配置
	Endorsers   PluginMapping    `yaml:"endorsers"`   // 背书者插件映射
	Validators  PluginMapping    `yaml:"validators"`  // 验证者插件映射
}

// PluginMapping stores a map between chaincode id to plugin config
type PluginMapping map[string]*HandlerConfig

// HandlerConfig 定义插件或编译处理程序的配置
type HandlerConfig struct {
	Name    string `yaml:"name"`
	Library string `yaml:"library"`
}

// LoadConfig 加载配置信息, 配置注册表的工厂方法和插件。
// 返回值：
//   - Config：Config 结构体，表示配置信息
//   - error：加载配置时可能发生的错误
func LoadConfig() (Config, error) {
	var authFilters, decorators []*HandlerConfig
	// 从 viper 中获取 "peer.handlers.authFilters" 的值，并将其解码为 []*HandlerConfig 类型的 authFilters 变量
	if err := mapstructure.Decode(viper.Get("peer.handlers.authFilters"), &authFilters); err != nil {
		return Config{}, err
	}

	// 从 viper 中获取 "peer.handlers.decorators" 的值，并将其解码为 []*HandlerConfig 类型的 decorators 变量
	if err := mapstructure.Decode(viper.Get("peer.handlers.decorators"), &decorators); err != nil {
		return Config{}, err
	}

	// 背书, 验证器
	endorsers, validators := make(PluginMapping), make(PluginMapping)
	e := viper.GetStringMap("peer.handlers.endorsers")
	// 遍历 "peer.handlers.endorsers" 的键，并根据键获取相应的值，构建 endorsers 映射
	for k := range e {
		name := viper.GetString("peer.handlers.endorsers." + k + ".name")
		library := viper.GetString("peer.handlers.endorsers." + k + ".library")
		endorsers[k] = &HandlerConfig{Name: name, Library: library}
	}

	v := viper.GetStringMap("peer.handlers.validators")
	// 遍历 "peer.handlers.validators" 的键，并根据键获取相应的值，构建 validators 映射
	for k := range v {
		name := viper.GetString("peer.handlers.validators." + k + ".name")
		library := viper.GetString("peer.handlers.validators." + k + ".library")
		validators[k] = &HandlerConfig{Name: name, Library: library}
	}

	// 返回构建的 Config 结构体, 配置了注册表的工厂方法和插件。
	return Config{
		AuthFilters: authFilters, // 认证过滤器配置
		Decorators:  decorators,  // 装饰器配置
		Endorsers:   endorsers,   // 背书者插件映射
		Validators:  validators,  // 验证者插件映射
	}, nil
}
