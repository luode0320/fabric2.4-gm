/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package library

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/handlers/auth"
	"github.com/hyperledger/fabric/core/handlers/decoration"
	endorsement2 "github.com/hyperledger/fabric/core/handlers/endorsement/api"
	validation "github.com/hyperledger/fabric/core/handlers/validation/api"
)

var logger = flogging.MustGetLogger("core.handlers")

// Registry defines an object that looks up
// handlers by name
type Registry interface {
	// Lookup returns a handler with a given
	// registered name, or nil if does not exist
	Lookup(HandlerType) interface{}
}

// HandlerType defines custom handlers that can filter and mutate
// objects passing within the peer
type HandlerType int

const (
	// Auth handler - reject or forward proposals from clients
	Auth HandlerType = iota
	// Decoration handler - append or mutate the chaincode input
	// passed to the chaincode
	Decoration
	Endorsement
	Validation

	authPluginFactory      = "NewFilter"
	decoratorPluginFactory = "NewDecorator"
	pluginFactory          = "NewPluginFactory"
)

type registry struct {
	filters    []auth.Filter                         // 认证过滤器
	decorators []decoration.Decorator                // 装饰器
	endorsers  map[string]endorsement2.PluginFactory // 背书者插件工厂
	validators map[string]validation.PluginFactory   // 验证者插件工厂
}

var (
	once sync.Once
	reg  registry
)

// InitRegistry 创建注册表的（唯一）实例
// 输入参数：
//   - c：Config 结构体，表示配置信息
//
// 返回值：
//   - Registry：Registry 接口的实例，表示注册表
func InitRegistry(c Config) Registry {
	once.Do(func() {
		reg = registry{
			endorsers:  make(map[string]endorsement2.PluginFactory), // 背书者插件工厂
			validators: make(map[string]validation.PluginFactory),   // 验证者插件工厂
		}
		// 加载配置的处理程序
		reg.loadHandlers(c)
	})
	return &reg
}

// loadHandlers 加载配置的处理程序
// 方法接收者：
//   - r：registry 结构体的指针，表示注册表
//
// 输入参数：
//   - c：Config 结构体，表示配置信息
func (r *registry) loadHandlers(c Config) {
	// 遍历配置中的 authFilters 列表
	for _, config := range c.AuthFilters {
		// 根据配置评估模式并加载处理程序
		r.evaluateModeAndLoad(config, Auth)
	}

	// 遍历配置中的 decorators 列表
	for _, config := range c.Decorators {
		// 根据配置评估模式并加载处理程序
		r.evaluateModeAndLoad(config, Decoration)
	}

	// 遍历配置中的 endorsers 映射
	for chaincodeID, config := range c.Endorsers {
		// 根据配置评估模式并加载处理程序，传递 chaincodeID 作为额外参数
		r.evaluateModeAndLoad(config, Endorsement, chaincodeID)
	}

	// 遍历配置中的 validators 映射
	for chaincodeID, config := range c.Validators {
		// 根据配置评估模式并加载处理程序，传递 chaincodeID 作为额外参数
		r.evaluateModeAndLoad(config, Validation, chaincodeID)
	}
}

// evaluateModeAndLoad 根据提供的库路径加载共享对象
// 方法接收者：
//   - r：registry 结构体的指针，表示注册表
//
// 输入参数：
//   - c：HandlerConfig 结构体的指针，表示处理程序的配置信息
//   - handlerType：HandlerType 类型，表示处理程序的类型
//   - extraArgs：可变参数，表示额外的参数
func (r *registry) evaluateModeAndLoad(c *HandlerConfig, handlerType HandlerType, extraArgs ...string) {
	if c.Library != "" {
		// 如果提供了库路径，则加载共享对象
		r.loadPlugin(c.Library, handlerType, extraArgs...)
	} else {
		// 如果未提供库路径，则加载静态编译的处理程序
		r.loadCompiled(c.Name, handlerType, extraArgs...)
	}
}

// loadCompiled 加载静态编译的处理程序
// 方法接收者：
//   - r：registry 结构体的指针，表示注册表
//
// 输入参数：
//   - handlerFactory：string 类型，表示处理程序工厂的名称
//   - handlerType：HandlerType 类型，表示处理程序的类型
//   - extraArgs：可变参数，表示额外的参数
func (r *registry) loadCompiled(handlerFactory string, handlerType HandlerType, extraArgs ...string) {
	// 创建 HandlerLibrary 结构体的反射值
	registryMD := reflect.ValueOf(&HandlerLibrary{})

	// 根据处理程序工厂的名称获取对应的方法
	o := registryMD.MethodByName(handlerFactory)
	if !o.IsValid() {
		logger.Panicf(fmt.Sprintf("方法 %s 不是 HandlerLibrary 的方法", handlerFactory))
	}

	// 调用方法并获取返回值的接口
	inst := o.Call(nil)[0].Interface()

	// 根据处理程序的类型将处理程序添加到相应的列表中
	if handlerType == Auth {
		// 将处理程序转换为 auth.Filter 接口类型，并将其添加到 filters 列表中
		r.filters = append(r.filters, inst.(auth.Filter))
	} else if handlerType == Decoration {
		// 将处理程序转换为 decoration.Decorator 接口类型，并将其添加到 decorators 列表中
		r.decorators = append(r.decorators, inst.(decoration.Decorator))
	} else if handlerType == Endorsement {
		// 确保 extraArgs 中有且只有一个参数
		if len(extraArgs) != 1 {
			logger.Panicf("extraArgs中应为1个参数")
		}
		// 将处理程序转换为 endorsement2.PluginFactory 接口类型，并将其添加到 endorsers 映射中
		r.endorsers[extraArgs[0]] = inst.(endorsement2.PluginFactory)
	} else if handlerType == Validation {
		// 确保 extraArgs 中有且只有一个参数
		if len(extraArgs) != 1 {
			logger.Panicf("extraArgs中应为1个参数")
		}
		// 将处理程序转换为 validation.PluginFactory 接口类型，，并将其添加到 validators 映射中
		r.validators[extraArgs[0]] = inst.(validation.PluginFactory)
	}
}

// Lookup returns a list of handlers with the given
// type, or nil if none exist
func (r *registry) Lookup(handlerType HandlerType) interface{} {
	if handlerType == Auth {
		return r.filters
	} else if handlerType == Decoration {
		return r.decorators
	} else if handlerType == Endorsement {
		return r.endorsers
	} else if handlerType == Validation {
		return r.validators
	}

	return nil
}
