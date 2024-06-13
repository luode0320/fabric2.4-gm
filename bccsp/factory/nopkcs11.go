//go:build !pkcs11
// +build !pkcs11

/*
当没有显示配置使用 pkcs11 时, 才编译此代码(默认编译)
*/
package factory

import (
	"reflect"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

const pkcs11Enabled = false

// FactoryOpts 保存用于初始化工厂实现的配置信息
type FactoryOpts struct {
	Default string `json:"default" yaml:"Default"`
	// SW: 软件, 里面包含可配置的属性
	SW *SwOpts `json:"SW,omitempty" yaml:"SW,omitempty"`
}

// InitFactories 函数用于在使用工厂接口之前初始化 BCCSP 工厂。
// 如果传入的配置为 nil，则使用默认配置。
// 只有在找不到默认的 BCCSP 时才会返回错误。
// 参数：
//   - config *FactoryOpts：BCCSP 配置的指针。
//
// 返回值：
//   - error：初始化过程中的错误，如果没有错误则为 nil。
func InitFactories(config *FactoryOpts) error {
	// 使用 sync.Once 确保只初始化一次, sync.Once是Go语言中的一个同步原语，它提供了一种只执行一次的机制
	// 确保了初始化操作只会执行一次，即使多次调用也不会重复执行
	factoriesInitOnce.Do(func() {
		// 调用 initFactories 函数进行实际的初始化，并将错误保存在 factoriesInitError 变量中
		factoriesInitError = initFactories(config)
	})

	return factoriesInitError
}

// initFactories 初始化工厂
func initFactories(config *FactoryOpts) error {
	// 如果没有任何自定义配置, 则对默认选项采取一些预防措施
	if config == nil {
		config = GetDefaultOpts()
	}

	if config.Default == "" {
		config.Default = "GM"
	}

	if config.SW == nil {
		config.SW = GetDefaultOpts().SW
	}

	// 加载区块链加密服务提供商工厂
	factory := LoadFactory(config.Default)
	if factory == nil {
		return errors.Errorf("加密服务提供商 %s 无效", config.Default)
	}

	var err error
	// 这就是初始化方法了, 我们知道, 这里只是调用了 get 方法
	// 返回 BCCSP 到全局的变量中, 也就是开始的 defaultBCCSP bccsp.BCCSP
	// 很明显详细的初始化过程根本不在这里
	// 那只能在 get 方法中了, SWFactory的 get 方法中
	defaultBCCSP, err = initBCCSP(factory, config)
	if err != nil {
		return errors.Wrapf(err, "初始化 BCCSP 失败")
	}

	if defaultBCCSP == nil {
		return errors.Errorf("找不到默认值 `%s` BCCSP", config.Default)
	}

	return nil
}

// GetBCCSPFromOpts 返回根据输入中传递的选项创建的BCCSP。
func GetBCCSPFromOpts(config *FactoryOpts) (bccsp.BCCSP, error) {
	var f BCCSPFactory
	switch config.Default {
	case "SW":
		f = &SWFactory{}
	default:
		return nil, errors.Errorf("Could not find BCCSP, no '%s' provider", config.Default)
	}

	csp, err := f.Get(config)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not initialize BCCSP %s", f.Name())
	}
	return csp, nil
}

// StringToKeyIds 函数返回一个 DecodeHookFunc，用于将字符串转换为 pkcs11.KeyIDMapping。
// 返回值：
//   - mapstructure.DecodeHookFunc：转换函数。
func StringToKeyIds() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		return data, nil
	}
}

// LoadFactory 加载区块链加密服务提供商工厂
func LoadFactory(name string) BCCSPFactory {
	// 补充GMFactory
	factories := []BCCSPFactory{&SWFactory{}, &GMFactory{}}

	for _, factory := range factories {
		if name == factory.Name() {
			return factory
		}
	}

	return nil
}
