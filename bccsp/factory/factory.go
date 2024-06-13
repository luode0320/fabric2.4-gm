package factory

import (
	"sync"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

var (
	FactoryName        string      // 工厂类型
	defaultBCCSP       bccsp.BCCSP // 默认 BCCSP 实例
	factoriesInitOnce  sync.Once   // 工厂初始化同步
	factoriesInitError error       // 工厂初始化错误

	bootBCCSP         bccsp.BCCSP // 当 InitFactories 尚未被调用时（应该只发生在测试用例中），暂时使用此 BCCSP 实例
	bootBCCSPInitOnce sync.Once   // bootBCCSP的实例化由该sync.Once的Do方法执行，即只会执行一遍

	logger = flogging.MustGetLogger("bccsp")
)

// BCCSPFactory 用于获取 BCCSP 接口的实例。工厂有一个用来称呼它的名称。
type BCCSPFactory interface {

	// Name 返回该工厂的名称
	Name() string

	// Get 使用 opts 选择返回 BCCSP 的实例。
	Get(opts *FactoryOpts) (bccsp.BCCSP, error)
}

// GetDefault 返回 BCCSP 实例, 并非初始化。
func GetDefault() bccsp.BCCSP {
	// 如果默认的 BCCSP 实例为空, 则使用备用的 BCCSP 实例
	if defaultBCCSP == nil {
		logger.Debug("使用 BCCSP 之前,请调用 InitFactories(). 回退到 bootBCCSP.")
		bootBCCSPInitOnce.Do(func() {
			var err error
			// 使用 GMFactory 工厂来初始化备用的实例
			bootBCCSP, err = (&GMFactory{}).Get(GetDefaultOpts())
			if err != nil {
				panic("BCCSP 内部错误，使用 GetDefaultOpts 初始化失败!")
			}
		})
		return bootBCCSP
	}
	return defaultBCCSP
}

// initBCCSP 初始化 BCCSP 实例
func initBCCSP(f BCCSPFactory, config *FactoryOpts) (bccsp.BCCSP, error) {
	// 调用工厂给出的 get 接口, 实现生成返回 BCCSP 的实例。
	csp, err := f.Get(config)
	if err != nil {
		return nil, errors.Errorf("无法初始化 BCCSP %s [%s]", f.Name(), err)
	}

	return csp, nil
}
