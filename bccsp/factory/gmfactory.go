package factory

import (
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/gm"
)

const (
	// GuomiBasedFactoryName 是基于软件的BCCSP实现的工厂名称
	GuomiBasedFactoryName = "GM"
)

// GMFactory 是基于guomi的BCCSP的工厂。
type GMFactory struct{}

// Name 返回此工厂的名称
func (f *GMFactory) Name() string {
	return GuomiBasedFactoryName
}

// Get 使用Opts返回BCCSP的实例。
func (f *GMFactory) Get(config *FactoryOpts) (bccsp.BCCSP, error) {
	// 验证参数
	if config == nil || config.SW == nil {
		return nil, errors.New("无效配置。它一定不是nil.")
	}

	gmOpts := config.SW
	FactoryName = config.Default

	var ks bccsp.KeyStore
	if gmOpts.FileKeystore != nil {
		// 创建密钥保存的文件夹
		fks, err := gm.NewFileBasedKeyStore(nil, gmOpts.FileKeystore.KeyStorePath, false)
		if err != nil {
			return nil, fmt.Errorf("初始化gm软件密钥存储失败: %s", err)
		}
		ks = fks
	} else {
		// 默认为DummyKeystore
		ks = gm.NewDummyKeyStore()
	}
	// 创建实例
	return gm.New(gmOpts.Security, gmOpts.Hash, ks)
}
