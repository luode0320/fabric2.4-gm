/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package factory

import (
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/sw"
	"github.com/pkg/errors"
)

const (
	// SoftwareBasedFactoryName 是基于软件的 BCCSP 实现的工厂名称
	SoftwareBasedFactoryName = "SW"
)

// SWFactory 是基于软件的BCCSP 的工厂。
type SWFactory struct{}

// Name 返回该工厂的名称(这是 BCCSPFactory 定义的接口方法)
func (f *SWFactory) Name() string {
	return SoftwareBasedFactoryName
}

// Get 使用 Opts 返回 BCCSP 的实例(这是 BCCSPFactory 定义的接口方法)
func (f *SWFactory) Get(config *FactoryOpts) (bccsp.BCCSP, error) {
	// 验证参数
	if config == nil || config.SW == nil {
		return nil, errors.New("配置无效。它一定不能为零。")
	}

	// 获取配置信息
	swOpts := config.SW
	FactoryName = config.Default

	// 创建KeyStore
	var ks bccsp.KeyStore
	switch {
	case swOpts.FileKeystore != nil:
		// 实例化基于文件的密钥目录, 通过KeyStorePath指定密钥存放路径(只创建目录, 不创建密钥文件)。
		fks, err := sw.NewFileBasedKeyStore(nil, swOpts.FileKeystore.KeyStorePath, false)
		if err != nil {
			return nil, errors.Wrapf(err, "无法初始化软件密钥存储")
		}
		ks = fks
	default:
		// 默认为临时内存密钥存储
		ks = sw.NewDummyKeyStore()
	}

	// 通过创建好的KeyStore，调用NewWithParams()方法获取swbccsp实例，该方法在BCCSP/sw/impl.go中
	return sw.NewWithParams(swOpts.Security, swOpts.Hash, ks)
}

// SwOpts 包含 SWFactory 的选项
type SwOpts struct {
	// 安全级别(如: 256位的算法和384位的算法)
	Security int `json:"security" yaml:"Security"`
	// hash算法(如: SHA2、SHA3)
	Hash string `json:"hash" yaml:"Hash"`
	// 存放密钥的路径
	FileKeystore *FileKeystoreOpts `json:"filekeystore,omitempty" yaml:"FileKeyStore,omitempty"`
}

// FileKeystoreOpts 存放密钥的路径。
type FileKeystoreOpts struct {
	KeyStorePath string `yaml:"KeyStore"`
}
