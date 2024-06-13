package gm

import (
	"fmt"

	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/hyperledger/fabric/bccsp"
)

// gmsm2KeyGenerator 定义国密SM2 keygen 结构体，实现 KeyGenerator 接口
type gmsm2KeyGenerator struct {
}

// KeyGen sm2生成非对称密钥
func (gm *gmsm2KeyGenerator) KeyGen(opts bccsp.KeyGenOpts) (k bccsp.Key, err error) {
	// 调用 SM2的注册证书方法
	privKey, err := sm2.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("生成GMSM2密钥失败  [%s]", err)
	}

	return &gmsm2PrivateKey{privKey}, nil
}

// gmsm4KeyGenerator 定义国密SM4 keygen 结构体，实现 KeyGenerator 接口
type gmsm4KeyGenerator struct {
	length int
}

// KeyGen sm4生成对称密钥
func (gm *gmsm4KeyGenerator) KeyGen(opts bccsp.KeyGenOpts) (k bccsp.Key, err error) {
	// 使用一个随机数做密钥
	lowLevelKey, err := GetRandomBytes(int(gm.length))
	if err != nil {
		return nil, fmt.Errorf("生成GMSM4失败 %d key [%s]", gm.length, err)
	}

	return &gmsm4PrivateKey{lowLevelKey, false}, nil
}
