package gm

import (
	"crypto/elliptic"
	"fmt"
	"hash"

	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
)

// config 算法配置。椭圆曲线、hash算法、对称和非对称密钥长度
type config struct {
	ellipticCurve elliptic.Curve
	hashFunction  func() hash.Hash
	aesBitLength  int
	rsaBitLength  int
}

// setSecurityLevel 根据安全级别设置hash算法。256、384
func (conf *config) setSecurityLevel(securityLevel int, hashFamily string) (err error) {
	switch hashFamily {
	case "GMSM3":
		err = conf.setSecurityLevelGMSM3(securityLevel)
	default:
		err = fmt.Errorf("gm不支持哈希算法 [%s]", hashFamily)
	}
	return
}

// setSecurityLevelGMSM3 根据难度级别设置hash算法。256、384
func (conf *config) setSecurityLevelGMSM3(level int) (err error) {
	switch level {
	case 256:
		conf.ellipticCurve = elliptic.P256()
		conf.hashFunction = sm3.New
		conf.rsaBitLength = 2048
		conf.aesBitLength = 32
	case 384:
		conf.ellipticCurve = elliptic.P384()
		conf.hashFunction = sm3.New
		conf.rsaBitLength = 3072
		conf.aesBitLength = 32
	default:
		err = fmt.Errorf("gm不支持安全级别 [%d]", level)
	}
	return
}
