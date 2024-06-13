/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sw

import (
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"

	"golang.org/x/crypto/sha3"
)

// config 算法配置。椭圆曲线、hash算法、对称密钥长度
type config struct {
	ellipticCurve elliptic.Curve
	hashFunction  func() hash.Hash
	aesBitLength  int
}

// setSecurityLevel 根据安全级别设置hash算法。256、384
func (conf *config) setSecurityLevel(securityLevel int, hashFamily string) (err error) {
	switch hashFamily {
	case "SHA2":
		err = conf.setSecurityLevelSHA2(securityLevel)
	case "SHA3":
		err = conf.setSecurityLevelSHA3(securityLevel)
	default:
		err = fmt.Errorf("不支持哈希算法 [%s]", hashFamily)
	}
	return
}

// setSecurityLevelGMSM3 根据难度级别设置hash算法。256、384
func (conf *config) setSecurityLevelSHA2(level int) (err error) {
	switch level {
	case 256:
		conf.ellipticCurve = elliptic.P256()
		conf.hashFunction = sha256.New
		conf.aesBitLength = 32
	case 384:
		conf.ellipticCurve = elliptic.P384()
		conf.hashFunction = sha512.New384
		conf.aesBitLength = 32
	default:
		err = fmt.Errorf("不支持安全级别 [%d]", level)
	}
	return
}

// setSecurityLevelGMSM3 根据难度级别设置hash算法。256、384
func (conf *config) setSecurityLevelSHA3(level int) (err error) {
	switch level {
	case 256:
		conf.ellipticCurve = elliptic.P256()
		conf.hashFunction = sha3.New256
		conf.aesBitLength = 32
	case 384:
		conf.ellipticCurve = elliptic.P384()
		conf.hashFunction = sha3.New384
		conf.aesBitLength = 32
	default:
		err = fmt.Errorf("不支持安全级别 [%d]", level)
	}
	return
}
