/*
Copyright IBM Corp. 2017 All Rights Reserved.

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

package sw

import (
	"crypto/ecdsa"
	"crypto/hmac"
	"errors"
	"fmt"
	"math/big"

	"github.com/hyperledger/fabric/bccsp"
)

type ecdsaPublicKeyKeyDeriver struct{}

// KeyDeriv ECDSA公钥派生
func (kd *ecdsaPublicKeyKeyDeriver) KeyDeriv(key bccsp.Key, opts bccsp.KeyDerivOpts) (bccsp.Key, error) {
	// 验证选择
	if opts == nil {
		return nil, errors.New("opts参数无效。它不能为零.")
	}

	ecdsaK := key.(*ecdsaPublicKey)

	// 重新随机化ECDSA私钥
	reRandOpts, ok := opts.(*bccsp.ECDSAReRandKeyOpts)
	if !ok {
		return nil, fmt.Errorf("提供了不受支持的 “keyderivopts” [%v]", opts)
	}

	tempSK := &ecdsa.PublicKey{
		Curve: ecdsaK.pubKey.Curve,
		X:     new(big.Int),
		Y:     new(big.Int),
	}

	k := new(big.Int).SetBytes(reRandOpts.ExpansionValue())
	one := new(big.Int).SetInt64(1)
	n := new(big.Int).Sub(ecdsaK.pubKey.Params().N, one)
	k.Mod(k, n)
	k.Add(k, one)

	// 计算临时公钥
	tempX, tempY := ecdsaK.pubKey.ScalarBaseMult(k.Bytes())
	tempSK.X, tempSK.Y = tempSK.Add(
		ecdsaK.pubKey.X, ecdsaK.pubKey.Y,
		tempX, tempY,
	)

	// 验证临时公钥是参考曲线上的有效点
	isOn := tempSK.Curve.IsOnCurve(tempSK.X, tempSK.Y)
	if !isOn {
		return nil, errors.New("临时公钥IsOnCurve检查失败.")
	}

	return &ecdsaPublicKey{tempSK}, nil
}

type ecdsaPrivateKeyKeyDeriver struct{}

// KeyDeriv ECDSA私钥派生
func (kd *ecdsaPrivateKeyKeyDeriver) KeyDeriv(key bccsp.Key, opts bccsp.KeyDerivOpts) (bccsp.Key, error) {
	// 验证选择
	if opts == nil {
		return nil, errors.New("opts参数无效。它一定不是nil.")
	}
	// 类型转换为ECDSA包装私钥
	ecdsaK := key.(*ecdsaPrivateKey)

	// 重新随机化ECDSA私钥
	reRandOpts, ok := opts.(*bccsp.ECDSAReRandKeyOpts)
	if !ok {
		return nil, fmt.Errorf("不支持 'KeyDerivOpts' 提供 [%v]", opts)
	}
	// 创建一个临时的ECDSA私钥tempSK，并将其公钥初始化为与原始私钥相同的曲线和参数
	tempSK := &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{
			Curve: ecdsaK.privKey.Curve,
			X:     new(big.Int),
			Y:     new(big.Int),
		},
		D: new(big.Int),
	}

	// 它根据opts中的扩展值计算一个随机数k
	k := new(big.Int).SetBytes(reRandOpts.ExpansionValue())
	one := new(big.Int).SetInt64(1)
	n := new(big.Int).Sub(ecdsaK.privKey.Params().N, one)
	k.Mod(k, n)
	k.Add(k, one)

	// 将其与原始私钥的D值相加，然后取模得到新的D值。这样就生成了一个新的私钥。
	tempSK.D.Add(ecdsaK.privKey.D, k)
	tempSK.D.Mod(tempSK.D, ecdsaK.privKey.PublicKey.Params().N)

	// 计算临时公钥, 并将其公钥初始化为与原始私钥相同的曲线和参数
	tempX, tempY := ecdsaK.privKey.PublicKey.ScalarBaseMult(k.Bytes())

	// 使用新的私钥`tempSK`计算临时公钥的坐标`tempX`和`tempY`。
	tempSK.PublicKey.X, tempSK.PublicKey.Y =
		tempSK.PublicKey.Add(
			ecdsaK.privKey.PublicKey.X, ecdsaK.privKey.PublicKey.Y,
			tempX, tempY,
		)

	// 验证临时公钥是否在参考曲线上
	isOn := tempSK.Curve.IsOnCurve(tempSK.PublicKey.X, tempSK.PublicKey.Y)
	if !isOn {
		return nil, errors.New("临时公钥IsOnCurve检查失败。")
	}

	return &ecdsaPrivateKey{tempSK}, nil
}

type aesPrivateKeyKeyDeriver struct {
	conf *config
}

func (kd *aesPrivateKeyKeyDeriver) KeyDeriv(k bccsp.Key, opts bccsp.KeyDerivOpts) (bccsp.Key, error) {
	// 验证选择
	if opts == nil {
		return nil, errors.New("opts参数无效。它不能为零.")
	}

	aesK := k.(*aesPrivateKey)

	switch hmacOpts := opts.(type) {
	case *bccsp.HMACTruncated256AESDeriveKeyOpts:
		mac := hmac.New(kd.conf.hashFunction, aesK.privKey)
		mac.Write(hmacOpts.Argument())
		return &aesPrivateKey{mac.Sum(nil)[:kd.conf.aesBitLength], false}, nil

	case *bccsp.HMACDeriveKeyOpts:
		mac := hmac.New(kd.conf.hashFunction, aesK.privKey)
		mac.Write(hmacOpts.Argument())
		return &aesPrivateKey{mac.Sum(nil), true}, nil

	default:
		return nil, fmt.Errorf("提供了不受支持的 “keyderivopts” [%v]", opts)
	}
}
