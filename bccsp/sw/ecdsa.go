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
package sw

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/utils"
)

// signECDSA 使用ECDSA算法对消息进行签名的函数。它接受一个ECDSA私钥（k）、消息的摘要（digest），并返回对消息进行签名后的结果
func signECDSA(k *ecdsa.PrivateKey, digest []byte, opts bccsp.SignerOpts) ([]byte, error) {
	// 使用提供的私钥对消息的摘要进行签名。签名过程中会使用一个随机数生成器（rand.Reader）来生成随机数
	// 返回大整数 R 和 S 。
	// R表示在椭圆曲线上的一个点的横坐标，用于证明签名的合法性。在ECDSA算法中，
	// R 是通过对消息的哈希值应用椭圆曲线上的点乘运算得到的。
	// S 用于表示对消息的签名。
	// S 是使用私钥对消息的哈希值和 R 进行计算得到的。
	r, s, err := ecdsa.Sign(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}
	// 将签名结果中的s值转换为低位形式。ECDSA签名中的s值有两种表示方式，高位和低位。在某些情况下，需要将s值转换为低位形式
	// 对S值进行处理
	// S 是一个大整数，其取值范围是 1 到椭圆曲线的阶 N-1。
	// 在一些实现中，s 取值可能会超过 N/2，这会导致签名不可验证。
	// 因此大于 N/2 的 s 在 ToLowS方法 中会被转化为 N - s
	s, err = utils.ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}
	// 将r和s按照ASN.1编码标准进行序列化，返回字节切片
	return utils.MarshalECDSASignature(r, s)
}

// verifyECDSA 使用ECDSA算法对签名进行验证的函数。它接受一个ECDSA公钥（k）、签名（signature）、消息的摘要（digest）(参数opts无用)
func verifyECDSA(k *ecdsa.PublicKey, signature, digest []byte, opts bccsp.SignerOpts) (bool, error) {
	// 对签名进行反序列化，将其拆分为r和s值
	// 返回大整数 R 和 S 。
	// R表示在椭圆曲线上的一个点的横坐标，用于证明签名的合法性。在ECDSA算法中，
	// R 是通过对消息的哈希值应用椭圆曲线上的点乘运算得到的。
	// S 用于表示对消息的签名。
	// S 是使用私钥对消息的哈希值和 R 进行计算得到的。
	r, s, err := utils.UnmarshalECDSASignature(signature)
	if err != nil {
		return false, fmt.Errorf("失败的unmasashalling签名 [%s]", err)
	}
	// 检查s值是否为低位形式。ECDSA签名中的s值有两种表示方式，高位和低位。在某些情况下，需要确保s值为低位形式
	// 对S值进行处理
	// S 是一个大整数，其取值范围是 1 到椭圆曲线的阶 N-1。
	// 在一些实现中，s 取值可能会超过 N/2，这会导致签名不可验证。
	// 因此大于 N/2 的 s 在 ToLowS方法 中会被转化为 N - s
	lowS, err := utils.IsLowS(k, s)
	if err != nil {
		return false, err
	}

	if !lowS {
		return false, fmt.Errorf("无效的S。必须小于订单的一半 [%s][%s].", s, utils.GetCurveHalfOrdersAt(k.Curve))
	}
	// 使用ECDSA公钥、消息的摘要、r值和s值调用ecdsa.Verify函数进行签名验证
	return ecdsa.Verify(k, digest, r, s), nil
}

type ecdsaSigner struct{}

// Sign ecdsa私钥签名(参数opts无用)
func (s *ecdsaSigner) Sign(k bccsp.Key, digest []byte, opts bccsp.SignerOpts) ([]byte, error) {
	// 调用ecdsa签名
	return signECDSA(k.(*ecdsaPrivateKey).privKey, digest, opts)
}

type ecdsaPrivateKeyVerifier struct{}

// Verify ecdsa私钥验签(参数opts无用)
func (v *ecdsaPrivateKeyVerifier) Verify(k bccsp.Key, signature, digest []byte, opts bccsp.SignerOpts) (bool, error) {
	// 提取公钥调用verifyECDSA
	return verifyECDSA(&(k.(*ecdsaPrivateKey).privKey.PublicKey), signature, digest, opts)
}

type ecdsaPublicKeyKeyVerifier struct{}

// Verify ecdsa公钥验签(参数opts无用)
func (v *ecdsaPublicKeyKeyVerifier) Verify(k bccsp.Key, signature, digest []byte, opts bccsp.SignerOpts) (bool, error) {
	// 直接用公钥调用verifyECDSA
	return verifyECDSA(k.(*ecdsaPublicKey).pubKey, signature, digest, opts)
}
