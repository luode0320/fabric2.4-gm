/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
)

// ECDSASignature R和S是ECDSA签名算法的输出，它们用于验证签名的有效性和完整性
// R是签名的一部分，它是一个大整数，表示椭圆曲线上的一个点的x坐标。
// S也是签名的一部分，它也是一个大整数，表示签名的附加信息。
// 通过将R和S值与公钥和消息进行一系列的计算和比较，可以验证签名的有效性。如果验证成功，意味着签名是由私钥对消息进行的，并且消息没有被篡改
// 在验证签名时，需要使用签名者的公钥和消息的哈希值。通过使用椭圆曲线上的点加法和乘法运算，可以将R和S值与公钥和消息的哈希值进行比较，以验证签名的有效性
type ECDSASignature struct {
	R, S *big.Int
}

// curveHalfOrders 存储椭圆曲线（elliptic curve）和对应的半阶数（half order）
// 在密码学中，椭圆曲线上的点加法运算需要使用半阶数来确保点的阶数是一个大素数的一半。
// 半阶数是椭圆曲线上的一个特殊值，它是椭圆曲线上的一个点乘以阶数的一半。

// 使用了四种椭圆曲线：P-224、P-256、P-384和P-521。
// 对于每种椭圆曲线，它使用相应的elliptic.Curve对象来获取椭圆曲线的参数，并使用Params().N来获取椭圆曲线的阶数。
// 然后，使用new(big.Int).Rsh方法将阶数右移一位，得到半阶数
var curveHalfOrders = map[elliptic.Curve]*big.Int{
	elliptic.P224(): new(big.Int).Rsh(elliptic.P224().Params().N, 1),
	elliptic.P256(): new(big.Int).Rsh(elliptic.P256().Params().N, 1),
	elliptic.P384(): new(big.Int).Rsh(elliptic.P384().Params().N, 1),
	elliptic.P521(): new(big.Int).Rsh(elliptic.P521().Params().N, 1),
}

// GetCurveHalfOrdersAt 接受一个椭圆曲线对象c作为参数，并返回该椭圆曲线对应的半阶数
func GetCurveHalfOrdersAt(c elliptic.Curve) *big.Int {
	return big.NewInt(0).Set(curveHalfOrders[c])
}

// MarshalECDSASignature 接受两个*big.Int类型的参数r和s，分别表示ECDSA签名的r和s值。将编码后asn1的结果作为字节数组返回
func MarshalECDSASignature(r, s *big.Int) ([]byte, error) {
	// 将ECDSASignature{r, s}结构体进行ASN.1编码
	return asn1.Marshal(ECDSASignature{r, s})
}

// UnmarshalECDSASignature 接受一个字节数组raw作为参数，表示经过ASN.1编码的ECDSA签名。返回ECDSASignature的R、S
func UnmarshalECDSASignature(raw []byte) (*big.Int, *big.Int, error) {
	// 创建一个用于存储解码后的签名值
	sig := new(ECDSASignature)
	// 解码，并将解码后的结果存储到sig中
	_, err := asn1.Unmarshal(raw, sig)
	if err != nil {
		return nil, nil, fmt.Errorf("失败的unmashalling签名[%s]", err)
	}

	// 验证sig
	if sig.R == nil {
		return nil, nil, errors.New("签名无效，R必须不同于nil")
	}
	if sig.S == nil {
		return nil, nil, errors.New("无效签名，S必须不同于nil")
	}

	if sig.R.Sign() != 1 {
		return nil, nil, errors.New("签名无效，R必须大于零")
	}
	if sig.S.Sign() != 1 {
		return nil, nil, errors.New("签名无效，S必须大于零")
	}

	return sig.R, sig.S, nil
}

// SignatureToLowS 将已经签名的signature处理为低S值, 再返回签名
func SignatureToLowS(k *ecdsa.PublicKey, signature []byte) ([]byte, error) {
	// UnmarshalECDSASignature 接受一个字节数组raw作为参数，表示经过ASN.1编码的ECDSA签名。返回ECDSASignature的R、S
	r, s, err := UnmarshalECDSASignature(signature)
	if err != nil {
		return nil, err
	}
	// ToLowS 接受一个ECDSA公钥k和一个大整数s作为参数，并返回一个经过处理的低S值
	s, err = ToLowS(k, s)
	if err != nil {
		return nil, err
	}
	// MarshalECDSASignature 接受两个*big.Int类型的参数r和s，分别表示ECDSA签名的r和s值。将编码后asn1的结果作为字节数组返回
	return MarshalECDSASignature(r, s)
}

// IsLowS 检查s是否为低-S
func IsLowS(k *ecdsa.PublicKey, s *big.Int) (bool, error) {
	// curveHalfOrders 存储椭圆曲线（elliptic curve）和对应的半阶数（half order）
	halfOrder, ok := curveHalfOrders[k.Curve]
	if !ok {
		return false, fmt.Errorf("曲线无法识别 [%s]", k.Curve)
	}

	return s.Cmp(halfOrder) != 1, nil
}

// ToLowS 接受一个ECDSA公钥k和一个大整数s作为参数，并返回一个经过处理的低S值
func ToLowS(k *ecdsa.PublicKey, s *big.Int) (*big.Int, error) {
	// 调用IsLowS函数来检查给定的S值是否已经是低S值
	lowS, err := IsLowS(k, s)
	if err != nil {
		return nil, err
	}

	if !lowS {
		// 公钥k的参数中获取椭圆曲线的阶数N
		// 使用k.Params().N获取椭圆曲线的阶数
		// 使用s.Sub(k.Params().N, s)将S值设置为N-s，即将S值替换为阶数减去S值
		s.Sub(k.Params().N, s)

		return s, nil
	}

	return s, nil
}
