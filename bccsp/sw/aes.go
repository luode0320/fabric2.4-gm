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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/hyperledger/fabric/bccsp"
)

// GetRandomBytes 返回len随机查找字节
func GetRandomBytes(len int) ([]byte, error) {
	if len < 0 {
		return nil, errors.New("Len必须大于 0")
	}

	buffer := make([]byte, len)

	n, err := rand.Read(buffer)
	if err != nil {
		return nil, err
	}
	if n != len {
		return nil, fmt.Errorf("缓冲区未填充。已请求 [%d], 得到了 [%d]", len, n)
	}

	return buffer, nil
}

// pkcs7Padding 填充对齐的数据
func pkcs7Padding(src []byte) []byte {
	padding := aes.BlockSize - len(src)%aes.BlockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(src, padtext...)
}

// pkcs7UnPadding 取消填充的数据
func pkcs7UnPadding(src []byte) ([]byte, error) {
	length := len(src)
	unpadding := int(src[length-1])

	if unpadding > aes.BlockSize || unpadding == 0 {
		return nil, errors.New("无效的pkcs7填充 (unpadding > aes.BlockSize || unpadding == 0)")
	}

	pad := src[len(src)-unpadding:]
	for i := 0; i < unpadding; i++ {
		if pad[i] != byte(unpadding) {
			return nil, errors.New("无效的pkcs7填充 (pad[i] != unpadding)")
		}
	}

	return src[:(length - unpadding)], nil
}

// aesCBCEncrypt 默认调用伪随机加密
func aesCBCEncrypt(key, s []byte) ([]byte, error) {
	return aesCBCEncryptWithRand(rand.Reader, key, s)
}

// aesCBCEncryptWithRand 使用AES算法和CBC模式进行加密的函数。接受一个伪随机数生成器（prng）、密钥（key）和明文（s），并返回加密后的密文
func aesCBCEncryptWithRand(prng io.Reader, key, s []byte) ([]byte, error) {
	// 首先检查明文的长度是否是AES块大小的倍数（通常是16字节）。如果明文长度不是块大小的倍数，则返回一个错误
	if len(s)%aes.BlockSize != 0 {
		return nil, errors.New("无效的明文。它必须是块大小的倍数")
	}

	// 使用提供的密钥创建一个AES密码块（cipher.Block）
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// 接下来，创建一个长度为 块大小加上明文长度 的字节切片（ciphertext）。
	// 前块大小的字节用于存储初始化向量（IV），后面的字节用于存储加密后的密文。
	ciphertext := make([]byte, aes.BlockSize+len(s))
	iv := ciphertext[:aes.BlockSize]
	// 通过调用伪随机数生成器的ReadFull方法，从伪随机数生成器中读取足够的随机字节来填充IV
	if _, err := io.ReadFull(prng, iv); err != nil {
		return nil, err
	}
	// 然后，使用AES密码块和IV创建一个CBC模式的加密器（cipher.BlockMode）
	// 加密器将使用IV作为初始向量，并对明文进行加密。
	mode := cipher.NewCBCEncrypter(block, iv)
	// 将IV和加密后的密文合并到一起，并返回加密后的密文
	mode.CryptBlocks(ciphertext[aes.BlockSize:], s)

	return ciphertext, nil
}

// aesCBCEncryptWithIV 使用IV作为初始向量，并对明文进行加密。
func aesCBCEncryptWithIV(IV []byte, key, s []byte) ([]byte, error) {
	if len(s)%aes.BlockSize != 0 {
		return nil, errors.New("无效的明文。它必须是块大小的倍数")
	}

	if len(IV) != aes.BlockSize {
		return nil, errors.New("无效的IV。它必须具有块大小的长度")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, aes.BlockSize+len(s))
	copy(ciphertext[:aes.BlockSize], IV)

	mode := cipher.NewCBCEncrypter(block, IV)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], s)

	return ciphertext, nil
}

// aesCBCDecrypt 使用AES算法和CBC模式进行解密的函数。它接受一个密钥（key）和密文（src），并返回解密后的明文。
func aesCBCDecrypt(key, src []byte) ([]byte, error) {
	// 使用提供的密钥创建一个AES密码块（cipher.Block）
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// 检查密文的长度是否小于一个块的大小。
	if len(src) < aes.BlockSize {
		return nil, errors.New("无效的密文。它必须是块大小的倍数")
	}
	// 从密文中提取出初始化向量（IV），它的长度等于一个块的大小。然后，将密文中的IV部分截取掉，只保留密文部分。
	iv := src[:aes.BlockSize]
	src = src[aes.BlockSize:]

	// 检查密文部分的长度是否是一个块的大小的倍数。
	if len(src)%aes.BlockSize != 0 {
		return nil, errors.New("无效的密文。它必须是块大小的倍数")
	}
	// 使用AES密码块和IV创建一个CBC模式的解密器（cipher.BlockMode）。解密器将使用IV作为初始向量，并对密文部分进行解密。
	mode := cipher.NewCBCDecrypter(block, iv)
	// 解密
	mode.CryptBlocks(src, src)

	return src, nil
}

// AESCBCPKCS7Encrypt 结合CBC加密和PKCS7填充
func AESCBCPKCS7Encrypt(key, src []byte) ([]byte, error) {
	// 第一个填充: 目的是让明文长度刚好是AES块大小的倍数（通常是16字节）
	tmp := pkcs7Padding(src)

	// 然后加密
	return aesCBCEncrypt(key, tmp)
}

// AESCBCPKCS7EncryptWithRand 结合CBC加密和PKCS7填充使用作为prng传递给函数
func AESCBCPKCS7EncryptWithRand(prng io.Reader, key, src []byte) ([]byte, error) {
	// 第一个填充: 目的是让明文长度刚好是AES块大小的倍数（通常是16字节）
	tmp := pkcs7Padding(src)

	// 然后伪随机加密
	return aesCBCEncryptWithRand(prng, key, tmp)
}

// AESCBCPKCS7Encrypt combines CBC encryption and PKCS7 padding, the IV used is the one passed to the function
func AESCBCPKCS7EncryptWithIV(IV []byte, key, src []byte) ([]byte, error) {
	// 第一个填充: 目的是让明文长度刚好是AES块大小的倍数（通常是16字节）
	tmp := pkcs7Padding(src)

	// 然后加密
	return aesCBCEncryptWithIV(IV, key, tmp)
}

// AESCBCPKCS7Decrypt 结合了CBC解密和PKCS7取消填充
func AESCBCPKCS7Decrypt(key, src []byte) ([]byte, error) {
	//第一次解密
	pt, err := aesCBCDecrypt(key, src)
	if err == nil {
		// 再删除加密时添加的填充数据
		return pkcs7UnPadding(pt)
	}
	return nil, err
}

type aescbcpkcs7Encryptor struct{}

// Encrypt AES加密
func (e *aescbcpkcs7Encryptor) Encrypt(k bccsp.Key, plaintext []byte, opts bccsp.EncrypterOpts) ([]byte, error) {
	// 加密选项的类型是bccsp.EncrypterOpts
	// 它可以是 bccsp.AESCBCPKCS7ModeOpts 或 bccsp.AESCBCPKCS7ModeOpts 的指针类型
	switch o := opts.(type) {
	case *bccsp.AESCBCPKCS7ModeOpts:
		// 具有PKCS7填充的CBC模式下的AES
		if len(o.IV) != 0 && o.PRNG != nil {
			return nil, errors.New("选项无效。IV或PRNG应与nil不同，或两者均为nil。")
		}

		if len(o.IV) != 0 {
			// 如果加密选项中指定了初始化向量（IV），则使用指定的IV进行加密，调用 AESCBCPKCS7EncryptWithIV 函数
			return AESCBCPKCS7EncryptWithIV(o.IV, k.(*aesPrivateKey).privKey, plaintext)
		} else if o.PRNG != nil {
			// 如果加密选项中指定了伪随机数生成器（PRNG），则使用PRNG进行加密，调用 AESCBCPKCS7EncryptWithRand 函数
			return AESCBCPKCS7EncryptWithRand(o.PRNG, k.(*aesPrivateKey).privKey, plaintext)
		}

		// 如果加密选项中既没有指定IV，也没有指定PRNG随机，则使用默认的加密方式(默认也是伪随机数的)，调用 AESCBCPKCS7Encrypt 函数
		return AESCBCPKCS7Encrypt(k.(*aesPrivateKey).privKey, plaintext)
	case bccsp.AESCBCPKCS7ModeOpts:
		// 如果加密选项是 bccsp.AESCBCPKCS7ModeOpts 类型的值，代码会将其转换为指针类型，并再次调用Encrypt函数
		return e.Encrypt(k, plaintext, &o)
	default:
		return nil, fmt.Errorf("模式无法识别 [%s]", opts)
	}
}

type aescbcpkcs7Decryptor struct{}

// Decrypt AES解密
func (*aescbcpkcs7Decryptor) Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) ([]byte, error) {
	// 检查模式
	switch opts.(type) {
	case *bccsp.AESCBCPKCS7ModeOpts, bccsp.AESCBCPKCS7ModeOpts:
		// 具有PKCS7填充的CBC模式下的AES
		return AESCBCPKCS7Decrypt(k.(*aesPrivateKey).privKey, ciphertext)
	default:
		return nil, fmt.Errorf("模式无法识别 [%s]", opts)
	}
}
