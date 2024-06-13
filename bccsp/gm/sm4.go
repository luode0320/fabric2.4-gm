package gm

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/Hyperledger-TWGC/tjfoc-gm/sm4"
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

// SM4Encrypt sm4结合了CBC加密和PKCS7填充
func SM4Encrypt(key, src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	// 解析密钥
	cipher, err := sm4.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// 加密
	cipher.Encrypt(dst, src)
	return dst, nil
}

// SM4Decrypt sm4结合了CBC解密和PKCS7取消填充
func SM4Decrypt(key, src []byte) ([]byte, error) {
	// 解析密钥
	cipher, err := sm4.NewCipher(key)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, len(src))
	// 解密
	cipher.Decrypt(dst, src)
	return dst, nil
}

type gmsm4Encryptor struct{}

// Encrypt sm4对称加密。(参数opts无用)
func (*gmsm4Encryptor) Encrypt(k bccsp.Key, plaintext []byte, opts bccsp.EncrypterOpts) (ciphertext []byte, err error) {
	if len(plaintext) < 16 {
		return nil, errors.New("字符需大于16字节")
	}
	// sm4对称加密
	return SM4Encrypt(k.(*gmsm4PrivateKey).privKey, plaintext)
}

type gmsm4Decryptor struct{}

// Decrypt sm4对称解密。(参数opts无用)
func (*gmsm4Decryptor) Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) (plaintext []byte, err error) {
	// 解密
	return SM4Decrypt(k.(*gmsm4PrivateKey).privKey, ciphertext)
}
