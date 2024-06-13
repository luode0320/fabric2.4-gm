package gm

import (
	"crypto/elliptic"
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/pkg/errors"
)

type gmsm2PrivateKey struct {
	privKey *sm2.PrivateKey
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *gmsm2PrivateKey) Bytes() (raw []byte, err error) {
	raw, err = x509.MarshalSm2PrivateKey(k.privKey, nil)
	if err != nil {
		return nil, fmt.Errorf("序列化密钥失败 [%s]", err)
	}
	return
}

// SKI 返回此密钥的主题密钥标识符。
func (k *gmsm2PrivateKey) SKI() (ski []byte) {
	if k.privKey == nil {
		return nil
	}

	//Marshall公钥
	raw := elliptic.Marshal(k.privKey.Curve, k.privKey.PublicKey.X, k.privKey.PublicKey.Y)

	// 散列它
	hash := sm3.New()
	hash.Write(raw)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥不对称，则为false
func (k *gmsm2PrivateKey) Symmetric() bool {
	return false
}

// Private 如果此密钥是私钥，则返回true，
// 否则为假。
func (k *gmsm2PrivateKey) Private() bool {
	return true
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *gmsm2PrivateKey) PublicKey() (bccsp.Key, error) {
	return &gmsm2PublicKey{&k.privKey.PublicKey}, nil
}

// Key 返回公钥/私钥
func (k *gmsm2PrivateKey) Key() interface{} {
	return k.privKey
}

type gmsm2PublicKey struct {
	pubKey *sm2.PublicKey
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *gmsm2PublicKey) Bytes() (raw []byte, err error) {
	raw, err = x509.MarshalSm2PublicKey(k.pubKey)
	if err != nil {
		return nil, fmt.Errorf("序列化密钥失败 [%s]", err)
	}
	return
}

// SKI 返回此密钥的主题密钥标识符。
func (k *gmsm2PublicKey) SKI() (ski []byte) {
	if k.pubKey == nil {
		return nil
	}

	// Marshall公钥
	raw := elliptic.Marshal(k.pubKey.Curve, k.pubKey.X, k.pubKey.Y)

	// 散列它
	hash := sm3.New()
	hash.Write(raw)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥不对称，则为false
func (k *gmsm2PublicKey) Symmetric() bool {
	return false
}

// Private 如果此密钥是私钥，则返回true，
// 否则为假。
func (k *gmsm2PublicKey) Private() bool {
	return false
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *gmsm2PublicKey) PublicKey() (bccsp.Key, error) {
	return k, nil
}

// Key 返回公钥/私钥
func (k *gmsm2PublicKey) Key() interface{} {
	return k.pubKey
}

type gmsm2Decryptor struct{}

// Decrypt sm2非对称私钥解密。(参数opts无用)
func (*gmsm2Decryptor) Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) (plaintext []byte, err error) {
	// 非对称私钥解密
	return sm2.Decrypt(k.(*gmsm2PrivateKey).privKey, ciphertext)
}

// Packingsm2Key 包装sm2 key
func Packingsm2Key(key interface{}) (bccsp.Key, error) {
	switch k := key.(type) {
	case *sm2.PrivateKey:
		return &gmsm2PrivateKey{privKey: k}, nil
	case *sm2.PublicKey:
		return &gmsm2PublicKey{pubKey: k}, nil
	default:
		return nil, errors.New("密钥类型无效。它必须是*sm2.PublicKey、*sm2.PublicKey")
	}
}
