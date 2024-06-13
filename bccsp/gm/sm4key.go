package gm

import (
	"errors"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/hyperledger/fabric/bccsp"
)

// gmsm4PrivateKey 定义国密 SM4 结构体，实现 bccsp Key 的接口
type gmsm4PrivateKey struct {
	privKey    []byte
	exportable bool
}

// Bytes 将此键转换为其字节表示形式，
// 如果允许此操作。
func (k *gmsm4PrivateKey) Bytes() (raw []byte, err error) {
	if k.exportable {
		return k.privKey, nil
	}

	return nil, errors.New("对称密钥不支持导出")
}

// SKI 返回此密钥的主题密钥标识符。
func (k *gmsm4PrivateKey) SKI() (ski []byte) {
	// 散列它
	hash := sm3.New()
	hash.Write([]byte{0x01})
	hash.Write(k.privKey)
	return hash.Sum(nil)
}

// Symmetric 如果此密钥是对称密钥，则返回true，
// 如果此密钥不对称，则为false
func (k *gmsm4PrivateKey) Symmetric() bool {
	return true
}

// Private 如果此密钥是私钥，则返回true，
// 否则为假。
func (k *gmsm4PrivateKey) Private() bool {
	return true
}

// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
// 此方法在对称密钥方案中返回错误。
func (k *gmsm4PrivateKey) PublicKey() (bccsp.Key, error) {
	return nil, errors.New("无法对对称密钥调用此方法")
}

// Key 返回公钥/私钥
func (k *gmsm4PrivateKey) Key() interface{} {
	return k.privKey
}
