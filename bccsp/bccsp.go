package bccsp

import (
	"crypto"
	"hash"
)

// Key 表示加密密钥
type Key interface {

	// Bytes 将此键转换为其字节表示形式，
	// 如果允许此操作。
	Bytes() ([]byte, error)

	// SKI 返回此密钥的主题密钥标识符。
	SKI() []byte

	// Symmetric 如果此密钥是对称密钥，则返回true，
	// false是这个键是不对称的
	Symmetric() bool

	// Private 如果此密钥是私钥，则返回true，
	// 否则为假。
	Private() bool

	// PublicKey 返回非对称公钥/私钥对的相应公钥部分。
	// 此方法在对称密钥方案中返回错误。
	PublicKey() (Key, error)

	//Key 返回非对称公钥/私钥
	Key() interface{}
}

// KeyGenOpts 包含使用CSP生成密钥的选项。
type KeyGenOpts interface {

	// Algorithm 返回密钥生成算法标识符 (要使用)。
	Algorithm() string

	// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
	// false otherwise.
	Ephemeral() bool
}

// KeyDerivOpts 包含使用CSP进行密钥派生的选项。
type KeyDerivOpts interface {

	// Algorithm 返回密钥派生算法标识符 (要使用)。
	Algorithm() string

	// Ephemeral 如果要派生的密钥必须是短暂的，则返回true，
	// 否则为假。
	Ephemeral() bool
}

// KeyImportOpts 包含用于使用CSP导入密钥的原材料的选项。
type KeyImportOpts interface {

	// Algorithm 返回密钥导入算法标识符 (要使用)。
	Algorithm() string

	// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
	// 否则为假。
	Ephemeral() bool
}

// HashOpts 包含使用CSP进行散列的选项。
type HashOpts interface {

	// Algorithm 返回哈希算法标识符 (要使用)。
	Algorithm() string
}

// SignerOpts 包含用于使用CSP进行签名的选项。
type SignerOpts interface {
	crypto.SignerOpts
}

// EncrypterOpts 包含使用CSP进行加密的选项。
type EncrypterOpts interface{}

// DecrypterOpts 包含使用CSP进行解密的选项。
type DecrypterOpts interface{}

// BCCSP 是区块链加密服务提供商，它提供密码标准和算法的实现。
type BCCSP interface {

	// KeyGen 使用opts选择参数生成密钥。
	KeyGen(opts KeyGenOpts) (k Key, err error)

	// KeyDeriv 密钥派生方法，根据已有密钥，通过密码学方法衍生得到新密钥
	KeyDeriv(k Key, opts KeyDerivOpts) (dk Key, err error)

	// KeyImport 密钥导入方法，将原始二进制字节数据转换为指定密钥类型
	KeyImport(raw interface{}, opts KeyImportOpts) (k Key, err error)

	// GetKey 获取密钥，根据密钥标识符SKI查找密钥
	GetKey(ski []byte) (k Key, err error)

	// Hash 使用选项选项散列hash消息msg。如果opts为nil，则将使用默认哈希函数。
	Hash(msg []byte, opts HashOpts) (hash []byte, err error)

	// GetHash 使用选项返回hash.Hash的实例。如果opts为nil，则返回默认哈希函数。
	GetHash(opts HashOpts) (h hash.Hash, err error)

	// Sign 使用密钥k签署摘要。签名（对大数据签名需要先做hash摘要）。(参数opts无用)
	Sign(k Key, digest []byte, opts SignerOpts) (signature []byte, err error)

	// Verify 验签。根据密钥k和摘要验证签名。(参数opts无用)
	Verify(k Key, signature, digest []byte, opts SignerOpts) (valid bool, err error)

	// Encrypt 加密。使用密钥k加密明文。
	Encrypt(k Key, plaintext []byte, opts EncrypterOpts) (ciphertext []byte, err error)

	// Decrypt 密钥。使用密钥k解密密文。
	Decrypt(k Key, ciphertext []byte, opts DecrypterOpts) (plaintext []byte, err error)
}
