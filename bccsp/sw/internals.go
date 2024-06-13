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
	"hash"

	"github.com/hyperledger/fabric/bccsp"
)

// KeyGenerator 是一个类似BCCSP的接口，提供密钥生成算法
type KeyGenerator interface {

	// KeyGen 使用opts生成密钥。
	KeyGen(opts bccsp.KeyGenOpts) (k bccsp.Key, err error)
}

// KeyDeriver 是一个类似BCCSP的接口，提供密钥派生算法
type KeyDeriver interface {

	// KeyDeriv 使用opts从k导出密钥。
	// opts参数应该适用于所使用的原语。
	KeyDeriv(k bccsp.Key, opts bccsp.KeyDerivOpts) (dk bccsp.Key, err error)
}

// KeyImporter 是一个类似BCCSP的接口，提供关键导入算法
type KeyImporter interface {

	// KeyImport 使用opts从其原始表示中导入密钥。
	// opts参数应该适用于所使用的原语。
	KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error)
}

// Encryptor 是一个类似BCCSP的接口，提供加密算法
type Encryptor interface {

	// Encrypt 使用密钥k加密明文。
	// opts参数应适用于所使用的算法。
	Encrypt(k bccsp.Key, plaintext []byte, opts bccsp.EncrypterOpts) (ciphertext []byte, err error)
}

// Decryptor 是一个类似BCCSP的接口，提供解密算法
type Decryptor interface {

	// Decrypt 使用密钥k解密密文。
	// opts参数应适用于所使用的算法。
	Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) (plaintext []byte, err error)
}

// Signer 是一个类似BCCSP的接口，提供签名算法
type Signer interface {

	// Sign 使用密钥k签署摘要。(参数opts无用)
	//
	// 请注意，当需要较大消息的哈希签名时，
	// 调用者负责散列较大的消息并传递
	// 哈希 (作为摘要)。
	Sign(k bccsp.Key, digest []byte, opts bccsp.SignerOpts) (signature []byte, err error)
}

// Verifier 是一个类似BCCSP的接口，提供验证算法
type Verifier interface {

	// Verify 根据密钥k和摘要验证签名。(参数opts无用)
	Verify(k bccsp.Key, signature, digest []byte, opts bccsp.SignerOpts) (valid bool, err error)
}

// Hasher 是一个类似BCCSP的接口，提供哈希算法
type Hasher interface {

	// Hash 使用选项选项散列消息msg。(参数opts无用)
	Hash(msg []byte, opts bccsp.HashOpts) (hash []byte, err error)

	// GetHash 使用选项返回hash.Hash的实例。(参数opts无用)
	GetHash(opts bccsp.HashOpts) (h hash.Hash, err error)
}
