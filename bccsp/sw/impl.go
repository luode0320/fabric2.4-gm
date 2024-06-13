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
	"reflect"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("bccsp_sw")

// CSP 提供基于 BCCSP 接口的通用实现在包装纸上。
// 它可以通过提供实现来定制以下基于算法的包装器:
// KeyGenerator, KeyDeriver, KeyImporter, Encryptor, Decryptor, Signer, Verifier, Hasher.
// 每个包装器都绑定到代表选项或键的 goland 类型。
type CSP struct {
	ks bccsp.KeyStore // 这个是必须的, 之前也看到了, 就是存放密钥的目录

	// reflect.Type作为Key类型，是Go语言反射的经典使用方式
	// 这样做的目的是，把一个类型和一个CSP工具给绑定在一起。
	// 打个比方，在 NewWithParams 方法中，有这样一句AddWrapper调用
	// swbccsp.AddWrapper(reflect.TypeOf(&bccsp.ECDSAKeyGenOpts{}), &ecdsaKeyGenerator{curve: conf.ellipticCurve})
	// 这个就是将 ECCDSAKeyGenOpts 的Type，与ecdsa的密钥生成器绑定在了一起
	// 这么多密码算法的密钥生成器工具，但是 KeyGen 方法只有这么一个
	// 那么调用时具体该用哪个生成器，就靠传入的这个类型决定。
	// 我们只需要一个传入bccsp.ECDSAKeyGenOpts类型
	// KeyGen 方法里面会调用反射API获取reflect.Type接口类型，从而自 CSP 的KeyGeneratorsMap中找到ECDSA的生成器，其他的工具也是如此。
	KeyGenerators map[reflect.Type]KeyGenerator // 密钥生成器
	KeyDerivers   map[reflect.Type]KeyDeriver   // 密钥派生器
	KeyImporters  map[reflect.Type]KeyImporter  // 密钥导入器
	Encryptors    map[reflect.Type]Encryptor    // 加密器
	Decryptors    map[reflect.Type]Decryptor    // 解密器
	Signers       map[reflect.Type]Signer       // 签名器
	Verifiers     map[reflect.Type]Verifier     // 验签器
	Hashers       map[reflect.Type]Hasher       // 哈希器
}

func New(keyStore bccsp.KeyStore) (*CSP, error) {
	if keyStore == nil {
		return nil, errors.Errorf("bccsp.KeyStore 无效。它一定不是nil。")
	}
	// 加密器、解密器、签名器、验签器、哈希器、密钥生成器、密钥派生器和密钥导入器
	encryptors := make(map[reflect.Type]Encryptor)
	decryptors := make(map[reflect.Type]Decryptor)
	signers := make(map[reflect.Type]Signer)
	verifiers := make(map[reflect.Type]Verifier)
	hashers := make(map[reflect.Type]Hasher)
	keyGenerators := make(map[reflect.Type]KeyGenerator)
	keyDerivers := make(map[reflect.Type]KeyDeriver)
	keyImporters := make(map[reflect.Type]KeyImporter)

	csp := &CSP{
		keyStore,
		keyGenerators, keyDerivers, keyImporters, encryptors,
		decryptors, signers, verifiers, hashers,
	}

	return csp, nil
}

// KeyGen 使用opts生成密钥。
func (csp *CSP) KeyGen(opts bccsp.KeyGenOpts) (k bccsp.Key, err error) {
	// 验证参数
	if opts == nil {
		return nil, errors.New("Opts参数无效。它一定不是nil.")
	}
	// 根据reflect.Type得到具体的工具
	keyGenerator, found := csp.KeyGenerators[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 'KeyGenOpts' [%v]", opts)
	}
	// 工具生成密钥k
	k, err = keyGenerator.KeyGen(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts生成密钥失败 [%v]", opts)
	}
	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "存储密钥失败 [%s]", opts.Algorithm())
		}
	}
	return k, nil
}

// KeyDeriv 密钥派生方法，根据已有密钥，通过密码学方法衍生得到新密钥
func (csp *CSP) KeyDeriv(k bccsp.Key, opts bccsp.KeyDerivOpts) (dk bccsp.Key, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它一定不是nil.")
	}
	if opts == nil {
		return nil, errors.New("无效的选择。它一定不是nil.")
	}

	// 派生工具, 传入 bccsp.Key 接口实现, 如: ecdsaPrivateKey , 就会得到 ecdsaPrivateKeyKeyDeriver 派生器
	keyDeriver, found := csp.KeyDerivers[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("不支持的 'Key' 提供 [%v]", k)
	}

	// 派生工具生成新密钥
	k, err = keyDeriver.KeyDeriv(k, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts派生密钥失败 [%v]", opts)
	}

	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "存储失败 key [%s]", opts.Algorithm())
		}
	}

	return k, nil
}

// KeyImport 使用opts从其原始表示中导入密钥。
// opts参数应该适合所使用的原语。
func (csp *CSP) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	// 验证参数
	if raw == nil {
		return nil, errors.New("raw无效。它一定不是nil.")
	}
	if opts == nil {
		return nil, errors.New("无效的选择。它一定不是nil.")
	}
	// 获取导入器
	keyImporter, found := csp.KeyImporters[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 “keyimportopts” [%v]", opts)
	}
	// 导入
	k, err = keyImporter.KeyImport(raw, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts导入密钥失败 [%v]", opts)
	}

	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "使用opts存储导入密钥失败 [%v]", opts)
		}
	}

	return
}

// GetKey 返回此CSP关联到的密钥
// 主题密钥标识符ski。
func (csp *CSP) GetKey(ski []byte) (k bccsp.Key, err error) {
	k, err = csp.ks.GetKey(ski)
	if err != nil {
		return nil, errors.Wrapf(err, "无法获取的密钥SKI [%v]", ski)
	}

	return
}

// Hash 使用选项选项散列消息msg。
func (csp *CSP) Hash(msg []byte, opts bccsp.HashOpts) (digest []byte, err error) {
	// 验证参数
	if opts == nil {
		return nil, errors.New("无效的选择。它一定不是nil.")
	}
	// 获取hash器
	hasher, found := csp.Hashers[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("不支持 'HashOpt' 提供 [%v]", opts)
	}
	// 执行hash计算(参数opts无用)
	digest, err = hasher.Hash(msg, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "散列失败 opts [%v]", opts)
	}

	return
}

// GetHash 使用选项返回hash.Hash的实例。
func (csp *CSP) GetHash(opts bccsp.HashOpts) (h hash.Hash, err error) {
	// 验证参数
	if opts == nil {
		return nil, errors.New("无效的选择。它不能是零。")
	}
	// 获取hash器
	hasher, found := csp.Hashers[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 'HashOpt' [%v]", opts)
	}
	// 获取hash函数
	h, err = hasher.GetHash(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts获取哈希函数失败 [%v]", opts)
	}

	return
}

// Sign 使用密钥k签署摘要。
//
// 请注意，当需要较大消息的哈希签名时，
// 调用者负责散列较大的消息传递哈希签名 (作为摘要)。
func (csp *CSP) Sign(k bccsp.Key, digest []byte, opts bccsp.SignerOpts) (signature []byte, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它一定不是nil.")
	}
	if len(digest) == 0 {
		return nil, errors.New("摘要无效。不能为空.")
	}
	// 获取签名器
	keyType := reflect.TypeOf(k)
	signer, found := csp.Signers[keyType]
	if !found {
		return nil, errors.Errorf("不支持 'SignKey' 提供 [%s]", keyType)
	}
	// 签名
	signature, err = signer.Sign(k, digest, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "与签名失败opts [%v]", opts)
	}

	return
}

// Verify 根据密钥k和摘要验证签名
func (csp *CSP) Verify(k bccsp.Key, signature, digest []byte, opts bccsp.SignerOpts) (valid bool, err error) {
	// 验证参数
	if k == nil {
		return false, errors.New("无效密钥。它一定不是nil.")
	}
	if len(signature) == 0 {
		return false, errors.New("签名无效。不能为空。")
	}
	if len(digest) == 0 {
		return false, errors.New("摘要无效。不能为空。")
	}
	// 获取验签器
	verifier, found := csp.Verifiers[reflect.TypeOf(k)]
	if !found {
		return false, errors.Errorf("不支持 'VerifyKey' 提供 [%v]", k)
	}
	// 验签
	valid, err = verifier.Verify(k, signature, digest, opts)
	if err != nil {
		return false, errors.Wrapf(err, "验证失败opts [%v]", opts)
	}

	return
}

// Encrypt 使用密钥k加密明文。
// 参数: 加密器key的反射类型、明文、自定义配置
func (csp *CSP) Encrypt(k bccsp.Key, plaintext []byte, opts bccsp.EncrypterOpts) ([]byte, error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效的结构体的反射类型。它不能是nil。")
	}

	// 因为 NewWithParams 方法添加Wrapper时，给加密器绑定的是 aesPrivateKey 结构类型, 该结构实现了 bccsp.Key 接口
	// 因此可以被当做 bccsp.Key 传入。
	// 而在对 bccsp.Key 使用TypeOf获取Type接口类型，可以从反射包中的rtype结构中获知详细类型信息，从而自Map中取用到正确的加密器。
	encryptor, found := csp.Encryptors[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("不支持 '密钥' 提供 [%v]", k)
	}
	// 然后，再通过该加密器执行加密方法即可
	return encryptor.Encrypt(k, plaintext, opts)
}

// Decrypt 使用密钥k解密密文。
func (csp *CSP) Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) (plaintext []byte, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它不能是nil")
	}
	// 获取解密器
	decryptor, found := csp.Decryptors[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("不支持 'DecryptKey' 提供 [%v]", k)
	}
	// 解密
	plaintext, err = decryptor.Decrypt(k, ciphertext, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "解密失败 opts [%v]", opts)
	}

	return
}

// AddWrapper 将传递的类型绑定到传递的包装器。
// 请注意，该包装器必须是以下接口之一的实例：
// KeyGenerator, KeyDeriver, KeyImporter, Encryptor, Decryptor, Signer, Verifier, Hasher.
func (csp *CSP) AddWrapper(t reflect.Type, w interface{}) error {
	if t == nil {
		return errors.Errorf("类型不能为 nil")
	}
	if w == nil {
		return errors.Errorf("包装器不能为nil")
	}
	// 可以看到, 工具结构实例被以空接口的形式传进来，并且使用switch搭配x.(type)用法获取工具类型
	// 与下面相应的case进行匹配，从而将工具添加进正确的Map中。
	switch dt := w.(type) {
	case KeyGenerator:
		csp.KeyGenerators[t] = dt
	case KeyImporter:
		csp.KeyImporters[t] = dt
	case KeyDeriver:
		csp.KeyDerivers[t] = dt
	case Encryptor:
		csp.Encryptors[t] = dt
	case Decryptor:
		csp.Decryptors[t] = dt
	case Signer:
		csp.Signers[t] = dt
	case Verifier:
		csp.Verifiers[t] = dt
	case Hasher:
		csp.Hashers[t] = dt
	default:
		return errors.Errorf("包装类型无效，必须位于: KeyGenerator, KeyDeriver, KeyImporter, Encryptor, Decryptor, Signer, Verifier, Hasher")
	}
	return nil
}
