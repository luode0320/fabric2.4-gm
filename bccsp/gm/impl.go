package gm

import (
	"hash"
	"reflect"

	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

var (
	logger = flogging.MustGetLogger("bccsp_gm")
)

// CSP 定义国密算法结构体
type CSP struct {
	ks bccsp.KeyStore // 这个是必须的, 之前也看到了, 就是存放密钥的目录

	KeyGenerators map[reflect.Type]KeyGenerator // 密钥生成器
	KeyDerivers   map[reflect.Type]KeyDeriver   // 密钥派生器(国密无派生器)
	KeyImporters  map[reflect.Type]KeyImporter  // 密钥导入器
	Encryptors    map[reflect.Type]Encryptor    // 加密器
	Decryptors    map[reflect.Type]Decryptor    // 解密器
	Signers       map[reflect.Type]Signer       // 签名器
	Verifiers     map[reflect.Type]Verifier     // 验签器
	Hashers       map[reflect.Type]Hasher       // 哈希器
}

// NewDefaultSecurityLevel 返回基于软件的BCCSP的新实例
// 在安全级别256，哈希系列GMSM3和使用FolderBasedKeyStore作为密钥库。
func NewDefaultSecurityLevel(keyStorePath string) (bccsp.BCCSP, error) {
	ks := &fileBasedKeyStore{}
	if err := ks.Init(nil, keyStorePath, false); err != nil {
		return nil, errors.Wrapf(err, "Failed initializing key store at [%v]", keyStorePath)
	}

	return New(256, "GMSM3", ks)
}

// NewDefaultSecurityLevelWithKeystore 返回基于软件的BCCSP的新实例
// 在安全级别256，散列族GMSM3和使用传递的密钥库。
func NewDefaultSecurityLevelWithKeystore(keyStore bccsp.KeyStore) (bccsp.BCCSP, error) {
	return New(256, "GMSM3", keyStore)
}

// New 实例化 返回支持国密算法的 bccsp.BCCSP
func New(securityLevel int, hashFamily string, keyStore bccsp.KeyStore) (bccsp.BCCSP, error) {
	// 初始化国密hash算法、配置
	conf := &config{}
	err := conf.setSecurityLevel(securityLevel, hashFamily)
	if err != nil {
		return nil, errors.Wrapf(err, "在初始化配置失败 [%v,%v]", securityLevel, hashFamily)
	}

	// 检查密钥库
	if keyStore == nil {
		return nil, errors.Errorf("无效的bccsp.KeyStore实例。它必须不同于nil.")
	}

	// 加密器、解密器、签名器、验签器、哈希器、密钥生成器、密钥派生器和密钥导入器
	gmbccsp := &CSP{
		ks:            keyStore,
		Encryptors:    make(map[reflect.Type]Encryptor),
		Decryptors:    make(map[reflect.Type]Decryptor),
		Signers:       make(map[reflect.Type]Signer),
		Verifiers:     make(map[reflect.Type]Verifier),
		Hashers:       make(map[reflect.Type]Hasher),
		KeyGenerators: make(map[reflect.Type]KeyGenerator),
		KeyDerivers:   make(map[reflect.Type]KeyDeriver),
		KeyImporters:  make(map[reflect.Type]KeyImporter),
	}

	// 设置加密器
	gmbccsp.Encryptors[reflect.TypeOf(&gmsm4PrivateKey{})] = &gmsm4Encryptor{} //sm4 对称加密选项

	// 设置解密器
	gmbccsp.Decryptors[reflect.TypeOf(&gmsm4PrivateKey{})] = &gmsm4Decryptor{} //sm4 对称解密选项
	gmbccsp.Decryptors[reflect.TypeOf(&gmsm2PrivateKey{})] = &gmsm2Decryptor{} //sm2 非对称解密选项

	// 设置签名者
	gmbccsp.Signers[reflect.TypeOf(&gmsm2PrivateKey{})] = &gmsm2Signer{} //sm2 非对称国密签名

	// 设置验签器
	gmbccsp.Verifiers[reflect.TypeOf(&gmsm2PrivateKey{})] = &gmsm2PrivateKeyVerifier{}  //sm2 非对称私钥验签
	gmbccsp.Verifiers[reflect.TypeOf(&gmsm2PublicKey{})] = &gmsm2PublicKeyKeyVerifier{} //sm2 非对称公钥验签

	// 设置散列器
	gmbccsp.Hashers[reflect.TypeOf(&bccsp.GMSM3Opts{})] = &hasher{hash: conf.hashFunction} //sm3 Hash选项

	// 设置密钥生成器
	gmbccsp.KeyGenerators[reflect.TypeOf(&bccsp.GMSM2KeyGenOpts{})] = &gmsm2KeyGenerator{}                          //sm2 非对称密钥生成器
	gmbccsp.KeyGenerators[reflect.TypeOf(&bccsp.GMSM4KeyGenOpts{})] = &gmsm4KeyGenerator{length: conf.aesBitLength} //sm2 对称密钥生成器

	// 设置密钥派生器(国密不提供派生器)

	// 设置密钥导入器
	gmbccsp.KeyImporters[reflect.TypeOf(&bccsp.GMSM4ImportKeyOpts{})] = &gmsm4ImportKeyOptsKeyImporter{}                         // sm4 第三方对称密钥导入
	gmbccsp.KeyImporters[reflect.TypeOf(&bccsp.GMSM2PrivateKeyImportOpts{})] = &gmsm2PrivateKeyImportOptsKeyImporter{}           // sm2 第三方非对称私钥导入
	gmbccsp.KeyImporters[reflect.TypeOf(&bccsp.GMSM2PublicKeyImportOpts{})] = &gmsm2PublicKeyImportOptsKeyImporter{}             // sm2 第三方非对称公钥导入
	gmbccsp.KeyImporters[reflect.TypeOf(&bccsp.X509PublicKeyImportOpts{})] = &x509PublicKeyImportOptsKeyImporter{bccsp: gmbccsp} // x509 sm2 第三方非对称公钥导入

	return gmbccsp, nil
}

// KeyGen 根据key生成选项opts生成一个key
func (csp *CSP) KeyGen(opts bccsp.KeyGenOpts) (k bccsp.Key, err error) {
	// 验证参数
	if opts == nil {
		return nil, errors.New("Opts参数无效。它一定不是nil.")
	}
	// 获取生成器
	keyGenerator, found := csp.KeyGenerators[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("不支持 'KeyGenOpts' 提供 [%v]", opts)
	}
	// 生成密钥
	k, err = keyGenerator.KeyGen(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "生成密钥失败opts [%v]", opts)
	}

	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "存储失败key [%s]", opts.Algorithm())
		}
	}

	return k, nil
}

// KeyDeriv 根据key获取选项opts从k中重新获取一个key(国密无派生器)
func (csp *CSP) KeyDeriv(k bccsp.Key, opts bccsp.KeyDerivOpts) (dk bccsp.Key, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它一定不是nil.")
	}
	if opts == nil {
		return nil, errors.New("无效的选择。它一定不是nil.")
	}

	keyDeriver, found := csp.KeyDerivers[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("不支持 'Key' 提供 [%v]", k)
	}

	k, err = keyDeriver.KeyDeriv(k, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts派生密钥失败 [%v]", opts)
	}

	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed storing key [%s]", opts.Algorithm())
		}
	}

	return k, nil
}

// KeyImport 根据key导入选项opts从一个key原始的数据中导入一个key
func (csp *CSP) KeyImport(raw interface{}, opts bccsp.KeyImportOpts) (k bccsp.Key, err error) {
	// 验证参数
	if raw == nil {
		return nil, errors.New("raw无效。它一定不是nil.")
	}
	if opts == nil {
		return nil, errors.New("无效的选择。它一定不是nil.")
	}
	// 获取密钥导入器
	keyImporter, found := csp.KeyImporters[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("不支持 'KeyImportOpts' 提供 [%v]", opts)
	}
	// 导入密钥, 解析导入密钥
	k, err = keyImporter.KeyImport(raw, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "解析导入密钥失败")
	}

	// 如果密钥不是临时的，则存储它。
	if !opts.Ephemeral() {
		// 存储密钥
		err = csp.ks.StoreKey(k)
		if err != nil {
			return nil, errors.Wrapf(err, "存储导入的密钥失败opts [%v]", opts)
		}
	}

	return
}

// GetKey 根据SKI返回与该接口实例有联系的key
func (csp *CSP) GetKey(ski []byte) (k bccsp.Key, err error) {
	// 获取密钥
	k, err = csp.ks.GetKey(ski)
	if err != nil {
		return nil, errors.Wrapf(err, "无法获取的密钥 SKI [%v]", ski)
	}

	return
}

// Hash 根据哈希选项opts哈希一个消息msg，如果opts为空，则使用默认选项
func (csp *CSP) Hash(msg []byte, opts bccsp.HashOpts) (digest []byte, err error) {
	// 验证参数
	if opts == nil {
		return nil, errors.New("无效的选择。它不能是零。")
	}
	// 获取hash器
	hasher, found := csp.Hashers[reflect.TypeOf(opts)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 'HashOpt' [%v]", opts)
	}
	// 计算hash(参数opts无用)
	digest, err = hasher.Hash(msg, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "与opts哈希失败 [%v]", opts)
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

// Sign 使用k对digest进行签名，注意如果需要对一个特别大的消息的hash值进行签名，调用者则负责对该特别大的消息进行hash后将其作为digest传入
// (参数opts无用)
func (csp *CSP) Sign(k bccsp.Key, digest []byte, opts bccsp.SignerOpts) (signature []byte, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它不能是零。")
	}
	if len(digest) == 0 {
		return nil, errors.New("摘要无效。不能为空。")
	}
	// 获取签名器
	signer, found := csp.Signers[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("提供的密钥类型不支持 [%s]", k)
	}
	// 签名
	signature, err = signer.Sign(k, digest, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts签名失败 [%v]", opts)
	}

	return
}

// Verify 根据鉴定者选项opts，通过对比k和digest，鉴定签名
func (csp *CSP) Verify(k bccsp.Key, signature, digest []byte, opts bccsp.SignerOpts) (valid bool, err error) {
	// 验证参数
	if k == nil {
		return false, errors.New("无效密钥。它不能是nil。")
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
		return false, errors.Errorf("提供了不受支持的 “密钥” [%v]", k)
	}
	// 验签
	valid, err = verifier.Verify(k, signature, digest, opts)
	if err != nil {
		return false, errors.Wrapf(err, "使用opts验证失败 [%v]", opts)
	}

	return
}

// Encrypt 根据加密者选项opts，使用k加密plaintext。(参数opts无用)
func (csp *CSP) Encrypt(k bccsp.Key, plaintext []byte, opts bccsp.EncrypterOpts) (ciphertext []byte, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它不能是nil。")
	}
	// 获取加密器
	encryptor, found := csp.Encryptors[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 “密钥” [%v]", k)
	}
	// 加密。(参数opts无用)
	return encryptor.Encrypt(k, plaintext, opts)
}

// Decrypt 根据解密者选项opts，使用k对ciphertext进行解密
func (csp *CSP) Decrypt(k bccsp.Key, ciphertext []byte, opts bccsp.DecrypterOpts) (plaintext []byte, err error) {
	// 验证参数
	if k == nil {
		return nil, errors.New("无效密钥。它不能是nil。")
	}
	// 获取解密器
	decryptor, found := csp.Decryptors[reflect.TypeOf(k)]
	if !found {
		return nil, errors.Errorf("提供了不受支持的 “密钥” [%v]", k)
	}
	// 解密
	plaintext, err = decryptor.Decrypt(k, ciphertext, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "使用opts解密失败 [%v]", opts)
	}

	return
}
