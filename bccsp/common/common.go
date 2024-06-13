package common

import "github.com/hyperledger/fabric/bccsp"

// 外部统一调整对称非对称算法, 使用此位置引用统一调整

// AsymmetricKey 非对称密钥
func AsymmetricKey() bccsp.KeyGenOpts {
	return &bccsp.GMSM2KeyGenOpts{}
}

// AsymmetricKeyTemp 非对称密钥, 并且生成临时的key
func AsymmetricKeyTemp() bccsp.KeyGenOpts {
	return &bccsp.GMSM2KeyGenOpts{Temporary: true}
}

// AsymmetricImportPrivateKey 导入非对称私钥
func AsymmetricImportPrivateKey() bccsp.KeyImportOpts {
	return &bccsp.GMSM2PrivateKeyImportOpts{}
}

// AsymmetricImportPrivateKeyTemp 导入非对称密钥, 并且生成临时的key
func AsymmetricImportPrivateKeyTemp() bccsp.KeyImportOpts {
	return &bccsp.GMSM2PrivateKeyImportOpts{Temporary: true}
}

// SymmetricKey 对称密钥
func SymmetricKey() bccsp.KeyGenOpts {
	return &bccsp.GMSM4KeyGenOpts{}
}

// SymmetricKeyOptsTemp 对称密钥, 并且生成临时的key
func SymmetricKeyOptsTemp() bccsp.KeyGenOpts {
	return &bccsp.GMSM4KeyGenOpts{Temporary: true}
}

// Hash hash算法
func Hash() bccsp.HashOpts {
	return &bccsp.GMSM3Opts{}
}

// SignatureHashFamily  签名散列族
func SignatureHashFamily() string {
	return bccsp.GMSM3
}

// X509ImportOpt  x509导入器
func X509ImportOpt() bccsp.KeyImportOpts {
	return &bccsp.X509PublicKeyImportOpts{Temporary: true}
}
