package common

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/hyperledger/fabric/bccsp/utils"
	"math/big"
	"os"
	"strings"
)

// LoadX509KeyPair 从一对文件中读取和解析公钥/私钥对。文件必须包含PEM编码的数据。
// 证书文件可以包含叶子证书之后的中间证书，从而形成一个证书链。成功返回证书。Leaf将为nil，因为不保留已解析的证书形式。
//
// @Author: zhaoruobo
// @Date: 2023/10/30
func LoadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := os.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEMBlock, err := os.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return X509KeyPair(certPEMBlock, keyPEMBlock)
}

// X509KeyPair 从一对PEM编码的数据中解析公钥/私钥对。成功返回，证书。Leaf将为nil，因为不保留已解析的证书形式。
//
// @Author: zhaoruobo
// @Date: 2023/10/30
func X509KeyPair(certPEMBlock, keyPEMBlock []byte) (tls.Certificate, error) {
	fail := func(err error) (tls.Certificate, error) { return tls.Certificate{}, err }

	var cert tls.Certificate
	var skippedBlockTypes []string
	for {
		var certDERBlock *pem.Block
		certDERBlock, certPEMBlock = pem.Decode(certPEMBlock)
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" {
			cert.Certificate = append(cert.Certificate, certDERBlock.Bytes)
		} else {
			skippedBlockTypes = append(skippedBlockTypes, certDERBlock.Type)
		}
	}

	if len(cert.Certificate) == 0 {
		if len(skippedBlockTypes) == 0 {
			return fail(errors.New("tls: failed to find any PEM data in certificate input"))
		}
		if len(skippedBlockTypes) == 1 && strings.HasSuffix(skippedBlockTypes[0], "PRIVATE KEY") {
			return fail(errors.New("tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched"))
		}
		return fail(fmt.Errorf("tls: failed to find \"CERTIFICATE\" PEM block in certificate input after skipping PEM blocks of the following types: %v", skippedBlockTypes))
	}

	skippedBlockTypes = skippedBlockTypes[:0]
	var keyDERBlock *pem.Block
	for {
		keyDERBlock, keyPEMBlock = pem.Decode(keyPEMBlock)
		if keyDERBlock == nil {
			if len(skippedBlockTypes) == 0 {
				return fail(errors.New("tls: failed to find any PEM data in key input"))
			}
			if len(skippedBlockTypes) == 1 && skippedBlockTypes[0] == "CERTIFICATE" {
				return fail(errors.New("tls: found a certificate rather than a key in the PEM for the private key"))
			}
			return fail(fmt.Errorf("tls: failed to find PEM block with type ending in \"PRIVATE KEY\" in key input after skipping PEM blocks of the following types: %v", skippedBlockTypes))
		}
		if keyDERBlock.Type == "PRIVATE KEY" || strings.HasSuffix(keyDERBlock.Type, " PRIVATE KEY") {
			break
		}
		skippedBlockTypes = append(skippedBlockTypes, keyDERBlock.Type)
	}

	var err error
	// 获取私钥
	cert.PrivateKey, err = utils.DERToPrivateKey(keyDERBlock.Bytes)
	if err != nil {
		return fail(err)
	}
	//// 私钥转ECDSA
	//cert.PrivateKey, err = ToECDSA(cert.PrivateKey)
	//if err != nil {
	//	return fail(err)
	//}
	// 我们不需要解析TLS的公钥，但我们还是这样做
	// 检查它看起来正常并与私钥匹配。
	x509Cert, err := ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fail(err)
	}

	// 使用私钥验证x509证书
	switch pub := x509Cert.PublicKey.(type) {
	case *rsa.PublicKey:
		priv, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			return fail(errors.New("tls: rsa私钥类型与公钥类型不匹配"))
		}
		if pub.N.Cmp(priv.N) != 0 {
			return fail(errors.New("tls: rsa私钥与公钥不匹配"))
		}
	case *ecdsa.PublicKey:
		pub, _ = x509Cert.PublicKey.(*ecdsa.PublicKey)
		switch pub.Curve {
		case sm2.P256Sm2():
			priv, ok := cert.PrivateKey.(*sm2.PrivateKey)
			if !ok {
				return fail(errors.New("tls: sm2私钥类型与公钥类型不匹配"))
			}
			if pub.X.Cmp(priv.X) != 0 || pub.Y.Cmp(priv.Y) != 0 {
				return fail(errors.New("tls: sm2私钥与公钥不匹配"))
			}
		default:
			priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
			if !ok {
				return fail(errors.New("tls: ecdsa私钥类型与公钥类型不匹配"))
			}
			if pub.X.Cmp(priv.X) != 0 || pub.Y.Cmp(priv.Y) != 0 {
				return fail(errors.New("tls: ecdsa私钥与公钥不匹配"))
			}
		}
	default:
		return fail(errors.New("tls: unknown public key algorithm"))
	}

	// 将原证书放入cert.leaf中返回
	cert.Leaf = x509Cert

	return cert, nil
}

func ToECDSA(privateKey interface{}) (*ecdsa.PrivateKey, error) {
	ecdsaPrivateKey := new(ecdsa.PrivateKey)

	switch key := privateKey.(type) {
	case *sm2.PrivateKey:
		// 将 SM2 的私钥转换为 ECDSA 的私钥
		ecdsaPrivateKey = &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{
				Curve: key.Curve,
				X:     key.X,
				Y:     key.Y,
			},
			D: new(big.Int).SetBytes(key.D.Bytes()),
		}

		if !ecdsaPrivateKey.Curve.IsOnCurve(ecdsaPrivateKey.PublicKey.X, ecdsaPrivateKey.PublicKey.Y) {
			return nil, errors.New("无法将SM2私钥转换为ECDSA: 生成的公钥不在曲线上")
		}
	case *ecdsa.PrivateKey:
		ecdsaPrivateKey = key
	default:
		return nil, errors.New("无法将SM2私钥转换为ECDSA: 无效的私钥类型")
	}

	return ecdsaPrivateKey, nil
}
