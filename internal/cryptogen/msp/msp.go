/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package msp

import (
	"crypto/x509"
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp/common"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/internal/cryptogen/ca"
	fabricmsp "github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const (
	CLIENT = iota
	ORDERER
	PEER
	ADMIN
)

const (
	CLIENTOU  = "client"
	PEEROU    = "peer"
	ADMINOU   = "admin"
	ORDEREROU = "orderer"
)

var nodeOUMap = map[int]string{
	CLIENT:  CLIENTOU,
	PEER:    PEEROU,
	ADMIN:   ADMINOU,
	ORDERER: ORDEREROU,
}

// GenerateLocalMSP 生成节点本地MSP
func GenerateLocalMSP(
	baseDir,
	name string,
	sans []string,
	signCA *ca.CA,
	tlsCA *ca.CA,
	nodeType int,
	nodeOUs bool,
) error {
	// 创建文件夹结构
	mspDir := filepath.Join(baseDir, "msp")
	tlsDir := filepath.Join(baseDir, "tls")

	err := createFolderStructure(mspDir, true)
	if err != nil {
		return err
	}

	err = os.MkdirAll(tlsDir, 0o755)
	if err != nil {
		return err
	}

	/*
		创建MSP标识项目
	*/
	// 获取密钥库路径
	keystore := filepath.Join(mspDir, "keystore")

	// 生成私钥
	priv, err := common.GeneratePrivateKey(keystore, common.PRIV_SK)
	if err != nil {
		return err
	}

	// 使用签名CA生成X509证书
	var ous []string
	if nodeOUs {
		ous = []string{nodeOUMap[nodeType]}
	}

	// 获取公钥
	publicKey, err := (*priv).PublicKey()
	if err != nil {
		return err
	}
	// 基于signCA证书生成签名证书
	cert, err := signCA.SignCertificate(
		filepath.Join(mspDir, "signcerts"),
		name,
		ous,
		nil,
		&publicKey,
		x509.KeyUsageDigitalSignature,
		[]x509.ExtKeyUsage{},
	)
	if err != nil {
		return err
	}

	// 将工件写入MSP文件夹
	// 签名CA证书进入cacerts
	err = x509Export(
		filepath.Join(mspDir, "cacerts", x509Filename(signCA.Name)),
		signCA.SignCert,
	)
	if err != nil {
		return err
	}
	// TLS CA证书进入tlscacerts
	err = x509Export(
		filepath.Join(mspDir, "tlscacerts", x509Filename(tlsCA.Name)),
		tlsCA.SignCert,
	)
	if err != nil {
		return err
	}

	// 如果需要，生成config.yaml
	if nodeOUs {
		exportConfig(mspDir, "cacerts/"+x509Filename(signCA.Name), true)
	}

	// 签名标识进入admincerts。
	// 这意味着签名标识
	// 这个MSP的 // 也是这个MSP的admin
	// 注意: admincerts文件夹将是
	// 无论如何都被copyadmintert清除了，但是
	// 我们现在留下一个有效的管理员
	// 单元测试
	if nodeType == ADMIN {
		err = x509Export(filepath.Join(mspDir, "admincerts", x509Filename(name)), cert)
		if err != nil {
			return err
		}
	}

	// 在TLS文件夹中生成TLS工件
	// 生成私钥
	tlsPrivKey, err := common.GeneratePrivateKey(tlsDir, common.PRIV_SK)
	if err != nil {
		return err
	}

	// 获取公钥
	publicKey, err = (*tlsPrivKey).PublicKey()
	if err != nil {
		return err
	}
	// 使用TLS CA生成X509证书
	_, err = tlsCA.SignCertificate(
		filepath.Join(tlsDir),
		name,
		nil,
		sans,
		&publicKey,
		x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment,
		[]x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	)
	if err != nil {
		return err
	}
	// 导出tlsCA证书到ca.crt
	err = x509Export(filepath.Join(tlsDir, "ca.crt"), tlsCA.SignCert)
	if err != nil {
		return err
	}

	// 重命名生成的TLS X509证书
	tlsFilePrefix := "server"
	if nodeType == CLIENT || nodeType == ADMIN {
		// todo luode 目前统一使用server, 用户目录client 改 server
		tlsFilePrefix = "server"
	}
	err = os.Rename(filepath.Join(tlsDir, x509Filename(name)),
		filepath.Join(tlsDir, tlsFilePrefix+".crt"))
	if err != nil {
		return err
	}

	err = keyExport(tlsDir, filepath.Join(tlsDir, tlsFilePrefix+".key"))
	if err != nil {
		return err
	}

	return nil
}

// GenerateVerifyingMSP 生成组织MSP
func GenerateVerifyingMSP(
	baseDir string,
	signCA,
	tlsCA *ca.CA,
	nodeOUs bool,
) error {
	// 创建msp文件夹结构并将工件写入正确的位置
	err := createFolderStructure(baseDir, false)
	if err != nil {
		return err
	}
	// 签名CA证书进入cacerts
	err = x509Export(
		filepath.Join(baseDir, "cacerts", x509Filename(signCA.Name)),
		signCA.SignCert,
	)
	if err != nil {
		return err
	}
	// TLS CA证书进入tlscacerts
	err = x509Export(
		filepath.Join(baseDir, "tlscacerts", x509Filename(tlsCA.Name)),
		tlsCA.SignCert,
	)
	if err != nil {
		return err
	}

	// 是否启用节点的组织单位, 如果需要，生成config.yaml
	if nodeOUs {
		exportConfig(baseDir, "cacerts/"+x509Filename(signCA.Name), true)
	}

	// 创建一个一次性证书作为管理证书
	// 注意: admincerts文件夹将是
	// 无论如何都被copyadmintert清除了，但是
	// 为了这个缘故，我们现在留下一个有效的管理员
	// 单元测试
	if nodeOUs {
		return nil
	}

	// 生成管理员证书, 默认 nodeOUs = true 不生成管理员证书
	ksDir := filepath.Join(baseDir, "keystore")
	err = os.Mkdir(ksDir, 0o755)
	defer os.RemoveAll(ksDir)
	if err != nil {
		return errors.WithMessage(err, "无法创建密钥库目录")
	}
	priv, err := common.GeneratePrivateKey(ksDir, common.PRIV_SK)
	if err != nil {
		return err
	}

	// 获取公钥
	publicKey, err := (*priv).PublicKey()
	if err != nil {
		return err
	}
	_, err = signCA.SignCertificate(
		filepath.Join(baseDir, "admincerts"),
		signCA.Name,
		nil,
		nil,
		&publicKey,
		x509.KeyUsageDigitalSignature,
		[]x509.ExtKeyUsage{},
	)
	if err != nil {
		return err
	}

	return nil
}

// 创建文件夹结构
func createFolderStructure(rootDir string, local bool) error {
	var folders []string
	// create admincerts, cacerts, keystore and signcerts folders
	folders = []string{
		filepath.Join(rootDir, "admincerts"),
		filepath.Join(rootDir, "cacerts"),
		filepath.Join(rootDir, "tlscacerts"),
	}
	if local {
		folders = append(folders, filepath.Join(rootDir, "keystore"),
			filepath.Join(rootDir, "signcerts"))
	}

	for _, folder := range folders {
		err := os.MkdirAll(folder, 0o755)
		if err != nil {
			return err
		}
	}

	return nil
}

// 证书统一名称格式
func x509Filename(name string) string {
	return name + "-cert.pem"
}

// x 509导出
func x509Export(path string, cert *x509.Certificate) error {
	return pemExport(path, "CERTIFICATE", cert.Raw)
}

// 密钥重命名
func keyExport(keystore, output string) error {
	return os.Rename(filepath.Join(keystore, "priv_sk"), output)
}

// pem导出
func pemExport(path, pemType string, bytes []byte) error {
	// 将pem写出到文件
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return pem.Encode(file, &pem.Block{Type: pemType, Bytes: bytes})
}

// exportConfig函数用于导出配置信息到指定的文件中。
// 输入参数：
//   - mspDir：配置文件保存的目录路径。
//   - caFile：CA证书文件的路径。
//   - enable：是否启用节点OU配置。
//
// 返回值：
//   - error：如果导出配置过程中出现错误，则返回相应的错误信息。
func exportConfig(mspDir, caFile string, enable bool) error {
	config := &fabricmsp.Configuration{
		NodeOUs: &fabricmsp.NodeOUs{
			Enable: enable, // 设置是否启用节点OU配置
			ClientOUIdentifier: &fabricmsp.OrganizationalUnitIdentifiersConfiguration{
				Certificate:                  caFile,   // 设置客户端OU标识符的证书文件路径
				OrganizationalUnitIdentifier: CLIENTOU, // 设置客户端OU标识符
			},
			PeerOUIdentifier: &fabricmsp.OrganizationalUnitIdentifiersConfiguration{
				Certificate:                  caFile, // 设置对等节点OU标识符的证书文件路径
				OrganizationalUnitIdentifier: PEEROU, // 设置对等节点OU标识符
			},
			AdminOUIdentifier: &fabricmsp.OrganizationalUnitIdentifiersConfiguration{
				Certificate:                  caFile,  // 设置管理员OU标识符的证书文件路径
				OrganizationalUnitIdentifier: ADMINOU, // 设置管理员OU标识符
			},
			OrdererOUIdentifier: &fabricmsp.OrganizationalUnitIdentifiersConfiguration{
				Certificate:                  caFile,    // 设置排序节点OU标识符的证书文件路径
				OrganizationalUnitIdentifier: ORDEREROU, // 设置排序节点OU标识符
			},
		},
	}

	configBytes, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	file, err := os.Create(filepath.Join(mspDir, "config.yaml"))
	if err != nil {
		return err
	}

	defer file.Close()
	_, err = file.WriteString(string(configBytes))

	return err
}
