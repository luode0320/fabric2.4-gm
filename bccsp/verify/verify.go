package verify

import (
	"crypto/x509"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"github.com/pkg/errors"
)

// VerifyByOpts 函数验证给定的x509证书。
//
// 输入参数：
//   - cert: *x509.Certificate 对象，要验证的证书
//   - opts: x509GM.VerifyOptions 对象，验证选项
//
// 返回值：
//   - []*x509.Certificate: x509.Certificate 对象的切片，表示验证链
//   - error: 如果证书验证无效或验证过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/27
func VerifyByOpts(cert *x509.Certificate, opts interface{}) ([]*x509.Certificate, error) {

	switch opt := opts.(type) {
	case x509.VerifyOptions:
		// 验证
		validationChains, err := cert.Verify(opt)
		if err != nil {
			return nil, errors.WithMessage(err, "证书验证无效")
		}

		// 我们只支持单个验证链；
		// 如果有多个验证链，则可能存在对标识所有者的不明确性
		if len(validationChains) != 1 {
			return nil, errors.Errorf("此 MSP 仅支持单个验证链，实际得到 %d 条", len(validationChains))
		}

		return validationChains[0], err
	case x509GM.VerifyOptions:
		// 验证
		verify, err := gm.ParseX509Certificate2Sm2(cert).Verify(opt)

		if err != nil {
			return nil, errors.WithMessage(err, "证书验证无效")
		}

		// 将gmx509 转 标准x509
		validationChains := [][]*x509.Certificate{
			make([]*x509.Certificate, 0, len(verify)),
		}
		for _, val := range verify {
			for _, val2 := range val {
				x509Certificate := val2.ToX509Certificate()
				validationChains[0] = append(validationChains[0], x509Certificate)
			}
		}

		// 我们只支持单个验证链；
		// 如果有多个验证链，则可能存在对标识所有者的不明确性
		if len(validationChains) != 1 {
			return nil, errors.Errorf("此 MSP 仅支持单个验证链，实际得到 %d 条", len(validationChains))
		}

		return validationChains[0], err
	default:
		return nil, errors.New("x509: 仅支持x509、x509GM")
	}

}

// VerifyBycert 函数验证给定的x509证书。
//
// 输入参数：
//   - cert: *x509.Certificate 对象，要验证的证书
//
// 返回值：
//   - bool: 是否验证通过
//   - error: 如果证书验证无效或验证过程中出现错误，则返回相应的错误信息
//
// @Author: 罗德
// @Date: 2023/10/27
func VerifyBycert(cert *x509.Certificate) (bool, error) {
	// 先对原始证书der内容做hash
	gmcert := gm.ParseX509Certificate2Sm2(cert)
	hash, _ := common.ComputeHASH(gmcert.RawTBSCertificate)
	gmcert.RawTBSCertificate = hash

	getDefault := factory.GetDefault()

	// 提取证书公钥
	certPubK, err := getDefault.KeyImport(gmcert, &bccsp.X509PublicKeyImportOpts{Temporary: true})
	if err != nil {
		return false, err
	}

	// 验证证书签名
	verify, err := getDefault.Verify(certPubK, gmcert.Signature, gmcert.RawTBSCertificate, nil)
	if err != nil {
		return false, err
	}

	return verify, nil
}
