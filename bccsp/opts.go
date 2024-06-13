package bccsp

const (
	// ECDSA 椭圆曲线数字签名算法 (密钥生成、导入、签名、验证)，
	// 在默认安全级别。
	// 每个BCCSP可能支持也可能不支持默认安全级别。如果不支持，则
	// 将返回一个错误。
	ECDSA = "ECDSA"

	// ECDSAP256 基于P-256曲线的椭圆曲线数字签名算法
	ECDSAP256 = "ECDSAP256"

	// ECDSAP384 基于P-384曲线的椭圆曲线数字签名算法
	ECDSAP384 = "ECDSAP384"

	// ECDSAReRand ECDSA 密钥重新随机化
	ECDSAReRand = "ECDSA_RERAND"

	// AES 默认安全级别的高级加密标准。
	// 每个BCCSP可能支持也可能不支持默认安全级别。如果不支持，则
	// 将返回一个错误。
	AES = "AES"
	// AES128 128位安全级别的高级加密标准
	AES128 = "AES128"
	// AES192 192位安全级别的高级加密标准
	AES192 = "AES192"
	// AES256 256位安全级别的高级加密标准
	AES256 = "AES256"

	// GMSM4
	GMSM4 = "GMSM4"
	// GMSM3
	GMSM3 = "GMSM3"
	// GMSM2
	GMSM2 = "GMSM2"

	// HMAC keyed-哈希消息身份验证代码
	HMAC = "HMAC"
	// HMACTruncated256 HMAC 在256位被截断。
	HMACTruncated256 = "HMAC_TRUNCATED_256"

	// SHA 使用默认族的安全哈希算法。
	// 每个BCCSP可能支持也可能不支持默认安全级别。如果不支持，则
	// 将返回一个错误。
	SHA = "SHA"

	// SHA2 是SHA2哈希族的标识符
	SHA2 = "SHA2"
	// SHA3 是SHA3哈希族的标识符
	SHA3 = "SHA3"

	// SHA256
	SHA256 = "SHA256"
	// SHA384
	SHA384 = "SHA384"
	// SHA3_256
	SHA3_256 = "SHA3_256"
	// SHA3_384
	SHA3_384 = "SHA3_384"

	// X509Certificate X509证书相关操作的标签
	X509Certificate = "X509Certificate"
)

// ECDSAKeyGenOpts 包含ECDSA密钥生成的选项。
type ECDSAKeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *ECDSAKeyGenOpts) Algorithm() string {
	return ECDSA
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAKeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

type KMSGMSM2KeyGenOpts struct {
	Temporary bool
}

func (opts *KMSGMSM2KeyGenOpts) Algorithm() string {
	return GMSM2
}

func (opts *KMSGMSM2KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

type KMSGMSM2KeyImportOpts struct {
	Temporary bool
}

func (opts *KMSGMSM2KeyImportOpts) Algorithm() string {
	return GMSM2
}

func (opts *KMSGMSM2KeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// ZHGMSM2KeyGenOpts 中环KeyGen选项
type ZHGMSM2KeyGenOpts struct {
	Temporary bool
}

func (opts *ZHGMSM2KeyGenOpts) Algorithm() string {
	return GMSM2
}

func (opts *ZHGMSM2KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// GMSM2KeyGenOpts 包含用于生成GMSM2密钥的选项。
type GMSM2KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *GMSM2KeyGenOpts) Algorithm() string {
	return GMSM2
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *GMSM2KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// GMSM4KeyGenOpts 包含用于生成GMSM2密钥的选项。
type GMSM4KeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *GMSM4KeyGenOpts) Algorithm() string {
	return GMSM4
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *GMSM4KeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// ECDSAPKIXPublicKeyImportOpts 包含以PKIX格式导入ECDSA公钥的选项
type ECDSAPKIXPublicKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *ECDSAPKIXPublicKeyImportOpts) Algorithm() string {
	return ECDSA
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAPKIXPublicKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// ECDSAPrivateKeyImportOpts 包含DER格式的ECDSA密钥导入选项
// 或PKCS #8格式。
type ECDSAPrivateKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *ECDSAPrivateKeyImportOpts) Algorithm() string {
	return ECDSA
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAPrivateKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// ECDSAGoPublicKeyImportOpts 包含从ECDSA.PublicKey导入ecdsa密钥的选项
type ECDSAGoPublicKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *ECDSAGoPublicKeyImportOpts) Algorithm() string {
	return ECDSA
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAGoPublicKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// ECDSAReRandKeyOpts 包含ECDSA密钥重新随机化的选项。
type ECDSAReRandKeyOpts struct {
	Temporary bool
	Expansion []byte
}

// Algorithm 返回密钥派生算法标识符 (要使用)。
func (opts *ECDSAReRandKeyOpts) Algorithm() string {
	return ECDSAReRand
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *ECDSAReRandKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// ExpansionValue 返回重新随机化因子
func (opts *ECDSAReRandKeyOpts) ExpansionValue() []byte {
	return opts.Expansion
}

// AESKeyGenOpts 包含用于在默认安全级别生成AES密钥的选项
type AESKeyGenOpts struct {
	Temporary bool
}

// Algorithm 返回密钥生成算法标识符 (要使用)。
func (opts *AESKeyGenOpts) Algorithm() string {
	return AES
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *AESKeyGenOpts) Ephemeral() bool {
	return opts.Temporary
}

// HMACTruncated256AESDeriveKeyOpts 包含HMAC截断选项
// 在256位密钥推导。
type HMACTruncated256AESDeriveKeyOpts struct {
	Temporary bool
	Arg       []byte
}

// Algorithm 返回密钥派生算法标识符 (要使用)。
func (opts *HMACTruncated256AESDeriveKeyOpts) Algorithm() string {
	return HMACTruncated256
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *HMACTruncated256AESDeriveKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// Argument 返回要传递给HMAC的参数
func (opts *HMACTruncated256AESDeriveKeyOpts) Argument() []byte {
	return opts.Arg
}

// HMACDeriveKeyOpts 包含用于HMAC密钥派生的选项。
type HMACDeriveKeyOpts struct {
	Temporary bool
	Arg       []byte
}

// Algorithm 返回密钥派生算法标识符 (要使用)。
func (opts *HMACDeriveKeyOpts) Algorithm() string {
	return HMAC
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *HMACDeriveKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// Argument 返回要传递给HMAC的参数
func (opts *HMACDeriveKeyOpts) Argument() []byte {
	return opts.Arg
}

// AES256ImportKeyOpts 包含用于导入AES 256密钥的选项。
type AES256ImportKeyOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *AES256ImportKeyOpts) Algorithm() string {
	return AES
}

// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *AES256ImportKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// GMSM4ImportKeyOpts  实现  bccsp.KeyImportOpts 接口
type GMSM4ImportKeyOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *GMSM4ImportKeyOpts) Algorithm() string {
	return GMSM4
}

// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *GMSM4ImportKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// GMSM2PrivateKeyImportOpts  实现  bccsp.KeyImportOpts 接口
type GMSM2PrivateKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *GMSM2PrivateKeyImportOpts) Algorithm() string {
	return GMSM2
}

// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *GMSM2PrivateKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// GMSM2PublicKeyImportOpts  实现  bccsp.KeyImportOpts 接口
type GMSM2PublicKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *GMSM2PublicKeyImportOpts) Algorithm() string {
	return GMSM2
}

// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *GMSM2PublicKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}

// HMACImportKeyOpts 包含用于导入HMAC密钥的选项。
type HMACImportKeyOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *HMACImportKeyOpts) Algorithm() string {
	return HMAC
}

// Ephemeral 如果生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *HMACImportKeyOpts) Ephemeral() bool {
	return opts.Temporary
}

// SHAOpts 包含用于计算SHA的选项。
type SHAOpts struct{}

// Algorithm 返回哈希算法标识符 (要使用)。
func (opts *SHAOpts) Algorithm() string {
	return SHA
}

// X509PublicKeyImportOpts 包含用于从x509证书导入公钥的选项
type X509PublicKeyImportOpts struct {
	Temporary bool
}

// Algorithm 返回密钥导入算法标识符 (要使用)。
func (opts *X509PublicKeyImportOpts) Algorithm() string {
	return X509Certificate
}

// Ephemeral 如果要生成的密钥必须是短暂的，则返回true，
// 否则为false。
func (opts *X509PublicKeyImportOpts) Ephemeral() bool {
	return opts.Temporary
}
