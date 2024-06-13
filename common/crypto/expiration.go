/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"bytes"
	"encoding/pem"
	"github.com/hyperledger/fabric/bccsp/common"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/pkg/errors"
)

// ExpiresAt 返回给定身份的过期时间，如果无法确定则返回零值的time.Time。
// 输入参数：
//   - identityBytes：序列化的身份。
//
// 返回值：
//   - time.Time：给定身份的过期时间，如果无法确定则返回零值的time.Time。
func ExpiresAt(identityBytes []byte) time.Time {
	// 创建SerializedIdentity对象
	sId := &msp.SerializedIdentity{}
	// 如果protobuf解析失败，则无法确定过期时间，返回零值的time.Time
	if err := proto.Unmarshal(identityBytes, sId); err != nil {
		return time.Time{}
	}
	// 调用certExpirationTime函数获取证书的过期时间
	return certExpirationTime(sId.IdBytes)
}

// certExpirationTime 返回证书的过期时间。
// 参数：
//   - pemBytes: []byte 类型，表示 PEM 编码的证书。
//
// 返回值：
//   - time.Time: 表示证书的过期时间。
func certExpirationTime(pemBytes []byte) time.Time {
	// 解码 PEM 格式的证书
	bl, _ := pem.Decode(pemBytes)
	if bl == nil {
		// 如果身份证书不是 PEM 块，则不确定过期时间
		return time.Time{}
	}

	// 解析证书
	cert, err := common.ParseCertificate(bl.Bytes)
	if err != nil {
		return time.Time{}
	}

	return cert.NotAfter
}

// MessageFunc notifies a message happened with the given format, and can be replaced with Warnf or Infof of a logger.
type MessageFunc func(format string, args ...interface{})

// Scheduler invokes f after d time, and can be replaced with time.AfterFunc.
type Scheduler func(d time.Duration, f func()) *time.Timer

// TrackExpiration 在证书过期前一周发出警告。
// 参数：
//   - tls: bool 类型，表示是否使用 TLS。
//   - serverCert: []byte 类型，表示服务器证书。
//   - clientCertChain: [][]byte 类型，表示客户端证书链。
//   - sIDBytes: []byte 类型，表示序列化的身份。
//   - info: MessageFunc 类型，表示信息函数。
//   - warn: MessageFunc 类型，表示警告函数。
//   - now: time.Time 类型，表示当前时间。
//   - s: Scheduler 类型，表示调度器。
func TrackExpiration(tls bool, serverCert []byte, clientCertChain [][]byte, sIDBytes []byte, info MessageFunc, warn MessageFunc, now time.Time, s Scheduler) {
	sID := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(sIDBytes, sID); err != nil {
		return
	}

	// 用户签名证书
	trackCertExpiration(sID.IdBytes, "用户签名证书: ", info, warn, now, s)

	if !tls {
		return
	}

	// 用户 tls 证书
	trackCertExpiration(serverCert, "用户 tls 证书: ", info, warn, now, s)

	if len(clientCertChain) == 0 || len(clientCertChain[0]) == 0 {
		return
	}

	// 节点 tls 证书
	trackCertExpiration(clientCertChain[0], "节点 tls 证书: ", info, warn, now, s)
}

// trackCertExpiration 跟踪证书的过期情况。
// 参数：
//   - rawCert: []byte 类型，表示原始证书。
//   - certRole: string 类型，表示证书的角色。
//   - info: MessageFunc 类型，表示信息函数。
//   - warn: MessageFunc 类型，表示警告函数。
//   - now: time.Time 类型，表示当前时间。
//   - sched: Scheduler 类型，表示调度器。
func trackCertExpiration(rawCert []byte, certRole string, info MessageFunc, warn MessageFunc, now time.Time, sched Scheduler) {
	// 获取证书的过期时间
	expirationTime := certExpirationTime(rawCert)
	if expirationTime.IsZero() {
		// 如果无法确定证书的过期时间，则返回
		return
	}

	// 计算距离过期的剩余时间
	timeLeftUntilExpiration := expirationTime.Sub(now)
	oneWeek := time.Hour * 24 * 7

	if timeLeftUntilExpiration < 0 {
		warn("%s 角色证书已过期", certRole)
		return
	}

	info("%s 角色证书将在 %s 过期", certRole, expirationTime)

	// 如果小于一周
	if timeLeftUntilExpiration < oneWeek {
		days := timeLeftUntilExpiration / (time.Hour * 24)
		hours := (timeLeftUntilExpiration - (days * time.Hour * 24)) / time.Hour
		warn("%s 角色证书将在 %d 天 %d 小时内过期", certRole, days, hours)
		return
	}

	timeLeftUntilOneWeekBeforeExpiration := timeLeftUntilExpiration - oneWeek

	// 在距离过期前一周时发出警告
	sched(timeLeftUntilOneWeekBeforeExpiration, func() {
		warn("%s 角色证书将在一周内过期", certRole)
	})
}

// ErrPubKeyMismatch is used by CertificatesWithSamePublicKey to indicate the two public keys mismatch
var ErrPubKeyMismatch = errors.New("public keys do not match")

// LogNonPubKeyMismatchErr logs an error which is not an ErrPubKeyMismatch error
func LogNonPubKeyMismatchErr(log func(template string, args ...interface{}), err error, cert1DER, cert2DER []byte) {
	cert1PEM := &pem.Block{Type: "CERTIFICATE", Bytes: cert1DER}
	cert2PEM := &pem.Block{Type: "CERTIFICATE", Bytes: cert2DER}
	log("Failed determining if public key of %s matches public key of %s: %s",
		string(pem.EncodeToMemory(cert1PEM)),
		string(pem.EncodeToMemory(cert2PEM)),
		err)
}

// CertificatesWithSamePublicKey returns nil if both byte slices
// are valid DER encoding of certificates with the same public key.
func CertificatesWithSamePublicKey(der1, der2 []byte) error {
	cert1canonized, err := publicKeyFromCertificate(der1)
	if err != nil {
		return err
	}

	cert2canonized, err := publicKeyFromCertificate(der2)
	if err != nil {
		return err
	}

	if bytes.Equal(cert1canonized, cert2canonized) {
		return nil
	}
	return ErrPubKeyMismatch
}

// publicKeyFromCertificate 返回给定ASN1 DER证书的公钥。
func publicKeyFromCertificate(der []byte) ([]byte, error) {
	cert, err := common.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return common.MarshalPKIXPublicKey(cert.PublicKey)
}
