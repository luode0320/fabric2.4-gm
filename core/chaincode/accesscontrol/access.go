/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package accesscontrol

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"github.com/hyperledger/fabric/common/flogging"
	"google.golang.org/grpc"
)

var logger = flogging.MustGetLogger("chaincode.accesscontrol")

// CertAndPrivKeyPair 结构体包含一个证书和其对应的私钥，以base64格式表示。
type CertAndPrivKeyPair struct {
	// Cert是一个x509证书
	Cert []byte
	// Key是对应证书的私钥
	Key []byte
}

// Authenticator 结构体用于身份验证。
type Authenticator struct {
	mapper *certMapper // 用于管理证书哈希和名称的映射关系。
}

// Wrap 方法用于包装给定的ChaincodeSupportServer，并添加身份验证功能。
// 输入参数：
//   - srv：要包装的ChaincodeSupportServer。
//
// 返回值：
//   - pb.ChaincodeSupportServer：包装后的ChaincodeSupportServer。
func (auth *Authenticator) Wrap(srv pb.ChaincodeSupportServer) pb.ChaincodeSupportServer {
	// 调用newInterceptor函数创建一个 新的拦截器，并传入srv和auth.authenticate作为参数
	return newInterceptor(srv, auth.authenticate)
}

// NewAuthenticator 函数返回一个新的身份验证器，用于包装链码服务。
// 输入参数：
//   - ca：用于生成客户端证书和私钥的CA。
//
// 返回值：
//   - *Authenticator：新的身份验证器实例。
func NewAuthenticator(ca tlsgen.CA) *Authenticator {
	return &Authenticator{
		mapper: newCertMapper(ca.NewClientCertKeyPair),
	}
}

// Generate 方法用于生成证书和私钥对，并将证书的哈希与给定的链码名称关联起来。
// 输入参数：
//   - ccName：链码名称。
//
// 返回值：
//   - *CertAndPrivKeyPair：证书和私钥对。
//   - error：如果生成证书失败，则返回错误。
func (ac *Authenticator) Generate(ccName string) (*CertAndPrivKeyPair, error) {
	// 调用mapper.genCert方法生成证书和私钥对
	cert, err := ac.mapper.genCert(ccName)
	if err != nil {
		return nil, err
	}
	// 创建CertAndPrivKeyPair结构体实例，并将生成的私钥和证书赋值给对应字段
	return &CertAndPrivKeyPair{
		Key:  cert.Key,
		Cert: cert.Cert,
	}, nil
}

// authenticate 方法用于对链码消息进行身份验证。
// 参数：
//   - msg *pb.ChaincodeMessage：要进行身份验证的链码消息。
//   - stream grpc.ServerStream：gRPC ServerStream 实例。
//
// 返回值：
//   - error：如果身份验证过程中出现错误，则返回错误。
func (ac *Authenticator) authenticate(msg *pb.ChaincodeMessage, stream grpc.ServerStream) error {
	// 验证消息类型是否为 REGISTER
	if msg.Type != pb.ChaincodeMessage_REGISTER {
		logger.Warning("Got message", msg, "但需要 ChaincodeMessage_REGISTER 链码注册的消息")
		return errors.New("第一个消息需要是一个链码注册的消息")
	}

	// 解析消息中的 ChaincodeID, 并将 ChaincodeID 合并到 msg 中
	chaincodeID := &pb.ChaincodeID{}
	err := proto.Unmarshal(msg.Payload, chaincodeID)
	if err != nil {
		logger.Warning("并将 ChaincodeID 合并到 msg 中失败:", err)
		return err
	}
	ccName := chaincodeID.Name

	// 从grpc流中获取证书哈希值
	hash := extractCertificateHashFromContext(stream.Context())
	if len(hash) == 0 {
		errMsg := fmt.Sprintf("TLS 是活动的，但链码 %s 未发送证书", ccName)
		logger.Warning(errMsg)
		return errors.New(errMsg)
	}

	// 在映射器中查找证书哈希对应的注册名称
	registeredName := ac.mapper.lookup(certHash(hash))
	if registeredName == "" {
		errMsg := fmt.Sprintf("链码 %s 使用给定的证书哈希 %v 在注册表中找不到", ccName, hash)
		logger.Warning(errMsg)
		return errors.New(errMsg)
	}

	// 验证注册名称与链码名称是否匹配
	if registeredName != ccName {
		errMsg := fmt.Sprintf("链码 %s 使用给定的证书哈希 %v 属于不同的链码", ccName, hash)
		logger.Warning(errMsg)
		return fmt.Errorf(errMsg)
	}

	logger.Debug("链码", ccName, "'s身份验证已授权")
	return nil
}
