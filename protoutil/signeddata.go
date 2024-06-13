/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
)

// SignedData 用于表示验证签名所需的一般三元组。
// 这是为了在加密方案中通用，尽管大多数加密方案将在 Data 中包含签名身份和随机数，但这留给加密实现。
type SignedData struct {
	Data      []byte // 数据
	Identity  []byte // 签名身份
	Signature []byte // 签名
}

// ConfigUpdateEnvelopeAsSignedData returns the set of signatures for the
// ConfigUpdateEnvelope as SignedData or an error indicating why this was not
// possible.
func ConfigUpdateEnvelopeAsSignedData(ce *common.ConfigUpdateEnvelope) ([]*SignedData, error) {
	if ce == nil {
		return nil, fmt.Errorf("No signatures for nil SignedConfigItem")
	}

	result := make([]*SignedData, len(ce.Signatures))
	for i, configSig := range ce.Signatures {
		sigHeader := &common.SignatureHeader{}
		err := proto.Unmarshal(configSig.SignatureHeader, sigHeader)
		if err != nil {
			return nil, err
		}

		result[i] = &SignedData{
			Data:      bytes.Join([][]byte{configSig.SignatureHeader, ce.ConfigUpdate}, nil),
			Identity:  sigHeader.Creator,
			Signature: configSig.Signature,
		}

	}

	return result, nil
}

// EnvelopeAsSignedData 函数将给定的 Envelope 转换为 SignedData 的签名数据切片，长度为1，或返回一个指示无法进行转换的错误。
// 方法接收者：无，是一个独立的函数。
// 输入参数：
//   - env *common.Envelope，表示要转换的 Envelope 对象。
//
// 返回值：
//   - []*SignedData，表示转换后的 SignedData 签名数据切片。
//   - error，表示转换过程中的错误，如果转换成功则返回nil。
func EnvelopeAsSignedData(env *common.Envelope) ([]*SignedData, error) {
	// 检查 Envelope 是否为 nil
	if env == nil {
		return nil, fmt.Errorf("无 Envelope 信封, 无签名")
	}

	// 解析 Envelope 的 Payload 字段为 Payload 对象
	payload := &common.Payload{}
	err := proto.Unmarshal(env.Payload, payload)
	if err != nil {
		return nil, err
	}

	// 检查 Payload 的 Header 字段是否为 nil
	if payload.Header == nil {
		return nil, fmt.Errorf("缺少 Header 标题")
	}

	// 解析 Payload 的 Header 的 SignatureHeader 字段为 SignatureHeader 对象
	shdr := &common.SignatureHeader{}
	err = proto.Unmarshal(payload.Header.SignatureHeader, shdr)
	if err != nil {
		return nil, fmt.Errorf("从字节 SignatureHeader 获取签名标头失败, err: %s", err)
	}

	// 构建 SignedData 对象，并将 Envelope 的 Payload、Signature 和 SignatureHeader
	return []*SignedData{{
		Data:      env.Payload,   // 数据
		Identity:  shdr.Creator,  // 签名身份
		Signature: env.Signature, // 签名
	}}, nil
}

// LogMessageForSerializedIdentity returns a string with seriealized identity information,
// or a string indicating why the serialized identity information cannot be returned.
// Any errors are intentially returned in the return strings so that the function can be used in single-line log messages with minimal clutter.
func LogMessageForSerializedIdentity(serializedIdentity []byte) string {
	id := &msp.SerializedIdentity{}
	err := proto.Unmarshal(serializedIdentity, id)
	if err != nil {
		return fmt.Sprintf("Could not unmarshal serialized identity: %s", err)
	}
	pemBlock, _ := pem.Decode(id.IdBytes)
	if pemBlock == nil {
		// not all identities are certificates so simply log the serialized
		// identity bytes
		return fmt.Sprintf("serialized-identity=%x", serializedIdentity)
	}
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return fmt.Sprintf("Could not parse certificate: %s", err)
	}
	return fmt.Sprintf("(mspid=%s subject=%s issuer=%s serialnumber=%d)", id.Mspid, cert.Subject, cert.Issuer, cert.SerialNumber)
}

func LogMessageForSerializedIdentities(signedData []*SignedData) (logMsg string) {
	var identityMessages []string
	for _, sd := range signedData {
		identityMessages = append(identityMessages, LogMessageForSerializedIdentity(sd.Identity))
	}
	return strings.Join(identityMessages, ", ")
}
