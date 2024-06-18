/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/pkg/errors"
)

// MarshalOrPanic 序列化protobuf消息，如果此操作失败，则会出现恐慌
func MarshalOrPanic(pm proto.Message) []byte {
	data, err := proto.Marshal(pm)
	if err != nil {
		panic(err)
	}
	return data
}

// Marshal serializes a protobuf message.
func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}

// CreateNonceOrPanic 使用common/crypto包生成nonce
// 如果此操作失败，则会出现恐慌。
func CreateNonceOrPanic() []byte {
	nonce, err := CreateNonce()
	if err != nil {
		panic(err)
	}
	return nonce
}

// CreateNonce 使用common/crypto包生成nonce。
func CreateNonce() ([]byte, error) {
	nonce, err := getRandomNonce()
	return nonce, errors.WithMessage(err, "生成随机随机数时出错")
}

// UnmarshalEnvelopeOfType 反序列化指定类型的envelope，包括反序列化payload数据。
// 输入参数：
//   - envelope：要反序列化的envelope。
//   - headerType：指定的header类型。
//   - message：反序列化后的消息对象。
//
// 返回值：
//   - *cb.ChannelHeader：反序列化后的channel header。
//   - error：如果反序列化过程中出现错误，则返回错误；否则返回nil。
func UnmarshalEnvelopeOfType(envelope *cb.Envelope, headerType cb.HeaderType, message proto.Message) (*cb.ChannelHeader, error) {
	// 反序列化payload
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, err
	}

	// 检查payload的Header字段是否为nil
	if payload.Header == nil {
		return nil, errors.New("负载数据必须有头部 payload.Header")
	}

	// 将字节反序列化通道头部[链码类型、交易id、时间戳、通道名称、链码名称]
	chdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	// 检查channel header的Type字段是否与指定的header类型相匹配
	if chdr.Type != int32(headerType) {
		return nil, errors.Errorf("无效类型 %s, 预期 %s", cb.HeaderType(chdr.Type), headerType)
	}

	// 反序列化payload的Data字段为指定类型的消息对象
	err = proto.Unmarshal(payload.Data, message)
	err = errors.Wrapf(err, "payload.Data 反序列化消息 message 时出错 %s", headerType)
	return chdr, err
}

// ExtractEnvelopeOrPanic retrieves the requested envelope from a given block
// and unmarshals it -- it panics if either of these operations fail
func ExtractEnvelopeOrPanic(block *cb.Block, index int) *cb.Envelope {
	envelope, err := ExtractEnvelope(block, index)
	if err != nil {
		panic(err)
	}
	return envelope
}

// ExtractEnvelope 从给定的块中检索请求的envelope带有签名的有效负载,以便可以对消息进行身份验证，并对其进行解组。
// 输入参数：
//   - block：要从中提取envelope的块。
//   - index：要提取的envelope的索引。
//
// 返回值：
//   - *cb.Envelope：提取的envelope。
//   - error：如果提取过程中出现错误，则返回错误；否则返回nil。
func ExtractEnvelope(block *cb.Block, index int) (*cb.Envelope, error) {
	if block.Data == nil {
		return nil, errors.New("block块数据为nil")
	}

	envelopeCount := len(block.Data.Data)
	if index < 0 || index >= envelopeCount {
		return nil, errors.New("有效负载索引位置出现数组越界异常")
	}

	// 从块的Data字段中获取指定索引的marshaledEnvelope
	marshaledEnvelope := block.Data.Data[index]
	// 调用GetEnvelopeFromBlock方法将marshaledEnvelope反序列化为Envelope带有签名的有效负载,以便可以对消息进行身份验证对象
	envelope, err := GetEnvelopeFromBlock(marshaledEnvelope)
	// 如果反序列化过程中出现错误，则返回错误，并在错误信息中指明索引位置
	err = errors.WithMessagef(err, "blcok块数据在索引 index=%d 处没有有效负载", index)
	return envelope, err
}

// MakeChannelHeader 函数用于创建一个 ChannelHeader 通道头部。
// 输入参数：
//   - headerType：头部类型，表示通道头部的类型。
//   - version：版本号，表示通道头部的版本号。
//   - chainID：通道ID，表示通道的唯一标识符。
//   - epoch：时期，表示通道的时期信息。
//
// 返回值：
//   - *cb.ChannelHeader：创建的 ChannelHeader 对象，包含了给定的头部类型、版本号、时间戳、通道ID和时期信息。
func MakeChannelHeader(headerType cb.HeaderType, version int32, chainID string, epoch uint64) *cb.ChannelHeader {
	return &cb.ChannelHeader{
		Type:    int32(headerType),
		Version: version,
		Timestamp: &timestamp.Timestamp{
			Seconds: time.Now().Unix(),
			Nanos:   0,
		},
		ChannelId: chainID,
		Epoch:     epoch,
	}
}

// MakeSignatureHeader 创建SignatureHeader。
func MakeSignatureHeader(serializedCreatorCertChain []byte, nonce []byte) *cb.SignatureHeader {
	return &cb.SignatureHeader{
		Creator: serializedCreatorCertChain,
		Nonce:   nonce,
	}
}

// SetTxID 根据提供的签名头生成事务id
// 并在通道标头中设置TxId字段
func SetTxID(channelHeader *cb.ChannelHeader, signatureHeader *cb.SignatureHeader) {
	channelHeader.TxId = ComputeTxID(
		signatureHeader.Nonce,
		signatureHeader.Creator,
	)
}

// MakePayloadHeader 创建负载标头。
func MakePayloadHeader(ch *cb.ChannelHeader, sh *cb.SignatureHeader) *cb.Header {
	return &cb.Header{
		ChannelHeader:   MarshalOrPanic(ch),
		SignatureHeader: MarshalOrPanic(sh),
	}
}

// NewSignatureHeader 函数用于返回一个带有有效随机数的 SignatureHeader。
// 输入参数：
//   - id：身份序列化器，用于生成创建者字段。
//
// 返回值：
//   - *cb.SignatureHeader：创建的 SignatureHeader 对象，包含了给定身份序列化器生成的创建者字段和一个有效的随机数。
func NewSignatureHeader(id identity.Serializer) (*cb.SignatureHeader, error) {
	creator, err := id.Serialize()
	if err != nil {
		return nil, err
	}
	nonce, err := CreateNonce()
	if err != nil {
		return nil, err
	}

	return &cb.SignatureHeader{
		Creator: creator,
		Nonce:   nonce,
	}, nil
}

// NewSignatureHeaderOrPanic returns a signature header and panics on error.
func NewSignatureHeaderOrPanic(id identity.Serializer) *cb.SignatureHeader {
	if id == nil {
		panic(errors.New("invalid signer. cannot be nil"))
	}

	signatureHeader, err := NewSignatureHeader(id)
	if err != nil {
		panic(fmt.Errorf("failed generating a new SignatureHeader: %s", err))
	}

	return signatureHeader
}

// SignOrPanic signs a message and panics on error.
func SignOrPanic(signer identity.Signer, msg []byte) []byte {
	if signer == nil {
		panic(errors.New("invalid signer. cannot be nil"))
	}

	sigma, err := signer.Sign(msg)
	if err != nil {
		panic(fmt.Errorf("failed generating signature: %s", err))
	}
	return sigma
}

// IsConfigBlock validates whenever given block contains configuration
// update transaction
func IsConfigBlock(block *cb.Block) bool {
	envelope, err := ExtractEnvelope(block, 0)
	if err != nil {
		return false
	}

	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return false
	}

	if payload.Header == nil {
		return false
	}

	hdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return false
	}

	return cb.HeaderType(hdr.Type) == cb.HeaderType_CONFIG || cb.HeaderType(hdr.Type) == cb.HeaderType_ORDERER_TRANSACTION
}

// ChannelHeader 根据给定的*cb.Envelope返回对应的*cb.ChannelHeader。
// 如果提取过程中出现任何错误，如结构缺失或解码失败，函数将返回错误信息。
func ChannelHeader(env *cb.Envelope) (*cb.ChannelHeader, error) {
	// 检查Envelope是否为nil，若为nil则返回错误
	if env == nil {
		return nil, errors.New("无效的信封负载。不能为nil")
	}

	// 解析信封的Payload部分
	envPayload, err := UnmarshalPayload(env.Payload)
	if err != nil {
		// 返回解码Payload时遇到的错误
		return nil, err
	}

	// 验证Header是否存在
	if envPayload.Header == nil {
		return nil, errors.New("头部未设置")
	}

	// 验证ChannelHeader字段是否已设置
	if envPayload.Header.ChannelHeader == nil {
		return nil, errors.New("通道头部未设置")
	}

	// 解码ChannelHeader
	chdr, err := UnmarshalChannelHeader(envPayload.Header.ChannelHeader)
	if err != nil {
		// 当解码通道头部出错时，附带错误信息返回
		return nil, errors.WithMessage(err, "解码通道头部时出错")
	}

	// 成功返回ChannelHeader实例
	return chdr, nil
}

// ChannelID returns the Channel ID for a given *cb.Envelope.
func ChannelID(env *cb.Envelope) (string, error) {
	chdr, err := ChannelHeader(env)
	if err != nil {
		return "", errors.WithMessage(err, "error retrieving channel header")
	}

	return chdr.ChannelId, nil
}

// EnvelopeToConfigUpdate 用于从类型为CONFIG_UPDATE的信封中提取ConfigUpdateEnvelope。
func EnvelopeToConfigUpdate(configtx *cb.Envelope) (*cb.ConfigUpdateEnvelope, error) {
	// 初始化一个ConfigUpdateEnvelope实例用于存储解码结果
	configUpdateEnv := &cb.ConfigUpdateEnvelope{}

	// 使用UnmarshalEnvelopeOfType函数解码configtx信封，预期信封类型为CONFIG_UPDATE
	// 若解码成功，则继续；否则返回解码过程中遇到的错误
	_, err := UnmarshalEnvelopeOfType(configtx, cb.HeaderType_CONFIG_UPDATE, configUpdateEnv)
	if err != nil {
		return nil, err
	}

	// 解码成功后，返回填充了数据的ConfigUpdateEnvelope实例
	return configUpdateEnv, nil
}

func getRandomNonce() ([]byte, error) {
	key := make([]byte, 24)

	_, err := rand.Read(key)
	if err != nil {
		return nil, errors.Wrap(err, "error getting random bytes")
	}
	return key, nil
}
