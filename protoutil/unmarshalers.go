/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
)

// the implicit contract of all these unmarshalers is that they
// will return a non-nil pointer whenever the error is nil

// UnmarshalBlock 将字节解组为一个Block块。
// 输入参数：
//   - encoded：要解组的字节。
//
// 返回值：
//   - *common.Block：解组后的Block。
//   - error：如果解组过程中出现错误，则返回错误；否则返回nil。
func UnmarshalBlock(encoded []byte) (*common.Block, error) {
	block := &common.Block{}
	err := proto.Unmarshal(encoded, block)
	return block, errors.Wrap(err, "反序列化区块block出错")
}

// UnmarshalChaincodeDeploymentSpec 将字节反序列化 ChaincodeDeploymentSpec 指定链码的部署对象(链码规范, 链码包)。
// 输入参数：
//   - code：要反序列化的字节。
//
// 返回值：
//   - *peer.ChaincodeDeploymentSpec：反序列化 ChaincodeDeploymentSpec 指定链码的部署对象。
//   - error：如果解组过程中出现错误，则返回错误；否则返回nil。
func UnmarshalChaincodeDeploymentSpec(code []byte) (*peer.ChaincodeDeploymentSpec, error) {
	cds := &peer.ChaincodeDeploymentSpec{}
	err := proto.Unmarshal(code, cds)
	return cds, errors.Wrap(err, "反序列化 ChaincodeDeploymentSpec 指定链码的部署对象时出错")
}

// UnmarshalChaincodeInvocationSpec 将字节反序列化链码规则(链码类型、链码名称、有效的数据如: 区块文件)
func UnmarshalChaincodeInvocationSpec(encoded []byte) (*peer.ChaincodeInvocationSpec, error) {
	cis := &peer.ChaincodeInvocationSpec{}
	err := proto.Unmarshal(encoded, cis)
	return cis, errors.Wrap(err, "反序列化链码规则出错")
}

// UnmarshalPayload 将字节反序列化为一个Payload有效负载是消息内容 (和允许签名的标头)。
// 输入参数：
//   - encoded：要解组的字节。
//
// 返回值：
//   - *common.Payload：解组后的Payload。
//   - error：如果解组过程中出现错误，则返回错误；否则返回nil。
func UnmarshalPayload(encoded []byte) (*common.Payload, error) {
	payload := &common.Payload{}
	err := proto.Unmarshal(encoded, payload)
	return payload, errors.Wrap(err, "反序列化一个Payload有效负载出错")
}

// UnmarshalEnvelope unmarshals bytes to a Envelope
func UnmarshalEnvelope(encoded []byte) (*common.Envelope, error) {
	envelope := &common.Envelope{}
	err := proto.Unmarshal(encoded, envelope)
	return envelope, errors.Wrap(err, "error unmarshalling Envelope")
}

// UnmarshalChannelHeader 将字节反序列化通道头部[链码类型、交易id、时间戳、通道名称、链码名称]
func UnmarshalChannelHeader(bytes []byte) (*common.ChannelHeader, error) {
	chdr := &common.ChannelHeader{}
	err := proto.Unmarshal(bytes, chdr)
	return chdr, errors.Wrap(err, "反序列化通道头部出错")
}

// UnmarshalChaincodeID unmarshals bytes to a ChaincodeID
func UnmarshalChaincodeID(bytes []byte) (*peer.ChaincodeID, error) {
	ccid := &peer.ChaincodeID{}
	err := proto.Unmarshal(bytes, ccid)
	return ccid, errors.Wrap(err, "error unmarshalling ChaincodeID")
}

// UnmarshalSignatureHeader 将字节反序列化签名者头部[随机数、签名者]
func UnmarshalSignatureHeader(bytes []byte) (*common.SignatureHeader, error) {
	sh := &common.SignatureHeader{}
	err := proto.Unmarshal(bytes, sh)
	return sh, errors.Wrap(err, "反序列化签名者头部出错")
}

func UnmarshalSerializedIdentity(bytes []byte) (*msp.SerializedIdentity, error) {
	sid := &msp.SerializedIdentity{}
	err := proto.Unmarshal(bytes, sid)
	return sid, errors.Wrap(err, "解码错误 SerializedIdentity")
}

// UnmarshalHeader 将字节反序列化提案头部(通道头部[链码类型、交易id、时间戳、通道名称、链码名称]、签名者头部[随机数、签名者])
func UnmarshalHeader(bytes []byte) (*common.Header, error) {
	hdr := &common.Header{}
	err := proto.Unmarshal(bytes, hdr)
	return hdr, errors.Wrap(err, "反序列化提案头部出错")
}

// UnmarshalChaincodeHeaderExtension 将字节反序列化链码消息[链码名称]
func UnmarshalChaincodeHeaderExtension(hdrExtension []byte) (*peer.ChaincodeHeaderExtension, error) {
	chaincodeHdrExt := &peer.ChaincodeHeaderExtension{}
	err := proto.Unmarshal(hdrExtension, chaincodeHdrExt)
	return chaincodeHdrExt, errors.Wrap(err, "反序列化链码消息出错")
}

// UnmarshalProposalResponse unmarshals bytes to a ProposalResponse
func UnmarshalProposalResponse(prBytes []byte) (*peer.ProposalResponse, error) {
	proposalResponse := &peer.ProposalResponse{}
	err := proto.Unmarshal(prBytes, proposalResponse)
	return proposalResponse, errors.Wrap(err, "error unmarshalling ProposalResponse")
}

// UnmarshalChaincodeAction 将字节反序列化到 ChaincodeAction (生成的读取集和写入集, 链码生成的事件, 调用的结果, 调用的ChaincodeID)
func UnmarshalChaincodeAction(caBytes []byte) (*peer.ChaincodeAction, error) {
	chaincodeAction := &peer.ChaincodeAction{}
	err := proto.Unmarshal(caBytes, chaincodeAction)
	return chaincodeAction, errors.Wrap(err, "反序列化 ChaincodeAction 时出错")
}

// UnmarshalResponse unmarshals bytes to a Response
func UnmarshalResponse(resBytes []byte) (*peer.Response, error) {
	response := &peer.Response{}
	err := proto.Unmarshal(resBytes, response)
	return response, errors.Wrap(err, "error unmarshalling Response")
}

// UnmarshalChaincodeEvents unmarshals bytes to a ChaincodeEvent
func UnmarshalChaincodeEvents(eBytes []byte) (*peer.ChaincodeEvent, error) {
	chaincodeEvent := &peer.ChaincodeEvent{}
	err := proto.Unmarshal(eBytes, chaincodeEvent)
	return chaincodeEvent, errors.Wrap(err, "error unmarshalling ChaicnodeEvent")
}

// UnmarshalProposalResponsePayload unmarshals bytes to a ProposalResponsePayload
func UnmarshalProposalResponsePayload(prpBytes []byte) (*peer.ProposalResponsePayload, error) {
	prp := &peer.ProposalResponsePayload{}
	err := proto.Unmarshal(prpBytes, prp)
	return prp, errors.Wrap(err, "error unmarshalling ProposalResponsePayload")
}

// UnmarshalProposal 将字节数反序列化到提案实例
func UnmarshalProposal(propBytes []byte) (*peer.Proposal, error) {
	prop := &peer.Proposal{}
	err := proto.Unmarshal(propBytes, prop)
	return prop, errors.Wrap(err, "反序列化提案实例时出错")
}

// UnmarshalTransaction unmarshals bytes to a Transaction
func UnmarshalTransaction(txBytes []byte) (*peer.Transaction, error) {
	tx := &peer.Transaction{}
	err := proto.Unmarshal(txBytes, tx)
	return tx, errors.Wrap(err, "error unmarshalling Transaction")
}

// UnmarshalChaincodeActionPayload unmarshals bytes to a ChaincodeActionPayload
func UnmarshalChaincodeActionPayload(capBytes []byte) (*peer.ChaincodeActionPayload, error) {
	cap := &peer.ChaincodeActionPayload{}
	err := proto.Unmarshal(capBytes, cap)
	return cap, errors.Wrap(err, "error unmarshalling ChaincodeActionPayload")
}

// UnmarshalChaincodeProposalPayload 将字节反序列化负载有效数据(链码规则、加密材料)
func UnmarshalChaincodeProposalPayload(bytes []byte) (*peer.ChaincodeProposalPayload, error) {
	cpp := &peer.ChaincodeProposalPayload{}
	err := proto.Unmarshal(bytes, cpp)
	return cpp, errors.Wrap(err, "反序列化负载有效数据出错")
}

// UnmarshalTxReadWriteSet unmarshals bytes to a TxReadWriteSet
func UnmarshalTxReadWriteSet(bytes []byte) (*rwset.TxReadWriteSet, error) {
	rws := &rwset.TxReadWriteSet{}
	err := proto.Unmarshal(bytes, rws)
	return rws, errors.Wrap(err, "error unmarshalling TxReadWriteSet")
}

// UnmarshalKVRWSet unmarshals bytes to a KVRWSet
func UnmarshalKVRWSet(bytes []byte) (*kvrwset.KVRWSet, error) {
	rws := &kvrwset.KVRWSet{}
	err := proto.Unmarshal(bytes, rws)
	return rws, errors.Wrap(err, "error unmarshalling KVRWSet")
}

// UnmarshalHashedRWSet unmarshals bytes to a HashedRWSet
func UnmarshalHashedRWSet(bytes []byte) (*kvrwset.HashedRWSet, error) {
	hrws := &kvrwset.HashedRWSet{}
	err := proto.Unmarshal(bytes, hrws)
	return hrws, errors.Wrap(err, "error unmarshalling HashedRWSet")
}

// UnmarshalSignaturePolicy unmarshals bytes to a SignaturePolicyEnvelope
func UnmarshalSignaturePolicy(bytes []byte) (*common.SignaturePolicyEnvelope, error) {
	sp := &common.SignaturePolicyEnvelope{}
	err := proto.Unmarshal(bytes, sp)
	return sp, errors.Wrap(err, "error unmarshalling SignaturePolicyEnvelope")
}

// UnmarshalPayloadOrPanic unmarshals bytes to a Payload structure or panics
// on error
func UnmarshalPayloadOrPanic(encoded []byte) *common.Payload {
	payload, err := UnmarshalPayload(encoded)
	if err != nil {
		panic(err)
	}
	return payload
}

// UnmarshalEnvelopeOrPanic unmarshals bytes to an Envelope structure or panics
// on error
func UnmarshalEnvelopeOrPanic(encoded []byte) *common.Envelope {
	envelope, err := UnmarshalEnvelope(encoded)
	if err != nil {
		panic(err)
	}
	return envelope
}

// UnmarshalBlockOrPanic unmarshals bytes to an Block or panics
// on error
func UnmarshalBlockOrPanic(encoded []byte) *common.Block {
	block, err := UnmarshalBlock(encoded)
	if err != nil {
		panic(err)
	}
	return block
}

// UnmarshalChannelHeaderOrPanic unmarshals bytes to a ChannelHeader or panics
// on error
func UnmarshalChannelHeaderOrPanic(bytes []byte) *common.ChannelHeader {
	chdr, err := UnmarshalChannelHeader(bytes)
	if err != nil {
		panic(err)
	}
	return chdr
}

// UnmarshalSignatureHeaderOrPanic unmarshals bytes to a SignatureHeader or panics
// on error
func UnmarshalSignatureHeaderOrPanic(bytes []byte) *common.SignatureHeader {
	sighdr, err := UnmarshalSignatureHeader(bytes)
	if err != nil {
		panic(err)
	}
	return sighdr
}
