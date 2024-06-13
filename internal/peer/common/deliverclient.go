/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var (
	logger = flogging.MustGetLogger("cli.common")

	seekNewest = &ab.SeekPosition{
		Type: &ab.SeekPosition_Newest{
			Newest: &ab.SeekNewest{},
		},
	}
	seekOldest = &ab.SeekPosition{
		Type: &ab.SeekPosition_Oldest{
			Oldest: &ab.SeekOldest{},
		},
	}
)

// DeliverClient holds the necessary information to connect a client
// to an orderer/peer deliver service
type DeliverClient struct {
	Signer      identity.SignerSerializer
	Service     ab.AtomicBroadcast_DeliverClient
	ChannelID   string
	TLSCertHash []byte
	BestEffort  bool
}

// seekSpecified 方法向对等方/排序方的交付服务发送请求以获取指定编号的块。
// 方法接收者：d *DeliverClient，表示 DeliverClient 对象。
// 输入参数：
//   - blockNumber uint64，表示要获取的块的编号。
//
// 返回值：
//   - error，表示可能发生的错误。
func (d *DeliverClient) seekSpecified(blockNumber uint64) error {
	// 创建 SeekPosition 对象，指定要获取的块的编号
	seekPosition := &ab.SeekPosition{
		Type: &ab.SeekPosition_Specified{
			Specified: &ab.SeekSpecified{
				Number: blockNumber, // 区块编号
			},
		},
	}

	// 使用 seekHelper 函数创建一个请求消息
	env := seekHelper(d.ChannelID, seekPosition, d.TLSCertHash, d.Signer, d.BestEffort)

	// 向交付服务发送请求消息
	return d.Service.Send(env)
}

func (d *DeliverClient) seekOldest() error {
	env := seekHelper(d.ChannelID, seekOldest, d.TLSCertHash, d.Signer, d.BestEffort)
	return d.Service.Send(env)
}

// seekNewest 方法向对等方/排序方的交付服务发送请求以获取最新的块。
// 方法接收者：d *DeliverClient，表示 DeliverClient 对象。
// 输入参数：无。
// 返回值：
//   - error，表示可能发生的错误。
func (d *DeliverClient) seekNewest() error {
	// 函数根据给定的通道ID、位置、TLS证书哈希、签名者、最佳努力标志创建一个带有TLS绑定的已签名的信封
	env := seekHelper(d.ChannelID, seekNewest, d.TLSCertHash, d.Signer, d.BestEffort)

	// 向交付服务发送请求消息
	return d.Service.Send(env)
}

func (d *DeliverClient) readBlock() (*cb.Block, error) {
	msg, err := d.Service.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "接收错误")
	}
	switch t := msg.Type.(type) {
	case *ab.DeliverResponse_Status:
		logger.Infof("预期获得块数据, 但实际获得状态: %v", t)
		return nil, errors.Errorf("无法读取块数据: %v", t)
	case *ab.DeliverResponse_Block:
		logger.Infof("已接收块信息: %v", t.Block.Header.Number)
		if resp, err := d.Service.Recv(); err != nil { // Flush the success message
			logger.Errorf("刷新消息失败: %s", err)
		} else if status := resp.GetStatus(); status != cb.Status_SUCCESS {
			logger.Errorf("预期是成功的, 得到了: %s", status)
		}

		return t.Block, nil
	default:
		return nil, errors.Errorf("响应错误: 未知类型 %T", t)
	}
}

// GetSpecifiedBlock 方法从对等方/排序方的交付服务获取指定的块。
// 方法接收者：d *DeliverClient，表示 DeliverClient 对象。
// 输入参数：
//   - num uint64，表示要获取的块的编号。
//
// 返回值：
//   - *cb.Block，表示获取到的块。
//   - error，表示可能发生的错误。
func (d *DeliverClient) GetSpecifiedBlock(num uint64) (*cb.Block, error) {
	// 方法向对等方/排序方的交付服务发送请求以获取指定编号的块
	err := d.seekSpecified(num)
	if err != nil {
		return nil, errors.WithMessage(err, "获取指定块时出错")
	}

	// 调用 readBlock 方法以读取块数据
	return d.readBlock()
}

// GetOldestBlock gets the oldest block from a peer/orderer's deliver service
func (d *DeliverClient) GetOldestBlock() (*cb.Block, error) {
	err := d.seekOldest()
	if err != nil {
		return nil, errors.WithMessage(err, "error getting oldest block")
	}

	return d.readBlock()
}

// GetNewestBlock 方法从对等方/排序方的交付服务获取最新的块。
// 方法接收者：d *DeliverClient，表示 DeliverClient 对象。
// 输入参数：无。
// 返回值：
//   - *cb.Block，表示最新的块。
//   - error，表示可能发生的错误。
func (d *DeliverClient) GetNewestBlock() (*cb.Block, error) {
	// 方法向对等方/排序方的交付服务发送请求以获取最新的块
	err := d.seekNewest()
	if err != nil {
		return nil, errors.WithMessage(err, "获取最新块时出错")
	}

	// 调用 readBlock 方法以读取块数据
	return d.readBlock()
}

// Close closes a deliver client's connection
func (d *DeliverClient) Close() error {
	return d.Service.CloseSend()
}

// seekHelper 函数根据给定的通道ID、位置、TLS证书哈希、签名者、最佳努力标志创建一个带有TLS绑定的已签名的信封。
// 方法接收者：无。
// 输入参数：
//   - channelID string，表示通道ID。
//   - position *ab.SeekPosition，表示位置信息。
//   - tlsCertHash []byte，表示TLS证书哈希。
//   - signer identity.SignerSerializer，表示签名者。
//   - bestEffort bool，表示最佳努力标志。
//
// 返回值：
//   - *cb.Envelope，表示带有TLS绑定的已签名的信封。
func seekHelper(
	channelID string, // 通道ID。
	position *ab.SeekPosition, // 位置信息。
	tlsCertHash []byte, // TLS证书哈希。
	signer identity.SignerSerializer, // 签名者。
	bestEffort bool, // 最佳努力标志。
) *cb.Envelope {
	// 创建 SeekInfo 对象, 指定要返回的请求块的范围，如果找不到开始位置
	seekInfo := &ab.SeekInfo{
		Start:    position,                      // 请求开始范围
		Stop:     position,                      // 请求结束范围
		Behavior: ab.SeekInfo_BLOCK_UNTIL_READY, // 搜索信息块，直到准备就绪
	}

	// 如果最佳努力标志为 true，则设置 SeekInfo 的 ErrorResponse 字段为 BEST_EFFORT
	if bestEffort {
		seekInfo.ErrorResponse = ab.SeekInfo_BEST_EFFORT // 寻求信息尽最大努力
	}

	// 使用 protoutil.CreateSignedEnvelopeWithTLSBinding 函数创建带有TLS绑定的已签名的信封
	env, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		cb.HeaderType_DELIVER_SEEK_INFO, // 信封的类型。查找信息
		channelID,                       // 通道的ID。
		signer,                          // 签名者。
		seekInfo,                        // 要封装的数据消息。
		int32(0),                        // 消息的版本。
		uint64(0),                       // 通道的时期。
		tlsCertHash,                     // TLS证书的哈希值。
	)
	if err != nil {
		logger.Errorf("创建带有TLS绑定的已签名的信封 Envelope 签名时出错:  %s", err)
		return nil
	}

	// 返回创建的信封
	return env
}

type ordererDeliverService struct {
	ab.AtomicBroadcast_DeliverClient
}

// NewDeliverClientForOrderer creates a new DeliverClient from an OrdererClient
func NewDeliverClientForOrderer(channelID string, signer identity.SignerSerializer, bestEffort bool) (*DeliverClient, error) {
	oc, err := NewOrdererClientFromEnv()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create deliver client for orderer")
	}

	dc, err := oc.Deliver()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create deliver client for orderer")
	}
	// check for client certificate and create hash if present
	var tlsCertHash []byte
	if len(oc.Certificate().Certificate) > 0 {
		tlsCertHash = util.ComputeSHA256(oc.Certificate().Certificate[0])
	}
	ds := &ordererDeliverService{dc}
	o := &DeliverClient{
		Signer:      signer,
		Service:     ds,
		ChannelID:   channelID,
		TLSCertHash: tlsCertHash,
		BestEffort:  bestEffort,
	}
	return o, nil
}

type peerDeliverService struct {
	pb.Deliver_DeliverClient
}

// NewDeliverClientForPeer 方法根据通道ID、签名者和最佳努力标志创建一个新的提交 DeliverClient。
// 方法接收者：无。
// 输入参数：
//   - channelID string，表示通道ID。
//   - signer identity.SignerSerializer，表示签名者。
//   - bestEffort bool，表示最佳努力标志。
//
// 返回值：
//   - *DeliverClient，表示 DeliverClient 对象。
//   - error，表示可能发生的错误。
func NewDeliverClientForPeer(channelID string, signer identity.SignerSerializer, bestEffort bool) (*DeliverClient, error) {
	var tlsCertHash []byte

	// 从环境变量创建 PeerClient
	pc, err := NewPeerClientFromEnv()
	if err != nil {
		return nil, errors.WithMessage(err, "无法为 peer 创建提交客户端")
	}

	// 创建 DeliverClient
	d, err := pc.Deliver()
	if err != nil {
		return nil, errors.WithMessage(err, "无法为 peer 创建提交客户端")
	}

	// 检查客户端证书并创建哈希值（如果存在）
	if len(pc.Certificate().Certificate) > 0 {
		tlsCertHash = util.ComputeSHA256(pc.Certificate().Certificate[0])
	}

	// 创建 peerDeliverService
	ds := &peerDeliverService{d}

	// 创建 DeliverClient
	p := &DeliverClient{
		Signer:      signer,
		Service:     ds,
		ChannelID:   channelID,
		TLSCertHash: tlsCertHash,
		BestEffort:  bestEffort,
	}

	return p, nil
}

func (p *peerDeliverService) Recv() (*ab.DeliverResponse, error) {
	pbResp, err := p.Deliver_DeliverClient.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "error receiving from peer deliver service")
	}

	abResp := &ab.DeliverResponse{}

	switch t := pbResp.Type.(type) {
	case *pb.DeliverResponse_Status:
		abResp.Type = &ab.DeliverResponse_Status{Status: t.Status}
	case *pb.DeliverResponse_Block:
		abResp.Type = &ab.DeliverResponse_Block{Block: t.Block}
	default:
		return nil, errors.Errorf("response error: unknown type %T", t)
	}

	return abResp, nil
}
