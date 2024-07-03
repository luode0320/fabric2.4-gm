/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider

import (
	"context"
	"math"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"
	"github.com/hyperledger/fabric/protoutil"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type sleeper struct {
	sleep func(time.Duration)
}

func (s sleeper) Sleep(d time.Duration, doneC chan struct{}) {
	if s.sleep == nil {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-doneC:
			timer.Stop()
		}
		return
	}
	s.sleep(d)
}

// LedgerInfo an adapter to provide the interface to query
// the ledger committer for current ledger height
//
//go:generate counterfeiter -o fake/ledger_info.go --fake-name LedgerInfo . LedgerInfo
type LedgerInfo interface {
	// LedgerHeight returns current local ledger height
	LedgerHeight() (uint64, error)
}

// GossipServiceAdapter serves to provide basic functionality
// required from gossip service by delivery service
//
//go:generate counterfeiter -o fake/gossip_service_adapter.go --fake-name GossipServiceAdapter . GossipServiceAdapter
type GossipServiceAdapter interface {
	// AddPayload 方法将新的payload（区块）添加到状态中。根据参数设置，该方法可能会阻塞直到区块被添加到payloads缓冲区中，或者在缓冲区满时丢弃区块。 添加完成会被保存到账本
	AddPayload(chainID string, payload *gossip.Payload) error

	// Gossip the message across the peers
	Gossip(msg *gossip.GossipMessage)
}

//go:generate counterfeiter -o fake/block_verifier.go --fake-name BlockVerifier . BlockVerifier
type BlockVerifier interface {
	VerifyBlock(channelID gossipcommon.ChannelID, blockNum uint64, block *common.Block) error
}

//go:generate counterfeiter -o fake/orderer_connection_source.go --fake-name OrdererConnectionSource . OrdererConnectionSource
type OrdererConnectionSource interface {
	RandomEndpoint() (*orderers.Endpoint, error)
}

//go:generate counterfeiter -o fake/dialer.go --fake-name Dialer . Dialer
type Dialer interface {
	Dial(address string, rootCerts [][]byte) (*grpc.ClientConn, error)
}

//go:generate counterfeiter -o fake/deliver_streamer.go --fake-name DeliverStreamer . DeliverStreamer
type DeliverStreamer interface {
	Deliver(context.Context, *grpc.ClientConn) (orderer.AtomicBroadcast_DeliverClient, error)
}

// Deliverer the actual implementation for BlocksProvider interface
type Deliverer struct {
	ChannelID       string
	Gossip          GossipServiceAdapter
	Ledger          LedgerInfo
	BlockVerifier   BlockVerifier
	Dialer          Dialer
	Orderers        OrdererConnectionSource
	DoneC           chan struct{}
	Signer          identity.SignerSerializer
	DeliverStreamer DeliverStreamer
	Logger          *flogging.FabricLogger
	YieldLeadership bool

	BlockGossipDisabled bool
	MaxRetryDelay       time.Duration
	InitialRetryDelay   time.Duration
	MaxRetryDuration    time.Duration

	// TLSCertHash should be nil when TLS is not enabled
	TLSCertHash []byte // util.ComputeSHA256(b.credSupport.GetClientCertificate().Certificate[0])

	sleeper sleeper
}

const backoffExponentBase = 1.2

// DeliverBlocks 方法用于从排序服务(orderer service)中拉取区块，并在对等节点(peer nodes)间进行分发。
func (d *Deliverer) DeliverBlocks() {
	failureCounter := 0               // 记录连续失败次数
	totalDuration := time.Duration(0) // 记录总的延迟时间

	// 初始重试延迟 * 幂指数基 ^ n > 最大重试延迟
	// 幂指数基 ^ n > 最大重试延迟 / 初始重试延迟
	// n * log(幂指数基) > log(最大重试延迟 / 初始重试延迟)
	// n > log(最大重试延迟 / 初始重试延迟) / log(幂指数基)
	maxFailures := int(math.Log(float64(d.MaxRetryDelay)/float64(d.InitialRetryDelay)) / math.Log(backoffExponentBase))
	for {
		select {
		case <-d.DoneC: // 如果接收到完成信号，退出循环
			return
		default: // 否则，继续执行
		}

		if failureCounter > 0 { // 如果存在失败计数
			var sleepDuration time.Duration
			if failureCounter-1 > maxFailures { // 如果失败次数超过最大阈值
				sleepDuration = d.MaxRetryDelay // 设置睡眠时间为最大重试延迟
			} else {
				sleepDuration = time.Duration(math.Pow(backoffExponentBase, float64(failureCounter-1))*100) * time.Millisecond
			}
			totalDuration += sleepDuration          // 累加总延迟时间
			if totalDuration > d.MaxRetryDuration { // 如果总延迟时间超过最大重试时长
				if d.YieldLeadership { // 如果配置为放弃领导权
					d.Logger.Warningf("尝试重试 peer 的块传递, 重新连接持续时间 %v, 放弃重试", d.MaxRetryDuration) // 输出警告日志
					return                                                                     // 退出方法
				}
				d.Logger.Warningf("peer 是静态领导者, 忽略 peer.Deliveryclient.Reconnecttotaltermethreshold")
			}
			d.Logger.Warningf("已从 orderer 服务断开. 尝试在 %v 中重新连接", sleepDuration) // 输出警告日志，记录重连尝试
			d.sleeper.Sleep(sleepDuration, d.DoneC)                           // 进行延迟等待
		}

		ledgerHeight, err := d.Ledger.LedgerHeight() // 获取账本高度
		if err != nil {
			d.Logger.Error("未返回通道账本高度, 严重错误", err)
			return
		}

		// 创建 SeekInfo 请求
		seekInfoEnv, err := d.createSeekInfo(ledgerHeight)
		if err != nil {
			d.Logger.Error("无法创建已签名的 SeekInfo 消息, 出现严重错误", err)
			return
		}

		// 连接到排序服务
		deliverClient, endpoint, cancel, err := d.connect(seekInfoEnv)
		if err != nil {
			d.Logger.Warningf("无法连接到 orderer 排序服务: %s", err) // 警告：无法连接到排序服务
			failureCounter++                                 // 增加失败计数
			continue                                         // 继续下一次循环
		}

		connLogger := d.Logger.With("orderer-address", endpoint.Address)   // 日志添加排序服务地址字段
		connLogger.Infow("从 orderer 排序服务中提取下一个块", "下一区块高度:", ledgerHeight) // 输出信息日志

		recv := make(chan *orderer.DeliverResponse) // 创建接收通道
		go func() {
			for {
				resp, err := deliverClient.Recv() // 接收响应
				if err != nil {
					connLogger.Warningf("从提交流读取时遇到错误: %s", err) // 警告：读取响应流错误
					close(recv)                                 // 关闭接收通道
					return                                      // 退出goroutine
				}
				select {
				case recv <- resp: // 将响应发送到接收通道
				case <-d.DoneC: // 如果接收到完成信号
					close(recv) // 关闭接收通道
					return      // 退出goroutine
				}
			}
		}()

	RecvLoop: // 循环接收区块，直到节点刷新或连接错误
		for {
			select {
			case <-endpoint.Refreshed: // 如果排序服务节点刷新
				connLogger.Infof("已刷新 orderer 排序节点, 使用更新的端点从交付中断到重新连接") // 输出信息日志
				break RecvLoop                                          // 退出循环，重新连接
			case response, ok := <-recv: // 从接收通道读取响应
				if !ok { // 如果通道关闭
					connLogger.Warningf("Orderer 排序节点挂断未发送状态") // 警告：排序服务挂断
					failureCounter++                           // 增加失败计数
					break RecvLoop                             // 退出循环
				}
				err = d.processMsg(response) // 方法用于处理从排序服务接收到的消息，包括验证区块、创建并传播gossip消息。
				if err != nil {
					connLogger.Warningf("尝试接收块时出错: %v", err) // 警告：接收区块错误
					failureCounter++                         // 增加失败计数
					break RecvLoop                           // 退出循环
				}
				failureCounter = 0 // 重置失败计数
			case <-d.DoneC:
				break RecvLoop
			}
		}

		// 取消连接，等待子goroutine退出
		cancel()
		<-recv // 等待接收通道关闭
	}
}

// 方法用于处理从排序服务接收到的消息，包括验证区块、创建并传播gossip消息。
func (d *Deliverer) processMsg(msg *orderer.DeliverResponse) error {
	switch t := msg.Type.(type) {
	case *orderer.DeliverResponse_Status: // 检查消息类型为状态
		if t.Status == common.Status_SUCCESS { // 如果状态为成功
			return errors.Errorf("收到不应完成的seek操作的成功状态")
		}

		return errors.Errorf("从 orderer 接收到错误状态: %v", t.Status)
	case *orderer.DeliverResponse_Block: // 检查消息类型为区块
		blockNum := t.Block.Header.Number // 获取区块编号
		if err := d.BlockVerifier.VerifyBlock(gossipcommon.ChannelID(d.ChannelID), blockNum, t.Block); err != nil {
			return errors.WithMessage(err, "无法验证来自orderer的区块")
		}

		marshaledBlock, err := proto.Marshal(t.Block)
		if err != nil {
			return errors.WithMessage(err, "无法重新序列化来自orderer的区块")
		}

		// 创建包含接收区块的payload
		payload := &gossip.Payload{
			Data:   marshaledBlock,
			SeqNum: blockNum,
		}

		// 使用payload创建gossip消息
		gossipMsg := &gossip.GossipMessage{
			Nonce:   0,
			Tag:     gossip.GossipMessage_CHAN_AND_ORG,
			Channel: []byte(d.ChannelID),
			Content: &gossip.GossipMessage_DataMsg{
				DataMsg: &gossip.DataMessage{
					Payload: payload,
				},
			},
		}

		d.Logger.Debugf("向本地缓冲区添加payload，blockNum = [%d]", blockNum) // 输出调试日志
		// 方法将新的payload（区块）添加到状态中。根据参数设置，该方法可能会阻塞直到区块被添加到payloads缓冲区中，或者在缓冲区满时丢弃区块。 添加完成会被保存到账本
		if err := d.Gossip.AddPayload(d.ChannelID, payload); err != nil {
			d.Logger.Warningf("从排序服务接收到的区块[%d]未能添加到payload缓冲区: %v", blockNum, err) // 输出警告日志
			return errors.WithMessage(err, "无法将区块作为payload添加")
		}
		if d.BlockGossipDisabled { // 检查是否禁用了区块gossip
			return nil
		}
		// 与其它节点传播gossip消息
		d.Logger.Debugf("gossip传播区块[%d]", blockNum) // 输出调试日志
		d.Gossip.Gossip(gossipMsg)                  // 执行gossip
		return nil
	default: // 处理未知消息类型
		d.Logger.Warningf("接收到未知类型: %v", t)           // 输出警告日志
		return errors.Errorf("未知消息类型 '%T'", msg.Type) // 抛出错误，报告未知消息类型
	}
}

// Stop stops blocks delivery provider
func (d *Deliverer) Stop() {
	// this select is not race-safe, but it prevents a panic
	// for careless callers multiply invoking stop
	select {
	case <-d.DoneC:
	default:
		close(d.DoneC)
	}
}

func (d *Deliverer) connect(seekInfoEnv *common.Envelope) (orderer.AtomicBroadcast_DeliverClient, *orderers.Endpoint, func(), error) {
	endpoint, err := d.Orderers.RandomEndpoint()
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "无法获取 orderer 节点信息")
	}

	conn, err := d.Dialer.Dial(endpoint.Address, endpoint.RootCerts)
	if err != nil {
		return nil, nil, nil, errors.WithMessagef(err, "无法连接节点 '%s'", endpoint.Address)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	deliverClient, err := d.DeliverStreamer.Deliver(ctx, conn)
	if err != nil {
		conn.Close()
		ctxCancel()
		return nil, nil, nil, errors.WithMessagef(err, "无法创建连接客户端节点 “% s”", endpoint.Address)
	}

	err = deliverClient.Send(seekInfoEnv)
	if err != nil {
		deliverClient.CloseSend()
		conn.Close()
		ctxCancel()
		return nil, nil, nil, errors.WithMessagef(err, "无法发送连接信息到握手节点 '%s'", endpoint.Address)
	}

	return deliverClient, endpoint, func() {
		deliverClient.CloseSend()
		ctxCancel()
		conn.Close()
	}, nil
}

func (d *Deliverer) createSeekInfo(ledgerHeight uint64) (*common.Envelope, error) {
	return protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		d.ChannelID,
		d.Signer,
		&orderer.SeekInfo{
			Start: &orderer.SeekPosition{
				Type: &orderer.SeekPosition_Specified{
					Specified: &orderer.SeekSpecified{
						Number: ledgerHeight,
					},
				},
			},
			Stop: &orderer.SeekPosition{
				Type: &orderer.SeekPosition_Specified{
					Specified: &orderer.SeekSpecified{
						Number: math.MaxUint64,
					},
				},
			},
			Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY,
		},
		int32(0),
		uint64(0),
		d.TLSCertHash,
	)
}
