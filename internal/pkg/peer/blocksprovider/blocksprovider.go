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
	// AddPayload adds payload to the local state sync buffer
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

// DeliverBlocks 用于从 orderer 服务中拉出块以在 peer 之间分发它们
func (d *Deliverer) DeliverBlocks() {
	failureCounter := 0
	totalDuration := time.Duration(0)

	// InitialRetryDelay * backoffExponentBase^n > MaxRetryDelay
	// backoffExponentBase^n > MaxRetryDelay / InitialRetryDelay
	// n * log(backoffExponentBase) > log(MaxRetryDelay / InitialRetryDelay)
	// n > log(MaxRetryDelay / InitialRetryDelay) / log(backoffExponentBase)
	maxFailures := int(math.Log(float64(d.MaxRetryDelay)/float64(d.InitialRetryDelay)) / math.Log(backoffExponentBase))
	for {
		select {
		case <-d.DoneC:
			return
		default:
		}

		if failureCounter > 0 {
			var sleepDuration time.Duration
			if failureCounter-1 > maxFailures {
				sleepDuration = d.MaxRetryDelay // configured from peer.deliveryclient.reConnectBackoffThreshold
			} else {
				sleepDuration = time.Duration(math.Pow(backoffExponentBase, float64(failureCounter-1))*100) * time.Millisecond
			}
			totalDuration += sleepDuration
			if totalDuration > d.MaxRetryDuration {
				if d.YieldLeadership {
					d.Logger.Warningf("尝试重试 peer 的块传递, 重新连接持续时间 %v, 放弃重试", d.MaxRetryDuration)
					return
				}
				d.Logger.Warningf("peer 是静态领导者, 忽略 peer.Deliveryclient.Reconnecttotaltermethreshold")
			}
			d.Logger.Warningf("已从 orderer 服务断开. 尝试在 %v 中重新连接", sleepDuration)
			d.sleeper.Sleep(sleepDuration, d.DoneC)
		}

		ledgerHeight, err := d.Ledger.LedgerHeight()
		if err != nil {
			d.Logger.Error("未返回通道账本高度, 严重错误", err)
			return
		}

		seekInfoEnv, err := d.createSeekInfo(ledgerHeight)
		if err != nil {
			d.Logger.Error("无法创建已签名的 SeekInfo 消息, 出现严重错误", err)
			return
		}

		deliverClient, endpoint, cancel, err := d.connect(seekInfoEnv)
		if err != nil {
			d.Logger.Warningf("无法连接到 orderer 服务: %s", err)
			failureCounter++
			continue
		}

		connLogger := d.Logger.With("orderer-address", endpoint.Address)
		connLogger.Infow("从 orderer 服务中提取下一个块", "下一区块高度:", ledgerHeight)

		recv := make(chan *orderer.DeliverResponse)
		go func() {
			for {
				resp, err := deliverClient.Recv()
				if err != nil {
					connLogger.Warningf("从提交流读取时遇到错误: %s", err)
					close(recv)
					return
				}
				select {
				case recv <- resp:
				case <-d.DoneC:
					close(recv)
					return
				}
			}
		}()

	RecvLoop: // Loop until the endpoint is refreshed, or there is an error on the connection
		for {
			select {
			case <-endpoint.Refreshed:
				connLogger.Infof("已刷新 orderer 节点, 使用更新的端点从交付中断到重新连接")
				break RecvLoop
			case response, ok := <-recv:
				if !ok {
					connLogger.Warningf("Orderer 挂断未发送状态")
					failureCounter++
					break RecvLoop
				}
				err = d.processMsg(response)
				if err != nil {
					connLogger.Warningf("尝试接收块时出错: %v", err)
					failureCounter++
					break RecvLoop
				}
				failureCounter = 0
			case <-d.DoneC:
				break RecvLoop
			}
		}

		// cancel and wait for our spawned go routine to exit
		cancel()
		<-recv
	}
}

func (d *Deliverer) processMsg(msg *orderer.DeliverResponse) error {
	switch t := msg.Type.(type) {
	case *orderer.DeliverResponse_Status:
		if t.Status == common.Status_SUCCESS {
			return errors.Errorf("received success for a seek that should never complete")
		}

		return errors.Errorf("从 orderer 接收到错误状态: %v", t.Status)
	case *orderer.DeliverResponse_Block:
		blockNum := t.Block.Header.Number
		if err := d.BlockVerifier.VerifyBlock(gossipcommon.ChannelID(d.ChannelID), blockNum, t.Block); err != nil {
			return errors.WithMessage(err, "block from orderer could not be verified")
		}

		marshaledBlock, err := proto.Marshal(t.Block)
		if err != nil {
			return errors.WithMessage(err, "block from orderer could not be re-marshaled")
		}

		// Create payload with a block received
		payload := &gossip.Payload{
			Data:   marshaledBlock,
			SeqNum: blockNum,
		}

		// Use payload to create gossip message
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

		d.Logger.Debugf("Adding payload to local buffer, blockNum = [%d]", blockNum)
		// Add payload to local state payloads buffer
		if err := d.Gossip.AddPayload(d.ChannelID, payload); err != nil {
			d.Logger.Warningf("Block [%d] received from ordering service wasn't added to payload buffer: %v", blockNum, err)
			return errors.WithMessage(err, "could not add block as payload")
		}
		if d.BlockGossipDisabled {
			return nil
		}
		// Gossip messages with other nodes
		d.Logger.Debugf("Gossiping block [%d]", blockNum)
		d.Gossip.Gossip(gossipMsg)
		return nil
	default:
		d.Logger.Warningf("Received unknown: %v", t)
		return errors.Errorf("unknown message type '%T'", msg.Type)
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
