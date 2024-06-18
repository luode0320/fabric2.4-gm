/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"context"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("common.deliver")

//go:generate counterfeiter -o mock/chain_manager.go -fake-name ChainManager . ChainManager

// ChainManager provides a way for the Handler to look up the Chain.
type ChainManager interface {
	GetChain(chainID string) Chain
}

//go:generate counterfeiter -o mock/chain.go -fake-name Chain . Chain

// Chain 封装了链操作和数据相关的功能。
type Chain interface {
	// Sequence 返回当前配置序列号，可用于检测配置变更
	Sequence() uint64

	// PolicyManager 返回由链配置指定的当前策略管理器
	PolicyManager() policies.Manager

	// Reader 提供针对该链的区块读取器
	Reader() blockledger.Reader

	// Errored 返回一个通道，当底层共识器发生错误时，此通道将被关闭
	Errored() <-chan struct{}
}

//go:generate counterfeiter -o mock/policy_checker.go -fake-name PolicyChecker . PolicyChecker

// PolicyChecker checks the envelope against the policy logic supplied by the
// function.
type PolicyChecker interface {
	CheckPolicy(envelope *cb.Envelope, channelID string) error
}

// PolicyCheckerFunc 是一个适配器，允许将普通函数用作PolicyChecker。
type PolicyCheckerFunc func(envelope *cb.Envelope, channelID string) error

// CheckPolicy calls pcf(envelope, channelID)
func (pcf PolicyCheckerFunc) CheckPolicy(envelope *cb.Envelope, channelID string) error {
	return pcf(envelope, channelID)
}

//go:generate counterfeiter -o mock/inspector.go -fake-name Inspector . Inspector

// Inspector verifies an appropriate binding between the message and the context.
type Inspector interface {
	Inspect(context.Context, proto.Message) error
}

// The InspectorFunc is an adapter that allows the use of an ordinary
// function as an Inspector.
type InspectorFunc func(context.Context, proto.Message) error

// Inspect calls inspector(ctx, p)
func (inspector InspectorFunc) Inspect(ctx context.Context, p proto.Message) error {
	return inspector(ctx, p)
}

// Handler 处理服务器请求。
type Handler struct {
	ExpirationCheckFunc func(identityBytes []byte) time.Time // 过期检查函数
	ChainManager        ChainManager                         // 链管理器
	TimeWindow          time.Duration                        // 时间窗口
	BindingInspector    Inspector                            // 绑定检查器
	Metrics             *Metrics                             // 指标
}

//go:generate counterfeiter -o mock/receiver.go -fake-name Receiver . Receiver

// Receiver is used to receive enveloped seek requests.
type Receiver interface {
	Recv() (*cb.Envelope, error)
}

//go:generate counterfeiter -o mock/response_sender.go -fake-name ResponseSender . ResponseSender

// ResponseSender 定义了处理器必须实现的接口，以便向客户端发送响应。
type ResponseSender interface {
	// SendStatusResponse 向客户端发送完成状态。
	SendStatusResponse(status cb.Status) error
	// SendBlockResponse 向客户端发送区块数据，以及可选的私有数据。
	SendBlockResponse(data *cb.Block, channelID string, chain Chain, signedData *protoutil.SignedData) error
	// DataType 返回发送者发送的数据类型
	DataType() string
}

// Filtered is a marker interface that indicates a response sender
// is configured to send filtered blocks
// Note: this is replaced by "data_type" label. Keep it for now until we decide how to take care of compatibility issue.
type Filtered interface {
	IsFiltered() bool
}

// Server is a polymorphic structure to support generalization of this handler
// to be able to deliver different type of responses.
type Server struct {
	Receiver
	PolicyChecker
	ResponseSender
}

// ExtractChannelHeaderCertHash 从通道标头中提取TLS证书哈希。
func ExtractChannelHeaderCertHash(msg proto.Message) []byte {
	chdr, isChannelHeader := msg.(*cb.ChannelHeader)
	if !isChannelHeader || chdr == nil {
		return nil
	}
	return chdr.TlsCertHash
}

// NewHandler 创建一个 Handler 接口的实现
// 输入参数：
//   - cm：ChainManager 接口的实例，用于管理链码
//   - timeWindow：时间窗口的持续时间，用于验证事务的时间戳
//   - mutualTLS：是否启用互相认证的 TLS
//   - metrics：Metrics 结构体的指针，用于记录指标数据
//   - expirationCheckDisabled：是否禁用链码过期检查
//
// 返回值：
//   - *Handler：Handler 接口的实例
func NewHandler(cm ChainManager, timeWindow time.Duration, mutualTLS bool, metrics *Metrics, expirationCheckDisabled bool) *Handler {
	// 根据是否禁用链码过期检查，选择相应的过期检查函数
	expirationCheck := crypto.ExpiresAt
	if expirationCheckDisabled {
		expirationCheck = noExpiration
	}

	// 创建并返回 Handler 的实例
	return &Handler{
		ChainManager:        cm,                                                                          // 链管理器
		TimeWindow:          timeWindow,                                                                  // 时间窗口
		BindingInspector:    InspectorFunc(NewBindingInspector(mutualTLS, ExtractChannelHeaderCertHash)), // 绑定检查器
		Metrics:             metrics,                                                                     // 指标
		ExpirationCheckFunc: expirationCheck,                                                             // 过期检查函数
	}
}

// Handle 接收传入的交付请求。
func (h *Handler) Handle(ctx context.Context, srv *Server) error {
	addr := util.ExtractRemoteAddress(ctx)
	logger.Debugf("为 %s 启动新的交付循环", addr)
	h.Metrics.StreamsOpened.Add(1)       // 增加打开的流计数
	defer h.Metrics.StreamsClosed.Add(1) // 在函数结束时增加关闭的流计数

	// 持续循环处理请求
	for {
		logger.Debugf("尝试从 %s 读取SeekInfo消息", addr)
		envelope, err := srv.Recv() // 从gRPC流接收数据包
		if err == io.EOF {          // 如果收到EOF，表示客户端已关闭连接
			logger.Debugf("从 %s 收到EOF，挂断连接", addr)
			return nil // 正常结束循环
		}
		if err != nil { // 其他错误情况
			logger.Warningf("从流 %s 读取时发生错误: %s", addr, err)
			return err // 返回错误并结束处理
		}

		// 处理区块交付逻辑，并获取处理状态
		status, err := h.deliverBlocks(ctx, srv, envelope)
		if err != nil {
			return err // 交付区块时遇到错误，返回错误
		}

		// 向客户端发送处理状态响应
		err = srv.SendStatusResponse(status)
		if status != cb.Status_SUCCESS { // 如果状态不是成功，可能需要处理或返回错误
			return err
		}
		if err != nil {
			logger.Warningf("向 %s 发送时出现错误: %s", addr, err)
			return err // 发送响应时出错，返回错误
		}

		// 等待客户端发送新的SeekInfo请求
		logger.Debugf("等待从 %s 收到新的SeekInfo", addr)
	}
}

func isFiltered(srv *Server) bool {
	if filtered, ok := srv.ResponseSender.(Filtered); ok {
		return filtered.IsFiltered()
	}
	return false
}

// deliverBlocks 负责处理和发送区块链数据给请求的客户端。
func (h *Handler) deliverBlocks(ctx context.Context, srv *Server, envelope *cb.Envelope) (status cb.Status, err error) {
	addr := util.ExtractRemoteAddress(ctx)
	// 解析信封内容，包括payload、通道头、签名头
	payload, chdr, shdr, err := h.parseEnvelope(ctx, envelope)
	if err != nil {
		logger.Warningf("从 %s 解析信封时发生错误: %s", addr, err)
		return cb.Status_BAD_REQUEST, nil
	}

	// 根据通道ID获取对应的链实例
	chain := h.ChainManager.GetChain(chdr.ChannelId)
	if chain == nil {
		// 如果找不到对应通道，记录调试日志，因为SDK可能会轮询等待通道创建
		logger.Debugf("拒绝 %s 对通道 %s 的交付请求，因为通道未找到", addr, chdr.ChannelId)
		return cb.Status_NOT_FOUND, nil
	}

	// 记录请求接收的指标
	labels := []string{"通道", chdr.ChannelId, "过滤", strconv.FormatBool(isFiltered(srv)), "数据类型", srv.DataType()}
	h.Metrics.RequestsReceived.With(labels...).Add(1)

	defer func() {
		// 在请求完成时记录指标，包含成功与否的标记
		labels = append(labels, "成功", strconv.FormatBool(status == cb.Status_SUCCESS))
		h.Metrics.RequestsCompleted.With(labels...).Add(1)
	}()

	// 反序列化客户端发送的SeekInfo请求
	seekInfo := &ab.SeekInfo{}
	if err = proto.Unmarshal(payload.Data, seekInfo); err != nil {
		logger.Warningf("[通道: %s] 从 %s 收到带有错误格式SeekInfo负载的签名交付请求: %s", chdr.ChannelId, addr, err)
		return cb.Status_BAD_REQUEST, nil
	}

	// 根据通道的错误通知通道(若开启“最佳努力”模式则忽略错误)
	erroredChan := chain.Errored()
	if seekInfo.ErrorResponse == ab.SeekInfo_BEST_EFFORT {
		erroredChan = nil // 忽略共识错误
	}

	select {
	case <-erroredChan: // 如果共识组件发生错误
		logger.Warningf("[通道: %s] 因共识者错误，拒绝向节点 %s 发送数据的请求", chdr.ChannelId, addr)
		return cb.Status_SERVICE_UNAVAILABLE, nil
	default: // 继续处理请求
	}

	// 创建会话访问控制对象，用于评估客户端的访问权限
	accessControl, err := NewSessionAC(chain, envelope, srv.PolicyChecker, chdr.ChannelId, h.ExpirationCheckFunc)
	if err != nil {
		logger.Warningf("[通道: %s] 创建访问控制对象失败，原因: %s", chdr.ChannelId, err)
		return cb.Status_BAD_REQUEST, nil
	}

	// 评估客户端访问权限，如果未授权则返回错误
	if err := accessControl.Evaluate(); err != nil {
		logger.Warningf("[通道: %s] 客户端 %s 无权访问: %s", chdr.ChannelId, addr, err)
		return cb.Status_FORBIDDEN, nil
	}

	// 验证SeekInfo请求中的起始和终止位置是否齐全，否则返回错误
	if seekInfo.Start == nil || seekInfo.Stop == nil {
		logger.Warningf("[通道: %s] 从 %s 收到缺少起始或终止点的seekInfo消息：%v, %v", chdr.ChannelId, addr, seekInfo.Start, seekInfo.Stop)
		return cb.Status_BAD_REQUEST, nil
	}

	// 记录日志，显示接收到的SeekInfo详情
	logger.Debugf("[通道: %s] 从 %s 收到 seekInfo (%p) %v", chdr.ChannelId, addr, seekInfo, seekInfo)

	// 根据SeekInfo的起始位置创建迭代器
	cursor, number := chain.Reader().Iterator(seekInfo.Start)
	defer cursor.Close() // 确保迭代器在函数结束时关闭

	// 处理SeekInfo的停止位置，确定停止的区块编号
	var stopNum uint64
	switch stop := seekInfo.Stop.Type.(type) {
	case *ab.SeekPosition_Oldest:
		// 使用链的起始高度作为停止位置
		stopNum = number
	case *ab.SeekPosition_Newest:
		// 特殊处理，如果开始和结束都指定了最新块，则直接使用当前高度
		if proto.Equal(seekInfo.Start, seekInfo.Stop) {
			stopNum = number
			break
		}
		// 否则，使用当前高度减一作为最新块
		stopNum = chain.Reader().Height() - 1
	case *ab.SeekPosition_Specified:
		// 使用指定的区块编号作为停止位置
		stopNum = stop.Specified.Number
		// 检查起始编号是否大于停止编号，如果是，则返回错误
		if stopNum < number {
			logger.Warningf("[通道: %s] 从 %s 收到无效的seekInfo消息：起始编号 %d 大于停止编号 %d", chdr.ChannelId, addr, number, stopNum)
			return cb.Status_BAD_REQUEST, nil
		}
	}

	for {
		// 检查SeekInfo行为，如果设置为FAIL_IF_NOT_READY且请求的区块尚未准备好，则返回NOT_FOUND
		if seekInfo.Behavior == ab.SeekInfo_FAIL_IF_NOT_READY {
			if number > chain.Reader().Height()-1 {
				return cb.Status_NOT_FOUND, nil
			}
		}

		// 初始化变量用于存储下一个区块和获取状态
		var block *cb.Block
		var status cb.Status

		// 创建迭代器通道用于异步获取下一个区块
		iterCh := make(chan struct{})
		go func() {
			block, status = cursor.Next() // 异步获取下一个区块
			close(iterCh)                 // 通知主goroutine迭代已完成
		}()

		select {
		// 监听上下文是否被取消，如果是，则返回错误
		case <-ctx.Done():
			logger.Debugf("上下文已取消，中断等待下一个区块")
			return cb.Status_INTERNAL_SERVER_ERROR, errors.Wrapf(ctx.Err(), "上下文在获取区块前已完成")
		// 如果通道发生错误，则中止交付请求
		case <-erroredChan:
			logger.Warningf("因底层共识实现指示错误，中止对请求的交付")
			return cb.Status_SERVICE_UNAVAILABLE, nil
		// 当迭代完成时，继续执行
		case <-iterCh:
			// 迭代器已设置block和status变量
		}

		// 如果读取区块时出现错误，记录错误并返回
		if status != cb.Status_SUCCESS {
			logger.Errorf("[通道: %s] 从通道读取时发生错误，原因为: %v", chdr.ChannelId, status)
			return status, nil
		}

		// 支持FAIL_IF_NOT_READY行为，递增区块编号
		number++

		// 重新评估客户端访问权限，若权限被撤销，则返回FORBIDDEN
		if err := accessControl.Evaluate(); err != nil {
			logger.Warningf("[通道: %s] 来自 %s 的交付请求客户端授权被撤销: %s", chdr.ChannelId, addr, err)
			return cb.Status_FORBIDDEN, nil
		}

		// 记录日志，显示正在发送的区块信息
		logger.Debugf("[通道: %s] 为 %s 交付区块 [%d]（%p）", chdr.ChannelId, addr, block.Header.Number, seekInfo)

		// 准备SignedData对象，并发送区块响应给客户端
		signedData := &protoutil.SignedData{Data: envelope.Payload, Identity: shdr.Creator, Signature: envelope.Signature}
		if err := srv.SendBlockResponse(block, chdr.ChannelId, chain, signedData); err != nil {
			logger.Warningf("[通道: %s] 向 %s 发送时发生错误: %s", chdr.ChannelId, addr, err)
			return cb.Status_INTERNAL_SERVER_ERROR, err
		}

		// 更新发送的区块计数指标
		h.Metrics.BlocksSent.With(labels...).Add(1)

		// 如果已达到停止的区块编号，则退出循环
		if stopNum == block.Header.Number {
			break
		}
	}

	logger.Debugf("[频道: %s] 已完成向 %s 的传送 (%p)", chdr.ChannelId, addr, seekInfo)

	return cb.Status_SUCCESS, nil
}

func (h *Handler) parseEnvelope(ctx context.Context, envelope *cb.Envelope) (*cb.Payload, *cb.ChannelHeader, *cb.SignatureHeader, error) {
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, nil, nil, err
	}

	if payload.Header == nil {
		return nil, nil, nil, errors.New("envelope has no header")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, nil, nil, err
	}

	shdr, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	if err != nil {
		return nil, nil, nil, err
	}

	err = h.validateChannelHeader(ctx, chdr)
	if err != nil {
		return nil, nil, nil, err
	}

	return payload, chdr, shdr, nil
}

func (h *Handler) validateChannelHeader(ctx context.Context, chdr *cb.ChannelHeader) error {
	if chdr.GetTimestamp() == nil {
		err := errors.New("envelope 信封中的通道标头必须包含时间戳")
		return err
	}

	envTime := time.Unix(chdr.GetTimestamp().Seconds, int64(chdr.GetTimestamp().Nanos)).UTC()
	serverTime := time.Now()

	if math.Abs(float64(serverTime.UnixNano()-envTime.UnixNano())) > float64(h.TimeWindow.Nanoseconds()) {
		err := errors.Errorf("envelope信封时间戳 %s 与当前服务器时间 %s 的间隔大于 %s", envTime, h.TimeWindow, serverTime)
		return err
	}

	err := h.BindingInspector.Inspect(ctx, chdr)
	if err != nil {
		return err
	}

	return nil
}

func noExpiration(_ []byte) time.Time {
	return time.Time{}
}
