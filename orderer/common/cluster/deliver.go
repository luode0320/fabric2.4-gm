/*
Copyright IBM Corp. 2018 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// BlockPuller 从远程排序节点拉取区块。
// 其操作不是线程安全的，因此在并发环境中必须小心使用。
type BlockPuller struct {
	// 配置
	MaxPullBlockRetries uint64                    // 最大拉取区块重试次数
	MaxTotalBufferBytes int                       // 最大总缓冲字节数
	Signer              identity.SignerSerializer // 签名者，用于签名和序列化身份信息
	TLSCert             []byte                    // TLS证书，用于安全连接
	Channel             string                    // 通道名称，用于识别区块所属的通道
	FetchTimeout        time.Duration             // 获取区块的超时时间
	RetryTimeout        time.Duration             // 重试之间的等待时间
	Logger              *flogging.FabricLogger    // 日志记录器，用于记录日志信息
	Dialer              Dialer                    // 拨号器，用于创建到远程节点的连接
	VerifyBlockSequence BlockSequenceVerifier     // 区块序列验证器，用于验证区块顺序的正确性
	Endpoints           []EndpointCriteria        // 远程节点的端点列表，用于确定从哪些节点拉取区块

	// 停止信道，一个“停止者”协程可以通过关闭此信道来通知正在服务于 PullBlock 和 HeightsByEndpoints 方法的协程停止。
	// 注意：BlockPuller 的所有方法都必须由同一个协程服务，它不是线程安全的。
	// “停止者”有责任确保信道只被关闭一次。
	StopChannel chan struct{}

	// 内部状态
	stream       *ImpatientStream // 流，用于与远程节点通信
	blockBuff    []*common.Block  // 区块缓冲区，存储已拉取但未处理的区块
	latestSeq    uint64           // 最新拉取的区块的序列号
	endpoint     string           // 当前连接的远程节点的端点
	conn         *grpc.ClientConn // gRPC客户端连接，用于与远程节点通信
	cancelStream func()           // 取消流的函数，用于关闭流
}

// Clone returns a copy of this BlockPuller initialized
// for the given channel
func (p *BlockPuller) Clone() *BlockPuller {
	// Clone by value
	copy := *p
	// Reset internal state
	copy.stream = nil
	copy.blockBuff = nil
	copy.latestSeq = 0
	copy.endpoint = ""
	copy.conn = nil
	copy.cancelStream = nil
	return &copy
}

// Close makes the BlockPuller close the connection and stream
// with the remote endpoint, and wipe the internal block buffer.
func (p *BlockPuller) Close() {
	p.disconnect()
	p.blockBuff = nil
}

func (p *BlockPuller) disconnect() {
	if p.cancelStream != nil {
		p.cancelStream()
	}
	p.cancelStream = nil

	if p.conn != nil {
		p.conn.Close()
	}
	p.conn = nil
	p.endpoint = ""
	p.latestSeq = 0
}

// PullBlock 方法会阻塞直到从远程排序节点获取到指定序列号的区块，
// 或者直到连续获取区块失败的次数超过 MaxPullBlockRetries。
// 参数：
// seq - 要获取的区块的序列号。
// 返回值：
// *common.Block - 获取到的区块数据，如果获取失败或者接收到停止信号，则返回 nil。
func (p *BlockPuller) PullBlock(seq uint64) *common.Block {
	retriesLeft := p.MaxPullBlockRetries // 初始化剩余重试次数
	for {
		block := p.tryFetchBlock(seq) // 尝试从远程节点获取区块
		if block != nil {             // 如果获取成功
			return block // 返回区块数据
		}
		retriesLeft--                                      // 重试次数减一
		if retriesLeft == 0 && p.MaxPullBlockRetries > 0 { // 如果重试次数用尽并且设置了最大重试次数
			p.Logger.Errorf("拉取块 [%d] 失败: 重试次数已耗尽 (%d)", seq, p.MaxPullBlockRetries)
			return nil
		}

		if waitOnStop(p.RetryTimeout, p.StopChannel) { // 检查是否接收到停止信号
			p.Logger.Info("收到停止信号") // 记录接收到停止信号的日志
			return nil              // 返回 nil 表示停止
		}
	}
}

// HeightsByEndpoints 方法用于查询并返回所有已知排序节点（Orderer Service Node, OSN）的最新区块高度。
// 功能描述：
// 调用 probeEndpoints 方法探测所有排序节点，获取它们的最新区块序列号。
// 遍历探测结果，关闭与每个排序节点的连接，并将节点地址与区块高度（序列号+1）的对应关系存入结果映射中。
// 返回结果映射和探测过程中可能遇到的错误。
func (p *BlockPuller) HeightsByEndpoints() (map[string]uint64, error) {
	// 调用 probeEndpoints 方法探测所有排序节点，获取它们的最新区块序列号
	endpointsInfo := p.probeEndpoints(0)
	res := make(map[string]uint64)

	// 遍历探测结果，关闭与每个排序节点的连接
	for endpoint, endpointInfo := range endpointsInfo.byEndpoints() {
		endpointInfo.conn.Close()
		// 将节点地址与区块高度（序列号+1）的对应关系存入结果映射中
		res[endpoint] = endpointInfo.lastBlockSeq + 1
	}

	// 打印日志，显示返回的节点高度映射
	p.Logger.Info("返回端点映射的 osn 的高度", res)

	// 返回结果映射和探测过程中可能遇到的错误
	return res, endpointsInfo.err
}

// UpdateEndpoints assigns the new endpoints and disconnects from the current one.
func (p *BlockPuller) UpdateEndpoints(endpoints []EndpointCriteria) {
	p.Logger.Debugf("Updating endpoints: %v", endpoints)
	p.Endpoints = endpoints
	// TODO FAB-18121 Disconnect only if the currently connected endpoint was dropped or has changes in its TLSRootCAs
	p.disconnect()
}

// waitOnStop waits duration, but returns immediately with true if the stop channel fires first.
func waitOnStop(duration time.Duration, stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	case <-time.After(duration):
		return false
	}
}

// tryFetchBlock 尝试从远程节点获取指定序列号的区块。
// 如果区块已经在缓冲区中，则直接返回；否则，尝试建立连接并拉取区块。
// 如果在尝试过程中遇到连接问题，会尝试重新连接直至成功或重试次数耗尽。
// 成功拉取后，还会验证区块序列的正确性。
func (p *BlockPuller) tryFetchBlock(seq uint64) *common.Block {
	// 尝试从缓冲区中弹出指定序列号的区块
	block := p.popBlock(seq)
	if block != nil {
		return block // 如果找到了，直接返回
	}

	var reConnected bool // 是否重新建立了连接

	// 当缓冲区为空且当前处于断开状态时，尝试重新连接
	for retriesLeft := p.MaxPullBlockRetries; p.isDisconnected(); retriesLeft-- {
		reConnected = true           // 标记为尝试重新连接
		p.connectToSomeEndpoint(seq) // 尝试连接到某个端点
		if p.isDisconnected() {      // 如果仍然无法连接
			p.Logger.Debugf("无法连接到某个终结点，将在中重试 %v", p.RetryTimeout)

			// 检查是否收到了停止信号
			if waitOnStop(p.RetryTimeout, p.StopChannel) {
				p.Logger.Info("收到停止信号")
				return nil // 收到停止信号，返回 nil
			}
		}
		if retriesLeft == 0 && p.MaxPullBlockRetries > 0 { // 如果重试次数用尽
			p.Logger.Errorf("无法连接到某些终结点，尝试次数已耗尽 (%d)，序列: %d，终结点: %v",
				p.MaxPullBlockRetries, seq, p.Endpoints)
			return nil // 无法连接，返回 nil
		}
	}

	// 缓冲区为空，需要从远程节点拉取区块填充缓冲区
	if err := p.pullBlocks(seq, reConnected); err != nil {
		p.Logger.Errorf("拉块失败: %v", err)
		// 拉取失败，断开连接并返回 nil
		p.Close()
		// 如果缓冲区中有区块，返回第一个区块
		if len(p.blockBuff) > 0 {
			return p.blockBuff[0]
		}
		return nil
	}

	// 验证拉取的区块序列的正确性
	if err := p.VerifyBlockSequence(p.blockBuff, p.Channel); err != nil {
		p.Close()
		p.Logger.Errorf("验证收到的块失败: %v", err)
		return nil // 验证失败，返回 nil
	}

	// 至此，缓冲区已满，移除第一个区块并返回
	return p.popBlock(seq)
}

func (p *BlockPuller) setCancelStreamFunc(f func()) {
	p.cancelStream = f
}

// pullBlocks 方法用于从当前连接的远程排序节点拉取一系列区块，直到缓冲区达到最大字节限制或到达最新已知的区块序列号。
// 参数：
// seq uint64 // 要开始拉取的区块的起始序列号
// reConnected bool // 是否重新建立了连接
// 功能描述：
// 创建一个用于拉取下一个区块的信封。
// 使用获得的信封和当前状态信息建立一个流。
// 在不超过最大缓冲字节限制的情况下，循环接收并处理从远程节点返回的区块。
// 对于每一个接收的区块，验证其序列号是否符合预期，计算并累加区块大小，将区块添加到缓冲区中。
// 记录拉取过程中的日志信息，包括拉取的区块序列号和大小。
func (p *BlockPuller) pullBlocks(seq uint64, reConnected bool) error {
	// 创建一个用于拉取下一个区块的信封
	env, err := p.seekNextEnvelope(seq)
	if err != nil {
		p.Logger.Errorf("创建查找信封失败: %v", err)
		return err
	}

	// 使用获得的信封和当前状态信息建立一个流
	stream, err := p.obtainStream(reConnected, env, seq)
	if err != nil {
		return err
	}

	// 初始化变量
	var totalSize int
	p.blockBuff = nil           // 清空缓冲区
	nextExpectedSequence := seq // 设置下一个期望的序列号

	// 循环接收并处理从远程节点返回的区块，直到缓冲区达到最大字节限制或到达最新已知的区块序列号
	for totalSize < p.MaxTotalBufferBytes && nextExpectedSequence <= p.latestSeq {
		resp, err := stream.Recv()
		if err != nil {
			p.Logger.Errorf("从接收下一个块失败 %s: %v", p.endpoint, err)
			return err
		}

		// 解析并验证区块
		block, err := extractBlockFromResponse(resp)
		if err != nil {
			p.Logger.Errorf("从收到一个坏的块 %s: %v", p.endpoint, err)
			return err
		}
		seq := block.Header.Number
		if seq != nextExpectedSequence {
			p.Logger.Errorf("预期接收序列 %d，但获得了 %d", nextExpectedSequence, seq)
			return errors.Errorf("得到意外的序列从 %s - (%d) 而不是 (%d)", p.endpoint, seq, nextExpectedSequence)
		}

		// 计算区块大小并累加到总大小
		size := blockSize(block)
		totalSize += size

		// 将区块添加到缓冲区中
		p.blockBuff = append(p.blockBuff, block)

		// 更新下一个期望的序列号
		nextExpectedSequence++

		// 记录拉取过程中的日志信息
		p.Logger.Infof("获得块 [%d] 的大小 %d KB 从 %s", seq, size/1024, p.endpoint)
	}
	return nil
}

// obtainStream 方法用于获取或重用一个与远程排序节点通信的流（ImpatientStream）。
// 参数：
// reConnected bool // 是否重新建立了连接
// env *common.Envelope // 用于拉取区块的信封
// seq uint64 // 请求的区块序列号
// 功能描述：
// 如果是重新建立了连接，则创建一个新的流，并发送拉取区块的请求信封。
// 如果不是重新建立连接，则重用上一次已经创建的流。
// 设置取消流的回调函数，以便在必要时能够关闭流。
func (p *BlockPuller) obtainStream(reConnected bool, env *common.Envelope, seq uint64) (*ImpatientStream, error) {
	var stream *ImpatientStream
	var err error

	// 如果是重新建立了连接，则创建一个新的流
	if reConnected {
		p.Logger.Infof("正在将块 [%d] 的请求发送到 %s", seq, p.endpoint)
		stream, err = p.requestBlocks(p.endpoint, NewImpatientStream(p.conn, p.FetchTimeout), env)
		if err != nil {
			return nil, err
		}
		// 流建立成功，下次调用此函数时重用此流
		p.stream = stream
	} else {
		// 重用之前的流
		stream = p.stream
	}

	// 设置取消流的回调函数
	p.setCancelStreamFunc(stream.cancelFunc)
	return stream, nil
}

// popBlock 方法从内存缓冲区中弹出一个区块并返回它，或者如果缓冲区为空或区块不匹配给定的期望序列号，则返回 nil。
// 参数：
// seq - 期望的区块序列号。
// 返回值：
// *common.Block - 弹出的区块，如果缓冲区为空或区块序列号不匹配，则返回 nil。

func (p *BlockPuller) popBlock(seq uint64) *common.Block {
	if len(p.blockBuff) == 0 { // 如果缓冲区为空
		return nil // 返回 nil
	}
	block, rest := p.blockBuff[0], p.blockBuff[1:] // 分离第一个区块和剩余的区块
	p.blockBuff = rest                             // 更新缓冲区为剩余的区块
	// 如果请求的区块序列号与当前区块的序列号不匹配
	if seq != block.Header.Number {
		p.blockBuff = nil // 清空缓冲区
		return nil        // 返回 nil
	}
	return block // 返回匹配的区块
}

func (p *BlockPuller) isDisconnected() bool {
	return p.conn == nil
}

// connectToSomeEndpoint 方法使 BlockPuller 连接到具有给定最小区块序列的某个端点。
// 参数：
// minRequestedSequence - 请求的最小区块序列号。
// 功能描述：
// 该方法首先并行探测所有端点，寻找具有给定最小区块序列号的端点，并将找到的符合条件的端点按照其地址排序存入一个映射表中。
// 如果没有找到符合条件的端点，将在日志中记录警告信息并返回。
// 如果找到了符合条件的端点，将从这些端点中随机选择一个，并断开与其余端点的连接，仅保留与所选端点的连接。
// 更新 BlockPuller 的连接信息和最新区块序列号，以及当前连接的端点信息，并在日志中记录连接详情。
func (p *BlockPuller) connectToSomeEndpoint(minRequestedSequence uint64) {
	// 并行探测所有端点，寻找具有给定最小区块序列号的端点，并将结果按端点地址排序存入映射表
	endpointsInfo := p.probeEndpoints(minRequestedSequence).byEndpoints()
	if len(endpointsInfo) == 0 { // 如果没有找到符合条件的端点
		p.Logger.Warningf("无法连接到的任何端点 %v", p.Endpoints) // 记录警告信息
		return                                          // 返回
	}

	// 从符合条件的端点中随机选择一个
	chosenEndpoint := randomEndpoint(endpointsInfo)
	// 断开与除了所选端点之外的所有端点的连接
	for endpoint, endpointInfo := range endpointsInfo {
		if endpoint == chosenEndpoint {
			continue // 如果是所选端点，跳过
		}
		endpointInfo.conn.Close() // 关闭与非所选端点的连接
	}

	// 更新 BlockPuller 的连接信息
	p.conn = endpointsInfo[chosenEndpoint].conn
	p.endpoint = chosenEndpoint
	p.latestSeq = endpointsInfo[chosenEndpoint].lastBlockSeq

	// 记录连接详情
	p.Logger.Infof("连接到 %s，最后一个块序列为 %d", p.endpoint, p.latestSeq)
}

// probeEndpoints 方法尝试接触所有已知的端点，并返回这些端点的最新区块序列号，以及到这些端点的 gRPC 连接。
// 参数：
// minRequestedSequence - 请求的最小区块序列号。
// 返回值：
// *endpointInfoBucket - 包含各端点信息的桶，其中包含每个端点的最新区块序列号和 gRPC 连接。

// 方法实现：
func (p *BlockPuller) probeEndpoints(minRequestedSequence uint64) *endpointInfoBucket {
	endpointsInfo := make(chan *endpointInfo, len(p.Endpoints)) // 创建一个通道，用于接收端点信息

	var wg sync.WaitGroup    // 创建一个 WaitGroup，用于等待所有子协程完成
	wg.Add(len(p.Endpoints)) // 设置 WaitGroup 的初始计数器为端点的数量

	var forbiddenErr uint32   // 禁止错误的计数器
	var unavailableErr uint32 // 不可用错误的计数器

	// 遍历所有端点
	for _, endpoint := range p.Endpoints {
		go func(endpoint EndpointCriteria) {
			defer wg.Done()                                            // 子协程结束时，将 WaitGroup 的计数器减一
			ei, err := p.probeEndpoint(endpoint, minRequestedSequence) // 探测端点，获取其信息
			if err != nil {                                            // 如果探测过程中出现错误
				p.Logger.Warningf("从 %s 接收到类型为 '%v' 的错误", endpoint.Endpoint, err)                 // 记录警告日志
				p.Logger.Debugf("%s's TLSRootCAs are %s", endpoint.Endpoint, endpoint.TLSRootCAs) // 输出详细调试信息
				if err == ErrForbidden {                                                          // 如果错误类型为禁止错误
					atomic.StoreUint32(&forbiddenErr, 1) // 设置禁止错误计数器为 1
				}
				if err == ErrServiceUnavailable { // 如果错误类型为服务不可用错误
					atomic.StoreUint32(&unavailableErr, 1) // 设置服务不可用错误计数器为 1
				}
				return // 结束子协程
			}
			endpointsInfo <- ei // 如果没有错误，将端点信息发送到通道中
		}(endpoint)
	}
	wg.Wait() // 等待所有子协程完成

	close(endpointsInfo) // 关闭通道

	// 创建并返回 endpointInfoBucket 实例
	eib := &endpointInfoBucket{
		bucket: endpointsInfo,
		logger: p.Logger,
	}

	// 检查是否有所有端点都返回了服务不可用错误
	if unavailableErr == 1 && len(endpointsInfo) == 0 {
		eib.err = ErrServiceUnavailable
	}
	// 检查是否有所有端点都返回了禁止错误
	if forbiddenErr == 1 && len(endpointsInfo) == 0 {
		eib.err = ErrForbidden
	}
	return eib // 返回 endpointInfoBucket 实例
}

// probeEndpoint 方法尝试连接到指定的远程排序节点端点，并获取该节点的最新区块序列号。
// 如果连接或获取过程中出现问题，将返回相应的错误。
// 参数：
// endpoint EndpointCriteria // 要连接的远程排序节点的端点信息，包括网络地址和 TLS 根证书
// minRequestedSequence uint64 // 请求的最小区块序列号，用于确保节点具有足够的区块历史
// 返回值：
// *endpointInfo, error // 返回一个包含连接信息和最新区块序列号的对象，以及可能的错误
func (p *BlockPuller) probeEndpoint(endpoint EndpointCriteria, minRequestedSequence uint64) (*endpointInfo, error) {
	// 尝试使用拨号器连接到远程排序节点
	conn, err := p.Dialer.Dial(endpoint)
	if err != nil { // 如果连接失败
		p.Logger.Warningf("连接至失败 %s: %v", endpoint, err) // 记录警告日志
		return nil, err                                  // 返回 nil 和错误
	}

	// 尝试获取远程排序节点的最新区块序列号
	lastBlockSeq, err := p.fetchLastBlockSeq(minRequestedSequence, endpoint.Endpoint, conn)
	if err != nil { // 如果获取失败
		conn.Close()    // 关闭连接
		return nil, err // 返回 nil 和错误
	}

	// 如果一切正常，创建并返回 endpointInfo 对象
	return &endpointInfo{
		conn:         conn,
		lastBlockSeq: lastBlockSeq,
		endpoint:     endpoint.Endpoint,
	}, nil
}

// randomEndpoint 返回给定endpointInfo的随机终结点
func randomEndpoint(endpointsToHeight map[string]*endpointInfo) string {
	var candidates []string
	for endpoint := range endpointsToHeight {
		candidates = append(candidates, endpoint)
	}

	rand.Seed(time.Now().UnixNano())
	return candidates[rand.Intn(len(candidates))]
}

// fetchLastBlockSeq 方法用于获取与指定 gRPC 连接的远程排序节点的最新区块序列号。
// 它首先创建一个用于查找的信封，然后使用这个信封发起一个区块请求。
// 接收远程节点响应的第一个区块，并从中提取区块信息。
// 如果接收到的区块序列号小于请求的最小区块序列号，则返回错误。
// 如果一切正常，返回远程节点的最新区块序列号。
// 参数：
// minRequestedSequence uint64 // 请求的最小区块序列号
// endpoint string // 远程排序节点的网络地址
// conn *grpc.ClientConn // 与远程排序节点的 gRPC 连接
// 返回值：
// uint64, error // 返回远程节点的最新区块序列号，以及可能的错误
func (p *BlockPuller) fetchLastBlockSeq(minRequestedSequence uint64, endpoint string, conn *grpc.ClientConn) (uint64, error) {
	// 创建一个用于查找的信封
	env, err := p.seekLastEnvelope()
	if err != nil {
		p.Logger.Errorf("创建查找信封失败 %s: %v", endpoint, err)
		return 0, err
	}

	// 使用信封发起一个区块请求
	stream, err := p.requestBlocks(endpoint, NewImpatientStream(conn, p.FetchTimeout), env)
	if err != nil {
		return 0, err
	}
	defer stream.abort() // 确保在函数退出时终止流

	// 接收远程节点响应的第一个区块
	resp, err := stream.Recv()
	if err != nil {
		p.Logger.Errorf("从 %s 接收最新块失败: %v", endpoint, err)
		return 0, err
	}

	// 从响应中提取区块信息
	block, err := extractBlockFromResponse(resp)
	if err != nil {
		p.Logger.Warningf("已收到 %v 从 %s: %v", resp, endpoint, err)
		return 0, err
	}
	stream.CloseSend() // 关闭发送流

	// 检查区块序列号是否满足要求
	seq := block.Header.Number
	if seq < minRequestedSequence {
		err := errors.Errorf("请求的最小顺序为 %d 但 %s 位于序列 %d 处", minRequestedSequence, endpoint, seq)
		p.Logger.Infof("正在跳过从 %s: %v", endpoint, err)
		return 0, err
	}

	// 如果一切正常，返回远程节点的最新区块序列号
	p.Logger.Infof("%s 是在块序列 %d", endpoint, seq)
	return block.Header.Number, nil
}

// requestBlocks 方法用于从给定的远程排序节点开始请求区块。
// 它使用给定的 ImpatientStreamCreator 创建一个流，并通过发送给定的信封来启动区块请求过程。
// 方法会返回一个用于拉取区块的流，或者在出现错误时返回错误。
// 参数：
// endpoint string // 远程排序节点的网络地址
// newStream ImpatientStreamCreator // 用于创建 ImpatientStream 的创建器
// env *common.Envelope // 用于启动区块请求的信封
// 返回值：
// *ImpatientStream, error // 返回一个用于拉取区块的流，以及可能的错误
func (p *BlockPuller) requestBlocks(endpoint string, newStream ImpatientStreamCreator, env *common.Envelope) (*ImpatientStream, error) {
	// 使用给定的创建器创建一个流
	stream, err := newStream()
	if err != nil {
		p.Logger.Warningf("与 %s 建立传送流失败", endpoint)
		return nil, err
	}

	// 通过流发送信封以启动区块请求
	if err := stream.Send(env); err != nil {
		p.Logger.Errorf("将seek信封发送到失败 %s: %v", endpoint, err)
		stream.abort() // 如果发送失败，终止流
		return nil, err
	}

	// 如果一切正常，返回流
	return stream, nil
}

// extractBlockFromResponse 方法用于从 DeliverResponse 响应中提取并验证区块数据。
// 参数：
// resp *orderer.DeliverResponse // 从远程排序节点接收到的响应
// 功能描述：
// 判断响应类型，如果是区块类型，则进行区块的非空检查，包括 Block、Data、Header 和 Metadata。
// 如果是非区块类型，如状态类型，根据状态判断是否为禁止或服务不可用的错误。
// 如果响应类型未知，则返回错误。
func extractBlockFromResponse(resp *orderer.DeliverResponse) (*common.Block, error) {
	switch t := resp.Type.(type) {
	// 如果响应类型为区块
	case *orderer.DeliverResponse_Block:
		block := t.Block
		// 检查区块的各个部分是否为 nil
		if block == nil {
			return nil, errors.New("block 块为nil")
		}
		if block.Data == nil {
			return nil, errors.New("block 块数据为nil")
		}
		if block.Header == nil {
			return nil, errors.New("block 块 Header 标头为nil")
		}
		if block.Metadata == nil || len(block.Metadata.Metadata) == 0 {
			return nil, errors.New("block 块 Metadata 元数据为空")
		}
		// 如果所有检查通过，返回区块
		return block, nil

	// 如果响应类型为状态
	case *orderer.DeliverResponse_Status:
		// 根据状态判断错误类型
		if t.Status == common.Status_FORBIDDEN {
			return nil, ErrForbidden
		}
		if t.Status == common.Status_SERVICE_UNAVAILABLE {
			return nil, ErrServiceUnavailable
		}
		// 其他状态返回错误
		return nil, errors.Errorf("故障节点, 已收到: %v", resp)

	// 如果响应类型未知
	default:
		// 返回错误信息
		return nil, errors.Errorf("响应的类型为 %v, 但预计会有一个区块", reflect.TypeOf(resp.Type))
	}
}

func (p *BlockPuller) seekLastEnvelope() (*common.Envelope, error) {
	return protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		p.Channel,
		p.Signer,
		last(),
		int32(0),
		uint64(0),
		util.ComputeGMSM3(p.TLSCert),
	)
}

// seekNextEnvelope 方法用于构建一个用于拉取下一个区块的签名信封。
// 参数：
// startSeq uint64 // 开始拉取的区块的起始序列号
// 功能描述：
// 使用提供的签名者、通道名称、起始序列号等信息，构建一个 DELIVER_SEEK_INFO 类型的信封。
// 该信封用于向远程排序节点请求从 startSeq 序列号开始的下一个区块。
// 计算并绑定 TLS 证书摘要，以确保信封的完整性和安全性。
func (p *BlockPuller) seekNextEnvelope(startSeq uint64) (*common.Envelope, error) {
	// 构建一个用于拉取下一个区块的签名信封
	return protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO, // 表示这是一个用于拉取区块的请求信封
		p.Channel,                           // 表示请求的目标通道名称
		p.Signer,                            // 表示用于签名信封的签名者对象
		nextSeekInfo(startSeq),              // 表示构建用于拉取从 startSeq 开始的下一个区块的 SeekInfo
		int32(0),                            // 表示版本号，这里设置为默认值
		uint64(0),                           // 最大消息长度，这里设置为默认值
		util.ComputeGMSM3(p.TLSCert),        // 表示计算并绑定 TLS 证书摘要，用于确保信封的完整性和安全性
	)
}

func last() *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Newest{Newest: &orderer.SeekNewest{}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func nextSeekInfo(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func blockSize(block *common.Block) int {
	return len(protoutil.MarshalOrPanic(block))
}

// endpointInfo 结构体用于存储关于远程排序节点的信息。
type endpointInfo struct {
	// 远程排序节点的网络地址，通常形式为 "host:port"
	endpoint string
	// 与远程排序节点的 gRPC 连接
	conn *grpc.ClientConn
	// 从该远程排序节点获取的最新区块的序列号
	lastBlockSeq uint64
}

type endpointInfoBucket struct {
	bucket <-chan *endpointInfo
	logger *flogging.FabricLogger
	err    error
}

func (eib endpointInfoBucket) byEndpoints() map[string]*endpointInfo {
	infoByEndpoints := make(map[string]*endpointInfo)
	for endpointInfo := range eib.bucket {
		if _, exists := infoByEndpoints[endpointInfo.endpoint]; exists {
			eib.logger.Warningf("Duplicate endpoint found(%s), skipping it", endpointInfo.endpoint)
			endpointInfo.conn.Close()
			continue
		}
		infoByEndpoints[endpointInfo.endpoint] = endpointInfo
	}
	return infoByEndpoints
}

// ImpatientStreamCreator creates an ImpatientStream
type ImpatientStreamCreator func() (*ImpatientStream, error)

// ImpatientStream aborts the stream if it waits for too long for a message.
type ImpatientStream struct {
	waitTimeout time.Duration
	orderer.AtomicBroadcast_DeliverClient
	cancelFunc func()
}

func (stream *ImpatientStream) abort() {
	stream.cancelFunc()
}

// Recv blocks until a response is received from the stream or the
// timeout expires.
func (stream *ImpatientStream) Recv() (*orderer.DeliverResponse, error) {
	// Initialize a timeout to cancel the stream when it expires
	timeout := time.NewTimer(stream.waitTimeout)
	defer timeout.Stop()

	responseChan := make(chan errorAndResponse, 1)

	// receive waitGroup ensures the goroutine below exits before
	// this function exits.
	var receive sync.WaitGroup
	receive.Add(1)
	defer receive.Wait()

	go func() {
		defer receive.Done()
		resp, err := stream.AtomicBroadcast_DeliverClient.Recv()
		responseChan <- errorAndResponse{err: err, resp: resp}
	}()

	select {
	case <-timeout.C:
		stream.cancelFunc()
		return nil, errors.Errorf("didn't receive a response within %v", stream.waitTimeout)
	case respAndErr := <-responseChan:
		return respAndErr.resp, respAndErr.err
	}
}

// NewImpatientStream returns a ImpatientStreamCreator that creates impatientStreams.
func NewImpatientStream(conn *grpc.ClientConn, waitTimeout time.Duration) ImpatientStreamCreator {
	return func() (*ImpatientStream, error) {
		abc := orderer.NewAtomicBroadcastClient(conn)
		ctx, cancel := context.WithCancel(context.Background())

		stream, err := abc.Deliver(ctx)
		if err != nil {
			cancel()
			return nil, err
		}

		once := &sync.Once{}
		return &ImpatientStream{
			waitTimeout: waitTimeout,
			// The stream might be canceled while Close() is being called, but also
			// while a timeout expires, so ensure it's only called once.
			cancelFunc: func() {
				once.Do(cancel)
			},
			AtomicBroadcast_DeliverClient: stream,
		}, nil
	}
}

type errorAndResponse struct {
	err  error
	resp *orderer.DeliverResponse
}
