/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"bytes"
	"time"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/extcc"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/container/ccintf"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/scc"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
)

const (
	// InitializedKeyName is the reserved key in a chaincode's namespace which
	// records the ID of the chaincode which initialized the namespace.
	// In this way, we can enforce Init exactly once semantics, whenever
	// the backing chaincode bytes change (but not be required to re-initialize
	// the chaincode say, when endorsement policy changes).
	InitializedKeyName = "\x00" + string(utf8.MaxRune) + "initialized"
)

// Runtime is used to manage chaincode runtime instances.
type Runtime interface {
	Build(ccid string) (*ccintf.ChaincodeServerInfo, error)
	Start(ccid string, ccinfo *ccintf.PeerConnection) error
	Stop(ccid string) error
	Wait(ccid string) (int, error)
}

// Launcher is used to launch chaincode runtimes.
type Launcher interface {
	Launch(ccid string, streamHandler extcc.StreamHandler) error
	Stop(ccid string) error
}

// Lifecycle provides a way to retrieve chaincode definitions and the packages necessary to run them
type Lifecycle interface {
	// ChaincodeEndorsementInfo looks up the chaincode info in the given channel.  It is the responsibility
	// of the implementation to add appropriate read dependencies for the information returned.
	ChaincodeEndorsementInfo(channelID, chaincodeName string, qe ledger.SimpleQueryExecutor) (*lifecycle.ChaincodeEndorsementInfo, error)
}

// ChaincodeSupport 负责与 Peer 上的链码进行接口交互。
type ChaincodeSupport struct {
	ACLProvider            ACLProvider                          // ACL 提供者
	AppConfig              ApplicationConfigRetriever           // 应用配置检索器
	BuiltinSCCs            scc.BuiltinSCCs                      // 内置 SCC
	DeployedCCInfoProvider ledger.DeployedChaincodeInfoProvider // 已部署链码信息提供者
	ExecuteTimeout         time.Duration                        // 执行超时时间
	InstallTimeout         time.Duration                        // 安装超时时间
	HandlerMetrics         *HandlerMetrics                      // 处理器指标
	HandlerRegistry        *HandlerRegistry                     // 处理器注册表
	Keepalive              time.Duration                        // Keepalive 时间
	Launcher               Launcher                             // 启动器
	Lifecycle              Lifecycle                            // 生命周期
	Peer                   *peer.Peer                           // Peer
	Runtime                Runtime                              // 运行时
	TotalQueryLimit        int                                  // 总查询限制
	UserRunsCC             bool                                 // 用户运行链码
}

// Launch starts executing chaincode if it is not already running. This method
// blocks until the peer side handler gets into ready state or encounters a fatal
// error. If the chaincode is already running, it simply returns.
func (cs *ChaincodeSupport) Launch(ccid string) (*Handler, error) {
	if h := cs.HandlerRegistry.Handler(ccid); h != nil {
		return h, nil
	}

	if err := cs.Launcher.Launch(ccid, cs); err != nil {
		return nil, errors.Wrapf(err, "could not launch chaincode %s", ccid)
	}

	h := cs.HandlerRegistry.Handler(ccid)
	if h == nil {
		return nil, errors.Errorf("claimed to start chaincode container for %s but could not find handler", ccid)
	}

	return h, nil
}

// LaunchInProc 是一个临时解决方案，由 inproccontroller 调用，用于允许系统链码注册
// 方法接收者：
//   - cs：ChaincodeSupport 结构体的指针，表示链码支持
//
// 输入参数：
//   - ccid：string 类型，表示链码的 ChaincodeID
//
// 返回值：
//   - <-chan struct{}：表示一个完成信号通道
func (cs *ChaincodeSupport) LaunchInProc(ccid string) <-chan struct{} {
	// 检查是否已经在启动中
	launchStatus, ok := cs.HandlerRegistry.Launching(ccid)
	if ok {
		chaincodeLogger.Panicf("attempted to launch an already launched system chaincode")
	}

	// 返回完成信号通道
	return launchStatus.Done()
}

// HandleChaincodeStream 为所有vm实现ccintf.HandleChaincodeStream以使用适当的流调用
func (cs *ChaincodeSupport) HandleChaincodeStream(stream ccintf.ChaincodeStream) error {
	var deserializerFactory privdata.IdentityDeserializerFactoryFunc = func(channelID string) msp.IdentityDeserializer {
		return cs.Peer.Channel(channelID).MSPManager()
	}
	handler := &Handler{
		Invoker:                cs,
		Keepalive:              cs.Keepalive,
		Registry:               cs.HandlerRegistry,
		ACLProvider:            cs.ACLProvider,
		TXContexts:             NewTransactionContexts(),
		ActiveTransactions:     NewActiveTransactions(),
		BuiltinSCCs:            cs.BuiltinSCCs,
		QueryResponseBuilder:   &QueryResponseGenerator{MaxResultLimit: 100},
		UUIDGenerator:          UUIDGeneratorFunc(util.GenerateUUID),
		LedgerGetter:           cs.Peer,
		IDDeserializerFactory:  deserializerFactory,
		DeployedCCInfoProvider: cs.DeployedCCInfoProvider,
		AppConfig:              cs.AppConfig,
		Metrics:                cs.HandlerMetrics,
		TotalQueryLimit:        cs.TotalQueryLimit,
	}

	return handler.ProcessStream(stream)
}

// Register the bidi stream entry point called by chaincode to register with the Peer.
func (cs *ChaincodeSupport) Register(stream pb.ChaincodeSupport_RegisterServer) error {
	return cs.HandleChaincodeStream(stream)
}

// ExecuteLegacyInit is a temporary method which should be removed once the old style lifecycle
// is entirely deprecated.  Ideally one release after the introduction of the new lifecycle.
// It does not attempt to start the chaincode based on the information from lifecycle, but instead
// accepts the container information directly in the form of a ChaincodeDeploymentSpec.
func (cs *ChaincodeSupport) ExecuteLegacyInit(txParams *ccprovider.TransactionParams, ccName, ccVersion string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error) {
	// FIXME: this is a hack, we shouldn't construct the
	// ccid manually but rather let lifecycle construct it
	// for us. However this is legacy code that will disappear
	// so it is acceptable for now (FAB-14627)
	ccid := ccName + ":" + ccVersion

	h, err := cs.Launch(ccid)
	if err != nil {
		return nil, nil, err
	}

	resp, err := cs.execute(pb.ChaincodeMessage_INIT, txParams, ccName, input, h)
	return processChaincodeExecutionResult(txParams.TxID, ccName, resp, err)
}

// Execute 调用链码并返回原始响应。
// 方法接收者：cs *ChaincodeSupport
// 输入参数：
//   - txParams：交易参数。
//   - chaincodeName：链码名称。
//   - input：链码输入。
//
// 返回值：
//   - *pb.Response：链码响应。
//   - *pb.ChaincodeEvent：链码事件。
//   - error：如果执行过程中出现错误，则返回错误；否则返回nil。
func (cs *ChaincodeSupport) Execute(txParams *ccprovider.TransactionParams, chaincodeName string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error) {
	// 将调用chaincode并返回包含响应的消息。如果链码尚未运行，则将启动链码
	resp, err := cs.Invoke(txParams, chaincodeName, input)
	return processChaincodeExecutionResult(txParams.TxID, chaincodeName, resp, err)
}

// 处理链码执行结果
func processChaincodeExecutionResult(txid, ccName string, resp *pb.ChaincodeMessage, err error) (*pb.Response, *pb.ChaincodeEvent, error) {
	if err != nil {
		return nil, nil, errors.Wrapf(err, "链码执行失败, 交易id: %s", txid)
	}
	if resp == nil {
		return nil, nil, errors.Errorf("来自链码执行结果无响应, 交易id: %s", txid)
	}

	// chaincode发出的事件。仅与Init或Invoke一起使用。
	if resp.ChaincodeEvent != nil {
		resp.ChaincodeEvent.ChaincodeId = ccName
		resp.ChaincodeEvent.TxId = txid
	}

	switch resp.Type {
	case pb.ChaincodeMessage_COMPLETED:
		res := &pb.Response{}
		err := proto.Unmarshal(resp.Payload, res)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "未能反序列化执行链码结果的响应 %s, 交易id", txid)
		}
		return res, resp.ChaincodeEvent, nil

	case pb.ChaincodeMessage_ERROR:
		return nil, resp.ChaincodeEvent, errors.Errorf("执行链码返回失败: %s", resp.Payload)

	default:
		return nil, nil, errors.Errorf("执行链码意外的响应类型 %d, 交易id: %s", resp.Type, txid)
	}
}

// Invoke 将调用链码并返回包含响应的消息。
// 如果链码尚未运行，将启动链码。
// 方法接收者：cs *ChaincodeSupport
// 输入参数：
//   - txParams：交易参数。
//   - chaincodeName：链码名称。
//   - input：链码输入。
//
// 返回值：
//   - *pb.ChaincodeMessage：包含响应的消息。
//   - error：如果执行过程中出现错误，则返回错误；否则返回nil。
func (cs *ChaincodeSupport) Invoke(txParams *ccprovider.TransactionParams, chaincodeName string, input *pb.ChaincodeInput) (*pb.ChaincodeMessage, error) {
	ccid, cctype, err := cs.CheckInvocation(txParams, chaincodeName, input)
	if err != nil {
		return nil, errors.WithMessage(err, "无效的调用")
	}

	h, err := cs.Launch(ccid)
	if err != nil {
		return nil, err
	}

	return cs.execute(cctype, txParams, chaincodeName, input, h)
}

// CheckInvocation 检查调用的参数，并确定如何以及到哪里路由该调用。
// 首先，我们根据生命周期实现，确保目标命名空间在通道上定义，并且在此对等节点上可调用。
// 然后，如果链码定义要求，此函数强制执行“仅初始化一次”的语义。
// 最后，它返回要路由到的链码ID和请求的消息类型（普通交易或初始化）。
// 方法接收者：cs *ChaincodeSupport
// 输入参数：
//   - txParams：交易参数。
//   - chaincodeName：链码名称。
//   - input：链码输入。
//
// 返回值：
//   - ccid：要路由到的链码ID。
//   - cctype：请求的消息类型（普通交易或初始化）。
//   - error：如果检查过程中出现错误，则返回错误；否则返回nil。
func (cs *ChaincodeSupport) CheckInvocation(txParams *ccprovider.TransactionParams, chaincodeName string, input *pb.ChaincodeInput) (ccid string, cctype pb.ChaincodeMessage_Type, err error) {
	chaincodeLogger.Debugf("[%s] getting chaincode data for %s on channel %s", shorttxid(txParams.TxID), chaincodeName, txParams.ChannelID)
	cii, err := cs.Lifecycle.ChaincodeEndorsementInfo(txParams.ChannelID, chaincodeName, txParams.TXSimulator)
	if err != nil {
		logDevModeError(cs.UserRunsCC)
		return "", 0, errors.Wrapf(err, "[channel %s] failed to get chaincode container info for %s", txParams.ChannelID, chaincodeName)
	}

	needsInitialization := false
	if cii.EnforceInit {

		value, err := txParams.TXSimulator.GetState(chaincodeName, InitializedKeyName)
		if err != nil {
			return "", 0, errors.WithMessage(err, "could not get 'initialized' key")
		}

		needsInitialization = !bytes.Equal(value, []byte(cii.Version))
	}

	// Note, IsInit is a new field for v2.0 and should only be set for invocations of non-legacy chaincodes.
	// Any invocation of a legacy chaincode with IsInit set will fail.  This is desirable, as the old
	// InstantiationPolicy contract enforces which users may call init.
	if input.IsInit {
		if !cii.EnforceInit {
			return "", 0, errors.Errorf("chaincode '%s' does not require initialization but called as init", chaincodeName)
		}

		if !needsInitialization {
			return "", 0, errors.Errorf("chaincode '%s' is already initialized but called as init", chaincodeName)
		}

		err = txParams.TXSimulator.SetState(chaincodeName, InitializedKeyName, []byte(cii.Version))
		if err != nil {
			return "", 0, errors.WithMessage(err, "could not set 'initialized' key")
		}

		return cii.ChaincodeID, pb.ChaincodeMessage_INIT, nil
	}

	if needsInitialization {
		return "", 0, errors.Errorf("chaincode '%s' has not been initialized for this version, must call as init first", chaincodeName)
	}

	return cii.ChaincodeID, pb.ChaincodeMessage_TRANSACTION, nil
}

// execute executes a transaction and waits for it to complete until a timeout value.
func (cs *ChaincodeSupport) execute(cctyp pb.ChaincodeMessage_Type, txParams *ccprovider.TransactionParams, namespace string, input *pb.ChaincodeInput, h *Handler) (*pb.ChaincodeMessage, error) {
	input.Decorations = txParams.ProposalDecorations

	payload, err := proto.Marshal(input)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create chaincode message")
	}

	ccMsg := &pb.ChaincodeMessage{
		Type:      cctyp,
		Payload:   payload,
		Txid:      txParams.TxID,
		ChannelId: txParams.ChannelID,
	}

	timeout := cs.executeTimeout(namespace, input)
	ccresp, err := h.Execute(txParams, namespace, ccMsg, timeout)
	if err != nil {
		return nil, errors.WithMessage(err, "error sending")
	}

	return ccresp, nil
}

func (cs *ChaincodeSupport) executeTimeout(namespace string, input *pb.ChaincodeInput) time.Duration {
	operation := chaincodeOperation(input.Args)
	switch {
	case namespace == "lscc" && operation == "install":
		return maxDuration(cs.InstallTimeout, cs.ExecuteTimeout)
	case namespace == lifecycle.LifecycleNamespace && operation == lifecycle.InstallChaincodeFuncName:
		return maxDuration(cs.InstallTimeout, cs.ExecuteTimeout)
	default:
		return cs.ExecuteTimeout
	}
}

func maxDuration(durations ...time.Duration) time.Duration {
	var result time.Duration
	for _, d := range durations {
		if d > result {
			result = d
		}
	}
	return result
}

func chaincodeOperation(args [][]byte) string {
	if len(args) == 0 {
		return ""
	}
	return string(args[0])
}

func logDevModeError(userRunsCC bool) {
	if userRunsCC {
		chaincodeLogger.Error("You are attempting to perform an action other than Deploy on Chaincode that is not ready and you are in developer mode. Did you forget to Deploy your chaincode?")
	}
}
