/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric-protos-go/transientstore"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var endorserLogger = flogging.MustGetLogger("endorser")

// The Jira issue that documents Endorser flow along with its relationship to
// the lifecycle chaincode - https://jira.hyperledger.org/browse/FAB-181

//go:generate counterfeiter -o fake/prvt_data_distributor.go --fake-name PrivateDataDistributor . PrivateDataDistributor

type PrivateDataDistributor interface {
	DistributePrivateData(channel string, txID string, privateData *transientstore.TxPvtReadWriteSetWithConfigInfo, blkHt uint64) error
}

// Support 包含了背书者执行任务所需的函数。
type Support interface {
	identity.SignerSerializer

	// GetTxSimulator 返回指定账本的交易模拟器
	// 客户端可以获取多个交易模拟器；通过提供的 txid 来使它们唯一
	GetTxSimulator(ledgername string, txid string) (ledger.TxSimulator, error)

	// GetHistoryQueryExecutor 返回指定账本的历史查询执行器
	GetHistoryQueryExecutor(ledgername string) (ledger.HistoryQueryExecutor, error)

	// GetTransactionByID 根据ID检索交易
	GetTransactionByID(chid, txID string) (*pb.ProcessedTransaction, error)

	// IsSysCC 如果名称匹配系统链码，则返回 true
	// 系统链码的名称是系统级别的，适用于整个链
	IsSysCC(name string) bool

	// Execute - 执行提案，返回链码的原始响应
	Execute(txParams *ccprovider.TransactionParams, name string, input *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error)

	// ExecuteLegacyInit - 执行部署提案，返回链码的原始响应
	ExecuteLegacyInit(txParams *ccprovider.TransactionParams, name, version string, spec *pb.ChaincodeInput) (*pb.Response, *pb.ChaincodeEvent, error)

	// ChaincodeEndorsementInfo 返回背书链码所需的生命周期信息
	ChaincodeEndorsementInfo(channelID, chaincodeID string, txsim ledger.QueryExecutor) (*lifecycle.ChaincodeEndorsementInfo, error)

	// CheckACL 检查通道上资源的访问控制列表（ACL）
	CheckACL(channelID string, signedProp *pb.SignedProposal) error

	// EndorseWithPlugin 使用插件对响应进行背书
	EndorseWithPlugin(pluginName, channnelID string, prpBytes []byte, signedProposal *pb.SignedProposal) (*pb.Endorsement, []byte, error)

	// GetLedgerHeight 返回给定通道ID的账本高度
	GetLedgerHeight(channelID string) (uint64, error)

	// GetDeployedCCInfoProvider 返回已部署的链码信息提供程序
	GetDeployedCCInfoProvider() ledger.DeployedChaincodeInfoProvider
}

//go:generate counterfeiter -o fake/channel_fetcher.go --fake-name ChannelFetcher . ChannelFetcher

// ChannelFetcher 是一个用于获取给定通道ID的通道上下文的接口。
type ChannelFetcher interface {
	// Channel 根据给定的通道ID返回对应的通道上下文。
	// 输入参数：
	//   - channelID：通道ID。
	// 返回值：
	//   - *Channel：给定通道ID的通道上下文。
	Channel(channelID string) *Channel
}

type Channel struct {
	IdentityDeserializer msp.IdentityDeserializer
}

// Endorser 提供 Endorser 背书服务的 ProcessProposal 方法
type Endorser struct {
	ChannelFetcher         ChannelFetcher           // ChannelFetcher 用于获取通道信息
	LocalMSP               msp.IdentityDeserializer // LocalMSP 是本地 MSP 的身份反序列化器
	PrivateDataDistributor PrivateDataDistributor   // PrivateDataDistributor 用于分发私有数据
	Support                Support                  // Support 是插件支持接口的实例
	PvtRWSetAssembler      PvtRWSetAssembler        // PvtRWSetAssembler 用于组装私有读写集
	Metrics                *Metrics                 // Metrics 是指标对象，用于记录和报告指标数据
}

// callChaincode 调用指定的链代码 (系统或用户)
func (e *Endorser) callChaincode(txParams *ccprovider.TransactionParams, input *pb.ChaincodeInput, chaincodeName string) (*pb.Response, *pb.ChaincodeEvent, error) {
	defer func(start time.Time) {
		logger := endorserLogger.WithOptions(zap.AddCallerSkip(1))
		logger = decorateLogger(logger, txParams)
		elapsedMillisec := time.Since(start).Milliseconds()
		logger.Infof("已完成的链码: %s 持续时间: %dms", chaincodeName, elapsedMillisec)
	}(time.Now())

	meterLabels := []string{
		"channel", txParams.ChannelID,
		"chaincode", chaincodeName,
	}

	// Execute - 执行提案，返回链码的原始响应
	res, ccevent, err := e.Support.Execute(txParams, chaincodeName, input)
	if err != nil {
		// 模拟失败的计数器
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	// 每 doc 任何状态码 <400 都可以作为TX发送.
	// fabric 错误状态码总是 >= 400 (即明确的错误)
	// “lscc” 将响应状态200或500 (即，明确的OK或错误)
	if res.Status >= shim.ERRORTHRESHOLD {
		return res, nil, nil
	}

	// 除非这是weirdo LSCC案例，否则只需返回
	if chaincodeName != "lscc" || len(input.Args) < 3 || (string(input.Args[0]) != "deploy" && string(input.Args[0]) != "upgrade") {
		return res, ccevent, nil
	}

	// ----- 开始-可能需要在LSCC中完成的部分 ------
	// 如果这是一个部署链码的调用，我们需要一种机制来将 TxSimulator 传递到 LSCC。
	// 直到制定出这个特殊的代码做实际的部署，
	// 在这里升级，以便收集一个TxSimulator下的所有状态
	//
	// 请注意，如果有一个错误的所有模拟，
	// 在lscc中包含链码表更改将被丢弃

	// 将字节反序列化 ChaincodeDeploymentSpec 指定链码的部署对象(链码规范, 链码包)。
	cds, err := protoutil.UnmarshalChaincodeDeploymentSpec(input.Args[2])
	if err != nil {
		// 模拟失败的计数器
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	// 这不应该是系统链代码
	if e.Support.IsSysCC(cds.ChaincodeSpec.ChaincodeId.Name) {
		// 模拟失败的计数器
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, errors.Errorf("正在尝试部署系统链码 %s/%s, 这是不支持的", cds.ChaincodeSpec.ChaincodeId.Name, txParams.ChannelID)
	}

	if len(cds.CodePackage) != 0 {
		// 模拟失败的计数器
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, errors.Errorf("lscc 升级/部署 不应包含链码包")
	}

	// ExecuteLegacyInit - 执行部署提案，返回链码的原始响应
	_, _, err = e.Support.ExecuteLegacyInit(txParams, cds.ChaincodeSpec.ChaincodeId.Name, cds.ChaincodeSpec.ChaincodeId.Version, cds.ChaincodeSpec.Input)
	if err != nil {
		// 递增失败以指示 安装/升级 失败
		meterLabels = []string{
			"channel", txParams.ChannelID,
			"chaincode", cds.ChaincodeSpec.ChaincodeId.Name,
		}
		// 初始化失败的计数器
		e.Metrics.InitFailed.With(meterLabels...).Add(1)
		return nil, nil, err
	}

	return res, ccevent, err
}

// simulateProposal 通过调用链码模拟提案
func (e *Endorser) simulateProposal(txParams *ccprovider.TransactionParams, chaincodeName string, chaincodeInput *pb.ChaincodeInput) (*pb.Response, []byte, *pb.ChaincodeEvent, *pb.ChaincodeInterest, error) {
	logger := decorateLogger(endorserLogger, txParams)

	meterLabels := []string{
		"channel", txParams.ChannelID,
		"chaincode", chaincodeName,
	}

	// ---3. 执行建议并获得模拟结果
	res, ccevent, err := e.callChaincode(txParams, chaincodeInput, chaincodeName)
	if err != nil {
		logger.Errorf("调用链码 %s 失败, 错误: % + v", chaincodeName, err)
		return nil, nil, nil, nil, err
	}

	if txParams.TXSimulator == nil {
		return res, nil, ccevent, nil, nil
	}

	// Note, this is a little goofy, as if there is private data, Done() gets called
	// early, so this is invoked multiple times, but that is how the code worked before
	// this change, so, should be safe.  Long term, let's move the Done up to the create.
	defer txParams.TXSimulator.Done()

	simResult, err := txParams.TXSimulator.GetTxSimulationResults()
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, nil, nil, err
	}

	if simResult.PvtSimulationResults != nil {
		if chaincodeName == "lscc" {
			// TODO: remove once we can store collection configuration outside of LSCC
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.New("Private data is forbidden to be used in instantiate")
		}
		pvtDataWithConfig, err := AssemblePvtRWSet(txParams.ChannelID, simResult.PvtSimulationResults, txParams.TXSimulator, e.Support.GetDeployedCCInfoProvider())
		// To read collection config need to read collection updates before
		// releasing the lock, hence txParams.TXSimulator.Done()  moved down here
		txParams.TXSimulator.Done()

		if err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.WithMessage(err, "failed to obtain collections config")
		}
		endorsedAt, err := e.Support.GetLedgerHeight(txParams.ChannelID)
		if err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, errors.WithMessage(err, fmt.Sprintf("failed to obtain ledger height for channel '%s'", txParams.ChannelID))
		}
		// Add ledger height at which transaction was endorsed,
		// `endorsedAt` is obtained from the block storage and at times this could be 'endorsement Height + 1'.
		// However, since we use this height only to select the configuration (3rd parameter in distributePrivateData) and
		// manage transient store purge for orphaned private writesets (4th parameter in distributePrivateData), this works for now.
		// Ideally, ledger should add support in the simulator as a first class function `GetHeight()`.
		pvtDataWithConfig.EndorsedAt = endorsedAt
		if err := e.PrivateDataDistributor.DistributePrivateData(txParams.ChannelID, txParams.TxID, pvtDataWithConfig, endorsedAt); err != nil {
			e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
			return nil, nil, nil, nil, err
		}
	}

	ccInterest, err := e.buildChaincodeInterest(simResult)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	pubSimResBytes, err := simResult.GetPubSimulationBytes()
	if err != nil {
		e.Metrics.SimulationFailure.With(meterLabels...).Add(1)
		return nil, nil, nil, nil, err
	}

	return res, pubSimResBytes, ccevent, ccInterest, nil
}

// preProcess 检查交易提案的头部、唯一性和访问控制列表（ACL）。
// 方法接收者：e *Endorser
// 输入参数：
//   - up：解包后的提案。
//   - channel：通道上下文。
//
// 返回值：
//   - error：如果预处理过程中出现错误，则返回错误；否则返回nil。
func (e *Endorser) preProcess(up *UnpackedProposal, channel *Channel) error {
	// 首先，我们检查消息是否有效
	err := up.Validate(channel.IdentityDeserializer)
	if err != nil {
		// 提案验证失败的计数器
		e.Metrics.ProposalValidationFailed.Add(1)
		return errors.WithMessage(err, "验证提案时出错")
	}

	if up.ChannelHeader.ChannelId == "" {
		// 无链的提案不会/不能影响账本，也不能作为交易提交
		// 忽略唯一性检查；此外，无链的提案不会根据链的策略进行验证
		// 因为根据定义，无链的提案没有链；它们会根据对等节点的本地MSP进行验证
		return nil
	}

	// 提供失败指标的标签
	meterLabels := []string{
		"channel", up.ChannelHeader.ChannelId,
		"chaincode", up.ChaincodeName,
	}

	// 在这里处理针对链的唯一性检查和ACL
	// 注意，ValidateProposalMessage已经验证了TxID的正确计算
	if _, err = e.Support.GetTransactionByID(up.ChannelHeader.ChannelId, up.ChannelHeader.TxId); err == nil {
		// 增加由于重复交易而导致的失败次数。对于检测重放攻击以及良性重试很有用
		e.Metrics.DuplicateTxsFailure.With(meterLabels...).Add(1)
		return errors.Errorf("找到重复的事务 [%s]. 签名者 [%x]", up.ChannelHeader.TxId, up.SignatureHeader.Creator)
	}

	// 仅对应用链码检查ACL；系统链码的ACL在其他地方检查
	if !e.Support.IsSysCC(up.ChaincodeName) {
		// 检查提案是否符合通道的写入者
		if err = e.Support.CheckACL(up.ChannelHeader.ChannelId, up.SignedProposal); err != nil {
			e.Metrics.ProposalACLCheckFailed.With(meterLabels...).Add(1)
			return err
		}
	}

	return nil
}

// ProcessProposal 处理提案
// 与提案本身相关的错误将返回一个导致grpc错误的错误。
// 与提案处理相关的错误 (基础结构错误或链码错误) 返回nil错误，
// 客户端需要查看ProposalResponse响应状态代码 (例如500) 和消息。
func (e *Endorser) ProcessProposal(ctx context.Context, signedProp *pb.SignedProposal) (*pb.ProposalResponse, error) {
	// 计算已成功批准的提案的经过时间度量的开始时间
	startTime := time.Now()
	// 收到的提案计数器
	e.Metrics.ProposalsReceived.Add(1)

	// 根据上下文，解析远端地址
	addr := util.ExtractRemoteAddress(ctx)
	endorserLogger.Debug("提案请求来自: ", addr)

	// 用于捕获提案持续时间度量的变量
	success := false

	// 解析提案
	up, err := UnpackProposal(signedProp)
	if err != nil {
		// 提案验证失败的计数器
		e.Metrics.ProposalValidationFailed.Add(1)
		endorserLogger.Warnw("解析提案失败", "error", err.Error())
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, err
	}

	var channel *Channel
	if up.ChannelID() != "" {
		// ChannelFetcher 用于获取通道信息, 根据给定的通道ID返回对应的通道上下文
		channel = e.ChannelFetcher.Channel(up.ChannelID())
		if channel == nil {
			return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: fmt.Sprintf("通道 '%s' 未找到", up.ChannelHeader.ChannelId)}}, nil
		}
	} else {
		// 配置身份信息
		channel = &Channel{
			IdentityDeserializer: e.LocalMSP,
		}
	}

	// 0 -- 检查和验证。检查交易提案的头部、唯一性和访问控制列表（ACL）
	err = e.preProcess(up, channel)
	if err != nil {
		endorserLogger.Warnw("未能预处理提案", "error", err.Error())
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, err
	}

	defer func() {
		meterLabels := []string{
			"channel", up.ChannelHeader.ChannelId,
			"chaincode", up.ChaincodeName,
			"success", strconv.FormatBool(success),
		}
		// 提案持续时间的直方图
		e.Metrics.ProposalDuration.With(meterLabels...).Observe(time.Since(startTime).Seconds())
	}()

	pResp, err := e.ProcessProposalSuccessfullyOrError(up)
	if err != nil {
		endorserLogger.Warnw("Failed to invoke chaincode", "channel", up.ChannelHeader.ChannelId, "chaincode", up.ChaincodeName, "error", err.Error())
		// Return a nil error since clients are expected to look at the ProposalResponse response status code (500) and message.
		return &pb.ProposalResponse{Response: &pb.Response{Status: 500, Message: err.Error()}}, nil
	}

	if pResp.Endorsement != nil || up.ChannelHeader.ChannelId == "" {
		// We mark the tx as successful only if it was successfully endorsed, or
		// if it was a system chaincode on a channel-less channel and therefore
		// cannot be endorsed.
		success = true

		// total failed proposals = ProposalsReceived-SuccessfulProposals
		e.Metrics.SuccessfulProposals.Add(1)
	}
	return pResp, nil
}

func (e *Endorser) ProcessProposalSuccessfullyOrError(up *UnpackedProposal) (*pb.ProposalResponse, error) {
	// 是与特定交易相关联的参数，用于调用链码。
	txParams := &ccprovider.TransactionParams{
		ChannelID:  up.ChannelHeader.ChannelId, // 通道ID
		TxID:       up.ChannelHeader.TxId,      // 交易ID
		SignedProp: up.SignedProposal,          // 签名的提案
		Proposal:   up.Proposal,                // 原始提案
	}

	logger := decorateLogger(endorserLogger, txParams)

	// 获取Tx交易模拟器, 确定是否应该为提案获取事务模拟器。不要为查询qscc和配置cscc系统链码获取模拟器。
	if acquireTxSimulator(up.ChannelHeader.ChannelId, up.ChaincodeName) {
		// 获取事务模拟器
		// Support 包含了背书者执行任务所需的函数。
		// GetTxSimulator 返回指定账本的交易模拟器
		// 客户端可以获取多个交易模拟器；通过提供的 txid 来使它们唯一
		txSim, err := e.Support.GetTxSimulator(up.ChannelID(), up.TxID())
		if err != nil {
			return nil, err
		}

		// txsim 在 stateDB 上获取了一个共享锁。由于这会影响区块提交（即将有效的写集提交到 stateDB），
		// 我们必须尽早释放锁定。因此，在模拟提案（simulateProposal）中，一旦模拟了事务并收集了 rwset 结果，
		// 就会立即关闭此 txsim 对象，并在必要时进行私有数据的 gossip 传播之前。为了安全起见，
		// 我们添加了下面的 defer 语句，当发生错误时非常有用。请注意，多次调用 txsim.Done() 不会引发任何问题。
		// 如果 txsim 已经被释放，下面的 txsim.Done() 只会返回。
		defer txSim.Done()

		// 获取历史查询执行器
		hqe, err := e.Support.GetHistoryQueryExecutor(up.ChannelID())
		if err != nil {
			return nil, err
		}

		// 将事务模拟器和历史查询执行器设置到 txParams 中
		txParams.TXSimulator = txSim
		txParams.HistoryQueryExecutor = hqe
	}

	// ChaincodeEndorsementInfo 返回背书链码所需的生命周期信息, 现需要用到事务模拟器
	cdLedger, err := e.Support.ChaincodeEndorsementInfo(up.ChannelID(), up.ChaincodeName, txParams.TXSimulator)
	if err != nil {
		return nil, errors.WithMessagef(err, "确保已在通道 %s 上成功定义链码 %s, 然后重试", up.ChannelID(), up.ChaincodeName)
	}

	// 1 -- 模拟提案
	res, simulationResult, ccevent, ccInterest, err := e.simulateProposal(txParams, up.ChaincodeName, up.Input)
	if err != nil {
		return nil, errors.WithMessage(err, "模拟提案错误")
	}

	cceventBytes, err := CreateCCEventBytes(ccevent)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal chaincode event")
	}

	prpBytes, err := protoutil.GetBytesProposalResponsePayload(up.ProposalHash, res, simulationResult, cceventBytes, &pb.ChaincodeID{
		Name:    up.ChaincodeName,
		Version: cdLedger.Version,
	})
	if err != nil {
		logger.Warning("Failed marshaling the proposal response payload to bytes", err)
		return nil, errors.WithMessage(err, "failed to create the proposal response")
	}

	// if error, capture endorsement failure metric
	meterLabels := []string{
		"channel", up.ChannelID(),
		"chaincode", up.ChaincodeName,
	}

	switch {
	case res.Status >= shim.ERROR:
		return &pb.ProposalResponse{
			Response: res,
			Payload:  prpBytes,
			Interest: ccInterest,
		}, nil
	case up.ChannelID() == "":
		// Chaincode invocations without a channel ID is a broken concept
		// that should be removed in the future.  For now, return unendorsed
		// success.
		return &pb.ProposalResponse{
			Response: res,
		}, nil
	case res.Status >= shim.ERRORTHRESHOLD:
		meterLabels = append(meterLabels, "chaincodeerror", strconv.FormatBool(true))
		e.Metrics.EndorsementsFailed.With(meterLabels...).Add(1)
		logger.Debugf("chaincode error %d", res.Status)
		return &pb.ProposalResponse{
			Response: res,
		}, nil
	}

	escc := cdLedger.EndorsementPlugin

	logger.Debugf("escc for chaincode %s is %s", up.ChaincodeName, escc)

	// Note, mPrpBytes is the same as prpBytes by default endorsement plugin, but others could change it.
	endorsement, mPrpBytes, err := e.Support.EndorseWithPlugin(escc, up.ChannelID(), prpBytes, up.SignedProposal)
	if err != nil {
		meterLabels = append(meterLabels, "chaincodeerror", strconv.FormatBool(false))
		e.Metrics.EndorsementsFailed.With(meterLabels...).Add(1)
		return nil, errors.WithMessage(err, "endorsing with plugin failed")
	}

	return &pb.ProposalResponse{
		Version:     1,
		Endorsement: endorsement,
		Payload:     mPrpBytes,
		Response:    res,
		Interest:    ccInterest,
	}, nil
}

// Using the simulation results, build the ChaincodeInterest structure that the client can pass to the discovery service
// to get the correct endorsement policy for the chaincode(s) and any collections encountered.
func (e *Endorser) buildChaincodeInterest(simResult *ledger.TxSimulationResults) (*pb.ChaincodeInterest, error) {
	// build a structure that collates all the information needed for the chaincode interest:
	policies, err := parseWritesetMetadata(simResult.WritesetMetadata)
	if err != nil {
		return nil, err
	}

	// There might be public states that are read and not written.  Need to add these to the policyRequired structure.
	// This will also include private reads, because the hashed read will appear in the public RWset.
	for _, nsrws := range simResult.PubSimulationResults.GetNsRwset() {
		if e.Support.IsSysCC(nsrws.Namespace) {
			// skip system chaincodes
			continue
		}
		if _, ok := policies.policyRequired[nsrws.Namespace]; !ok {
			// There's a public RWset for this namespace, but no public or private writes, so chaincode policy is required.
			policies.add(nsrws.Namespace, "", true)
		}
	}

	for chaincode, collections := range simResult.PrivateReads {
		for collection := range collections {
			policies.add(chaincode, collection, true)
		}
	}

	ccInterest := &pb.ChaincodeInterest{}
	for chaincode, collections := range policies.policyRequired {
		if e.Support.IsSysCC(chaincode) {
			// skip system chaincodes
			continue
		}
		for collection := range collections {
			ccCall := &pb.ChaincodeCall{
				Name: chaincode,
			}
			if collection == "" { // the empty collection name here represents the public RWset
				keyPolicies := policies.sbePolicies[chaincode]
				if len(keyPolicies) > 0 {
					// For simplicity, we'll always add the SBE policies to the public ChaincodeCall, and set the disregard flag if the chaincode policy is not required.
					ccCall.KeyPolicies = keyPolicies
					if !policies.requireChaincodePolicy(chaincode) {
						ccCall.DisregardNamespacePolicy = true
					}
				} else if !policies.requireChaincodePolicy(chaincode) {
					continue
				}
			} else {
				// Since each collection in a chaincode could have different values of the NoPrivateReads flag, create a new Chaincode entry for each.
				ccCall.CollectionNames = []string{collection}
				ccCall.NoPrivateReads = !simResult.PrivateReads.Exists(chaincode, collection)
			}
			ccInterest.Chaincodes = append(ccInterest.Chaincodes, ccCall)
		}
	}

	endorserLogger.Debug("ccInterest", ccInterest)
	return ccInterest, nil
}

type metadataPolicies struct {
	// Map of SBE policies: namespace -> array of policies.
	sbePolicies map[string][]*common.SignaturePolicyEnvelope
	// Whether the chaincode/collection policy is required for endorsement: namespace -> collection -> isRequired
	// Empty collection name represents the public rwset
	// Each entry in this map represents a ChaincodeCall structure in the final ChaincodeInterest.  The boolean
	// flag isRequired is used to control whether the DisregardNamespacePolicy flag should be set.
	policyRequired map[string]map[string]bool
}

func parseWritesetMetadata(metadata ledger.WritesetMetadata) (*metadataPolicies, error) {
	mp := &metadataPolicies{
		sbePolicies:    map[string][]*common.SignaturePolicyEnvelope{},
		policyRequired: map[string]map[string]bool{},
	}
	for ns, cmap := range metadata {
		mp.policyRequired[ns] = map[string]bool{"": false}
		for coll, kmap := range cmap {
			// look through each of the states that were written to
			for _, stateMetadata := range kmap {
				if policyBytes, sbeExists := stateMetadata[pb.MetaDataKeys_VALIDATION_PARAMETER.String()]; sbeExists {
					policy, err := protoutil.UnmarshalSignaturePolicy(policyBytes)
					if err != nil {
						return nil, err
					}
					mp.sbePolicies[ns] = append(mp.sbePolicies[ns], policy)
				} else {
					// the state metadata doesn't contain data relating to SBE policy, so the chaincode/collection policy is required
					mp.policyRequired[ns][coll] = true
				}
			}
		}
	}

	return mp, nil
}

func (mp *metadataPolicies) add(ns string, coll string, required bool) {
	if entry, ok := mp.policyRequired[ns]; ok {
		entry[coll] = required
	} else {
		mp.policyRequired[ns] = map[string]bool{coll: required}
	}
}

func (mp *metadataPolicies) requireChaincodePolicy(ns string) bool {
	// if any of the states (keys) were written to without those states having a SBE policy, then the chaincode policy will be required for this namespace
	return mp.policyRequired[ns][""]
}

// acquireTxSimulator 确定是否应该为提案获取事务模拟器。
// 输入参数：
//   - chainID：链ID。
//   - chaincodeName：链码名称。
//
// 返回值：
//   - bool：如果应该获取事务模拟器，则返回true；否则返回false。
func acquireTxSimulator(chainID string, chaincodeName string) bool {
	if chainID == "" {
		return false
	}

	// ¯\_(ツ)_/¯ 锁定。
	// 不要为查询和配置系统链码获取模拟器。
	// 这些链码不需要模拟器，并且其读锁会导致死锁。
	switch chaincodeName {
	case "qscc", "cscc":
		return false
	default:
		return true
	}
}

// shorttxid replicates the chaincode package function to shorten txids.
// ~~TODO utilize a common shorttxid utility across packages.~~
// TODO use a formal type for transaction ID and make it a stringer
func shorttxid(txid string) string {
	if len(txid) < 8 {
		return txid
	}
	return txid[0:8]
}

func CreateCCEventBytes(ccevent *pb.ChaincodeEvent) ([]byte, error) {
	if ccevent == nil {
		return nil, nil
	}

	return proto.Marshal(ccevent)
}

func decorateLogger(logger *flogging.FabricLogger, txParams *ccprovider.TransactionParams) *flogging.FabricLogger {
	return logger.With("channel", txParams.ChannelID, "txID", shorttxid(txParams.TxID))
}
