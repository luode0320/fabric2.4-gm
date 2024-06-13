/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package cscc chaincode configer provides functions to manage
// configuration transactions as the network is being reconfigured. The
// configuration transactions arrive from the ordering service to the committer
// who calls this chaincode. The chaincode also provides peer configuration
// services such as joining a chain or getting configuration data.
package cscc

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/aclmgmt"
	"github.com/hyperledger/fabric/core/aclmgmt/resources"
	"github.com/hyperledger/fabric/core/committer/txvalidator/v20/plugindispatcher"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// New 创建一个新的 CSCC 实例
// 通常，每个 Peer 实例只会创建一个 CSCC 实例
// 输入参数：
//   - aclProvider：aclmgmt.ACLProvider 接口的实例，用于提供 ACL（访问控制列表）
//   - deployedCCInfoProvider：ledger.DeployedChaincodeInfoProvider 接口的实例，用于提供已部署链码的信息
//   - lr：plugindispatcher.LifecycleResources 结构体，表示旧的生命周期资源
//   - nr：plugindispatcher.CollectionAndLifecycleResources 结构体，表示新的生命周期资源
//   - p：peer.Peer 结构体的指针，表示 Peer 节点
//   - bccsp：bccsp.BCCSP 接口的实例，表示 BCCSP（区块链加密服务提供者）
//
// 返回值：
//   - *PeerConfiger：PeerConfiger 结构体的指针，表示 Peer 的配置器
func New(
	aclProvider aclmgmt.ACLProvider,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	lr plugindispatcher.LifecycleResources,
	nr plugindispatcher.CollectionAndLifecycleResources,
	p *peer.Peer,
	bccsp bccsp.BCCSP,
) *PeerConfiger {
	return &PeerConfiger{
		aclProvider:            aclProvider,            // ACL 提供者
		deployedCCInfoProvider: deployedCCInfoProvider, // 已部署链码信息提供者
		legacyLifecycle:        lr,                     // 旧的生命周期资源
		newLifecycle:           nr,                     // 新的生命周期资源
		peer:                   p,                      // Peer
		bccsp:                  bccsp,                  // BCCSP
	}
}

func (e *PeerConfiger) Name() string              { return "cscc" }
func (e *PeerConfiger) Chaincode() shim.Chaincode { return e }

// PeerConfiger 实现了 Peer 的配置处理程序。对于从订购服务传入的每个配置交易，提交者调用此系统链码来处理交易。
type PeerConfiger struct {
	aclProvider            aclmgmt.ACLProvider                              // ACL 提供者
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider             // 已部署链码信息提供者
	legacyLifecycle        plugindispatcher.LifecycleResources              // 旧的生命周期资源
	newLifecycle           plugindispatcher.CollectionAndLifecycleResources // 新的生命周期资源
	peer                   *peer.Peer                                       // Peer
	bccsp                  bccsp.BCCSP                                      // BCCSP
}

var cnflogger = flogging.MustGetLogger("cscc")

// 这些是调用第一个参数的函数名
const (
	JoinChain            string = "JoinChain"
	JoinChainBySnapshot  string = "JoinChainBySnapshot"
	JoinBySnapshotStatus string = "JoinBySnapshotStatus"
	GetConfigBlock       string = "GetConfigBlock"
	GetChannelConfig     string = "GetChannelConfig"
	GetChannels          string = "GetChannels"
)

// Init is mostly useless from an SCC perspective
func (e *PeerConfiger) Init(stub shim.ChaincodeStubInterface) pb.Response {
	cnflogger.Info("Init CSCC")
	return shim.Success(nil)
}

// Invoke is called for the following:
// # to process joining a chain (called by app as a transaction proposal)
// # to get the current configuration block (called by app)
// # to update the configuration block (called by committer)
// Peer calls this function with 2 arguments:
// # args[0] is the function name, which must be JoinChain, GetConfigBlock or
// UpdateConfigBlock
// # args[1] is a configuration Block if args[0] is JoinChain or
// UpdateConfigBlock; otherwise it is the chain id
// TODO: Improve the scc interface to avoid marshal/unmarshal args
func (e *PeerConfiger) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	args := stub.GetArgs()

	if len(args) < 1 {
		return shim.Error(fmt.Sprintf("执行链码参数数目不正确, %d", len(args)))
	}

	fname := string(args[0])

	if fname != GetChannels && fname != JoinBySnapshotStatus && len(args) < 2 {
		return shim.Error(fmt.Sprintf("执行链码参数数目不正确, %d", len(args)))
	}

	cnflogger.Debugf("调用函数: %s", fname)

	// 处理 ACL:
	// 1. 获取已签署的提案
	sp, err := stub.GetSignedProposal()
	if err != nil {
		return shim.Error(fmt.Sprintf("获取已签名的提案失败: [%s]", err))
	}

	// 从SignedProposal的提案字节中解析出链码名称。
	name, err := protoutil.InvokedChaincodeName(sp.ProposalBytes)
	if err != nil {
		return shim.Error(fmt.Sprintf("无法识别调用的链码: %s", err))
	}

	// name 不对于 cscc
	if name != e.Name() {
		return shim.Error(fmt.Sprintf("执行调用cscc链码, 当前链码: '%s'", name))
	}

	return e.InvokeNoShim(args, sp)
}

func (e *PeerConfiger) InvokeNoShim(args [][]byte, sp *pb.SignedProposal) pb.Response {
	var err error
	fname := string(args[0])

	switch fname {
	case JoinChain:
		if args[1] == nil {
			return shim.Error("加入通道block块不能为nil")
		}

		block, err := protoutil.UnmarshalBlock(args[1])
		if err != nil {
			return shim.Error(fmt.Sprintf("无法反序列化创世块, %s", err))
		}
		// 获取通道名称
		cid, err := protoutil.GetChannelIDFromBlock(block)
		if err != nil {
			return shim.Error(fmt.Sprintf("\"JoinChain\" 请求无法提取 "+
				"channel 通道名称: [%s]", err))
		}

		// 1. 验证配置块，检查其中是否包含有效的配置交易。
		if err := validateConfigBlock(block, e.bccsp); err != nil {
			return shim.Error(fmt.Sprintf(" JoinChain for channelID = %s 验证失败, 因为 %s", cid, err))
		}

		// 2. 检查加入的权限策略。
		if err = e.aclProvider.CheckACL(resources.Cscc_JoinChain, "", sp); err != nil {
			return shim.Error(fmt.Sprintf("拒绝访问: 没有权限执行 [%s] 对通道 [%s]: [%s]", fname, cid, err))
		}

		// 如果txsFilter还不存在，则初始化它。我们可以安全地做到这一点，反正是创世块
		// txsFilter是事务验证代码的数组。它在提交者验证块时使用(就是换了一个地方存 byte[])。
		// BlockMetadataIndex_TRANSACTIONS_FILTER = 块元数据索引事务筛选器
		txsFilter := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
		if len(txsFilter) == 0 {
			// 将验证代码的数组硬编码添加到有效
			txsFilter = txflags.NewWithValues(len(block.Data.Data), pb.TxValidationCode_VALID)
			block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsFilter
		}

		// cid=通道名称, block=创世块, deployedCCInfoProvider=已部署链码信息提供者, legacyLifecycle=旧的生命周期资源, newLifecycle=新的生命周期资源
		return e.joinChain(cid, block, e.deployedCCInfoProvider, e.legacyLifecycle, e.newLifecycle)
	case JoinChainBySnapshot:
		if len(args[1]) == 0 {
			return shim.Error("Cannot join the channel, no snapshot directory provided")
		}
		// check policy
		if err = e.aclProvider.CheckACL(resources.Cscc_JoinChainBySnapshot, "", sp); err != nil {
			return shim.Error(fmt.Sprintf("access denied for [%s]: [%s]", fname, err))
		}
		snapshotDir := string(args[1])
		return e.JoinChainBySnapshot(snapshotDir, e.deployedCCInfoProvider, e.legacyLifecycle, e.newLifecycle)
	case JoinBySnapshotStatus:
		if err = e.aclProvider.CheckACL(resources.Cscc_JoinBySnapshotStatus, "", sp); err != nil {
			return shim.Error(fmt.Sprintf("access denied for [%s]: %s", fname, err))
		}
		return e.joinBySnapshotStatus()
	case GetConfigBlock:
		// 2. check policy
		if err = e.aclProvider.CheckACL(resources.Cscc_GetConfigBlock, string(args[1]), sp); err != nil {
			return shim.Error(fmt.Sprintf("access denied for [%s][%s]: %s", fname, args[1], err))
		}

		return e.getConfigBlock(args[1])
	case GetChannelConfig:
		if len(args[1]) == 0 {
			return shim.Error("empty channel name provided")
		}
		if err = e.aclProvider.CheckACL(resources.Cscc_GetChannelConfig, string(args[1]), sp); err != nil {
			return shim.Error(fmt.Sprintf("access denied for [%s][%s]: %s", fname, args[1], err))
		}
		return e.getChannelConfig(args[1])
	case GetChannels:
		// 2. check get channels policy
		if err = e.aclProvider.CheckACL(resources.Cscc_GetChannels, "", sp); err != nil {
			return shim.Error(fmt.Sprintf("access denied for [%s]: %s", fname, err))
		}

		return e.getChannels()

	}
	return shim.Error(fmt.Sprintf("未找到请求的函数 %s ", fname))
}

// validateConfigBlock 验证配置块，检查其中是否包含有效的配置交易。
// 输入参数：
//   - block：要验证的配置块。
//   - bccsp：BCCSP（区块链加密服务提供者）实例。
//
// 返回值：
//   - error：如果配置块验证失败，则返回错误；否则返回nil。
func validateConfigBlock(block *common.Block, bccsp bccsp.BCCSP) error {
	// 提取配置块的第一个envelope带有签名的有效负载,以便可以对消息进行身份验证
	envelopeConfig, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return errors.Errorf("提取配置块的第一个envelope失败 %s", err)
	}

	// 解组配置envelope, 链包含 _all_配置，而不依赖于以前的配置事务
	configEnv := &common.ConfigEnvelope{}
	_, err = protoutil.UnmarshalEnvelopeOfType(envelopeConfig, common.HeaderType_CONFIG, configEnv)
	if err != nil {
		return errors.Errorf("从第一个区块中解析数据失败: %s", err)
	}

	// 检查配置envelope的Config字段是否为nil
	if configEnv.Config == nil {
		return errors.New("第一个区块中的 Config 配置为空")
	}

	// 检查配置envelope的ChannelGroup字段是否为nil
	if configEnv.Config.ChannelGroup == nil {
		return errors.New("第一个区块中 Config.ChannelGroup 通道组为空")
	}

	// 检查配置envelope的ChannelGroup字段的Groups字段是否为nil
	if configEnv.Config.ChannelGroup.Groups == nil {
		return errors.New("第一个区块中没有可用的通道配置组 Config.ChannelGroup.Groups")
	}

	// 检查配置envelope的ChannelGroup字段的Groups字段中是否存在ApplicationGroupKey对应的组
	_, exists := configEnv.Config.ChannelGroup.Groups[channelconfig.ApplicationGroupKey]
	if !exists {
		return errors.Errorf("配置块无效，缺少 %s "+
			"配置组", channelconfig.ApplicationGroupKey)
	}

	// 检查配置块的功能要求, 验证对等节点是否能够满足给定配置块中的能力要求。
	if err = channelconfig.ValidateCapabilities(block, bccsp); err != nil {
		return errors.Errorf("配置块功能检查失败: [%s]", err)
	}

	return nil
}

// joinChain 将在配置块中加入指定的链。
// 由于它是第一个块，因此它是包含此链配置的 genesis 块，
// 因此，我们想使用此信息更新链对象
// 输入参数：
//   - cid：通道ID。
//   - cb：创世块。
//   - deployedCCInfoProvider：已部署链码信息提供者。
//   - legacyLifecycleValidation：旧生命周期验证。
//   - newLifecycleValidation：新生命周期验证。
func (e *PeerConfiger) joinChain(
	channelID string,
	block *common.Block,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	lr plugindispatcher.LifecycleResources,
	nr plugindispatcher.CollectionAndLifecycleResources,
) pb.Response {
	// 创建一个新的通道和通道账本
	if err := e.peer.CreateChannel(channelID, block, deployedCCInfoProvider, lr, nr); err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(nil)
}

// JohnChainBySnapshot will join the channel by the specified snapshot.
func (e *PeerConfiger) JoinChainBySnapshot(
	snapshotDir string,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	lr plugindispatcher.LifecycleResources,
	nr plugindispatcher.CollectionAndLifecycleResources,
) pb.Response {
	if err := e.peer.CreateChannelFromSnapshot(snapshotDir, deployedCCInfoProvider, lr, nr); err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(nil)
}

// Return the current configuration block for the specified channelID. If the
// peer doesn't belong to the channel, return error
func (e *PeerConfiger) getConfigBlock(channelID []byte) pb.Response {
	if channelID == nil {
		return shim.Error("ChannelID must not be nil.")
	}

	channel := e.peer.Channel(string(channelID))
	if channel == nil {
		return shim.Error(fmt.Sprintf("Unknown channel ID, %s", string(channelID)))
	}
	block, err := peer.ConfigBlockFromLedger(channel.Ledger())
	if err != nil {
		return shim.Error(err.Error())
	}

	blockBytes, err := protoutil.Marshal(block)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(blockBytes)
}

func (e *PeerConfiger) getChannelConfig(channelID []byte) pb.Response {
	channel := e.peer.Channel(string(channelID))
	if channel == nil {
		return shim.Error(fmt.Sprintf("unknown channel ID, %s", string(channelID)))
	}
	channelConfig, err := peer.RetrievePersistedChannelConfig(channel.Ledger())
	if err != nil {
		return shim.Error(err.Error())
	}

	channelConfigBytes, err := protoutil.Marshal(channelConfig)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(channelConfigBytes)
}

// getChannels returns information about all channels for this peer
func (e *PeerConfiger) getChannels() pb.Response {
	channelInfoArray := e.peer.GetChannelsInfo()

	// add array with info about all channels for this peer
	cqr := &pb.ChannelQueryResponse{Channels: channelInfoArray}

	cqrbytes, err := proto.Marshal(cqr)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(cqrbytes)
}

// joinBySnapshotStatus returns information about joinbysnapshot running status.
func (e *PeerConfiger) joinBySnapshotStatus() pb.Response {
	status := e.peer.JoinBySnaphotStatus()

	statusBytes, err := proto.Marshal(status)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(statusBytes)
}
