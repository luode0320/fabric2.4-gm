/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peer

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/flogging"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/semaphore"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/committer/txvalidator/plugin"
	validatorv14 "github.com/hyperledger/fabric/core/committer/txvalidator/v14"
	validatorv20 "github.com/hyperledger/fabric/core/committer/txvalidator/v20"
	"github.com/hyperledger/fabric/core/committer/txvalidator/v20/plugindispatcher"
	vir "github.com/hyperledger/fabric/core/committer/txvalidator/v20/valinforetriever"
	"github.com/hyperledger/fabric/core/common/privdata"
	validation "github.com/hyperledger/fabric/core/handlers/validation/api/state"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/gossip/api"
	gossipservice "github.com/hyperledger/fabric/gossip/service"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"
	"github.com/hyperledger/fabric/msp"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var peerLogger = flogging.MustGetLogger("peer")

type CollectionInfoShim struct {
	plugindispatcher.CollectionAndLifecycleResources
	ChannelID string
}

func (cis *CollectionInfoShim) CollectionValidationInfo(chaincodeName, collectionName string, validationState validation.State) ([]byte, error, error) {
	return cis.CollectionAndLifecycleResources.CollectionValidationInfo(cis.ChannelID, chaincodeName, collectionName, validationState)
}

func ConfigBlockFromLedger(ledger ledger.PeerLedger) (*common.Block, error) {
	peerLogger.Debugf("Getting config block")

	// get last block.  Last block number is Height-1
	blockchainInfo, err := ledger.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	lastBlock, err := ledger.GetBlockByNumber(blockchainInfo.Height - 1)
	if err != nil {
		return nil, err
	}

	// get most recent config block location from last block metadata
	configBlockIndex, err := protoutil.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		return nil, err
	}

	// get most recent config block
	configBlock, err := ledger.GetBlockByNumber(configBlockIndex)
	if err != nil {
		return nil, err
	}

	peerLogger.Debugf("Got config block[%d]", configBlockIndex)
	return configBlock, nil
}

// updates the trusted roots for the peer based on updates to channels
func (p *Peer) updateTrustedRoots(cm channelconfig.Resources) {
	if !p.ServerConfig.SecOpts.UseTLS {
		return
	}

	// this is triggered on per channel basis so first update the roots for the channel
	peerLogger.Debugf("Updating trusted root authorities for channel %s", cm.ConfigtxValidator().ChannelID())

	p.CredentialSupport.BuildTrustedRootsForChain(cm)

	// now iterate over all roots for all app and orderer channels
	var trustedRoots [][]byte
	for _, roots := range p.CredentialSupport.AppRootCAsByChain() {
		trustedRoots = append(trustedRoots, roots...)
	}
	trustedRoots = append(trustedRoots, p.ServerConfig.SecOpts.ClientRootCAs...)
	trustedRoots = append(trustedRoots, p.ServerConfig.SecOpts.ServerRootCAs...)

	// now update the client roots for the peerServer
	err := p.server.SetClientRootCAs(trustedRoots)
	if err != nil {
		msg := "Failed to update trusted roots from latest config block. " +
			"This peer may not be able to communicate with members of channel %s (%s)"
		peerLogger.Warningf(msg, cm.ConfigtxValidator().ChannelID(), err)
	}
}

//
//  Deliver service support structs for the peer
//

// DeliverChainManager provides access to a channel for performing deliver
type DeliverChainManager struct {
	Peer *Peer
}

func (d DeliverChainManager) GetChain(chainID string) deliver.Chain {
	if channel := d.Peer.Channel(chainID); channel != nil {
		return channel
	}
	return nil
}

// fileLedgerBlockStore implements the interface expected by
// common/ledger/blockledger/file to interact with a file ledger for deliver
type fileLedgerBlockStore struct {
	ledger.PeerLedger
}

func (flbs fileLedgerBlockStore) AddBlock(*common.Block) error {
	return nil
}

func (flbs fileLedgerBlockStore) RetrieveBlocks(startBlockNumber uint64) (commonledger.ResultsIterator, error) {
	return flbs.GetBlocksIterator(startBlockNumber)
}

func (flbs fileLedgerBlockStore) Shutdown() {}

// Peer 顶级结构体, 包含peer的属性和初始化通道、创建通道、解析通道、获取账本、获取mspid等peer节点的方法
type Peer struct {
	// gRPC相关属性, 包括连接超时、请求限流、请求日志、tls证书
	ServerConfig comm.ServerConfig
	// 管理用于gRPC客户端连接的凭据, 包括tls证书, tls根证书
	CredentialSupport *comm.CredentialSupport
	// leveldb存储相关
	StoreProvider transientstore.StoreProvider
	GossipService *gossipservice.GossipService
	LedgerMgr     *ledgermgmt.LedgerMgr
	// orderer节点覆盖, 包括需要覆盖的节点addr, 证书
	OrdererEndpointOverrides map[string]*orderers.Endpoint
	// 加密、签名、获取密钥实例
	CryptoProvider bccsp.BCCSP

	// validationWorkersSemaphore is used to limit the number of concurrent validation
	// go routines.
	validationWorkersSemaphore semaphore.Semaphore

	server             *comm.GRPCServer
	pluginMapper       plugin.Mapper
	channelInitializer func(cid string)

	// channels is a map of channelID to channel
	mutex    sync.RWMutex
	channels map[string]*Channel

	configCallbacks []channelconfig.BundleActor
}

// AddConfigCallbacks adds one or more BundleActor functions to list of callbacks that
// get invoked when a channel configuration update event is received via gossip.
func (p *Peer) AddConfigCallbacks(callbacks ...channelconfig.BundleActor) {
	p.configCallbacks = append(p.configCallbacks, callbacks...)
}

func (p *Peer) openStore(cid string) (*transientstore.Store, error) {
	store, err := p.StoreProvider.OpenStore(cid)
	if err != nil {
		return nil, err
	}

	return store, nil
}

// CreateChannel 创建一个新的通道。
// 方法接收者：p（Peer类型的指针）
// 输入参数：
//   - cid：通道ID。
//   - cb：创世块。
//   - deployedCCInfoProvider：已部署链码信息提供者。
//   - legacyLifecycleValidation：旧生命周期验证。
//   - newLifecycleValidation：新生命周期验证。
//
// 返回值：
//   - error：如果创建通道失败，则返回错误；否则返回nil。
func (p *Peer) CreateChannel(
	cid string,
	cb *common.Block,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
) error {
	// 从创世块创建通道账本
	l, err := p.LedgerMgr.CreateLedger(cid, cb)
	if err != nil {
		return errors.WithMessage(err, "无法从创世块创建通道账本")
	}

	// 创建通道
	if err := p.createChannel(cid, l, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation); err != nil {
		return err
	}

	// 初始化通道
	p.initChannel(cid)
	return nil
}

// CreateChannelFromSnapshot creates a channel from the specified snapshot.
func (p *Peer) CreateChannelFromSnapshot(
	snapshotDir string,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
) error {
	channelCallback := func(l ledger.PeerLedger, cid string) {
		if err := p.createChannel(cid, l, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation); err != nil {
			logger.Errorf("error creating channel for %s", cid)
			return
		}
		p.initChannel(cid)
	}

	err := p.LedgerMgr.CreateLedgerFromSnapshot(snapshotDir, channelCallback)
	if err != nil {
		return errors.WithMessagef(err, "cannot create ledger from snapshot %s", snapshotDir)
	}

	return nil
}

// RetrievePersistedChannelConfig 从 statedb 状态数据库中检索持久化的通道配置。
// 输入参数：
//   - ledger：ledger.PeerLedger，表示对等节点账本
//
// 返回值：
//   - *common.Config：表示通道配置的指针
//   - error：表示检索过程中可能出现的错误
func RetrievePersistedChannelConfig(ledger ledger.PeerLedger) (*common.Config, error) {
	// 创建查询执行器, 客户端可以获取多个 'QueryExecutor' 以进行并行执行。
	qe, err := ledger.NewQueryExecutor()
	if err != nil {
		return nil, err
	}
	defer qe.Done()

	// 从查询执行器中检索通道配置。
	return retrieveChannelConfig(qe)
}

// createChannel 创建新的通道对象并将其插入到通道切片中。
func (p *Peer) createChannel(
	cid string,
	l ledger.PeerLedger,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
) error {
	// 从 statedb 状态数据库中检索持久化的通道配置。
	chanConf, err := RetrievePersistedChannelConfig(l)
	if err != nil {
		return err
	}

	// 创建一个新的不可变的通道配置。
	bundle, err := channelconfig.NewBundle(cid, chanConf, p.CryptoProvider)
	if err != nil {
		return err
	}

	// 检查通道的能力是否受支持，如果不受支持则引发 panic。
	capabilitiesSupportedOrPanic(bundle)

	// 日志健全性检查
	channelconfig.LogSanityChecks(bundle)

	// 配置更新路由
	gossipEventer := p.GossipService.NewConfigEventer()

	gossipCallbackWrapper := func(bundle *channelconfig.Bundle) {
		// 获取应用程序配置
		ac, ok := bundle.ApplicationConfig()
		if !ok {
			ac = nil
		}

		// 处理配置更新事件
		gossipEventer.ProcessConfigUpdate(gossipservice.ConfigUpdate{
			ChannelID:        bundle.ConfigtxValidator().ChannelID(),
			Organizations:    ac.Organizations(),
			OrdererAddresses: bundle.ChannelConfig().OrdererAddresses(),
			Sequence:         bundle.ConfigtxValidator().Sequence(),
		})

		// 标记可疑的对等节点
		p.GossipService.SuspectPeers(func(identity api.PeerIdentityType) bool {
			// TODO: 这是一个占位符，用于使 MSP 层怀疑给定的证书是否被吊销，或者其中间 CA 是否被吊销。
			// 在我们拥有这样的能力之前，我们返回 true 以验证所有身份。
			return true
		})
	}

	// 受信任的根回调包装
	trustedRootsCallbackWrapper := func(bundle *channelconfig.Bundle) {
		p.updateTrustedRoots(bundle)
	}

	mspCallback := func(bundle *channelconfig.Bundle) {
		// TODO 一旦对mspmgmt的所有引用都从对等代码中删除
		mspmgmt.XXXSetMSPManager(cid, bundle.MSPManager())
	}

	osLogger := flogging.MustGetLogger("peer.orderers")
	namedOSLogger := osLogger.With("channel", cid)
	ordererSource := orderers.NewConnectionSource(namedOSLogger, p.OrdererEndpointOverrides)

	ordererSourceCallback := func(bundle *channelconfig.Bundle) {
		// 获取全局的Orderer地址列表
		globalAddresses := bundle.ChannelConfig().OrdererAddresses()

		// 存储每个组织的Orderer地址和证书
		orgAddresses := map[string]orderers.OrdererOrg{}

		// 检查是否存在Orderer配置
		if ordererConfig, ok := bundle.OrdererConfig(); ok {
			// 遍历每个组织的Orderer配置
			for orgName, org := range ordererConfig.Organizations() {
				// 获取组织的TLS根证书和中间证书
				var certs [][]byte
				certs = append(certs, org.MSP().GetTLSRootCerts()...)
				certs = append(certs, org.MSP().GetTLSIntermediateCerts()...)

				// 构建OrdererOrg对象，包含地址和证书信息
				orgAddresses[orgName] = orderers.OrdererOrg{
					Addresses: org.Endpoints(),
					RootCerts: certs,
				}
			}
		}

		// 更新Orderer地址和证书信息
		ordererSource.Update(globalAddresses, orgAddresses)
	}

	channel := &Channel{
		ledger:         l,
		resources:      bundle,
		cryptoProvider: p.CryptoProvider,
	}

	callbacks := []channelconfig.BundleActor{
		ordererSourceCallback,
		gossipCallbackWrapper,
		trustedRootsCallbackWrapper,
		mspCallback,
		channel.bundleUpdate,
	}
	callbacks = append(callbacks, p.configCallbacks...)

	channel.bundleSource = channelconfig.NewBundleSource(
		bundle,
		callbacks...,
	)

	committer := committer.NewLedgerCommitter(l)
	validator := &txvalidator.ValidationRouter{
		CapabilityProvider: channel,
		V14Validator: validatorv14.NewTxValidator(
			cid,
			p.validationWorkersSemaphore,
			channel,
			p.pluginMapper,
			p.CryptoProvider,
		),
		V20Validator: validatorv20.NewTxValidator(
			cid,
			p.validationWorkersSemaphore,
			channel,
			channel.Ledger(),
			&vir.ValidationInfoRetrieveShim{
				New:    newLifecycleValidation,
				Legacy: legacyLifecycleValidation,
			},
			&CollectionInfoShim{
				CollectionAndLifecycleResources: newLifecycleValidation,
				ChannelID:                       bundle.ConfigtxValidator().ChannelID(),
			},
			p.pluginMapper,
			policies.PolicyManagerGetterFunc(p.GetPolicyManager),
			p.CryptoProvider,
		),
	}

	// TODO: does someone need to call Close() on the transientStoreFactory at shutdown of the peer?
	store, err := p.openStore(bundle.ConfigtxValidator().ChannelID())
	if err != nil {
		return errors.Wrapf(err, "[channel %s] failed opening transient store", bundle.ConfigtxValidator().ChannelID())
	}
	channel.store = store

	var idDeserializerFactory privdata.IdentityDeserializerFactoryFunc = func(channelID string) msp.IdentityDeserializer {
		return p.Channel(channelID).MSPManager()
	}
	simpleCollectionStore := privdata.NewSimpleCollectionStore(l, deployedCCInfoProvider, idDeserializerFactory)
	p.GossipService.InitializeChannel(bundle.ConfigtxValidator().ChannelID(), ordererSource, store, gossipservice.Support{
		Validator:            validator,
		Committer:            committer,
		CollectionStore:      simpleCollectionStore,
		IdDeserializeFactory: idDeserializerFactory,
		CapabilityProvider:   channel,
	})

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.channels == nil {
		p.channels = map[string]*Channel{}
	}
	p.channels[cid] = channel

	return nil
}

func (p *Peer) Channel(cid string) *Channel {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	if c, ok := p.channels[cid]; ok {
		return c
	}
	return nil
}

func (p *Peer) StoreForChannel(cid string) *transientstore.Store {
	if c := p.Channel(cid); c != nil {
		return c.Store()
	}
	return nil
}

// GetChannelsInfo returns an array with information about all channels for
// this peer.
func (p *Peer) GetChannelsInfo() []*pb.ChannelInfo {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var channelInfos []*pb.ChannelInfo
	for key := range p.channels {
		ci := &pb.ChannelInfo{ChannelId: key}
		channelInfos = append(channelInfos, ci)
	}
	return channelInfos
}

// GetChannelConfig returns the channel configuration of the channel with channel ID. Note that this
// call returns nil if channel cid has not been created.
func (p *Peer) GetChannelConfig(cid string) channelconfig.Resources {
	if c := p.Channel(cid); c != nil {
		return c.Resources()
	}
	return nil
}

// GetStableChannelConfig 返回具有指定通道ID的通道的稳定通道配置。
// 注意，如果通道cid尚未创建，则此调用返回nil。
// 参数：
//   - cid: string 类型，表示通道ID。
//
// 返回值：
//   - channelconfig.Resources: 表示通道的稳定通道配置。
func (p *Peer) GetStableChannelConfig(cid string) channelconfig.Resources {
	// 获取指定通道ID的通道
	if c := p.Channel(cid); c != nil {
		// 返回通道的资源配置
		return c.Resources()
	}
	return nil
}

// GetLedger returns the ledger of the channel with channel ID. Note that this
// call returns nil if channel cid has not been created.
func (p *Peer) GetLedger(cid string) ledger.PeerLedger {
	if c := p.Channel(cid); c != nil {
		return c.Ledger()
	}
	return nil
}

// GetMSPIDs 返回在该通道上定义的每个应用程序 MSP 的 ID
// 方法接收者：
//   - p：Peer 结构体的指针，表示 Peer 节点
//
// 输入参数：
//   - cid：通道 ID，用于获取相应通道的 MSP ID
//
// 返回值：
//   - []string：字符串切片，表示每个应用程序 MSP 的 ID
func (p *Peer) GetMSPIDs(cid string) []string {
	if c := p.Channel(cid); c != nil {
		return c.GetMSPIDs()
	}
	return nil
}

// GetPolicyManager 返回具有指定通道ID的通道的策略管理器。
// 注意，如果通道cid尚未创建，则此调用返回nil。
// 参数：
//   - cid: string 类型，表示通道ID。
//
// 返回值：
//   - policies.Manager: 表示策略管理器。
func (p *Peer) GetPolicyManager(cid string) policies.Manager {
	// 获取指定通道ID的通道
	if c := p.Channel(cid); c != nil {
		// 返回通道的策略管理器
		return c.Resources().PolicyManager()
	}
	return nil
}

// JoinBySnaphotStatus queries ledger mgr to get the status of joinbysnapshot
func (p *Peer) JoinBySnaphotStatus() *pb.JoinBySnapshotStatus {
	return p.LedgerMgr.JoinBySnapshotStatus()
}

// initChannel 注意在加入对等体后初始化通道，例如部署系统CCs
func (p *Peer) initChannel(cid string) {
	if p.channelInitializer != nil {
		// 初始化链码，即部署系统CC
		peerLogger.Debugf("初始化通道 %s", cid)
		p.channelInitializer(cid)
	}
}

func (p *Peer) GetApplicationConfig(cid string) (channelconfig.Application, bool) {
	cc := p.GetChannelConfig(cid)
	if cc == nil {
		return nil, false
	}

	return cc.ApplicationConfig()
}

// Initialize 设置了节点从持久化存储中获取的任何通道。在启动时，当账本和 gossip 准备好时，应调用此函数。
// 输入参数：
//   - init：func(string) 类型，表示初始化函数
//   - server：*comm.GRPCServer 类型的指针，表示 GRPC 服务器
//   - pm：plugin.Mapper 接口的实例，表示插件映射器
//   - deployedCCInfoProvider：ledger.DeployedChaincodeInfoProvider 接口的实例，表示已部署链码信息提供程序
//   - legacyLifecycleValidation：plugindispatcher.LifecycleResources 接口的实例，表示旧的生命周期验证
//   - newLifecycleValidation：plugindispatcher.CollectionAndLifecycleResources 接口的实例，表示新的生命周期验证
//   - nWorkers：int 类型，表示工作线程数
func (p *Peer) Initialize(
	init func(string),
	server *comm.GRPCServer,
	pm plugin.Mapper,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
	nWorkers int,
) {
	// TODO: 导出的dep字段或构造函数
	p.server = server
	p.validationWorkersSemaphore = semaphore.New(nWorkers)
	p.pluginMapper = pm
	p.channelInitializer = init

	// 获取所有账本的 ID
	ledgerIds, err := p.LedgerMgr.GetLedgerIDs()
	if err != nil {
		panic(fmt.Errorf("初始化 ledgermgmt 账本时出错: %s", err))
	}

	// 遍历所有账本
	for _, cid := range ledgerIds {
		peerLogger.Infof("加载账本 %s", cid)
		// 打开账本
		ledger, err := p.LedgerMgr.OpenLedger(cid)
		if err != nil {
			peerLogger.Errorf("加载分类账本失败 %s(%+v)", cid, err)
			peerLogger.Debugf("加载分类账本时出错 %s 消息 %s. 我们继续加载下一个分类账本，而不是中止.", cid, err)
			continue
		}
		// 如果获取到有效的账本和配置块，则创建通道, 账本id,账本实例,已部署链码信息提供程序,旧的生命周期验证,新的生命周期验证
		err = p.createChannel(cid, ledger, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation)
		if err != nil {
			peerLogger.Errorf("加载分类账本失败 %s(%s)", cid, err)
			peerLogger.Debugf("重新加载分类账本链条时出错 %s 错误消息 %s. 我们继续加载下一个分类账本链条，而不是中止.", cid, err)
			continue
		}

		// 初始化通道
		p.initChannel(cid)
	}
}

func (flbs fileLedgerBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return flbs.GetBlockByNumber(blockNum)
}
