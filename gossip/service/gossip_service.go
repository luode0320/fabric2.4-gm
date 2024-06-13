/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package service

import (
	"fmt"
	"sync"

	gproto "github.com/hyperledger/fabric-protos-go/gossip"
	tspb "github.com/hyperledger/fabric-protos-go/transientstore"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/deliverservice"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/election"
	"github.com/hyperledger/fabric/gossip/filter"
	"github.com/hyperledger/fabric/gossip/gossip"
	gossipmetrics "github.com/hyperledger/fabric/gossip/metrics"
	gossipprivdata "github.com/hyperledger/fabric/gossip/privdata"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/hyperledger/fabric/gossip/state"
	"github.com/hyperledger/fabric/gossip/util"
	corecomm "github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/internal/pkg/peer/blocksprovider"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// gossipSvc is the interface of the gossip component.
type gossipSvc interface {
	// SelfMembershipInfo returns the peer's membership information
	SelfMembershipInfo() discovery.NetworkMember

	// SelfChannelInfo returns the peer's latest StateInfo message of a given channel
	SelfChannelInfo(common.ChannelID) *protoext.SignedGossipMessage

	// Send sends a message to remote peers
	Send(msg *gproto.GossipMessage, peers ...*comm.RemotePeer)

	// SendByCriteria sends a given message to all peers that match the given SendCriteria
	SendByCriteria(*protoext.SignedGossipMessage, gossip.SendCriteria) error

	// GetPeers returns the NetworkMembers considered alive
	Peers() []discovery.NetworkMember

	// PeersOfChannel returns the NetworkMembers considered alive
	// and also subscribed to the channel given
	PeersOfChannel(common.ChannelID) []discovery.NetworkMember

	// UpdateMetadata updates the self metadata of the discovery layer
	// the peer publishes to other peers
	UpdateMetadata(metadata []byte)

	// UpdateLedgerHeight updates the ledger height the peer
	// publishes to other peers in the channel
	UpdateLedgerHeight(height uint64, channelID common.ChannelID)

	// UpdateChaincodes 更新对等方发布给通道中其他对等方的链码
	UpdateChaincodes(chaincode []*gproto.Chaincode, channelID common.ChannelID)

	// Gossip sends a message to other peers to the network
	Gossip(msg *gproto.GossipMessage)

	// PeerFilter receives a SubChannelSelectionCriteria and returns a RoutingFilter that selects
	// only peer identities that match the given criteria, and that they published their channel participation
	PeerFilter(channel common.ChannelID, messagePredicate api.SubChannelSelectionCriteria) (filter.RoutingFilter, error)

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *gproto.GossipMessage, <-chan protoext.ReceivedMessage)

	// JoinChan makes the Gossip instance join a channel
	JoinChan(joinMsg api.JoinChannelMessage, channelID common.ChannelID)

	// LeaveChan makes the Gossip instance leave a channel.
	// It still disseminates stateInfo message, but doesn't participate
	// in block pulling anymore, and can't return anymore a list of peers
	// in the channel.
	LeaveChan(channelID common.ChannelID)

	// SuspectPeers makes the gossip instance validate identities of suspected peers, and close
	// any connections to peers with identities that are found invalid
	SuspectPeers(s api.PeerSuspector)

	// IdentityInfo returns information known peer identities
	IdentityInfo() api.PeerIdentitySet

	// IsInMyOrg checks whether a network member is in this peer's org
	IsInMyOrg(member discovery.NetworkMember) bool

	// Stop stops the gossip component
	Stop()
}

// GossipServiceAdapter serves to provide basic functionality
// required from gossip service by delivery service
type GossipServiceAdapter interface {
	// PeersOfChannel returns slice with members of specified channel
	PeersOfChannel(common.ChannelID) []discovery.NetworkMember

	// AddPayload adds payload to the local state sync buffer
	AddPayload(channelID string, payload *gproto.Payload) error

	// Gossip the message across the peers
	Gossip(msg *gproto.GossipMessage)
}

// DeliveryServiceFactory factory to create and initialize delivery service instance
type DeliveryServiceFactory interface {
	// Returns an instance of delivery client
	Service(g GossipServiceAdapter, ordererSource *orderers.ConnectionSource, msc api.MessageCryptoService, isStaticLead bool) deliverservice.DeliverService
}

// deliveryFactoryImpl 是交付服务工厂的实现。
type deliveryFactoryImpl struct {
	signer               identity.SignerSerializer            // 签名者序列化器
	credentialSupport    *corecomm.CredentialSupport          // 凭据支持
	deliverServiceConfig *deliverservice.DeliverServiceConfig // 交付服务配置
}

// Returns an instance of delivery client
func (df *deliveryFactoryImpl) Service(g GossipServiceAdapter, ordererSource *orderers.ConnectionSource, mcs api.MessageCryptoService, isStaticLeader bool) deliverservice.DeliverService {
	return deliverservice.NewDeliverService(&deliverservice.Config{
		IsStaticLeader:       isStaticLeader,
		CryptoSvc:            mcs,
		Gossip:               g,
		Signer:               df.signer,
		DeliverServiceConfig: df.deliverServiceConfig,
		OrdererSource:        ordererSource,
	})
}

type privateHandler struct {
	support     Support
	coordinator gossipprivdata.Coordinator
	distributor gossipprivdata.PvtDataDistributor
	reconciler  gossipprivdata.PvtDataReconciler
}

func (p privateHandler) close() {
	p.coordinator.Close()
	p.reconciler.Stop()
}

// GossipService 处理 gossip 服务与对等方之间的交互。
type GossipService struct {
	gossipSvc                                                   // gossip 服务
	privateHandlers   map[string]privateHandler                 // 私有处理程序
	chains            map[string]state.GossipStateProvider      // 链状态提供者
	leaderElection    map[string]election.LeaderElectionService // 领导选举服务
	deliveryService   map[string]deliverservice.DeliverService  // 交付服务
	deliveryFactory   DeliveryServiceFactory                    // 交付服务工厂
	lock              sync.RWMutex                              // 读写锁
	mcs               api.MessageCryptoService                  // 消息加密服务
	peerIdentity      []byte                                    // 对等方身份
	secAdv            api.SecurityAdvisor                       // 安全顾问
	metrics           *gossipmetrics.GossipMetrics              // gossip 指标
	serviceConfig     *ServiceConfig                            // 服务配置
	privdataConfig    *gossipprivdata.PrivdataConfig            // 私有数据配置
	anchorPeerTracker *anchorPeerTracker                        // 锚定对等方追踪器
}

// This is an implementation of api.JoinChannelMessage.
type joinChannelMessage struct {
	seqNum              uint64
	members2AnchorPeers map[string][]api.AnchorPeer
}

func (jcm *joinChannelMessage) SequenceNumber() uint64 {
	return jcm.seqNum
}

// Members returns the organizations of the channel
func (jcm *joinChannelMessage) Members() []api.OrgIdentityType {
	members := make([]api.OrgIdentityType, 0, len(jcm.members2AnchorPeers))
	for org := range jcm.members2AnchorPeers {
		members = append(members, api.OrgIdentityType(org))
	}
	return members
}

// AnchorPeersOf returns the anchor peers of the given organization
func (jcm *joinChannelMessage) AnchorPeersOf(org api.OrgIdentityType) []api.AnchorPeer {
	return jcm.members2AnchorPeers[string(org)]
}

// anchorPeerTracker maintains anchor peer endpoints for all the channels.
type anchorPeerTracker struct {
	// allEndpoints contains anchor peer endpoints for all the channels,
	// its key is channel name, value is map of anchor peer endpoints
	allEndpoints map[string]map[string]struct{}
	mutex        sync.RWMutex
}

// update overwrites the anchor peer endpoints for the channel
func (t *anchorPeerTracker) update(channelName string, endpoints map[string]struct{}) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.allEndpoints[channelName] = endpoints
}

// IsAnchorPeer checks if an endpoint is an anchor peer in any channel
func (t *anchorPeerTracker) IsAnchorPeer(endpoint string) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	for _, endpointsForChannel := range t.allEndpoints {
		if _, ok := endpointsForChannel[endpoint]; ok {
			return true
		}
	}
	return false
}

var logger = util.GetLogger(util.ServiceLogger, "")

// New 创建一个 gossip 服务。
//
// 输入参数：
//   - peerIdentity：身份签名者序列化器的实例，用于对身份进行签名和序列化。
//   - gossipMetrics：GossipMetrics 实例，用于度量 gossip 相关的指标。
//   - endpoint：gossip 服务的端点地址。
//   - s：GRPC 服务器实例。
//   - mcs：MessageCryptoService 实例，用于消息加密。
//   - secAdv：SecurityAdvisor 实例，用于提供安全建议。
//   - secureDialOpts：PeerSecureDialOpts 实例，用于安全拨号选项。
//   - credSupport：CredentialSupport 实例，用于凭证支持。
//   - gossipConfig：gossip 配置。
//   - serviceConfig：ServiceConfig 实例，用于配置 gossip 服务。
//   - privdataConfig：PrivdataConfig 实例，用于配置私有数据。
//   - deliverServiceConfig：DeliverServiceConfig 实例，用于配置 Deliver 服务。
//
// 返回值：
//   - *GossipService：GossipService 实例。
//   - error：如果初始化过程中出现错误，则返回相应的错误信息。
func New(
	peerIdentity identity.SignerSerializer,
	gossipMetrics *gossipmetrics.GossipMetrics,
	endpoint string,
	s *grpc.Server,
	mcs api.MessageCryptoService,
	secAdv api.SecurityAdvisor,
	secureDialOpts api.PeerSecureDialOpts,
	credSupport *corecomm.CredentialSupport,
	gossipConfig *gossip.Config,
	serviceConfig *ServiceConfig,
	privdataConfig *gossipprivdata.PrivdataConfig,
	deliverServiceConfig *deliverservice.DeliverServiceConfig,
) (*GossipService, error) {
	serializedIdentity, err := peerIdentity.Serialize()
	if err != nil {
		return nil, err
	}

	logger.Infof("初始化 gossip 节点 %s", endpoint)

	anchorPeerTracker := &anchorPeerTracker{allEndpoints: map[string]map[string]struct{}{}}
	// 创建一个附加到 gRPC 服务器的 gossip 实例。启动心跳检测、检查存活、重新连接等协程。
	gossipComponent := gossip.New(
		gossipConfig,
		s,
		secAdv,
		mcs,
		serializedIdentity,
		secureDialOpts,
		gossipMetrics,
		anchorPeerTracker,
	)

	// 处理 gossip 服务与对等方之间的交互。
	return &GossipService{
		gossipSvc:       gossipComponent,                                 // gossip 服务
		mcs:             mcs,                                             // 消息加密服务
		privateHandlers: make(map[string]privateHandler),                 // 私有处理程序
		chains:          make(map[string]state.GossipStateProvider),      // 链状态提供者
		leaderElection:  make(map[string]election.LeaderElectionService), // 领导选举服务
		deliveryService: make(map[string]deliverservice.DeliverService),  // 交付服务
		deliveryFactory: &deliveryFactoryImpl{ // 交付服务工厂
			signer:               peerIdentity,         // 签名者序列化器
			credentialSupport:    credSupport,          // 凭据支持
			deliverServiceConfig: deliverServiceConfig, // 交付服务配置
		},
		peerIdentity:      serializedIdentity, // 对等方身份
		secAdv:            secAdv,             // 安全顾问
		metrics:           gossipMetrics,      // gossip 指标
		serviceConfig:     serviceConfig,      // 服务配置
		privdataConfig:    privdataConfig,     // 私有数据配置
		anchorPeerTracker: anchorPeerTracker,  // 锚定对等方追踪器
	}, nil
}

// DistributePrivateData distribute private read write set inside the channel based on the collections policies
func (g *GossipService) DistributePrivateData(channelID string, txID string, privData *tspb.TxPvtReadWriteSetWithConfigInfo, blkHt uint64) error {
	g.lock.RLock()
	handler, exists := g.privateHandlers[channelID]
	g.lock.RUnlock()
	if !exists {
		return errors.Errorf("No private data handler for %s", channelID)
	}

	if err := handler.distributor.Distribute(txID, privData, blkHt); err != nil {
		err := errors.WithMessagef(err, "failed to distribute private collection, txID %s, channel %s", txID, channelID)
		logger.Error(err)
		return err
	}

	if err := handler.coordinator.StorePvtData(txID, privData, blkHt); err != nil {
		logger.Error("Failed to store private data into transient store, txID",
			txID, "channel", channelID, "due to", err)
		return err
	}
	return nil
}

// NewConfigEventer 创建一个ConfigProcessor，channelconfig.BundleSource最终可以将配置更新路由到
func (g *GossipService) NewConfigEventer() ConfigProcessor {
	return newConfigEventer(g)
}

// Support aggregates functionality of several
// interfaces required by gossip service
type Support struct {
	Validator            txvalidator.Validator
	Committer            committer.Committer
	CollectionStore      privdata.CollectionStore
	IdDeserializeFactory gossipprivdata.IdentityDeserializerFactory
	CapabilityProvider   gossipprivdata.CapabilityProvider
}

// InitializeChannel allocates the state provider and should be invoked once per channel per execution
func (g *GossipService) InitializeChannel(channelID string, ordererSource *orderers.ConnectionSource, store *transientstore.Store, support Support) {
	g.lock.Lock()
	defer g.lock.Unlock()
	// Initialize new state provider for given committer
	logger.Debug("Creating state provider for channelID", channelID)
	servicesAdapter := &state.ServicesMediator{GossipAdapter: g, MCSAdapter: g.mcs}

	// Initialize private data fetcher
	dataRetriever := gossipprivdata.NewDataRetriever(channelID, store, support.Committer)
	collectionAccessFactory := gossipprivdata.NewCollectionAccessFactory(support.IdDeserializeFactory)
	fetcher := gossipprivdata.NewPuller(g.metrics.PrivdataMetrics, support.CollectionStore, g.gossipSvc, dataRetriever,
		collectionAccessFactory, channelID, g.serviceConfig.BtlPullMargin)

	coordinatorConfig := gossipprivdata.CoordinatorConfig{
		TransientBlockRetention:        g.serviceConfig.TransientstoreMaxBlockRetention,
		PullRetryThreshold:             g.serviceConfig.PvtDataPullRetryThreshold,
		SkipPullingInvalidTransactions: g.serviceConfig.SkipPullingInvalidTransactionsDuringCommit,
	}
	selfSignedData := g.createSelfSignedData()
	mspID := string(g.secAdv.OrgByPeerIdentity(selfSignedData.Identity))
	coordinator := gossipprivdata.NewCoordinator(mspID, gossipprivdata.Support{
		ChainID:            channelID,
		CollectionStore:    support.CollectionStore,
		Validator:          support.Validator,
		Committer:          support.Committer,
		Fetcher:            fetcher,
		CapabilityProvider: support.CapabilityProvider,
	}, store, selfSignedData, g.metrics.PrivdataMetrics, coordinatorConfig,
		support.IdDeserializeFactory)

	var reconciler gossipprivdata.PvtDataReconciler

	if g.privdataConfig.ReconciliationEnabled {
		reconciler = gossipprivdata.NewReconciler(channelID, g.metrics.PrivdataMetrics,
			support.Committer, fetcher, g.privdataConfig)
	} else {
		reconciler = &gossipprivdata.NoOpReconciler{}
	}

	pushAckTimeout := g.serviceConfig.PvtDataPushAckTimeout
	g.privateHandlers[channelID] = privateHandler{
		support:     support,
		coordinator: coordinator,
		distributor: gossipprivdata.NewDistributor(channelID, g, collectionAccessFactory, g.metrics.PrivdataMetrics, pushAckTimeout),
		reconciler:  reconciler,
	}
	g.privateHandlers[channelID].reconciler.Start()

	blockingMode := !g.serviceConfig.NonBlockingCommitMode
	stateConfig := state.GlobalConfig()
	g.chains[channelID] = state.NewGossipStateProvider(
		flogging.MustGetLogger(util.StateLogger),
		channelID,
		servicesAdapter,
		coordinator,
		g.metrics.StateMetrics,
		blockingMode,
		stateConfig)
	if g.deliveryService[channelID] == nil {
		g.deliveryService[channelID] = g.deliveryFactory.Service(g, ordererSource, g.mcs, g.serviceConfig.OrgLeader)
	}

	// Delivery service might be nil only if it was not able to get connected
	// to the ordering service
	if g.deliveryService[channelID] != nil {
		// Parameters:
		//              - peer.gossip.useLeaderElection
		//              - peer.gossip.orgLeader
		//
		// are mutual exclusive, setting both to true is not defined, hence
		// peer will panic and terminate
		leaderElection := g.serviceConfig.UseLeaderElection
		isStaticOrgLeader := g.serviceConfig.OrgLeader

		if leaderElection && isStaticOrgLeader {
			logger.Panic("Setting both orgLeader and useLeaderElection to true isn't supported, aborting execution")
		}

		if leaderElection {
			logger.Debug("Delivery uses dynamic leader election mechanism, channel", channelID)
			g.leaderElection[channelID] = g.newLeaderElectionComponent(channelID, g.onStatusChangeFactory(channelID,
				support.Committer), g.metrics.ElectionMetrics)
		} else if isStaticOrgLeader {
			logger.Debug("This peer is configured to connect to ordering service for blocks delivery, channel", channelID)
			g.deliveryService[channelID].StartDeliverForChannel(channelID, support.Committer, func() {})
		} else {
			logger.Debug("This peer is not configured to connect to ordering service for blocks delivery, channel", channelID)
		}
	} else {
		logger.Warning("Delivery client is down won't be able to pull blocks for chain", channelID)
	}
}

func (g *GossipService) createSelfSignedData() protoutil.SignedData {
	msg := make([]byte, 32)
	sig, err := g.mcs.Sign(msg)
	if err != nil {
		logger.Panicf("Failed creating self signed data because message signing failed: %v", err)
	}
	return protoutil.SignedData{
		Data:      msg,
		Signature: sig,
		Identity:  g.peerIdentity,
	}
}

// updateAnchors 构造一个 joinChannelMessage 并将其发送给 gossipSvc。
// 方法接收者：g（GossipService类型的指针）
// 输入参数：
//   - configUpdate：ConfigUpdate类型，表示配置更新。
func (g *GossipService) updateAnchors(configUpdate ConfigUpdate) {
	// 获取当前节点所属的组织
	myOrg := string(g.secAdv.OrgByPeerIdentity(api.PeerIdentityType(g.peerIdentity)))

	// 检查当前节点所属的组织是否在配置更新中
	if !g.amIinChannel(myOrg, configUpdate) {
		logger.Error("已尝试加入通道", configUpdate.ChannelID, "但是我们的组织(", myOrg, "), 不是 "+
			"在通道的组织中:", orgListFromConfigUpdate(configUpdate), ", 中止.")
		return
	}

	// 创建 joinChannelMessage
	jcm := &joinChannelMessage{seqNum: configUpdate.Sequence, members2AnchorPeers: map[string][]api.AnchorPeer{}}
	anchorPeerEndpoints := map[string]struct{}{}

	// 遍历配置更新中的组织列表
	for _, appOrg := range configUpdate.Organizations {
		logger.Debug(appOrg.MSPID(), "peer 锚节点:", appOrg.AnchorPeers())

		// 创建 AnchorPeer 列表，并将其添加到 joinChannelMessage 中
		jcm.members2AnchorPeers[appOrg.MSPID()] = []api.AnchorPeer{}
		for _, ap := range appOrg.AnchorPeers() {
			anchorPeer := api.AnchorPeer{
				Host: ap.Host,
				Port: int(ap.Port),
			}
			jcm.members2AnchorPeers[appOrg.MSPID()] = append(jcm.members2AnchorPeers[appOrg.MSPID()], anchorPeer)
			anchorPeerEndpoints[fmt.Sprintf("%s:%d", ap.Host, ap.Port)] = struct{}{}
		}
	}

	// 更新 anchorPeerTracker
	g.anchorPeerTracker.update(configUpdate.ChannelID, anchorPeerEndpoints)

	// 创建新的状态提供程序，并加入通道
	logger.Debug("为channelID创建状态提供程序", configUpdate.ChannelID)
	g.JoinChan(jcm, common.ChannelID(configUpdate.ChannelID))
}

// AddPayload appends message payload to for given chain
func (g *GossipService) AddPayload(channelID string, payload *gproto.Payload) error {
	g.lock.RLock()
	defer g.lock.RUnlock()
	return g.chains[channelID].AddPayload(payload)
}

// Stop stops the gossip component
func (g *GossipService) Stop() {
	g.lock.Lock()
	defer g.lock.Unlock()

	for chainID := range g.chains {
		logger.Info("Stopping chain", chainID)
		if le, exists := g.leaderElection[chainID]; exists {
			logger.Infof("Stopping leader election for %s", chainID)
			le.Stop()
		}
		g.chains[chainID].Stop()
		g.privateHandlers[chainID].close()

		if g.deliveryService[chainID] != nil {
			g.deliveryService[chainID].Stop()
		}
	}
	g.gossipSvc.Stop()
}

func (g *GossipService) newLeaderElectionComponent(channelID string, callback func(bool),
	electionMetrics *gossipmetrics.ElectionMetrics) election.LeaderElectionService {
	PKIid := g.mcs.GetPKIidOfCert(g.peerIdentity)
	adapter := election.NewAdapter(g, PKIid, common.ChannelID(channelID), electionMetrics)
	config := election.ElectionConfig{
		StartupGracePeriod:       g.serviceConfig.ElectionStartupGracePeriod,
		MembershipSampleInterval: g.serviceConfig.ElectionMembershipSampleInterval,
		LeaderAliveThreshold:     g.serviceConfig.ElectionLeaderAliveThreshold,
		LeaderElectionDuration:   g.serviceConfig.ElectionLeaderElectionDuration,
	}
	return election.NewLeaderElectionService(adapter, string(PKIid), callback, config)
}

func (g *GossipService) amIinChannel(myOrg string, configUpdate ConfigUpdate) bool {
	for _, orgName := range orgListFromConfigUpdate(configUpdate) {
		if orgName == myOrg {
			return true
		}
	}
	return false
}

func (g *GossipService) onStatusChangeFactory(channelID string, committer blocksprovider.LedgerInfo) func(bool) {
	return func(isLeader bool) {
		if isLeader {
			yield := func() {
				g.lock.RLock()
				le := g.leaderElection[channelID]
				g.lock.RUnlock()
				le.Yield()
			}
			logger.Info("Elected as a leader, starting delivery service for channel", channelID)
			if err := g.deliveryService[channelID].StartDeliverForChannel(channelID, committer, yield); err != nil {
				logger.Errorf("Delivery service is not able to start blocks delivery for chain, due to %+v", err)
			}
		} else {
			logger.Info("Renounced leadership, stopping delivery service for channel", channelID)
			if err := g.deliveryService[channelID].StopDeliverForChannel(channelID); err != nil {
				logger.Errorf("Delivery service is not able to stop blocks delivery for chain, due to %+v", err)
			}
		}
	}
}

func orgListFromConfigUpdate(config ConfigUpdate) []string {
	var orgList []string
	for _, appOrg := range config.Organizations {
		orgList = append(orgList, appOrg.MSPID())
	}
	return orgList
}
