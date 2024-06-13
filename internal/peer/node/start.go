/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	discprotos "github.com/hyperledger/fabric-protos-go/discovery"
	gatewayprotos "github.com/hyperledger/fabric-protos-go/gateway"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/cauthdsl"
	ccdef "github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/fabhttp"
	"github.com/hyperledger/fabric/common/flogging"
	floggingmetrics "github.com/hyperledger/fabric/common/flogging/metrics"
	"github.com/hyperledger/fabric/common/grpclogging"
	"github.com/hyperledger/fabric/common/grpcmetrics"
	"github.com/hyperledger/fabric/common/metadata"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/policydsl"
	"github.com/hyperledger/fabric/core/aclmgmt"
	"github.com/hyperledger/fabric/core/cclifecycle"
	"github.com/hyperledger/fabric/core/chaincode"
	"github.com/hyperledger/fabric/core/chaincode/accesscontrol"
	"github.com/hyperledger/fabric/core/chaincode/extcc"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/chaincode/persistence"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	"github.com/hyperledger/fabric/core/committer/txvalidator/plugin"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/privdata"
	coreconfig "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/container"
	"github.com/hyperledger/fabric/core/container/dockercontroller"
	"github.com/hyperledger/fabric/core/container/externalbuilder"
	"github.com/hyperledger/fabric/core/deliverservice"
	"github.com/hyperledger/fabric/core/dispatcher"
	"github.com/hyperledger/fabric/core/endorser"
	authHandler "github.com/hyperledger/fabric/core/handlers/auth"
	endorsement2 "github.com/hyperledger/fabric/core/handlers/endorsement/api"
	endorsement3 "github.com/hyperledger/fabric/core/handlers/endorsement/api/identities"
	"github.com/hyperledger/fabric/core/handlers/library"
	validation "github.com/hyperledger/fabric/core/handlers/validation/api"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	"github.com/hyperledger/fabric/core/ledger/snapshotgrpc"
	"github.com/hyperledger/fabric/core/operations"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/policy"
	"github.com/hyperledger/fabric/core/scc"
	"github.com/hyperledger/fabric/core/scc/cscc"
	"github.com/hyperledger/fabric/core/scc/lscc"
	"github.com/hyperledger/fabric/core/scc/qscc"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/discovery"
	"github.com/hyperledger/fabric/discovery/endorsement"
	discsupport "github.com/hyperledger/fabric/discovery/support"
	discacl "github.com/hyperledger/fabric/discovery/support/acl"
	ccsupport "github.com/hyperledger/fabric/discovery/support/chaincode"
	"github.com/hyperledger/fabric/discovery/support/config"
	"github.com/hyperledger/fabric/discovery/support/gossip"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
	gossipgossip "github.com/hyperledger/fabric/gossip/gossip"
	gossipmetrics "github.com/hyperledger/fabric/gossip/metrics"
	gossipprivdata "github.com/hyperledger/fabric/gossip/privdata"
	gossipservice "github.com/hyperledger/fabric/gossip/service"
	peergossip "github.com/hyperledger/fabric/internal/peer/gossip"
	"github.com/hyperledger/fabric/internal/peer/version"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/gateway"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	chaincodeListenAddrKey = "peer.chaincodeListenAddress"
	defaultChaincodePort   = 7052
)

var chaincodeDevMode bool

// startCmd 函数用于创建并返回一个用于启动节点的 Cobra 命令。
// 返回值：
//   - *cobra.Command：用于启动节点的 Cobra 命令。
func startCmd() *cobra.Command {
	// 在节点启动命令上设置标志。
	flags := nodeStartCmd.Flags()

	// 定义并绑定一个名为 "peer-chaincodedev" 的布尔型标志，用于指定是否以链码开发模式启动节点。
	flags.BoolVarP(&chaincodeDevMode, "peer-chaincodedev", "", false, "在chaincode开发模式下启动peer")

	return nodeStartCmd
}

// nodeStartCmd 是一个用于表示启动节点命令的 Cobra 命令。
var nodeStartCmd = &cobra.Command{
	Use:   "start",       // 命令的使用方式
	Short: "启动节点.",       // 命令的简短描述
	Long:  `启动与网络交互的节点.`, // 命令的详细描述
	// 启动节点, 启动之前需执行完成InitCmd方法, 加载msp
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("检测到后续还有参数args")
		}
		// 解析命令行参数已完成，因此静默显示命令的使用方式
		cmd.SilenceUsage = true
		// 启动peer节点服务
		return serve(args)
	},
}

// externalVMAdapter adapts coerces the result of Build to the
// container.Interface type expected by the VM interface.
type externalVMAdapter struct {
	detector *externalbuilder.Detector
}

// Build 构建外部虚拟机适配器的实例。
// 方法接收者：e（externalVMAdapter类型）
// 输入参数：
//   - ccid：string类型，表示链码ID。
//   - mdBytes：[]byte类型，表示链码元数据的字节数据。
//   - codePackage：io.Reader类型，表示链码代码包的读取器。
//
// 返回值：
//   - container.Instance：表示构建的实例。
//   - error：如果构建过程中出错，则返回错误。
func (e externalVMAdapter) Build(
	ccid string,
	mdBytes []byte,
	codePackage io.Reader,
) (container.Instance, error) {
	// 调用外部构建器的 Build 方法构建实例, 执行外部构建器的检测和构建过程
	i, err := e.detector.Build(ccid, mdBytes, codePackage)
	if err != nil {
		return nil, err
	}

	// 确保返回 <nil> 而不是 (*externalbuilder.Instance)(nil)
	if i == nil {
		return nil, nil
	}
	return i, err
}

type disabledDockerBuilder struct{}

func (disabledDockerBuilder) Build(string, *persistence.ChaincodePackageMetadata, io.Reader) (container.Instance, error) {
	return nil, errors.New("docker build is disabled")
}

type endorserChannelAdapter struct {
	peer *peer.Peer
}

func (e endorserChannelAdapter) Channel(channelID string) *endorser.Channel {
	if peerChannel := e.peer.Channel(channelID); peerChannel != nil {
		return &endorser.Channel{
			IdentityDeserializer: peerChannel.MSPManager(),
		}
	}

	return nil
}

// 一个适配器，用于将 chaincode.Launcher 和 extcc.StreamHandler 结合起来使用
type custodianLauncherAdapter struct {
	launcher      chaincode.Launcher  // 启动器
	streamHandler extcc.StreamHandler // 流处理器
}

func (c custodianLauncherAdapter) Launch(ccid string) error {
	return c.launcher.Launch(ccid, c.streamHandler)
}

func (c custodianLauncherAdapter) Stop(ccid string) error {
	return c.launcher.Stop(ccid)
}

// peer节点启动方法。800行
func serve(args []string) error {
	// 目前，对等体仅适用于标准MSP，因为在某些情况下，MSP必须确保从单个凭据中只有一个 “身份”。
	// Idemix不支持这个 * 还 * 但它可以很容易地固定支持它。现在，我们只是确保对等体只提供标准MSP
	mspType := mgmt.GetLocalMSP(factory.GetDefault()).GetType()
	if mspType != msp.FABRIC {
		panic("不支持的msp类型 " + msp.ProviderTypeToString(mspType))
	}

	// 使用e golang.org/x/net/trace包跟踪rpc。
	// 这被移出了交付服务连接工厂，因为它具有广泛的进程含义，并且在初始化gRPC客户端和服务器方面很活泼。
	grpc.EnableTracing = true

	logger.Infof("启动 %s", version.GetInfo())

	// 读取core.yaml配置
	coreConfig, err := peer.GlobalConfig()
	if err != nil {
		return err
	}

	// 注册Fabric支持的链码平台的规范列表(java,go,node)
	platformRegistry := platforms.NewRegistry(platforms.SupportedPlatforms...)

	// 创建一个http服务器, 健康检查注册表，用于注册和管理健康检查
	opsSystem := newOperationsSystem(coreConfig)
	// 启动http
	err = opsSystem.Start()
	if err != nil {
		return errors.WithMessage(err, "无法初始化http服务")
	}
	defer opsSystem.Stop()
	logger.Infof("启动用于注册和管理健康检查的http服务: " + opsSystem.Addr())

	// 记录日志条数工具, 指标提供者，用于收集和暴露指标数据
	metricsProvider := opsSystem.Provider
	logObserver := floggingmetrics.NewObserver(metricsProvider)
	flogging.SetObserver(logObserver)

	// 创建链码安装包信息目录 {peer.fileSystemPath}/lifecycle/chaincodes
	chaincodeInstallPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "lifecycle", "chaincodes")
	ccStore := persistence.NewStore(chaincodeInstallPath)
	// 提供解析链代码包的能力。
	ccPackageParser := &persistence.ChaincodePackageParser{
		// MetadataAsTarEntries 函数有从链码包中提取元数据的功能
		MetadataProvider: ccprovider.PersistenceAdapter(ccprovider.MetadataAsTarEntries),
	}

	// 提取peer节点地址
	peerHost, _, err := net.SplitHostPort(coreConfig.PeerAddress)
	if err != nil {
		return fmt.Errorf("对等地址的格式不是host:port: %v", err)
	}
	// 提取监听地址, 也会是gRPC的地址
	listenAddr := coreConfig.ListenAddress

	// 对等方的 gRPC 服务器配置(tls证书, keepalive连接配置, gRPC 接收发生解析限制100m)
	serverConfig, err := peer.GetServerConfig()
	if err != nil {
		logger.Fatalf("加载peer节点的 gRPC 配置时出错 (%s)", err)
	}

	// 拦截器配置
	serverConfig.Logger = flogging.MustGetLogger("core.comm").With("server", "PeerServer")
	serverConfig.ServerStatsHandler = comm.NewServerStatsHandler(metricsProvider)
	// 打印请求耗时拦截器 和 请求响应消息
	serverConfig.UnaryInterceptors = append(
		serverConfig.UnaryInterceptors,
		grpcmetrics.UnaryServerInterceptor(grpcmetrics.NewUnaryMetrics(metricsProvider)),
		grpclogging.UnaryServerInterceptor(flogging.MustGetLogger("comm.grpc.server").Zap()),
	)
	// 打印请求耗时拦截器 和 请求响应消息。Stream流式请求。
	serverConfig.StreamInterceptors = append(
		serverConfig.StreamInterceptors,
		grpcmetrics.StreamServerInterceptor(grpcmetrics.NewStreamMetrics(metricsProvider)),
		grpclogging.StreamServerInterceptor(flogging.MustGetLogger("comm.grpc.server").Zap()),
	)

	// 请求并发拦截器配置, 请求限流配置
	semaphores := initGrpcSemaphores(coreConfig)
	if len(semaphores) != 0 {
		serverConfig.UnaryInterceptors = append(serverConfig.UnaryInterceptors, unaryGrpcLimiter(semaphores))
		serverConfig.StreamInterceptors = append(serverConfig.StreamInterceptors, streamGrpcLimiter(semaphores))
	}

	// tls证书验证, 返回tls证书、根证书、证书链
	cs := comm.NewCredentialSupport()
	if serverConfig.SecOpts.UseTLS {
		logger.Info("peer节点已启用TLS验证")
		cs = comm.NewCredentialSupport(serverConfig.SecOpts.ServerRootCAs...)

		// 获取并验证使用的tls证书
		clientCert, err := peer.GetClientCertificate()
		if err != nil {
			logger.Fatalf("设置TLS客户端证书失败 (%s)", err)
		}
		cs.SetClientCertificate(clientCert)
	}

	// 创建leverdb存储目录/var/hyperledger/production/transientstore, 并启动leveldb
	transientStoreProvider, err := transientstore.NewStoreProvider(
		filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "transientstore"),
	)
	if err != nil {
		return errors.WithMessage(err, "启动leveldb失败")
	}

	// 提交服务配置
	deliverServiceConfig := deliverservice.GlobalConfig()
	// 初始化 peer 顶级结构体, 包含peer的属性和初始化通道、创建通道、解析通道、获取账本、获取mspid等peer节点的方法
	peerInstance := &peer.Peer{
		ServerConfig:             serverConfig,                                  // gRPC相关属性, 包括连接超时、请求限流、请求日志、tls证书
		CredentialSupport:        cs,                                            // 管理用于gRPC客户端连接的凭据, 包括tls证书, tls根证书
		StoreProvider:            transientStoreProvider,                        // leveldb存储相关
		CryptoProvider:           factory.GetDefault(),                          // 加密、签名、获取密钥实例
		OrdererEndpointOverrides: deliverServiceConfig.OrdererEndpointOverrides, // orderer节点覆盖, 包括需要覆盖的节点addr, 证书
	}

	// 根据通道名称获取对应的身份反序列化器，用于处理与身份相关的操作
	identityDeserializerFactory := func(channelName string) msp.IdentityDeserializer {
		// 获取指定通道名称的通道实例
		if channel := peerInstance.Channel(channelName); channel != nil {
			// 返回通道实例的 MSPManage
			return channel.MSPManager()
		}
		return nil
	}

	mspID := coreConfig.LocalMSPID
	// 加载msp目录证书, 返回msp实例
	localMSP := mgmt.GetLocalMSP(factory.GetDefault())

	// 获取签名身份, 包括mspid, 签名证书、签名证书公钥、签名实例
	signingIdentity, err := localMSP.GetDefaultSigningIdentity()
	if err != nil {
		logger.Panicf("无法从本地MSP获取默认签名实例: [%+v]", err)
	}

	// 对x509证书编码为byte
	signingIdentityBytes, err := signingIdentity.Serialize()
	if err != nil {
		logger.Panicf("无法序列化签名标识: %v", err)
	}

	// 新会员信息提供商, 成员信息提供者，用于获取网络成员的信息
	membershipInfoProvider := privdata.NewMembershipInfoProvider(
		mspID,
		createSelfSignedData(signingIdentity), // 创建自签名数据
		identityDeserializerFactory,           // 根据通道名称获取对应的身份反序列化器，用于处理与身份相关的操作
	)

	// 在证书过期前一周发出警告, 包含查询注册证书、服务端证书、客户端证书的过程时间, 计算距离过期的剩余时间
	expirationLogger := flogging.MustGetLogger("certmonitor")
	crypto.TrackExpiration(
		serverConfig.SecOpts.UseTLS,           // 是否启用tls
		serverConfig.SecOpts.Certificate,      // 用户 tls 证书
		cs.GetClientCertificate().Certificate, // 节点 tls 证书
		signingIdentityBytes,                  // 序列化的身份, 包含签名证书
		expirationLogger.Infof,                // 信息函数日志
		expirationLogger.Warnf,                // 警告函数日志
		time.Now(),                            // 当前时间
		time.AfterFunc,                        // 时间调度器
	)

	// 配置策略管理器, 指定通道ID的通道的策略管理器
	policyMgr := policies.PolicyManagerGetterFunc(peerInstance.GetPolicyManager)
	// 新的策略检查器
	policyChecker := policy.NewPolicyChecker(
		policyMgr,                              // 指定通道ID的通道的策略管理器
		mgmt.GetLocalMSP(factory.GetDefault()), // msp实例
	)

	// 使用默认ACL提供程序 (基于资源和默认1.0策略) 启动 aclmgmt。
	// 用户可以将自己的ACLProvider传递给RegisterACLProvider (当前单元测试这样做)
	// ACL 是一种用于控制对资源的访问权限的机制，它定义了哪些用户、组或角色可以执行特定操作或访问特定资源
	// 这里面定义了 _lifecycle、LSCC、QSCC、CSCC、事件的策略, 分表属于Admins、Members角色, 还是各自的读写权限配置
	aclProvider := aclmgmt.NewACLProvider(
		aclmgmt.ResourceGetter(peerInstance.GetStableChannelConfig), // 具有指定通道ID的通道的稳定通道配置回调
		policyChecker, // 指定通道ID的通道的策略管理器
	)

	// TODO, 很不幸, 生命周期初始化目前非常不干净。
	// 这是因为 ccprovider.SetChaincodePath 仅在 ledgermgmt.Initialize 之后才起作用，
	// 但是 ledgermgmt.Initialize 需要引用生命周期。
	// 最后，lscc 需要引用系统链码提供程序才能创建，这需要链码支持，
	// 这也需要，你猜对了，生命周期。一旦我们删除了v1.0生命周期，
	// 我们应该很好地将生命周期的所有 初始化 折叠到这一点。
	lifecycleResources := &lifecycle.Resources{
		Serializer:          &lifecycle.Serializer{},
		ChannelConfigSource: peerInstance,
		ChaincodeStore:      ccStore,
		PackageParser:       ccPackageParser,
	}

	// gossip私有数据配置: gossip式节点之间进行通信和共享信息的协议。它是一种去中心化的通信协议，旨在实现高效的消息传递和状态同步。
	// Gossip 协议的主要目标是在 Fabric 网络中维护一致的状态视图，并确保所有对等节点之间的数据一致性。
	// 它通过对等节点之间的相互通信来传播和交换区块、交易和状态信息。
	// Gossip 协议的工作原理如下：
	//
	// 1. 对等节点通过周期性地向其他对等节点发送消息来传播信息。这些消息包含有关区块、交易和状态的摘要信息。
	// 2. 当一个对等节点接收到来自其他对等节点的消息时，它会将这些消息存储在本地的消息池中。
	// 3. 对等节点会定期选择一些消息发送给其他对等节点，以便将信息传播到整个网络。选择的消息通常是具有较高优先级的消息或者是尚未在接收节点中存在的消息。
	// 4. 接收到消息的对等节点会验证消息的有效性，并将其存储在本地的区块链中。
	// 5. 对等节点还会定期与其他对等节点进行状态同步，以确保它们的状态视图保持一致。
	privdataConfig := gossipprivdata.GlobalConfig()

	// 验证器和提交者的结构体。
	// 部署链码信息提供者，用于获取已部署链码的信息
	lifecycleValidatorCommitter := &lifecycle.ValidatorCommitter{
		CoreConfig:                   coreConfig,         // core.yaml配置
		PrivdataConfig:               privdataConfig,     // gossip配置
		Resources:                    lifecycleResources, // 生命周期
		LegacyDeployedCCInfoProvider: &lscc.DeployedCCInfoProvider{},
	}

	// 在调用 ccInfoFSImpl.ListInstalledChaincodes() 之前创建链码包存储
	lsccInstallPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "chaincodes")
	ccprovider.SetChaincodesPath(lsccInstallPath)
	// 赋值hash算法
	ccInfoFSImpl := &ccprovider.CCInfoFSImpl{GetHasher: factory.GetDefault()}

	// legacyMetadataManager从旧版生命周期 (lscc) 收集元数据信息。
	// 这预计会随着FAB-15061而消失。
	legacyMetadataManager, err := cclifecycle.NewMetadataManager(
		// 枚举已安装的链码
		cclifecycle.EnumerateFunc(
			func() ([]ccdef.InstalledChaincode, error) {
				// GetChaincodeInstallPath 返回已安装的链代码的路径。
				// ListInstalledChaincodes 用于检索已安装的链码。
				// LoadPackage 提取链码信息。
				return ccInfoFSImpl.ListInstalledChaincodes(ccInfoFSImpl.GetChaincodeInstallPath(), ioutil.ReadDir, ccprovider.LoadPackage)
			},
		),
	)
	if err != nil {
		logger.Panicf("创建LegacyMetadataManager失败: +%v", err)
	}

	// metadataManager聚合来自 _lifecycle和legacy lifecycle (lscc) 的元数据信息。
	metadataManager := lifecycle.NewMetadataManager()

	// 由于我们正在从lscc过渡到 _lifecycle，这两个管理器的目的是将每个通道的链码数据馈送到gossip中，
	// 在v2.1之前，我们仍然有两个这样的信息提供者，
	// 在其中我们将删除遗留。
	//
	// 信息流如下
	//
	// gossip <-- metadataManager <-- lifecycleCache  (for _lifecycle)
	//                             \
	//                              - legacyMetadataManager (for lscc)
	//
	// FAB-15061跟踪删除LSCC所需的工作，此时我们将能够简化流程
	//
	// gossip <-- lifecycleCache

	// 新的链码保管人, 负责在链码可用时将其构建和启动排队
	chaincodeCustodian := lifecycle.NewChaincodeCustodian()
	// 构建外部缓存目录
	externalBuilderOutput := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "externalbuilder", "builds")
	if err := os.MkdirAll(externalBuilderOutput, 0o700); err != nil {
		logger.Panicf("无法创建 externalbuilder build 输出方向: %s", err)
	}

	// 外部构建器的元数据提供者
	ebMetadataProvider := &externalbuilder.MetadataProvider{
		DurablePath: externalBuilderOutput,
	}

	// 链码生命周期事件提供者，用于获取链码生命周期事件
	lifecycleCache := lifecycle.NewCache(
		lifecycleResources,
		mspID,
		metadataManager,
		chaincodeCustodian,
		ebMetadataProvider,
	)

	// 允许在自定义事务的提交时间内生成模拟结果。自定义交易处理器映射，存储不同类型交易的处理器
	txProcessors := map[cb.HeaderType]ledger.CustomTxProcessor{
		cb.HeaderType_CONFIG: &peer.ConfigTxProcessor{},
	}

	// 账本管理器实例, 管理所有通道的账本, 并生成账本目录
	peerInstance.LedgerMgr = ledgermgmt.NewLedgerMgr(
		// Initializer 封装了账本模块的所有外部配置。
		&ledgermgmt.Initializer{
			CustomTxProcessors:              txProcessors,                           // 自定义交易处理器映射，存储不同类型交易的处理器
			DeployedChaincodeInfoProvider:   lifecycleValidatorCommitter,            // 部署链码信息提供者，用于获取已部署链码的信息
			MembershipInfoProvider:          membershipInfoProvider,                 // 成员信息提供者，用于获取网络成员的信息
			ChaincodeLifecycleEventProvider: lifecycleCache,                         // 链码生命周期事件提供者，用于获取链码生命周期事件
			MetricsProvider:                 metricsProvider,                        // 指标提供者，用于收集和暴露指标数据
			HealthCheckRegistry:             opsSystem,                              // 健康检查注册表，用于注册和管理健康检查
			StateListeners:                  []ledger.StateListener{lifecycleCache}, // 状态监听器列表，存储状态变更的监听器
			Config:                          ledgerConfig(),                         // 账本配置
			HashProvider:                    factory.GetDefault(),                   // 哈希提供者，用于计算哈希值
			EbMetadataProvider:              ebMetadataProvider,                     // 外部构建器的元数据提供者
		},
	)

	// 新建GRPC服务器
	peerServer, err := comm.NewGRPCServer(listenAddr, serverConfig)
	if err != nil {
		logger.Fatalf("无法创建peer服务器 (%s)", err)
	}

	// FIXME: 创建gossip服务的副作用是启动一堆go例程并向grpc服务器注册。
	// 加载gossip配置。创建一个附加到 gRPC 服务器的 gossip 实例。启动心跳检测、检查存活、重新连接等协程。
	gossipService, err := initGossipService(
		policyMgr,              // 配置策略管理器, 指定通道ID的通道的策略管理器
		metricsProvider,        // 记录日志条数工具, 指标提供者，用于收集和暴露指标数据
		peerServer,             // peer服务器实例。
		signingIdentity,        // 签名身份, 包括mspid, 签名证书、签名证书公钥、签名实例
		cs,                     // tls证书。
		coreConfig.PeerAddress, // 对等节点地址。
		deliverServiceConfig,   // 提交服务配置
		privdataConfig,         // 私有数据配置。
	)
	if err != nil {
		return errors.WithMessage(err, "初始化gossip服务失败")
	}
	defer gossipService.Stop()

	peerInstance.GossipService = gossipService

	// 初始化本地链码。应该在缓存创建后调用一次 (时间无关紧要，尽管已经安装的链码在完成之前不会被调用)。
	if err := lifecycleCache.InitializeLocalChaincodes(); err != nil {
		return errors.WithMessage(err, "无法初始化本地链码")
	}

	// 在缓存任何参数之前，必须处理参数覆盖。
	// 缓存失败会导致服务器立即终止。
	if chaincode.IsDevMode() {
		logger.Info("在chaincode开发模式下运行, 用户在本地机器上从命令行启动peer后运行链码")
		logger.Info("禁用加载有效性系统链代码")

		viper.Set("chaincode.mode", chaincode.DevModeUserRunsChaincode)
	}

	mutualTLS := serverConfig.SecOpts.UseTLS && serverConfig.SecOpts.RequireClientCert
	policyCheckerProvider := func(resourceName string) deliver.PolicyCheckerFunc {
		return func(env *cb.Envelope, channelID string) error {
			return aclProvider.CheckACL(resourceName, channelID, env)
		}
	}

	// 结构体用于记录各种指标的计数器。已打开的流、已关闭的流、已接收的请求、已完成的请求、已发送的区块
	metrics := deliver.NewMetrics(metricsProvider)
	// 持有创建交付服务器所需的依赖关系。
	abServer := &peer.DeliverServer{
		// 交付处理程序, 处理服务器请求。
		DeliverHandler: deliver.NewHandler(
			&peer.DeliverChainManager{Peer: peerInstance}, // 接口的实例，用于管理链码
			coreConfig.AuthenticationTimeWindow,           // 时间窗口的持续时间，用于验证事务的时间戳
			mutualTLS,                                     // 是否启用互相认证的 TLS
			metrics,                                       // 结构体的指针，用于记录指标数据
			false,                                         // 是否禁用链码过期检查
		),
		// 策略检查器提供者
		PolicyCheckerProvider: policyCheckerProvider,
	}
	// 注册交付/提交服务器
	pb.RegisterDeliverServer(peerServer.Server(), abServer)

	// 为链码服务创建自签名CA
	ca, err := tlsgen.NewCA()
	if err != nil {
		logger.Panic("链码自签名CA证书创建失败:", err)
	}

	// 创建链码服务
	ccSrv, ccEndpoint, err := createChaincodeServer(coreConfig, ca, peerHost)
	if err != nil {
		logger.Panicf("创建链码gRPC服务器失败: %s", err)
	}

	// 获取用户模式 dev/docker
	userRunsCC := chaincode.IsDevMode()
	tlsEnabled := coreConfig.PeerTLSEnabled

	// 创建特定于链码的tls CA
	authenticator := accesscontrol.NewAuthenticator(ca)

	// 构造一个HandlerRegistry维护链码处理程序实例。
	chaincodeHandlerRegistry := chaincode.NewHandlerRegistry(userRunsCC)
	// 获取事务查询执行器。
	lifecycleTxQueryExecutorGetter := &chaincode.TxQueryExecutorGetter{
		CCID:            scc.ChaincodeID(lifecycle.LifecycleNamespace), // 链码 ID
		HandlerRegistry: chaincodeHandlerRegistry,                      // 处理程序注册表
	}

	// 如果需要定义外部链码, VMEndpoint必须为"", 并且配置外部链码ExternalBuilders
	if coreConfig.VMEndpoint == "" && len(coreConfig.ExternalBuilders) == 0 {
		logger.Panic("vm.endpoint 未设置且未定义 chaincode.externalBuilders")
	}

	chaincodeConfig := chaincode.GlobalConfig()

	// 构建链码
	var dockerBuilder container.DockerBuilder
	if coreConfig.VMEndpoint != "" {
		// 创建一个链码 Docker 客户端
		client, err := createDockerClient(coreConfig)
		if err != nil {
			logger.Panicf("无法创建链码docker客户端: %s", err)
		}

		// 是一个虚拟机。它由一个镜像 ID 来标识。
		dockerVM := &dockercontroller.DockerVM{
			PeerID:        coreConfig.PeerID,                                    // 对等节点 ID
			NetworkID:     coreConfig.NetworkID,                                 // 网络 ID
			BuildMetrics:  dockercontroller.NewBuildMetrics(opsSystem.Provider), // 构建指标
			Client:        client,                                               // Docker 客户端
			AttachStdOut:  coreConfig.VMDockerAttachStdout,                      // 是否附加标准输出
			HostConfig:    getDockerHostConfig(),                                // 主机配置
			ChaincodePull: coreConfig.ChaincodePull,                             // 是否拉取链码
			NetworkMode:   coreConfig.VMNetworkMode,                             // 网络模式
			PlatformBuilder: &platforms.Builder{ // 平台构建器
				Registry: platformRegistry, // 注册表
				Client:   client,           // Docker 客户端
			},
			// 这个字段对于使用v2.0二进制文件构建的链码来说是多余的，
			// 为了防止用户被迫重建离开现在，但它应该在未来被删除。
			LoggingEnv: []string{
				"CORE_CHAINCODE_LOGGING_LEVEL=" + chaincodeConfig.LogLevel,
				"CORE_CHAINCODE_LOGGING_SHIM=" + chaincodeConfig.ShimLogLevel,
				"CORE_CHAINCODE_LOGGING_FORMAT=" + chaincodeConfig.LogFormat,
			},
			MSPID: mspID, // MSP ID
		}
		if err := opsSystem.RegisterChecker("docker", dockerVM); err != nil {
			logger.Panicf("注册链码docker健康检查失败: %s", err)
		}
		dockerBuilder = dockerVM
	}

	// 当我们缺少docker配置时，docker被禁用
	if dockerBuilder == nil {
		dockerBuilder = &disabledDockerBuilder{}
	}

	// 负责协调外部构建器的检测和构建过程。
	externalVM := &externalbuilder.Detector{
		Builders:    externalbuilder.CreateBuilders(coreConfig.ExternalBuilders, mspID), // DurablePath 是链码资产持久化的文件系统位置。
		DurablePath: externalBuilderOutput,                                              // Builders 是检测和构建过程将使用的构建器。
	}

	// 用于注册链码构建状态。
	buildRegistry := &container.BuildRegistry{}

	// 负责路由和管理链码实例。
	containerRouter := &container.Router{
		DockerBuilder:   dockerBuilder,                 // Docker 构建器
		ExternalBuilder: externalVMAdapter{externalVM}, // 外部构建器
		PackageProvider: &persistence.FallbackPackageLocator{ // 包提供者, 一个回退的链码包定位器。
			ChaincodePackageLocator: &persistence.ChaincodePackageLocator{ // 链码包定位器
				ChaincodeDir: chaincodeInstallPath,
			},
			LegacyCCPackageLocator: &ccprovider.CCInfoFSImpl{GetHasher: factory.GetDefault()}, // 旧版链码包定位器
		},
	}

	builtinSCCs := map[string]struct{}{
		"lscc":       {},
		"qscc":       {},
		"cscc":       {},
		"_lifecycle": {},
	}

	// 实现了链码的生命周期和相关策略。
	lsccInst := &lscc.SCC{
		BuiltinSCCs: builtinSCCs, // BuiltinSCCs 是内置的系统链码
		Support: &lscc.SupportImpl{ // Support 提供了几个静态函数的实现
			GetMSPIDs:               peerInstance.GetMSPIDs,      // GetMSPIDs 是获取 MSP ID 的函数
			GetIdentityDeserializer: identityDeserializerFactory, // GetIdentityDeserializer 是获取身份反序列化器的函数
		},
		SCCProvider: &lscc.PeerShim{Peer: peerInstance}, // SCCProvider 是传递给系统链码的接口，用于访问系统的其他部分
		ACLProvider: aclProvider,                        // aclProvider 负责访问控制评估
		GetMSPIDs:   peerInstance.GetMSPIDs,             // GetMSPIDs 是获取 MSP ID 的函数
		GetMSPManager: func(channelName string) msp.MSPManager { // GetMSPManager 是获取 MSP 管理器的函数
			return peerInstance.Channel(channelName).MSPManager()
		},
		BCCSP:              factory.GetDefault(), // BCCSP 实例
		BuildRegistry:      buildRegistry,        // BuildRegistry 是构建注册表
		ChaincodeBuilder:   containerRouter,      // ChaincodeBuilder 是链码构建器
		EbMetadataProvider: ebMetadataProvider,   // EbMetadataProvider 是外部构建器的元数据提供者
	}

	// 提供链码背书信息的来源。
	chaincodeEndorsementInfo := &lifecycle.ChaincodeEndorsementInfoSource{
		LegacyImpl:  lsccInst,           // 旧版实现
		Resources:   lifecycleResources, // 资源
		Cache:       lifecycleCache,     // 链码信息缓存
		BuiltinSCCs: builtinSCCs,        // 内置的系统链码
		UserRunsCC:  userRunsCC,         // 用户运行链码
	}

	// 负责管理容器化的链码。
	containerRuntime := &chaincode.ContainerRuntime{
		BuildRegistry:   buildRegistry,   // BuildRegistry 是构建注册表
		ContainerRouter: containerRouter, // ContainerRouter 是容器路由器
	}

	// ExternalFunctions 主要用于支持 SCC 函数。
	// 一般来说，它的方法签名会产生写操作（必须作为背书流程的一部分提交），或者返回人类可读的错误（例如指示找不到链码），而不是哨兵值。
	// 当需要时，可以使用附加到生命周期资源的实用函数。
	lifecycleFunctions := &lifecycle.ExternalFunctions{
		Resources:                 lifecycleResources, // 资源
		InstallListener:           lifecycleCache,     // 安装监听器
		InstalledChaincodesLister: lifecycleCache,     // 已安装链码列表
		ChaincodeBuilder:          containerRouter,    // 链码构建器
		BuildRegistry:             buildRegistry,      // 构建注册表
	}

	// SCC 实现了满足链码接口所需的方法。
	// 它将调用路由到后端实现。
	lifecycleSCC := &lifecycle.SCC{
		Dispatcher: &dispatcher.Dispatcher{ // 处理 SCC 函数输入和输出的协议缓冲区操作
			Protobuf: &dispatcher.ProtobufImpl{}, // Protobuf 应该在生产路径中传递给 Google Protobuf
		},
		DeployedCCInfoProvider: lifecycleValidatorCommitter,    // 已部署链码信息提供者
		QueryExecutorProvider:  lifecycleTxQueryExecutorGetter, // 查询执行器提供者
		Functions:              lifecycleFunctions,             // lifecycle 的后端实现
		OrgMSPID:               mspID,                          // 组织的 MSP ID
		ChannelConfigSource:    peerInstance,                   // 通道配置来源
		ACLProvider:            aclProvider,                    // ACL 提供者
	}

	// RuntimeLauncher 负责启动链码运行时。
	chaincodeLauncher := &chaincode.RuntimeLauncher{
		Metrics:           chaincode.NewLaunchMetrics(opsSystem.Provider), // 启动指标
		Registry:          chaincodeHandlerRegistry,                       // 启动注册表
		Runtime:           containerRuntime,                               // 运行时
		StartupTimeout:    chaincodeConfig.StartupTimeout,                 // 启动超时时间
		CertGenerator:     authenticator,                                  // 证书生成器
		CACert:            ca.CertBytes(),                                 // CA 证书
		PeerAddress:       ccEndpoint,                                     // 对等节点地址
		ConnectionHandler: &extcc.ExternalChaincodeRuntime{},              // 连接处理器
	}

	// 根据是否开启tls, 调整是否使用证书生成器
	if !chaincodeConfig.TLSEnabled {
		chaincodeLauncher.CertGenerator = nil
	}

	// 负责与 Peer 上的链码进行接口交互。
	chaincodeSupport := &chaincode.ChaincodeSupport{
		ACLProvider:            aclProvider,                                     // ACL 提供者
		AppConfig:              peerInstance,                                    // 应用配置检索器
		DeployedCCInfoProvider: lifecycleValidatorCommitter,                     // 已部署链码信息提供者
		ExecuteTimeout:         chaincodeConfig.ExecuteTimeout,                  // 执行超时时间
		InstallTimeout:         chaincodeConfig.InstallTimeout,                  // 安装超时时间
		HandlerRegistry:        chaincodeHandlerRegistry,                        // 处理器注册表
		HandlerMetrics:         chaincode.NewHandlerMetrics(opsSystem.Provider), // 处理器指标
		Keepalive:              chaincodeConfig.Keepalive,                       // Keepalive 时间
		Launcher:               chaincodeLauncher,                               // 启动器
		Lifecycle:              chaincodeEndorsementInfo,                        // 生命周期
		Peer:                   peerInstance,                                    // Peer
		Runtime:                containerRuntime,                                // 运行时
		BuiltinSCCs:            builtinSCCs,                                     // 内置 SCC
		TotalQueryLimit:        chaincodeConfig.TotalQueryLimit,                 // 总查询限制
		UserRunsCC:             userRunsCC,                                      // 用户运行链码
	}

	// 一个适配器，用于将 chaincode.Launcher 和 extcc.StreamHandler 结合起来使用
	custodianLauncher := custodianLauncherAdapter{
		launcher:      chaincodeLauncher, // 启动器
		streamHandler: chaincodeSupport,  // 流处理器
	}

	// 用于执行链码的构建、启动和停止任务
	go chaincodeCustodian.Work(buildRegistry, containerRouter, custodianLauncher)

	// 是ChaincodeSupport服务的服务端API。用于注册 ChaincodeSupport_RegisterServer
	ccSupSrv := pb.ChaincodeSupportServer(chaincodeSupport)
	if tlsEnabled {
		// 包装一个ccSupSrv拦截器, 用于tls验证
		ccSupSrv = authenticator.Wrap(ccSupSrv)
	}

	// New 创建一个新的 CSCC 实例
	// 通常，每个 Peer 实例只会创建一个 CSCC 实例
	csccInst := cscc.New(
		aclProvider,                 // ACL 提供者
		lifecycleValidatorCommitter, // 用于提供已部署链码的信息
		lsccInst,                    // 表示旧的生命周期资源
		lifecycleValidatorCommitter, // 新的生命周期资源
		peerInstance,                // Peer 节点
		factory.GetDefault(),        // BCCSP（区块链加密服务提供者）
	)
	// New 返回一个 QSCC 实例
	// 通常每个 Peer 调用一次该函数
	// 实现了账本查询功能，包括：
	// - GetChainInfo 返回 BlockchainInfo
	// - GetBlockByNumber 返回一个区块
	// - GetBlockByHash 返回一个区块
	// - GetTransactionByID 返回一个交易
	querier := qscc.New(aclProvider, peerInstance)
	// 自描述系统链码的接口, 返回链码名称和底层链码
	qsccInst := scc.SelfDescribingSysCC(querier)

	// 将链码服务、tls拦截器及其实现注册到gRPC服务器。
	pb.RegisterChaincodeSupportServer(ccSrv.Server(), ccSupSrv)

	// 启动特定于chaincode的gRPC侦听服务, 启动底层的 gRPC 服务
	go ccSrv.Start()

	logger.Debugf("正在运行 peer")

	// 加载配置信息, 配置注册表的工厂方法和插件。
	libConf, err := library.LoadConfig()
	if err != nil {
		return errors.WithMessage(err, "无法解析 peer.handlers.* 配置")
	}

	// 创建插件处理程序 peer.handlers.*
	reg := library.InitRegistry(libConf)

	// 一个背书过滤器，用于拦截 ProcessProposal 背书方法。
	authFilters := reg.Lookup(library.Auth).([]authHandler.Filter)
	// SupportImpl 提供了 endorser.Support 接口的实现，通过调用对等节点的各种静态方法来发出调用。
	endorserSupport := &endorser.SupportImpl{
		SignerSerializer: signingIdentity,  // 签名者序列化程序 对 Sign 和 Serialize 方法进行分组。
		Peer:             peerInstance,     // 对等操作 包含支持背书者所需的对等操作。
		ChaincodeSupport: chaincodeSupport, // 链码支持 负责与 Peer 上的链码进行接口交互。
		ACLProvider:      aclProvider,      // ACL提供程序 是访问控制列表（ACL）提供者。
		BuiltinSCCs:      builtinSCCs,      // 内置SC Cs 是特殊的系统链码，与其他（插件）系统链码区分开来。
	}

	// endorsementPluginsByName 是一个通过名称查找的 Endorsement Plugin 工厂映射
	endorsementPluginsByName := reg.Lookup(library.Endorsement).(map[string]endorsement2.PluginFactory)

	// validationPluginsByName 是一个通过名称查找的 Validation Plugin 工厂映射
	validationPluginsByName := reg.Lookup(library.Validation).(map[string]validation.PluginFactory)

	// signingIdentityFetcher 是一个 Endorsement3.SigningIdentityFetcher 接口，根据提案获取签名身份的接口, 用于获取签名身份
	signingIdentityFetcher := (endorsement3.SigningIdentityFetcher)(endorserSupport)

	// channelStateRetriever 是一个 Endorser.ChannelStateRetriever 接口，检索通道状态的接口, 用于检索通道状态
	channelStateRetriever := endorser.ChannelStateRetriever(endorserSupport)

	// pluginMapper 是一个基于映射的 PluginMapper，用于将插件映射到其名称
	pluginMapper := endorser.MapBasedPluginMapper(endorsementPluginsByName)

	// pluginEndorser 是一个使用插件的 Endorser 实例，用于背书交易, 插件背书者操作所需的支持接口的聚合体
	pluginEndorser := endorser.NewPluginEndorser(&endorser.PluginSupport{
		ChannelStateRetriever:   channelStateRetriever,  // 信道状态检索器 用于检索通道状态
		TransientStoreRetriever: peerInstance,           // 临时存储检索器 用于检索瞬态存储
		PluginMapper:            pluginMapper,           // 插件映射器 用于管理插件
		SigningIdentityFetcher:  signingIdentityFetcher, // 签名身份获取程序 用于获取签名身份
	})

	// 将 pluginEndorser 分配给 endorserSupport.PluginEndorser，以便在后续的操作中使用
	endorserSupport.PluginEndorser = pluginEndorser

	// channelFetcher 是一个 endorserChannelAdapter 结构体，用于获取通道信息
	channelFetcher := endorserChannelAdapter{
		peer: peerInstance, // peer节点
	}

	// serverEndorser 是一个 Endorser 实例，提供 Endorser 背书服务的 ProcessProposal 方法, 用于处理背书请求
	serverEndorser := &endorser.Endorser{
		PrivateDataDistributor: gossipService,                        // 私人数据分配器 用于分发私有数据
		ChannelFetcher:         channelFetcher,                       // 信道提取器 用于获取通道信息
		LocalMSP:               localMSP,                             // 本地MSP 是本地 MSP 的身份反序列化器
		Support:                endorserSupport,                      // 支持 是插件支持接口的实例
		Metrics:                endorser.NewMetrics(metricsProvider), // 指标 是指标对象，用于记录和报告指标数据
	}

	// 部署系统链码
	for _, cc := range []scc.SelfDescribingSysCC{lsccInst, csccInst, qsccInst, lifecycleSCC} {
		// 判断是否启用系统链码
		if enabled, ok := chaincodeConfig.SCCAllowlist[cc.Name()]; !ok || !enabled {
			logger.Infof("未部署链码 %s，因为它未启用", cc.Name())
			continue
		}
		// 是系统链码的钩子函数，用于将系统链码注册到 Fabric 中。
		scc.DeploySysCC(cc, chaincodeSupport)
	}

	logger.Infof("已部署系统链码")

	// 注册 lifecycleemetadatamanager 以从旧版chaincode获取更新；
	// lifecycleMetadataManager会将这些更新与新生命周期中的更新进行聚合，并同时提供
	// 这预计会随着FAB-15061而消失
	legacyMetadataManager.AddListener(metadataManager)

	// 将gossip注册为 lifecyclestemetadatamanager 更新的侦听器
	// HandleMetadataUpdateFunc 在链码生命周期发生变化时触发。
	metadataManager.AddListener(lifecycle.HandleMetadataUpdateFunc(func(channel string, chaincodes ccdef.MetadataSet) {
		// 更新对等方发布给通道中其他对等方的链码
		gossipService.UpdateChaincodes(chaincodes.AsChaincodes(), gossipcommon.ChannelID(channel))
	}))

	// Initialize 设置了节点从持久化存储中获取的任何通道。在启动时，当账本和 gossip 准备好时，应调用此函数。
	peerInstance.Initialize(
		// 初始化通道函数
		func(cid string) {
			// 初始化此通道的元数据。
			// 此调用将预填充此通道的链码信息，但不会对其侦听器触发任何更新
			lifecycleCache.InitializeMetadata(cid)

			// 初始化此通道的legacyMetadataManager。
			// 此调用将预填充此通道的遗留生命周期中的链码信息；
			// 它还将触发侦听器，该侦听器将级联到metadataManager，并最终级联到gossip以预填充数据结构。
			// 这预计会随着FAB-15061而消失
			sub, err := legacyMetadataManager.NewChannelSubscription(cid, cclifecycle.QueryCreatorFunc(func() (cclifecycle.Query, error) {
				return peerInstance.GetLedger(cid).NewQueryExecutor()
			}))
			if err != nil {
				logger.Panicf("订阅链码生命周期更新失败")
			}

			// register this channel's legacyMetadataManager (sub) to get ledger updates
			// this is expected to disappear with FAB-15061
			cceventmgmt.GetMgr().Register(cid, sub)
		},
		peerServer, // GRPC 服务器
		plugin.MapBasedMapper(validationPluginsByName), // 表示插件映射器
		lifecycleValidatorCommitter,                    // 表示已部署链码信息提供程序
		lsccInst,                                       // 表示旧的生命周期验证
		lifecycleValidatorCommitter,                    // 表示新的生命周期验证
		coreConfig.ValidatorPoolSize,                   // 表示工作线程数
	)

	var discoveryService *discovery.Service
	if coreConfig.DiscoveryEnabled {
		// 创建一个 Discovery Service 实例。
		discoveryService = createDiscoveryService(
			coreConfig,   // 表示 Peer 的核心配置
			peerInstance, // 表示 Peer 实例
			peerServer,   // 表示 GRPC 服务器
			policyMgr,    // 表示策略管理器获取器
			lifecycle.NewMetadataProvider( // 表示生命周期元数据提供器
				lifecycleCache,        // 链码信息提供者
				legacyMetadataManager, // 旧版元数据提供者
				peerInstance,          // 通道策略引用提供者
			),
			gossipService, // 表示 Gossip 服务
		)
		logger.Info("发现服务已激活")
		// 注册发现服务器
		discprotos.RegisterDiscoveryServer(peerServer.Server(), discoveryService)
	}

	// 网关
	if coreConfig.GatewayOptions.Enabled {
		if coreConfig.DiscoveryEnabled {
			logger.Info("peer节点启动网关")

			// 创建一个嵌入式的 Gateway 实例。
			gatewayServer := gateway.CreateServer(
				serverEndorser,            // 表示本地背书服务器
				discoveryService,          // 表示发现服务
				peerInstance,              // 表示 Peer 实例
				&serverConfig.SecOpts,     // 表示安全选项
				aclProvider,               // 表示 ACL 检查器
				coreConfig.LocalMSPID,     // 表示本地 MSP ID
				coreConfig.GatewayOptions, // 表示配置选项
				builtinSCCs,               // 表示系统链码
			)
			// 注册网关服务器
			gatewayprotos.RegisterGatewayServer(peerServer.Server(), gatewayServer)
		} else {
			logger.Warning("嵌入式的网关必须启用发现服务")
		}
	}

	logger.Infof("启动peer节点 with ID=[%s], network ID=[%s], address=[%s]", coreConfig.PeerID, coreConfig.NetworkID, coreConfig.PeerAddress)

	// 在开始go例程之前获取配置，以避免在测试中错误
	// 确定是否在对等端中启用了go ppprof端点。
	profileEnabled := coreConfig.ProfileEnabled
	// 是ppprof服务器应接受连接的地址。
	profileListenAddress := coreConfig.ProfileListenAddress

	// 启动grpc服务器。在goroutine中完成，因此我们可以根据需要部署genesis块。
	serve := make(chan error)

	// 开始分析http端点 (如果已启用)
	if profileEnabled {
		go func() {
			logger.Infof("启动分析服务器 listenAddress = %s", profileListenAddress)
			if profileErr := http.ListenAndServe(profileListenAddress, nil); profileErr != nil {
				logger.Errorf("启动分析服务器时出错: %s", profileErr)
			}
		}()
	}

	// 处理操作系统信号，并执行相应的操作。
	// 调用 addPlatformSignals 函数，将操作系统信号与处理函数进行映射
	handleSignals(addPlatformSignals(map[os.Signal]func(){
		// syscall.SIGINT：表示中断信号（通常由 Ctrl+C 触发），对应的处理函数会执行 containerRouter.Shutdown(5 * time.Second) 来关闭容器路由器，
		//并向 serve 通道发送一个 nil 值。
		syscall.SIGINT: func() { containerRouter.Shutdown(5 * time.Second); serve <- nil },
		// syscall.SIGTERM：表示终止信号（通常由操作系统发送），对应的处理函数会执行 containerRouter.Shutdown(5 * time.Second) 来关闭容器路由器，
		// 并向 serve 通道发送一个 nil 值。
		syscall.SIGTERM: func() { containerRouter.Shutdown(5 * time.Second); serve <- nil },
	}))

	logger.Infof("已启动peer节点 with ID=[%s], network ID=[%s], address=[%s]", coreConfig.PeerID, coreConfig.NetworkID, coreConfig.PeerAddress)

	// 获取分类帐id的列表，并加载这些分类账本id的预正确文件
	ledgerIDs, err := peerInstance.LedgerMgr.GetLedgerIDs()
	if err != nil {
		return errors.WithMessage(err, "无法获取分类账本id")
	}

	// 检查对等分类账本是否已重置
	rootFSPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "ledgersData")
	// 返回指定账本的最后记录的块高度
	preResetHeights, err := kvledger.LoadPreResetHeight(rootFSPath, ledgerIDs)
	if err != nil {
		return fmt.Errorf("加载最后记录的块高度时出错: %s", err)
	}

	for cid, height := range preResetHeights {
		logger.Infof("分类账本重建: 通道 [%s]: 最后记录的块高度: [%d]", cid, height)
	}

	if len(preResetHeights) > 0 {
		logger.Info("分类账本重建: 进入循环以检查当前分类帐高度是否超过预集分类帐高度。背书请求处理将被禁用.")
		resetFilter := &reset{
			reject: true,
		}
		authFilters = append(authFilters, resetFilter)
		go resetLoop(resetFilter, preResetHeights, ledgerIDs, peerInstance.GetLedger, 10*time.Second)
	}

	// 将给定的认证过滤器按照提供的顺序进行链式连接。
	auth := authHandler.ChainFilters(serverEndorser, authFilters...)

	logger.Infof("注册背书服务器")
	pb.RegisterEndorserServer(peerServer.Server(), auth)

	logger.Infof("注册快照服务器")
	snapshotSvc := &snapshotgrpc.SnapshotService{LedgerGetter: peerInstance, ACLProvider: aclProvider}
	pb.RegisterSnapshotServer(peerServer.Server(), snapshotSvc)

	go func() {
		var grpcErr error
		if grpcErr = peerServer.Start(); grpcErr != nil {
			grpcErr = fmt.Errorf("grpc 服务器已退出，出现错误: %s", grpcErr)
		}
		serve <- grpcErr
	}()

	logger.Infof("peer节点启动完成...")
	// 阻止，直到grpc服务器退出
	return <-serve
}

// handleSignals 处理信号的函数。
// 输入参数：
//   - handlers：map[os.Signal]func()，表示信号和对应的处理函数的映射
//
// 返回值：
//   - 无
func handleSignals(handlers map[os.Signal]func()) {
	// 创建一个信号切片
	var signals []os.Signal
	for sig := range handlers {
		signals = append(signals, sig)
	}

	// 创建一个信号通道，并将要监听的信号添加到通道中
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signals...)

	// 启动一个 goroutine 来处理接收到的信号
	go func() {
		for sig := range signalChan {
			logger.Infof("接收信号: %d (%s)", sig, sig)
			handlers[sig]()
		}
	}()
}

func localPolicy(policyObject proto.Message) policies.Policy {
	localMSP := mgmt.GetLocalMSP(factory.GetDefault())
	pp := cauthdsl.NewPolicyProvider(localMSP)
	policy, _, err := pp.NewPolicy(protoutil.MarshalOrPanic(policyObject))
	if err != nil {
		logger.Panicf("Failed creating local policy: +%v", err)
	}
	return policy
}

// createSelfSignedData 创建自签名数据, 用一个空消息自签名一次。
// 参数：
//   - sID: msp.SigningIdentity 类型，表示签名身份。
//
// 返回值：
//   - protoutil.SignedData: 表示自签名数据。
func createSelfSignedData(sID msp.SigningIdentity) protoutil.SignedData {
	// 创建一个消息
	msg := make([]byte, 32)

	// 对空消息进行自签名
	sig, err := sID.Sign(msg)
	if err != nil {
		logger.Panicf("创建自签名数据失败失败，因为消息签名失败: %v", err)
	}

	// 序列化对等方身份, 序列化证书
	peerIdentity, err := sID.Serialize()
	if err != nil {
		logger.Panicf("创建自签名数据失败，因为无法序列化peer标识: %v", err)
	}

	return protoutil.SignedData{
		Data:      msg,          // 签名的数据
		Signature: sig,          // 签名值
		Identity:  peerIdentity, // 序列化证书
	}
}

// createDiscoveryService 创建一个 Discovery Service 实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - coreConfig：*peer.Config，表示 Peer 的核心配置
//   - peerInstance：*peer.Peer，表示 Peer 实例
//   - peerServer：*comm.GRPCServer，表示 GRPC 服务器
//   - polMgr：policies.ChannelPolicyManagerGetter，表示策略管理器获取器
//   - metadataProvider：*lifecycle.MetadataProvider，表示生命周期元数据提供器
//   - gossipService：*gossipservice.GossipService，表示 Gossip 服务
//
// 返回值：
//   - *discovery.Service：表示 Discovery Service 实例
func createDiscoveryService(
	coreConfig *peer.Config,
	peerInstance *peer.Peer,
	peerServer *comm.GRPCServer,
	polMgr policies.ChannelPolicyManagerGetter,
	metadataProvider *lifecycle.MetadataProvider,
	gossipService *gossipservice.GossipService,
) *discovery.Service {
	// 获取本地 MSP ID
	mspID := coreConfig.LocalMSPID

	// 根据配置决定本地访问策略
	localAccessPolicy := localPolicy(policydsl.SignedByAnyAdmin([]string{mspID}))
	if coreConfig.DiscoveryOrgMembersAllowed {
		localAccessPolicy = localPolicy(policydsl.SignedByAnyMember([]string{mspID}))
	}

	// 创建通道验证器
	channelVerifier := discacl.NewChannelVerifier(policies.ChannelApplicationWriters, polMgr)

	// 创建 ACL（访问控制列表）
	acl := discacl.NewDiscoverySupport(channelVerifier, localAccessPolicy, discacl.ChannelConfigGetterFunc(peerInstance.GetStableChannelConfig))

	// 创建 Gossip 支持
	gSup := gossip.NewDiscoverySupport(gossipService)

	// 创建 Chaincode 支持
	ccSup := ccsupport.NewDiscoverySupport(metadataProvider)

	// 创建背书分析器
	ea := endorsement.NewEndorsementAnalyzer(gSup, ccSup, acl, metadataProvider)

	// 创建配置支持
	confSup := config.NewDiscoverySupport(config.CurrentConfigGetterFunc(func(channelID string) *cb.Config {
		channel := peerInstance.Channel(channelID)
		if channel == nil {
			return nil
		}
		config, err := peer.RetrievePersistedChannelConfig(channel.Ledger())
		if err != nil {
			logger.Errorw("获取通道配置失败", "error", err)
			return nil
		}
		return config
	}))
	support := discsupport.NewDiscoverySupport(acl, gSup, ea, confSup, acl)

	// 创建一个新的 Discovery Service 实例。
	return discovery.NewService(discovery.Config{
		TLS:                          peerServer.TLSEnabled(),                          // 是否启用 TLS
		AuthCacheEnabled:             coreConfig.DiscoveryAuthCacheEnabled,             // 是否启用身份验证缓存
		AuthCacheMaxSize:             coreConfig.DiscoveryAuthCacheMaxSize,             // 身份验证缓存的最大大小
		AuthCachePurgeRetentionRatio: coreConfig.DiscoveryAuthCachePurgeRetentionRatio, // 身份验证缓存清理保留比例
	}, support)
}

// 使用 peer.chaincodeListenAddress 创建CC侦听器 (如果未设置，请使用peer.peerAddress)
func createChaincodeServer(coreConfig *peer.Config, ca tlsgen.CA, peerHostname string) (srv *comm.GRPCServer, ccEndpoint string, err error) {
	// 在可能设置 chaincodeListenAddress 之前，首先计算链码节点
	ccEndpoint, err = computeChaincodeEndpoint(coreConfig.ChaincodeAddress, coreConfig.ChaincodeListenAddress, peerHostname)
	if err != nil {
		if chaincode.IsDevMode() {
			// 如果dev模式有任何错误，我们使用0.0.0.0:7052
			ccEndpoint = fmt.Sprintf("%s:%d", "0.0.0.0", defaultChaincodePort)
			logger.Warningf("由于computeChaincodeEndpoint中的错误，使用 %s 作为链码端点: %s", ccEndpoint, err)
		} else {
			// 对于非dev模式，我们必须返回错误
			logger.Errorf("计算链码端点时出错: %s", err)
			return nil, "", err
		}
	}

	host, _, err := net.SplitHostPort(ccEndpoint)
	if err != nil {
		logger.Panic("链码服务host", ccEndpoint, "不是有效的 hostname:", err)
	}

	cclistenAddress := coreConfig.ChaincodeListenAddress
	if cclistenAddress == "" {
		cclistenAddress = fmt.Sprintf("%s:%d", peerHostname, defaultChaincodePort)
		logger.Warningf("%s 未设置，使用 %s", chaincodeListenAddrKey, cclistenAddress)
		coreConfig.ChaincodeListenAddress = cclistenAddress
	}

	// 对等方的gRPC服务器配置
	config, err := peer.GetServerConfig()
	if err != nil {
		logger.Errorf("获取服务器配置时出错: %s", err)
		return nil, "", err
	}

	// 设置服务器的log记录器
	config.Logger = flogging.MustGetLogger("core.comm").With("server", "ChaincodeServer")

	// 覆盖TLS配置 (如果TLS适用)
	if config.SecOpts.UseTLS {
		// 使用与计算出的链码终端节点匹配的SAN创建自签名TLS证书
		certKeyPair, err := ca.NewServerCertKeyPair(host)
		if err != nil {
			logger.Panicf("为chaincode服务生成TLS证书失败: +%v", err)
		}
		config.SecOpts = comm.SecureOptions{
			UseTLS:            true,                     // 是否使用TLS进行通信
			RequireClientCert: true,                     // 是否要求TLS客户端提供证书进行身份验证
			ClientRootCAs:     [][]byte{ca.CertBytes()}, // 仅信任我们自己签名的客户端证书, 用于服务器验证客户端证书的一组PEM编码的X509证书
			Certificate:       certKeyPair.Cert,         // 使用我们自己的自签名TLS证书和密钥
			Key:               certKeyPair.Key,          // 使用我们自己的自签名TLS证书和密钥
			ServerRootCAs:     nil,                      // 指定服务器根CAs没有意义，因为此TLS配置仅用于gRPC服务器而不是客户端
		}
	}

	// Chaincode keepalive选项-静态现在
	chaincodeKeepaliveOptions := comm.KeepaliveOptions{
		ServerInterval:    time.Duration(2) * time.Hour,    // 2h - 在该持续时间之后，如果服务器没有看到来自客户端的任何活动，它将对客户端执行pings以查看其是否处于活动状态
		ServerTimeout:     time.Duration(20) * time.Second, // 20s - 服务器在关闭连接之前发送ping之后等待客户端响应的持续时间
		ServerMinInterval: time.Duration(1) * time.Minute,  // 1m - 客户端更频繁地发送pings，则服务器将断开它们的连接
	}
	config.KaOpts = chaincodeKeepaliveOptions

	// 启用服务器的gRPC健康检查协议。
	config.HealthCheckEnabled = true

	// 创建一个给定监听地址的GRPCServer的新实现
	srv, err = comm.NewGRPCServer(cclistenAddress, config)
	if err != nil {
		logger.Errorf("创建链码GRPC服务器时出错: %s", err)
		return nil, "", err
	}

	return srv, ccEndpoint, nil
}

// computeChaincodeEndpoint 将利用链码地址chaincode address, chaincode listen链码监听地址 (这两个来自viper) 和对等地址来计算链码端点。
// 可能有以下计算链码端点的情况:
// Case A: 如果设置了 chaincodeAddress ，如果没有，请使用它 "0.0.0.0" (or "::")
// Case B: 否则，如果设置了 chaincodeListenAddress 而不是 "0.0.0.0" or ("::"), 使用它
// Case C: 否则使用对等地址，如果不是 "0.0.0.0" (or "::")
// Case D: 其他返回错误
func computeChaincodeEndpoint(chaincodeAddress string, chaincodeListenAddress string, peerHostname string) (ccEndpoint string, err error) {
	logger.Infof("使用 peerHostname 赋值 computeChaincodeEndpoint : %s", peerHostname)
	// Case A: 设置了 chaincodeAddrKey
	if chaincodeAddress != "" {
		host, _, err := net.SplitHostPort(chaincodeAddress)
		if err != nil {
			logger.Errorf("无法拆分配置 peer.chaincodeAddress: %s", err)
			return "", err
		}
		ccIP := net.ParseIP(host)
		if ccIP != nil && ccIP.IsUnspecified() {
			logger.Errorf("peer.chaincodeAddress 不能是 %s", ccIP)
			return "", errors.New("链码连接的端点 peer.chaincodeAddress 无效")
		}
		logger.Infof("使用 cc端点: %s", chaincodeAddress)
		return chaincodeAddress, nil
	}

	// Case B: chaincodeListenAddress 已设置
	if chaincodeListenAddress != "" {
		ccEndpoint = chaincodeListenAddress
		host, port, err := net.SplitHostPort(ccEndpoint)
		if err != nil {
			logger.Errorf("chaincodeListenAddress为nil, 且无法拆分配置 peer.chaincodeListenAddress: %s", err)
			return "", err
		}

		ccListenerIP := net.ParseIP(host)
		// 忽略其他值，如多播地址等。.. 作为服务器
		// 反正不会用这个地址启动
		if ccListenerIP != nil && ccListenerIP.IsUnspecified() {
			// Case C: 如果 “0.0.0.0” 或 “::”，我们必须使用对等地址与监听端口
			peerIP := net.ParseIP(peerHostname)
			if peerIP != nil && peerIP.IsUnspecified() {
				// Case D: 我们所拥有的只是 “0.0.0.0” 或 “::”，链码无法连接到
				logger.Error("chaincodeListenAddress为nil，而  peer.chaincodeListenAddress  和 peer.address 均为0.0.0.0")
				return "", errors.New("链码连接的端点 peer.chaincodeListenAddress 无效")
			}
			ccEndpoint = fmt.Sprintf("%s:%s", peerHostname, port)
		}
		logger.Infof("使用 cc端点: %s", ccEndpoint)
		return ccEndpoint, nil
	}

	// Case C: 未设置chaincodeListenAddrKey，请使用对等地址address
	peerIP := net.ParseIP(peerHostname)
	if peerIP != nil && peerIP.IsUnspecified() {
		// Case D: 我们所拥有的只是 “0.0.0.0” 或 “::”，链码无法连接到
		logger.Errorf("链码无法连接 peer.address 为0.0.0.0, peerIP为 %s", peerIP)
		return "", errors.New("链码连接的端点无效")
	}

	// 使用 peerAddress:defaultChaincodePort
	ccEndpoint = fmt.Sprintf("%s:%d", peerHostname, defaultChaincodePort)

	logger.Infof("Exit with ccEndpoint: %s", ccEndpoint)
	return ccEndpoint, nil
}

// createDockerClient 创建一个 Docker 客户端
// 输入参数：
//   - coreConfig：peer.Config 结构体的指针，表示 Peer 节点的配置信息
//
// 返回值：
//   - *docker.Client：docker.Client 结构体的指针，表示 Docker 客户端
//   - error：如果创建 Docker 客户端时发生错误，则返回相应的错误信息
func createDockerClient(coreConfig *peer.Config) (*docker.Client, error) {
	if coreConfig.VMDockerTLSEnabled {
		// 使用 TLS 配置创建一个启用 TLS 的 Docker 客户端
		return docker.NewTLSClient(coreConfig.VMEndpoint, coreConfig.DockerCert, coreConfig.DockerKey, coreConfig.DockerCA)
	}
	// 创建一个不启用 TLS 的 Docker 客户端
	return docker.NewClient(coreConfig.VMEndpoint)
}

// secureDialOpts 是 gossip 服务的安全拨号选项的回拨功能
func secureDialOpts(credSupport *comm.CredentialSupport) func() []grpc.DialOption {
	return func() []grpc.DialOption {
		var dialOpts []grpc.DialOption
		// 设置最大发送
		maxRecvMsgSize := comm.DefaultMaxRecvMsgSize
		if viper.IsSet("peer.maxRecvMsgSize") {
			maxRecvMsgSize = int(viper.GetInt32("peer.maxRecvMsgSize"))
		}
		maxSendMsgSize := comm.DefaultMaxSendMsgSize
		if viper.IsSet("peer.maxSendMsgSize") {
			maxSendMsgSize = int(viper.GetInt32("peer.maxSendMsgSize"))
		}
		dialOpts = append(
			dialOpts,
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize), grpc.MaxCallSendMsgSize(maxSendMsgSize)),
		)
		// 设置keepalive选项
		kaOpts := comm.DefaultKeepaliveOptions
		if viper.IsSet("peer.keepalive.client.interval") {
			kaOpts.ClientInterval = viper.GetDuration("peer.keepalive.client.interval")
		}
		if viper.IsSet("peer.keepalive.client.timeout") {
			kaOpts.ClientTimeout = viper.GetDuration("peer.keepalive.client.timeout")
		}
		dialOpts = append(dialOpts, kaOpts.ClientKeepaliveOptions()...)

		if viper.GetBool("peer.tls.enabled") {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(credSupport.GetPeerCredentials()))
		} else {
			dialOpts = append(dialOpts, grpc.WithInsecure())
		}
		return dialOpts
	}
}

// initGossipService 通过以下步骤初始化 gossip 服务：
// 1. 如果配置了 TLS，则启用 TLS；
// 2. 初始化消息加密服务；
// 3. 初始化安全顾问；
// 4. 初始化与 gossip 相关的结构体。
//
// 输入参数：
//   - policyMgr：通道策略管理器获取器。
//   - metricsProvider：度量提供器。
//   - peerServer：GRPC 服务器实例。
//   - signer：签名身份。
//   - credSupport：凭证支持。
//   - peerAddress：对等节点地址。
//   - deliverServiceConfig：提交服务配置。
//   - privdataConfig：私有数据配置。
//
// 返回值：
//   - *gossipservice.GossipService：初始化的 GossipService 实例。
//   - error：如果初始化过程中出现错误，则返回相应的错误信息。
func initGossipService(
	policyMgr policies.ChannelPolicyManagerGetter,
	metricsProvider metrics.Provider,
	peerServer *comm.GRPCServer,
	signer msp.SigningIdentity,
	credSupport *comm.CredentialSupport,
	peerAddress string,
	deliverServiceConfig *deliverservice.DeliverServiceConfig,
	privdataConfig *gossipprivdata.PrivdataConfig,
) (*gossipservice.GossipService, error) {
	var certs *gossipcommon.TLSCertificates
	if peerServer.TLSEnabled() {
		// 返回服务端tls证书
		serverCert := peerServer.ServerCertificate()
		// 返回用于gRPC客户端连接的TLS证书。
		clientCert, err := peer.GetClientCertificate()
		if err != nil {
			return nil, errors.Wrap(err, "获取客户端证书失败")
		}
		certs = &gossipcommon.TLSCertificates{}
		// 存储服务端tls证书、客户端TLS证书
		certs.TLSServerCert.Store(&serverCert)
		certs.TLSClientCert.Store(&clientCert)
	}

	// 加载msp
	localMSP := mgmt.GetLocalMSP(factory.GetDefault())
	// 新的反序列化程序管理器
	deserManager := peergossip.NewDeserializersManager(localMSP)
	// 是gossip组件与对等方的密码层之间的契约，用于gossip组件验证，
	// 并对远程对等体及其发送的数据进行身份验证，并验证从订购服务接收到的块。
	messageCryptoService := peergossip.NewMCS(
		policyMgr,            // 通过 Manager 方法获取给定通道的策略管理器的通道策略管理器获取器。
		signer,               // 身份签名者序列化器的实例。
		deserManager,         // 身份反序列化器管理器。
		factory.GetDefault(), // 哈希器。
	)
	// 新的安全顾问, 并且加入反序列化器, 该反序列化器在 gossip.OrgByPeerIdentity() 获取组织时使用
	secAdv := peergossip.NewSecurityAdvisor(deserManager)
	bootstrap := viper.GetStringSlice("peer.gossip.bootstrap")

	// gossip配置加载, 加载peer节点的远程调用协议
	serviceConfig := gossipservice.GlobalConfig()
	if serviceConfig.Endpoint != "" {
		peerAddress = serviceConfig.Endpoint
	}

	// 再次加载更多gossip配置
	gossipConfig, err := gossipgossip.GlobalConfig(peerAddress, certs, bootstrap...)
	if err != nil {
		return nil, errors.Wrap(err, "获取 peer.gossip 配置失败")
	}

	//  身份签名者序列化器的实例，用于对身份进行签名和序列化。
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

	// 创建gossip服务。创建一个附加到 gRPC 服务器的 gossip 实例。启动心跳检测、检查存活、重新连接等协程。
	return gossipservice.New(
		signer, // 身份签名者序列化器的实例，用于对身份进行签名和序列化。
		gossipmetrics.NewGossipMetrics(metricsProvider), // GossipMetrics 实例，用于度量 gossip 相关的指标。
		peerAddress,                 // gossip 服务的端点地址
		peerServer.Server(),         // GRPC 服务器实例。
		messageCryptoService,        // MessageCryptoService 实例，用于消息加密。
		secAdv,                      // SecurityAdvisor 实例，用于提供安全建议。
		secureDialOpts(credSupport), // PeerSecureDialOpts 实例，用于安全拨号选项。
		credSupport,                 // CredentialSupport 实例，用于凭证支持。
		gossipConfig,                //gossip 配置。
		serviceConfig,               // ServiceConfig 实例，用于配置 gossip 服务。
		privdataConfig,              // PrivdataConfig 实例，用于配置私有数据。
		deliverServiceConfig,        // DeliverServiceConfig 实例，用于配置 提交 服务。
	)
}

// newOperationsSystem 函数创建一个新的operations.System实例。
//
// 参数：
//   - coreConfig: *peer.Config，表示核心配置
//
// 返回值：
//   - *operations.System: operations.System结构体指针，表示operations.System实例
func newOperationsSystem(coreConfig *peer.Config) *operations.System {
	return operations.NewSystem(operations.Options{
		Options: fabhttp.Options{
			Logger:        flogging.MustGetLogger("peer.operations"), // 创建一个名为"peer.operations"的日志记录器
			ListenAddress: coreConfig.OperationsListenAddress,        // 设置监听地址为核心配置中的OperationsListenAddress字段值
			TLS: fabhttp.TLS{
				Enabled:            coreConfig.OperationsTLSEnabled,            // 设置TLS是否启用为核心配置中的OperationsTLSEnabled字段值
				CertFile:           coreConfig.OperationsTLSCertFile,           // 设置TLS证书文件路径为核心配置中的OperationsTLSCertFile字段值
				KeyFile:            coreConfig.OperationsTLSKeyFile,            // 设置TLS私钥文件路径为核心配置中的OperationsTLSKeyFile字段值
				ClientCertRequired: coreConfig.OperationsTLSClientAuthRequired, // 设置是否需要客户端证书验证为核心配置中的OperationsTLSClientAuthRequired字段值
				ClientCACertFiles:  coreConfig.OperationsTLSClientRootCAs,      // 设置客户端CA证书文件路径为核心配置中的OperationsTLSClientRootCAs字段值
			},
		},
		Metrics: operations.MetricsOptions{
			Provider: coreConfig.MetricsProvider, // 设置指标提供者为核心配置中的MetricsProvider字段值
			Statsd: &operations.Statsd{
				Network:       coreConfig.StatsdNetwork,       // 设置Statsd网络类型为核心配置中的StatsdNetwork字段值
				Address:       coreConfig.StatsdAaddress,      // 设置Statsd地址为核心配置中的StatsdAaddress字段值
				WriteInterval: coreConfig.StatsdWriteInterval, // 设置Statsd写入间隔为核心配置中的StatsdWriteInterval字段值
				Prefix:        coreConfig.StatsdPrefix,        // 设置Statsd前缀为核心配置中的StatsdPrefix字段值
			},
		},
		Version: metadata.Version, // 设置版本号为metadata包中的Version字段值
	})
}

func getDockerHostConfig() *docker.HostConfig {
	// 根据key获取配置
	dockerKey := func(key string) string { return "vm.docker.hostConfig." + key }
	// 根据key获取ine64配置
	getInt64 := func(key string) int64 { return int64(viper.GetInt(dockerKey(key))) }

	var logConfig docker.LogConfig
	err := viper.UnmarshalKey(dockerKey("LogConfig"), &logConfig)
	if err != nil {
		logger.Panicf("配置 vm.docker.hostConfig.LogConfig 无法解析Docker LogConfig: %s", err)
	}

	networkMode := viper.GetString(dockerKey("NetworkMode"))
	if networkMode == "" {
		networkMode = "host"
	}

	memorySwappiness := getInt64("MemorySwappiness")
	oomKillDisable := viper.GetBool(dockerKey("OomKillDisable"))

	// 包含与在上启动容器相关的容器选项给定的主机
	return &docker.HostConfig{
		CapAdd:  viper.GetStringSlice(dockerKey("CapAdd")),
		CapDrop: viper.GetStringSlice(dockerKey("CapDrop")),

		DNS:         viper.GetStringSlice(dockerKey("Dns")),
		DNSSearch:   viper.GetStringSlice(dockerKey("DnsSearch")),
		ExtraHosts:  viper.GetStringSlice(dockerKey("ExtraHosts")),
		NetworkMode: networkMode,
		IpcMode:     viper.GetString(dockerKey("IpcMode")),
		PidMode:     viper.GetString(dockerKey("PidMode")),
		UTSMode:     viper.GetString(dockerKey("UTSMode")),
		LogConfig:   logConfig,

		ReadonlyRootfs:   viper.GetBool(dockerKey("ReadonlyRootfs")),
		SecurityOpt:      viper.GetStringSlice(dockerKey("SecurityOpt")),
		CgroupParent:     viper.GetString(dockerKey("CgroupParent")),
		Memory:           getInt64("Memory"),
		MemorySwap:       getInt64("MemorySwap"),
		MemorySwappiness: &memorySwappiness,
		OOMKillDisable:   &oomKillDisable,
		CPUShares:        getInt64("CpuShares"),
		CPUSet:           viper.GetString(dockerKey("Cpuset")),
		CPUSetCPUs:       viper.GetString(dockerKey("CpusetCPUs")),
		CPUSetMEMs:       viper.GetString(dockerKey("CpusetMEMs")),
		CPUQuota:         getInt64("CpuQuota"),
		CPUPeriod:        getInt64("CpuPeriod"),
		BlkioWeight:      getInt64("BlkioWeight"),
	}
}

//go:generate counterfeiter -o mock/get_ledger.go -fake-name GetLedger . getLedger
//go:generate counterfeiter -o mock/peer_ledger.go -fake-name PeerLedger . peerLedger

type peerLedger interface {
	ledger.PeerLedger
}

type getLedger func(string) ledger.PeerLedger

func resetLoop(
	resetFilter *reset,
	preResetHeights map[string]uint64,
	ledgerIDs []string,
	pLedger getLedger,
	interval time.Duration,
) {
	ledgerDataPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "ledgersData")

	// periodically check to see if current ledger height(s) surpass prereset height(s)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		logger.Info("Ledger rebuild: Checking if current ledger heights surpass prereset ledger heights")
		logger.Debugf("Ledger rebuild: Number of ledgers still rebuilding before check: %d", len(preResetHeights))

		for cid, height := range preResetHeights {
			l := pLedger(cid)
			if l == nil {
				logger.Warningf("No ledger found for channel [%s]", cid)
				continue
			}

			bcInfo, err := l.GetBlockchainInfo()
			if err != nil {
				logger.Warningf("Ledger rebuild: could not retrieve info for channel [%s]: %s", cid, err.Error())
				continue
			}
			if bcInfo == nil {
				continue
			}

			logger.Debugf("Ledger rebuild: channel [%s]: currentHeight [%d] : preresetHeight [%d]", cid, bcInfo.GetHeight(), height)
			if bcInfo.GetHeight() >= height {
				delete(preResetHeights, cid)
			}
		}

		logger.Debugf("Ledger rebuild: Number of ledgers still rebuilding after check: %d", len(preResetHeights))
		if len(preResetHeights) == 0 {
			logger.Infof("Ledger rebuild: Complete, all ledgers surpass prereset heights. Endorsement request processing will be enabled.")

			err := kvledger.ClearPreResetHeight(ledgerDataPath, ledgerIDs)
			if err != nil {
				logger.Warningf("Ledger rebuild: could not clear off prerest files: error=%s", err)
			}
			resetFilter.setReject(false)
			return
		}
	}
}

// reset implements the auth.Filter interface.
type reset struct {
	lock   sync.RWMutex
	next   pb.EndorserServer
	reject bool
}

func (r *reset) setReject(reject bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.reject = reject
}

// Init initializes Reset with the next EndorserServer.
func (r *reset) Init(next pb.EndorserServer) {
	r.next = next
}

// ProcessProposal processes a signed proposal.
func (r *reset) ProcessProposal(ctx context.Context, signedProp *pb.SignedProposal) (*pb.ProposalResponse, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.reject {
		return nil, errors.New("endorse requests are blocked while ledgers are being rebuilt")
	}
	return r.next.ProcessProposal(ctx, signedProp)
}
