/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof" // This is essentially the main package for the orderer
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/healthz"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/fabhttp"
	"github.com/hyperledger/fabric/common/flogging"
	floggingmetrics "github.com/hyperledger/fabric/common/flogging/metrics"
	"github.com/hyperledger/fabric/common/grpclogging"
	"github.com/hyperledger/fabric/common/grpcmetrics"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/core/operations"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/orderer/common/bootstrap/file"
	"github.com/hyperledger/fabric/orderer/common/channelparticipation"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/metadata"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/common/onboarding"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/etcdraft"
	"github.com/hyperledger/fabric/orderer/consensus/kafka"
	"github.com/hyperledger/fabric/orderer/consensus/solo"
	"github.com/hyperledger/fabric/protoutil"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var logger = flogging.MustGetLogger("orderer.common.server")

// command line flags
var (
	//创建一个命令行应用程序对象
	app = kingpin.New("orderer", "Hyperledger Fabric orderer node")
	//设置对应的命令行操作
	_       = app.Command("start", "Start the orderer node").Default() // preserved for cli compatibility
	version = app.Command("version", "Show version information")

	clusterTypes = map[string]struct{}{"etcdraft": {}}
)

// Main orderer服务的执行入口
func Main() {
	fullCmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// 对比输入的命令行是否符合app中这是的命令行，是的话执行对应的逻辑
	if fullCmd == version.FullCommand() {
		fmt.Println(metadata.GetVersionInfo())
		return
	}

	// 读取orderer配置, orderer.yaml
	conf, err := localconfig.Load()
	if err != nil {
		logger.Error("failed to parse config: ", err)
		os.Exit(1)
	}
	initializeLogging()

	prettyPrintStruct(conf)

	//获取一个区块链加密服务对象，该对象提供了加密标准和算法的实现
	cryptoProvider := factory.GetDefault()

	//获取SigningIdentity类型的对象， SigningIdentity是Identity的扩展，包含了签名功能。
	signer, signErr := loadLocalMSP(conf).GetDefaultSigningIdentity()
	if signErr != nil {
		logger.Panicf("Failed to get local MSP identity: %s", signErr)
	}

	//创建和初始化了orderer服务的操作系统
	//负责处理和管理Orderer节点的各种操作和功能。它提供了一种机制来处理和记录与Orderer节点操作相关的日志、统计信息和性能指标，并提供了一些管理和监控Node的功能。
	opsSystem := newOperationsSystem(conf.Operations, conf.Metrics)
	if err = opsSystem.Start(); err != nil {
		logger.Panicf("failed to start operations subsystem: %s", err)
	}
	defer opsSystem.Stop()
	metricsProvider := opsSystem.Provider
	logObserver := floggingmetrics.NewObserver(metricsProvider)
	flogging.SetObserver(logObserver)

	//创建grpc的服务端，用于监听从grpc客户端请求的信息
	serverConfig := initializeServerConfig(conf, metricsProvider)
	grpcServer := initializeGrpcServer(conf, serverConfig)
	caMgr := &caManager{
		appRootCAsByChain:     make(map[string][][]byte),
		ordererRootCAsByChain: make(map[string][][]byte),
		clientRootCAs:         serverConfig.SecOpts.ClientRootCAs,
	}

	//创建和初始化orderer服务的账本工厂
	lf, err := createLedgerFactory(conf, metricsProvider)
	if err != nil {
		logger.Panicf("Failed to create ledger factory: %v", err)
	}

	var bootstrapBlock *cb.Block
	switch conf.General.BootstrapMethod {
	case "file":
		if len(lf.ChannelIDs()) > 0 {
			logger.Info("Not bootstrapping the system channel because of existing channels")
			break
		}

		//读取引导区块文件并返回区块对象
		bootstrapBlock = file.New(conf.General.BootstrapFile).GenesisBlock()
		if err := onboarding.ValidateBootstrapBlock(bootstrapBlock, cryptoProvider); err != nil {
			logger.Panicf("Failed validating bootstrap block: %v", err)
		}

		if bootstrapBlock.Header.Number > 0 {
			logger.Infof("Not bootstrapping the system channel because the bootstrap block number is %d (>0), replication is needed", bootstrapBlock.Header.Number)
			break
		}

		//用一个创世区块引导(即引导区块号= 0)
		//生成系统通道与一个创世块。
		logger.Info("Bootstrapping the system channel")
		initializeBootstrapChannel(bootstrapBlock, lf)
	case "none":
		bootstrapBlock = initSystemChannelWithJoinBlock(conf, cryptoProvider, lf)
	default:
		logger.Panicf("Unknown bootstrap method: %s", conf.General.BootstrapMethod)
	}

	// 获取集群引导区块，如果是通过集群配置，则需要该区块信息
	sysChanConfigBlock := extractSystemChannel(lf, cryptoProvider)
	clusterBootBlock := selectClusterBootBlock(bootstrapBlock, sysChanConfigBlock)

	// 根据返回的集群引导区块信息，获取集群类型，以及选择相应的处理逻辑
	var isClusterType bool
	if clusterBootBlock == nil {
		logger.Infof("Starting without a system channel")
		isClusterType = true
	} else {
		// 根据集群引导区块，获取系统链码ID
		sysChanID, err := protoutil.GetChannelIDFromBlock(clusterBootBlock)
		if err != nil {
			logger.Panicf("Failed getting channel ID from clusterBootBlock: %s", err)
		}

		// 获取共识类型
		consensusTypeName := consensusType(clusterBootBlock, cryptoProvider)
		logger.Infof("Starting with system channel: %s, consensus type: %s", sysChanID, consensusTypeName)
		// 根据获取到的共识类型，判断是否是集群类型
		_, isClusterType = clusterTypes[consensusTypeName]
	}

	// 如果是集群类型(isClusterType == ture)则需要对以下组件进行配置
	var repInitiator *onboarding.ReplicationInitiator
	clusterServerConfig := serverConfig
	clusterGRPCServer := grpcServer // by default, cluster shares the same grpc server
	var clusterClientConfig comm.ClientConfig
	var clusterDialer *cluster.PredicateDialer

	var reuseGrpcListener bool
	var serversToUpdate []*comm.GRPCServer

	if isClusterType {
		logger.Infof("Setting up cluster")
		// 初始化集群客户端的配置
		clusterClientConfig, reuseGrpcListener = initializeClusterClientConfig(conf)
		clusterDialer = &cluster.PredicateDialer{
			Config: clusterClientConfig,
		}

		// 如果不是重用grpc客户端，需要新创建
		if !reuseGrpcListener {
			clusterServerConfig, clusterGRPCServer = configureClusterListener(conf, serverConfig, ioutil.ReadFile)
		}

		//有一个单独的gRPC服务器集群，
		//需要更新其TLS CA证书池
		serversToUpdate = append(serversToUpdate, clusterGRPCServer)

		// 如果orderer有一个系统通道并且是集群类型的，那么它可能必须首先进行复制。
		if clusterBootBlock != nil {
			//当我们用一个编号为>0的clusterBootBlock进行引导时，执行复制。仅在集群中配置可以复制最近的配置块(例如>0)。
			//这将复制所有通道，如果clusterBootBlock号>系统通道高度(即在分类账中有一个缺口)。
			repInitiator = onboarding.NewReplicationInitiator(lf, clusterBootBlock, conf, clusterClientConfig.SecOpts, signer, cryptoProvider)
			repInitiator.ReplicateIfNeeded(clusterBootBlock)
			//当BootstrapMethod == "none"时，bootstrapBlock来自一个join-block。如果它存在，我们需要从filerepo中删除系统通道连接块。
			if conf.General.BootstrapMethod == "none" && bootstrapBlock != nil {
				// 当BootstrapMethod == "none"时，bootstrapBlock来自一个join-block。如果它存在，我们需要从filerepo中删除系统通道连接块。
				discardSystemChannelJoinBlock(conf, bootstrapBlock)
			}
		}
	}

	identityBytes, err := signer.Serialize()
	if err != nil {
		logger.Panicf("Failed serializing signing identity: %v", err)
	}

	expirationLogger := flogging.MustGetLogger("certmonitor")
	// 在其中一个证书到期前一周发出警告
	crypto.TrackExpiration(
		serverConfig.SecOpts.UseTLS,                       // 表示是否使用 TLS。
		serverConfig.SecOpts.Certificate,                  // 用户 tls 证书
		[][]byte{clusterClientConfig.SecOpts.Certificate}, // 节点 tls 证书。
		identityBytes, // 表示序列化的身份。
		expirationLogger.Infof,
		expirationLogger.Warnf, // 这可用于将来搭载度量事件
		time.Now(),
		time.AfterFunc)

	// 如果集群正在重用面向客户端的服务器，那么此时它已经被附加到serversToUpdate。
	if grpcServer.MutualTLSRequired() && !reuseGrpcListener {
		serversToUpdate = append(serversToUpdate, grpcServer)
	}

	tlsCallback := func(bundle *channelconfig.Bundle) {
		logger.Debug("Executing callback to update root CAs")
		caMgr.updateTrustedRoots(bundle, serversToUpdate...)
		if isClusterType {
			caMgr.updateClusterDialer(
				clusterDialer,
				clusterClientConfig.SecOpts.ServerRootCAs,
			)
		}
	}

	// 初始化多通道注册器，用于在网络中注册通道和对等节点
	manager := initializeMultichannelRegistrar(
		clusterBootBlock,
		repInitiator,
		clusterDialer,
		clusterServerConfig,
		clusterGRPCServer,
		conf,
		signer,
		metricsProvider,
		opsSystem,
		lf,
		cryptoProvider,
		tlsCallback,
	)

	// 创建一个AdminServer对象。AdminServer是一个用于管理和操作Fabric网络的API服务器。它提供了一组RESTful API，可以用于管理、查询和控制网络中的组织、通道、智能合约等。
	// 通过AdminServer 可以使用一些API接口操作和管理Fabric网络。
	adminServer := newAdminServer(conf.Admin)
	adminServer.RegisterHandler(
		channelparticipation.URLBaseV1,
		channelparticipation.NewHTTPHandler(conf.ChannelParticipation, manager),
		conf.Admin.TLS.Enabled,
	)
	if err = adminServer.Start(); err != nil {
		logger.Panicf("failed to start admin server: %s", err)
	}
	defer adminServer.Stop()

	//创建一个基于grpc实现的Atomic.Broadcast 服务，该服务作为orderer服务的核心，主要负责处理、传播交易、生成新区块
	//该服务提供了一组gRPC API，用于接收来自客户端的交易请求，并将交易打包进新的区块中。然后，它会使用共识算法对新区块进行确认，确保整个网络对该区块的接受达成一致。
	//最后，ab.AtomicBroadcastServer将新的区块广播给其他Orderer节点和Peer节点。
	mutualTLS := serverConfig.SecOpts.UseTLS && serverConfig.SecOpts.RequireClientCert
	server := NewServer(
		manager,
		metricsProvider,
		&conf.Debug,
		conf.General.Authentication.TimeWindow,
		mutualTLS,
		conf.General.Authentication.NoExpirationChecks,
	)

	logger.Infof("Starting %s", metadata.GetVersionInfo())
	handleSignals(addPlatformSignals(map[os.Signal]func(){
		syscall.SIGTERM: func() {
			grpcServer.Stop()
			if clusterGRPCServer != grpcServer {
				clusterGRPCServer.Stop()
			}
		},
	}))

	if !reuseGrpcListener && isClusterType {
		logger.Info("Starting cluster listener on", clusterGRPCServer.Address())
		go clusterGRPCServer.Start()
	}

	if conf.General.Profile.Enabled {
		go initializeProfilingService(conf)
	}
	ab.RegisterAtomicBroadcastServer(grpcServer.Server(), server)
	logger.Info("正在监听请求...")
	if err := grpcServer.Start(); err != nil {
		logger.Fatalf("Atomic Broadcast gRPC server has terminated while serving requests due to: %v", err)
	}
}

// 搜索是否存在系统通道的联接块，如果存在，并且是一个生成块，
// 用它初始化分类帐。如果找到连接块，则返回连接块。
func initSystemChannelWithJoinBlock(
	config *localconfig.TopLevel,
	cryptoProvider bccsp.BCCSP,
	lf blockledger.Factory,
) (bootstrapBlock *cb.Block) {
	if !config.ChannelParticipation.Enabled {
		return nil
	}

	joinBlockFileRepo, err := multichannel.InitJoinBlockFileRepo(config)
	if err != nil {
		logger.Panicf("Failed initializing join-block file repo: %v", err)
	}

	joinBlockFiles, err := joinBlockFileRepo.List()
	if err != nil {
		logger.Panicf("Failed listing join-block file repo: %v", err)
	}

	var systemChannelID string
	for _, fileName := range joinBlockFiles {
		channelName := joinBlockFileRepo.FileToBaseName(fileName)
		blockBytes, err := joinBlockFileRepo.Read(channelName)
		if err != nil {
			logger.Panicf("Failed reading join-block for channel '%s', error: %v", channelName, err)
		}
		block, err := protoutil.UnmarshalBlock(blockBytes)
		if err != nil {
			logger.Panicf("Failed unmarshalling join-block for channel '%s', error: %v", channelName, err)
		}
		if err = onboarding.ValidateBootstrapBlock(block, cryptoProvider); err == nil {
			bootstrapBlock = block
			systemChannelID = channelName
			break
		}
	}

	if bootstrapBlock == nil {
		logger.Debug("No join-block was found for the system channel")
		return nil
	}

	if bootstrapBlock.Header.Number == 0 {
		initializeBootstrapChannel(bootstrapBlock, lf)
	}

	logger.Infof("Join-block was found for the system channel: %s, number: %d", systemChannelID, bootstrapBlock.Header.Number)
	return bootstrapBlock
}

func discardSystemChannelJoinBlock(config *localconfig.TopLevel, bootstrapBlock *cb.Block) {
	if !config.ChannelParticipation.Enabled {
		return
	}

	systemChannelName, err := protoutil.GetChannelIDFromBlock(bootstrapBlock)
	if err != nil {
		logger.Panicf("Failed to extract system channel name from join-block: %s", err)
	}
	joinBlockFileRepo, err := multichannel.InitJoinBlockFileRepo(config)
	if err != nil {
		logger.Panicf("Failed initializing join-block file repo: %v", err)
	}
	err = joinBlockFileRepo.Remove(systemChannelName)
	if err != nil {
		logger.Panicf("Failed to remove join-block for system channel: %s", err)
	}
}

func reuseListener(conf *localconfig.TopLevel) bool {
	clusterConf := conf.General.Cluster
	//如果没有配置监听地址，也没有配置TLS证书，
	//这意味着我们使用节点的通用侦听器。
	if clusterConf.ListenPort == 0 && clusterConf.ServerCertificate == "" && clusterConf.ListenAddress == "" && clusterConf.ServerPrivateKey == "" {
		logger.Info("Cluster listener is not configured, defaulting to use the general listener on port", conf.General.ListenPort)

		if !conf.General.TLS.Enabled {
			logger.Panicf("TLS is required for running ordering nodes of cluster type.")
		}

		return true
	}

	// 否则，上述属性中的一个已定义，因此应定义所有4个属性。
	if clusterConf.ListenPort == 0 || clusterConf.ServerCertificate == "" || clusterConf.ListenAddress == "" || clusterConf.ServerPrivateKey == "" {
		logger.Panic("Options: General.Cluster.ListenPort, General.Cluster.ListenAddress, General.Cluster.ServerCertificate, General.Cluster.ServerPrivateKey, should be defined altogether.")
	}

	return false
}

// extractSystemChannel 循环遍历所有通道，并返回系统通道的最后一个配置块。如果未找到系统通道，则返回nil。
func extractSystemChannel(lf blockledger.Factory, bccsp bccsp.BCCSP) *cb.Block {
	for _, cID := range lf.ChannelIDs() {
		channelLedger, err := lf.GetOrCreate(cID)
		if err != nil {
			logger.Panicf("Failed getting channel %v's ledger: %v", cID, err)
		}
		if channelLedger.Height() == 0 {
			continue // Some channels may have an empty ledger and (possibly) a join-block, skip those
		}

		channelConfigBlock := multichannel.ConfigBlockOrPanic(channelLedger)

		err = onboarding.ValidateBootstrapBlock(channelConfigBlock, bccsp)
		if err == nil {
			logger.Infof("Found system channel config block, number: %d", channelConfigBlock.Header.Number)
			return channelConfigBlock
		}
	}
	return nil
}

// Select cluster boot block
// 从系统引导区块和系统通道配置区块中选择集群引导区块
// 集群引导区块是一个特殊的区块，它包含了关于集群配置和节点加入的初始信息。它用于引导整个集群的启动和初始化。
// 主要内容：
// 从系统通道配置区块中获取集群的初始配置信息，比如MSP身份等
// 从系统引导区块中获取适合当前集群的特定引导区块，并返回
func selectClusterBootBlock(bootstrapBlock, sysChanLastConfig *cb.Block) *cb.Block {
	if sysChanLastConfig == nil {
		logger.Debug("Selected bootstrap block, because system channel last config block is nil")
		return bootstrapBlock
	}

	if bootstrapBlock == nil {
		logger.Debug("Selected system channel last config block, because bootstrap block is nil")
		return sysChanLastConfig
	}

	if sysChanLastConfig.Header.Number > bootstrapBlock.Header.Number {
		logger.Infof("Cluster boot block is system channel last config block; Blocks Header.Number system-channel=%d, bootstrap=%d",
			sysChanLastConfig.Header.Number, bootstrapBlock.Header.Number)
		return sysChanLastConfig
	}

	logger.Infof("Cluster boot block is bootstrap (genesis) block; Blocks Header.Number system-channel=%d, bootstrap=%d",
		sysChanLastConfig.Header.Number, bootstrapBlock.Header.Number)
	return bootstrapBlock
}

func initializeLogging() {
	loggingSpec := os.Getenv("FABRIC_LOGGING_SPEC")
	loggingFormat := os.Getenv("FABRIC_LOGGING_FORMAT")
	flogging.Init(flogging.Config{
		Format:  loggingFormat,
		Writer:  os.Stderr,
		LogSpec: loggingSpec,
	})
}

// Start the profiling service if enabled.
func initializeProfilingService(conf *localconfig.TopLevel) {
	logger.Info("Starting Go pprof profiling service on:", conf.General.Profile.Address)
	// The ListenAndServe() call does not return unless an error occurs.
	logger.Panic("Go pprof service failed:", http.ListenAndServe(conf.General.Profile.Address, nil))
}

func handleSignals(handlers map[os.Signal]func()) {
	var signals []os.Signal
	for sig := range handlers {
		signals = append(signals, sig)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signals...)

	go func() {
		for sig := range signalChan {
			logger.Infof("Received signal: %d (%s)", sig, sig)
			handlers[sig]()
		}
	}()
}

type loadPEMFunc func(string) ([]byte, error)

// configureClusterListener returns a new ServerConfig and a new gRPC server (with its own TLS listener).
func configureClusterListener(conf *localconfig.TopLevel, generalConf comm.ServerConfig, loadPEM loadPEMFunc) (comm.ServerConfig, *comm.GRPCServer) {
	clusterConf := conf.General.Cluster

	// 这里的loadPEM为 ioutil.ReadFile函数 ，通过读取配置中的证书路径，返回证书信息
	cert, err := loadPEM(clusterConf.ServerCertificate)
	if err != nil {
		logger.Panicf("Failed to load cluster server certificate from '%s' (%s)", clusterConf.ServerCertificate, err)
	}

	key, err := loadPEM(clusterConf.ServerPrivateKey)
	if err != nil {
		logger.Panicf("Failed to load cluster server key from '%s' (%s)", clusterConf.ServerPrivateKey, err)
	}

	port := fmt.Sprintf("%d", clusterConf.ListenPort)
	bindAddr := net.JoinHostPort(clusterConf.ListenAddress, port)

	var clientRootCAs [][]byte
	for _, serverRoot := range conf.General.Cluster.RootCAs {
		rootCACert, err := loadPEM(serverRoot)
		if err != nil {
			logger.Panicf("Failed to load CA cert file '%s' (%s)", serverRoot, err)
		}
		clientRootCAs = append(clientRootCAs, rootCACert)
	}

	// 配置grpc服务信息
	serverConf := comm.ServerConfig{
		StreamInterceptors: generalConf.StreamInterceptors,
		UnaryInterceptors:  generalConf.UnaryInterceptors,
		ConnectionTimeout:  generalConf.ConnectionTimeout,
		ServerStatsHandler: generalConf.ServerStatsHandler,
		Logger:             generalConf.Logger,
		KaOpts:             generalConf.KaOpts,
		SecOpts: comm.SecureOptions{
			TimeShift:         conf.General.Cluster.TLSHandshakeTimeShift,
			CipherSuites:      comm.DefaultTLSCipherSuites,
			ClientRootCAs:     clientRootCAs,
			RequireClientCert: true,
			Certificate:       cert,
			UseTLS:            true,
			Key:               key,
		},
	}

	srv, err := comm.NewGRPCServer(bindAddr, serverConf)
	if err != nil {
		logger.Panicf("Failed creating gRPC server on %s:%d due to %v", clusterConf.ListenAddress, clusterConf.ListenPort, err)
	}

	return serverConf, srv
}

// 初始化集群客户端配置
// 在该函数中，解析和读取集群配置文件，配置集群客户端，确保在orderer服务启动的时候，能正常启动集群客户端，以便orderer节点之间进行通信和协调共识
func initializeClusterClientConfig(conf *localconfig.TopLevel) (comm.ClientConfig, bool) {
	cc := comm.ClientConfig{
		AsyncConnect:   true,
		KaOpts:         comm.DefaultKeepaliveOptions,
		DialTimeout:    conf.General.Cluster.DialTimeout,
		SecOpts:        comm.SecureOptions{},
		MaxRecvMsgSize: int(conf.General.MaxRecvMsgSize),
		MaxSendMsgSize: int(conf.General.MaxSendMsgSize),
	}

	// 重用已经存在的监听器
	reuseGrpcListener := reuseListener(conf)

	certFile := conf.General.Cluster.ClientCertificate
	keyFile := conf.General.Cluster.ClientPrivateKey
	if certFile == "" && keyFile == "" {
		if !reuseGrpcListener {
			return cc, reuseGrpcListener
		}
		certFile = conf.General.TLS.Certificate
		keyFile = conf.General.TLS.PrivateKey
	}

	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		logger.Fatalf("Failed to load client TLS certificate file '%s' (%s)", certFile, err)
	}

	keyBytes, err := ioutil.ReadFile(keyFile)
	if err != nil {
		logger.Fatalf("Failed to load client TLS key file '%s' (%s)", keyFile, err)
	}

	var serverRootCAs [][]byte
	for _, serverRoot := range conf.General.Cluster.RootCAs {
		rootCACert, err := ioutil.ReadFile(serverRoot)
		if err != nil {
			logger.Fatalf("Failed to load ServerRootCAs file '%s' (%s)", serverRoot, err)
		}
		serverRootCAs = append(serverRootCAs, rootCACert)
	}

	timeShift := conf.General.TLS.TLSHandshakeTimeShift
	if !reuseGrpcListener {
		timeShift = conf.General.Cluster.TLSHandshakeTimeShift
	}

	cc.SecOpts = comm.SecureOptions{
		TimeShift:         timeShift,
		RequireClientCert: true,
		CipherSuites:      comm.DefaultTLSCipherSuites,
		ServerRootCAs:     serverRootCAs,
		Certificate:       certBytes,
		Key:               keyBytes,
		UseTLS:            true,
	}

	return cc, reuseGrpcListener
}

func initializeServerConfig(conf *localconfig.TopLevel, metricsProvider metrics.Provider) comm.ServerConfig {
	// 安全服务器配置
	secureOpts := comm.SecureOptions{
		UseTLS:            conf.General.TLS.Enabled,
		RequireClientCert: conf.General.TLS.ClientAuthRequired,
		TimeShift:         conf.General.TLS.TLSHandshakeTimeShift,
	}
	// 检查配置中是否启用了TLS
	if secureOpts.UseTLS {
		msg := "TLS"
		// 从文件中加载加密材料
		serverCertificate, err := ioutil.ReadFile(conf.General.TLS.Certificate)
		if err != nil {
			logger.Fatalf("Failed to load server Certificate file '%s' (%s)",
				conf.General.TLS.Certificate, err)
		}
		serverKey, err := ioutil.ReadFile(conf.General.TLS.PrivateKey)
		if err != nil {
			logger.Fatalf("Failed to load PrivateKey file '%s' (%s)",
				conf.General.TLS.PrivateKey, err)
		}
		var serverRootCAs, clientRootCAs [][]byte
		for _, serverRoot := range conf.General.TLS.RootCAs {
			root, err := ioutil.ReadFile(serverRoot)
			if err != nil {
				logger.Fatalf("Failed to load ServerRootCAs file '%s' (%s)",
					err, serverRoot)
			}
			serverRootCAs = append(serverRootCAs, root)
		}
		if secureOpts.RequireClientCert {
			for _, clientRoot := range conf.General.TLS.ClientRootCAs {
				root, err := ioutil.ReadFile(clientRoot)
				if err != nil {
					logger.Fatalf("Failed to load ClientRootCAs file '%s' (%s)",
						err, clientRoot)
				}
				clientRootCAs = append(clientRootCAs, root)
			}
			msg = "mutual TLS"
		}
		secureOpts.Key = serverKey
		secureOpts.Certificate = serverCertificate
		secureOpts.ServerRootCAs = serverRootCAs
		secureOpts.ClientRootCAs = clientRootCAs
		logger.Infof("Starting orderer with %s enabled", msg)
	}
	kaOpts := comm.DefaultKeepaliveOptions
	// keepalive设置
	// ServerMinInterval必须大于0
	if conf.General.Keepalive.ServerMinInterval > time.Duration(0) {
		kaOpts.ServerMinInterval = conf.General.Keepalive.ServerMinInterval
	}
	kaOpts.ServerInterval = conf.General.Keepalive.ServerInterval
	kaOpts.ServerTimeout = conf.General.Keepalive.ServerTimeout

	commLogger := flogging.MustGetLogger("core.comm").With("server", "Orderer")

	if metricsProvider == nil {
		metricsProvider = &disabled.Provider{}
	}

	return comm.ServerConfig{
		SecOpts:            secureOpts,
		KaOpts:             kaOpts,
		Logger:             commLogger,
		ServerStatsHandler: comm.NewServerStatsHandler(metricsProvider),
		ConnectionTimeout:  conf.General.ConnectionTimeout,
		StreamInterceptors: []grpc.StreamServerInterceptor{
			grpcmetrics.StreamServerInterceptor(grpcmetrics.NewStreamMetrics(metricsProvider)),
			grpclogging.StreamServerInterceptor(flogging.MustGetLogger("comm.grpc.server").Zap()),
		},
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			grpcmetrics.UnaryServerInterceptor(grpcmetrics.NewUnaryMetrics(metricsProvider)),
			grpclogging.UnaryServerInterceptor(
				flogging.MustGetLogger("comm.grpc.server").Zap(),
				grpclogging.WithLeveler(grpclogging.LevelerFunc(grpcLeveler)),
			),
		},
		MaxRecvMsgSize: int(conf.General.MaxRecvMsgSize),
		MaxSendMsgSize: int(conf.General.MaxSendMsgSize),
	}
}

func grpcLeveler(ctx context.Context, fullMethod string) zapcore.Level {
	switch fullMethod {
	case "/orderer.Cluster/Step":
		return flogging.DisabledLevel
	default:
		return zapcore.InfoLevel
	}
}

func extractBootstrapBlock(conf *localconfig.TopLevel) *cb.Block {
	var bootstrapBlock *cb.Block

	//选择自启动机制
	switch conf.General.BootstrapMethod {
	case "file": // 目前，“file”是唯一支持的生成方法
		bootstrapBlock = file.New(conf.General.BootstrapFile).GenesisBlock()
	case "none": // 只需遵循配置值即可
		return nil
	default:
		logger.Panic("Unknown genesis method:", conf.General.BootstrapMethod)
	}

	return bootstrapBlock
}

// 初始化引导通道
// 接收参数： genesisBlock(刚才创建的创始区块) lf(账本工厂)
func initializeBootstrapChannel(genesisBlock *cb.Block, lf blockledger.Factory) {
	channelID, err := protoutil.GetChannelIDFromBlock(genesisBlock)
	if err != nil {
		logger.Fatal("Failed to parse channel ID from genesis block:", err)
	}
	gl, err := lf.GetOrCreate(channelID)
	if err != nil {
		logger.Fatal("Failed to create the system channel:", err)
	}
	if gl.Height() == 0 {
		if err := gl.Append(genesisBlock); err != nil {
			logger.Fatal("Could not write genesis block to ledger:", err)
		}
	}
	logger.Infof("Initialized the system channel '%s' from bootstrap block", channelID)
}

func isClusterType(genesisBlock *cb.Block, bccsp bccsp.BCCSP) bool {
	_, exists := clusterTypes[consensusType(genesisBlock, bccsp)]
	return exists
}

func consensusType(genesisBlock *cb.Block, bccsp bccsp.BCCSP) string {
	if genesisBlock == nil || genesisBlock.Data == nil || len(genesisBlock.Data.Data) == 0 {
		logger.Fatalf("Empty genesis block")
	}
	env := &cb.Envelope{}
	if err := proto.Unmarshal(genesisBlock.Data.Data[0], env); err != nil {
		logger.Fatalf("Failed to unmarshal the genesis block's envelope: %v", err)
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(env, bccsp)
	if err != nil {
		logger.Fatalf("Failed creating bundle from the genesis block: %v", err)
	}
	ordConf, exists := bundle.OrdererConfig()
	if !exists {
		logger.Fatalf("Orderer config doesn't exist in bundle derived from genesis block")
	}
	return ordConf.ConsensusType()
}

func initializeGrpcServer(conf *localconfig.TopLevel, serverConfig comm.ServerConfig) *comm.GRPCServer {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.General.ListenAddress, conf.General.ListenPort))
	if err != nil {
		logger.Fatal("Failed to listen:", err)
	}

	// 创建GRPC服务， 如果在这个过程中发生错误，就返回
	grpcServer, err := comm.NewGRPCServerFromListener(lis, serverConfig)
	if err != nil {
		logger.Fatal("Failed to return new GRPC server:", err)
	}

	return grpcServer
}

func loadLocalMSP(conf *localconfig.TopLevel) msp.MSP {
	// 必须先调用 GetLocalMspConfig，以便在 LoadByType 之前正确初始化默认的 BCCSP（区块链密码服务提供商）。
	mspConfig, err := msp.GetLocalMspConfig(conf.General.LocalMSPDir, conf.General.BCCSP, conf.General.LocalMSPID)
	if err != nil {
		logger.Panicf("Failed to get local msp config: %v", err)
	}

	typ := msp.ProviderTypeToString(msp.FABRIC)
	opts, found := msp.Options[typ]
	if !found {
		logger.Panicf("MSP option for type %s is not found", typ)
	}

	localmsp, err := msp.New(opts, factory.GetDefault())
	if err != nil {
		logger.Panicf("Failed to load local MSP: %v", err)
	}

	if err = localmsp.Setup(mspConfig); err != nil {
		logger.Panicf("无法使用 config 设置本地 msp : %v", err)
	}

	return localmsp
}

//go:generate counterfeiter -o mocks/health_checker.go -fake-name HealthChecker . healthChecker

// HealthChecker为健康检查器定义契约
type healthChecker interface {
	RegisterChecker(component string, checker healthz.HealthChecker) error
}

func initializeMultichannelRegistrar(
	bootstrapBlock *cb.Block,
	repInitiator *onboarding.ReplicationInitiator,
	clusterDialer *cluster.PredicateDialer,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	conf *localconfig.TopLevel,
	signer identity.SignerSerializer,
	metricsProvider metrics.Provider,
	healthChecker healthChecker,
	lf blockledger.Factory,
	bccsp bccsp.BCCSP,
	callbacks ...channelconfig.BundleActor,
) *multichannel.Registrar {
	// 新建一个Registrar 类型的实例
	registrar := multichannel.NewRegistrar(*conf, lf, signer, metricsProvider, bccsp, clusterDialer, callbacks...)

	consenters := map[string]consensus.Consenter{}

	var icr etcdraft.InactiveChainRegistry
	if conf.General.BootstrapMethod == "file" || conf.General.BootstrapMethod == "none" {
		if bootstrapBlock != nil && isClusterType(bootstrapBlock, bccsp) {
			// 通过系统通道
			etcdConsenter := initializeEtcdraftConsenter(consenters, conf, lf, clusterDialer, bootstrapBlock, repInitiator, srvConf, srv, registrar, metricsProvider, bccsp)
			icr = etcdConsenter.InactiveChainRegistry
		} else if bootstrapBlock == nil {
			// 没有系统通道:假设集群类型，InactiveChainRegistry == nil，没有go-routine。
			consenters["etcdraft"] = etcdraft.New(clusterDialer, conf, srvConf, srv, registrar, nil, metricsProvider, bccsp)
		}
	}

	consenters["solo"] = solo.New()
	var kafkaMetrics *kafka.Metrics
	consenters["kafka"], kafkaMetrics = kafka.New(conf.Kafka, metricsProvider, healthChecker, icr, registrar.CreateChain)

	//注意，我们在这里传递了一个'nil'通道，我们可以传递一个
	//如果我们希望在退出时清除这个例程，将关闭。
	go kafkaMetrics.PollGoMetricsUntilStop(time.Minute, nil)
	registrar.Initialize(consenters)
	return registrar
}

func initializeEtcdraftConsenter(
	consenters map[string]consensus.Consenter,
	conf *localconfig.TopLevel,
	lf blockledger.Factory,
	clusterDialer *cluster.PredicateDialer,
	bootstrapBlock *cb.Block,
	ri *onboarding.ReplicationInitiator,
	srvConf comm.ServerConfig,
	srv *comm.GRPCServer,
	registrar *multichannel.Registrar,
	metricsProvider metrics.Provider,
	bccsp bccsp.BCCSP,
) *etcdraft.Consenter {
	systemChannelName, err := protoutil.GetChannelIDFromBlock(bootstrapBlock)
	if err != nil {
		logger.Panicf("Failed extracting system channel name from bootstrap block: %v", err)
	}
	systemLedger, err := lf.GetOrCreate(systemChannelName)
	if err != nil {
		logger.Panicf("Failed obtaining system channel (%s) ledger: %v", systemChannelName, err)
	}
	getConfigBlock := func() *cb.Block {
		return multichannel.ConfigBlockOrPanic(systemLedger)
	}

	icr := onboarding.NewInactiveChainReplicator(ri, getConfigBlock, ri.RegisterChain, conf.General.Cluster.ReplicationBackgroundRefreshInterval)

	//使用inactiveChainReplicator作为通道列表，因为它知道所有非活动链。
	//这是为了防止我们在试图枚举系统中的通道时拉出整个系统链。
	ri.ChannelLister = icr

	go icr.Run()
	raftConsenter := etcdraft.New(clusterDialer, conf, srvConf, srv, registrar, icr, metricsProvider, bccsp)
	consenters["etcdraft"] = raftConsenter
	return raftConsenter
}

// 初始化orderer节点使用的操作系统
// metrics 参数主要记录Orderer节点的各种度量指标，如区块处理时间、交易数量等。在操作系统中，可以使用该参数来初始化度量组件并将度量指标信息记录到Metrics中。
func newOperationsSystem(ops localconfig.Operations, metrics localconfig.Metrics) *operations.System {
	return operations.NewSystem(operations.Options{
		Options: fabhttp.Options{
			Logger:        flogging.MustGetLogger("orderer.operations"),
			ListenAddress: ops.ListenAddress,
			TLS: fabhttp.TLS{
				Enabled:            ops.TLS.Enabled,
				CertFile:           ops.TLS.Certificate,
				KeyFile:            ops.TLS.PrivateKey,
				ClientCertRequired: ops.TLS.ClientAuthRequired,
				ClientCACertFiles:  ops.TLS.ClientRootCAs,
			},
		},
		Metrics: operations.MetricsOptions{
			Provider: metrics.Provider,
			Statsd: &operations.Statsd{
				Network:       metrics.Statsd.Network,
				Address:       metrics.Statsd.Address,
				WriteInterval: metrics.Statsd.WriteInterval,
				Prefix:        metrics.Statsd.Prefix,
			},
		},
		Version: metadata.Version,
	})
}

func newAdminServer(admin localconfig.Admin) *fabhttp.Server {
	return fabhttp.NewServer(fabhttp.Options{
		Logger:        flogging.MustGetLogger("orderer.admin"),
		ListenAddress: admin.ListenAddress,
		TLS: fabhttp.TLS{
			Enabled:            admin.TLS.Enabled,
			CertFile:           admin.TLS.Certificate,
			KeyFile:            admin.TLS.PrivateKey,
			ClientCertRequired: admin.TLS.ClientAuthRequired,
			ClientCACertFiles:  admin.TLS.ClientRootCAs,
		},
	})
}

// caMgr 管理按通道限定范围的证书颁发机构
type caManager struct {
	sync.Mutex
	appRootCAsByChain     map[string][][]byte
	ordererRootCAsByChain map[string][][]byte
	clientRootCAs         [][]byte
}

func (mgr *caManager) updateTrustedRoots(
	cm channelconfig.Resources,
	servers ...*comm.GRPCServer,
) {
	mgr.Lock()
	defer mgr.Unlock()

	appRootCAs := [][]byte{}
	ordererRootCAs := [][]byte{}
	appOrgMSPs := make(map[string]struct{})
	ordOrgMSPs := make(map[string]struct{})

	if ac, ok := cm.ApplicationConfig(); ok {
		// loop through app orgs and build map of MSPIDs
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	if ac, ok := cm.OrdererConfig(); ok {
		// loop through orderer orgs and build map of MSPIDs
		for _, ordOrg := range ac.Organizations() {
			ordOrgMSPs[ordOrg.MSPID()] = struct{}{}
		}
	}

	if cc, ok := cm.ConsortiumsConfig(); ok {
		for _, consortium := range cc.Consortiums() {
			// loop through consortium orgs and build map of MSPIDs
			for _, consortiumOrg := range consortium.Organizations() {
				appOrgMSPs[consortiumOrg.MSPID()] = struct{}{}
			}
		}
	}

	cid := cm.ConfigtxValidator().ChannelID()
	logger.Debugf("updating root CAs for channel [%s]", cid)
	msps, err := cm.MSPManager().GetMSPs()
	if err != nil {
		logger.Errorf("Error getting root CAs for channel %s (%s)", cid, err)
		return
	}
	for k, v := range msps {
		// check to see if this is a FABRIC MSP
		if v.GetType() == msp.FABRIC {
			for _, root := range v.GetTLSRootCerts() {
				// check to see of this is an app org MSP
				if _, ok := appOrgMSPs[k]; ok {
					logger.Debugf("adding app root CAs for MSP [%s]", k)
					appRootCAs = append(appRootCAs, root)
				}
				// check to see of this is an orderer org MSP
				if _, ok := ordOrgMSPs[k]; ok {
					logger.Debugf("adding orderer root CAs for MSP [%s]", k)
					ordererRootCAs = append(ordererRootCAs, root)
				}
			}
			for _, intermediate := range v.GetTLSIntermediateCerts() {
				// check to see of this is an app org MSP
				if _, ok := appOrgMSPs[k]; ok {
					logger.Debugf("adding app root CAs for MSP [%s]", k)
					appRootCAs = append(appRootCAs, intermediate)
				}
				// check to see of this is an orderer org MSP
				if _, ok := ordOrgMSPs[k]; ok {
					logger.Debugf("adding orderer root CAs for MSP [%s]", k)
					ordererRootCAs = append(ordererRootCAs, intermediate)
				}
			}
		}
	}
	mgr.appRootCAsByChain[cid] = appRootCAs
	mgr.ordererRootCAsByChain[cid] = ordererRootCAs

	// now iterate over all roots for all app and orderer chains
	trustedRoots := [][]byte{}
	for _, roots := range mgr.appRootCAsByChain {
		trustedRoots = append(trustedRoots, roots...)
	}
	for _, roots := range mgr.ordererRootCAsByChain {
		trustedRoots = append(trustedRoots, roots...)
	}
	// also need to append statically configured root certs
	if len(mgr.clientRootCAs) > 0 {
		trustedRoots = append(trustedRoots, mgr.clientRootCAs...)
	}

	// now update the client roots for the gRPC server
	for _, srv := range servers {
		err = srv.SetClientRootCAs(trustedRoots)
		if err != nil {
			msg := "Failed to update trusted roots for orderer from latest config " +
				"block.  This orderer may not be able to communicate " +
				"with members of channel %s (%s)"
			logger.Warningf(msg, cm.ConfigtxValidator().ChannelID(), err)
		}
	}
}

func (mgr *caManager) updateClusterDialer(
	clusterDialer *cluster.PredicateDialer,
	localClusterRootCAs [][]byte,
) {
	mgr.Lock()
	defer mgr.Unlock()

	// Iterate over all orderer root CAs for all chains and add them
	// to the root CAs
	clusterRootCAs := make(cluster.StringSet)
	for _, orgRootCAs := range mgr.ordererRootCAsByChain {
		for _, rootCA := range orgRootCAs {
			clusterRootCAs[string(rootCA)] = struct{}{}
		}
	}

	// Add the local root CAs too
	for _, localRootCA := range localClusterRootCAs {
		clusterRootCAs[string(localRootCA)] = struct{}{}
	}

	// Convert StringSet to byte slice
	var clusterRootCAsBytes [][]byte
	for root := range clusterRootCAs {
		clusterRootCAsBytes = append(clusterRootCAsBytes, []byte(root))
	}

	// Update the cluster config with the new root CAs
	clusterDialer.UpdateRootCAs(clusterRootCAsBytes)
}

func prettyPrintStruct(i interface{}) {
	params := localconfig.Flatten(i)
	var buffer bytes.Buffer
	for i := range params {
		buffer.WriteString("\n\t")
		buffer.WriteString(params[i])
	}
	//logger.Infof("Orderer config values:%s\n", buffer.String())
}
