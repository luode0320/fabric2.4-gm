/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	pcommon "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/scc/cscc"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/msp"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

// UndefinedParamValue 定义了命令行中未定义参数的初始值。
const (
	UndefinedParamValue = ""                // 未定义参数的初始值为空字符串
	CmdRoot             = "core"            // 配置文件的名称
	CmdRootPeerBCCSP    = "CORE_PEER_BCCSP" // CORE_PEER_BCCSP 环境变量的名称
)

var (
	// 使用flogging库创建的日志记录器, 获取一个名为"main"的日志记录器。这个日志记录器可以用来输出日志消息
	mainLogger = flogging.MustGetLogger("main")
	// 指向标准错误输出（os.Stderr）的变量。它表示日志的输出位置，即日志消息将被写入到标准错误输出流中。
	logOutput = os.Stderr
)

var (
	defaultConnTimeout = 3 * time.Second
	// These function variables (xyzFnc) can be used to invoke corresponding xyz function
	// this will allow the invoking packages to mock these functions in their unit test cases

	// GetEndorserClientFnc is a function that returns a new endorser client connection
	// to the provided peer address using the TLS root cert file,
	// by default it is set to GetEndorserClient function
	GetEndorserClientFnc func(address, tlsRootCertFile string) (pb.EndorserClient, error)

	// GetPeerDeliverClientFnc is a function that returns a new deliver client connection
	// to the provided peer address using the TLS root cert file,
	// by default it is set to GetDeliverClient function
	GetPeerDeliverClientFnc func(address, tlsRootCertFile string) (pb.DeliverClient, error)

	// GetDeliverClientFnc is a function that returns a new deliver client connection
	// to the provided peer address using the TLS root cert file,
	// by default it is set to GetDeliverClient function
	GetDeliverClientFnc func(address, tlsRootCertFile string) (pb.Deliver_DeliverClient, error)

	// GetDefaultSignerFnc 是一个返回默认签名者的函数 (default/PERR)
	// 默认设置为GetDefaultSigner函数
	GetDefaultSignerFnc func() (msp.SigningIdentity, error)

	// GetBroadcastClientFnc 返回 BroadcastClient 广播客户端接口的实例
	// 默认设置为GetBroadcastClient函数
	GetBroadcastClientFnc func() (BroadcastClient, error)

	// GetOrdererEndpointOfChainFnc 返回给定链的 orderer 节点
	// 默认设置为 GetOrdererEndpointOfChain 函数
	GetOrdererEndpointOfChainFnc func(chainID string, signer Signer,
		endorserClient pb.EndorserClient, cryptoProvider bccsp.BCCSP) ([]string, error)

	// GetClientCertificateFnc 是返回客户端TLS证书的函数
	GetClientCertificateFnc func() (tls.Certificate, error)
)

// CommonClient 一个通用的客户端结构体
type CommonClient struct {
	clientConfig comm.ClientConfig // 客户端配置
	address      string            // 地址
}

// 一个通用的客户端结构体
func newCommonClient(address string, clientConfig comm.ClientConfig) (*CommonClient, error) {
	return &CommonClient{
		clientConfig: clientConfig, // 客户端配置
		address:      address,      // 地址
	}, nil
}

func (cc *CommonClient) Certificate() tls.Certificate {
	if !cc.clientConfig.SecOpts.RequireClientCert {
		return tls.Certificate{}
	}
	cert, err := cc.clientConfig.SecOpts.ClientCertificate()
	if err != nil {
		panic(err)
	}
	return cert
}

// Dial will create a new gRPC client connection to the provided
// address. The options used for the dial are sourced from the
// ClientConfig provided to the constructor.
func (cc *CommonClient) Dial(address string) (*grpc.ClientConn, error) {
	return cc.clientConfig.Dial(address)
}

// 函数用于初始化一系列函数指针，这些函数指针在包加载时即被设定，
// 以便在整个程序中提供统一的访问点来获取各种客户端和服务连接。
// 这些初始化对于确保后续代码能够顺利调用到正确的实例化逻辑至关重要。
func init() {
	// 设置获取背书服务客户端的函数
	GetEndorserClientFnc = GetEndorserClient
	// 设置获取默认签名者的函数
	GetDefaultSignerFnc = GetDefaultSigner
	// 设置获取广播服务客户端的函数
	GetBroadcastClientFnc = GetBroadcastClient
	// 设置根据链标识获取排序服务端点的函数
	GetOrdererEndpointOfChainFnc = GetOrdererEndpointOfChain
	// 设置获取交付服务客户端的函数
	GetDeliverClientFnc = GetDeliverClient
	// 设置获取对等节点交付服务客户端的函数
	GetPeerDeliverClientFnc = GetPeerDeliverClient
	// 设置获取客户端证书的函数
	GetClientCertificateFnc = GetClientCertificate
}

// InitConfig 函数用于初始化 viper 配置。
// 参数：
//   - cmdRoot string：命令的根目录。
//
// 返回值：
//   - error：如果初始化过程中出现错误，则返回错误。
func InitConfig(cmdRoot string) error {
	// 初始化 path 目录, 保存至viper.config中
	err := config.InitViper(nil, cmdRoot)
	if err != nil {
		return err
	}
	// 查找并读取配置文件, 保存至viper.config中
	err = viper.ReadInConfig()
	if err != nil {
		// 处理读取配置文件时的错误
		// 我们使用的 Viper 版本声称不支持配置类型，但实际上是因为找不到配置文件
		// 显示更有帮助的消息，以避免让用户感到困惑。
		if strings.Contains(fmt.Sprint(err), "不支持的配置类型") {
			return errors.New(fmt.Sprintf("找不到节点配置文件. "+
				"请确保将 FABRIC_CFG_PATH 环境变量路径 "+
				"包含 %s.yaml", cmdRoot))
		} else {
			return errors.WithMessagef(err, "读取时出错 %s.yaml 配置文件", cmdRoot)
		}
	}

	return nil
}

// InitBCCSPConfig 函数用于初始化 BCCSP（区块链加密算法软件包）的配置。
// 参数：
//   - bccspConfig *factory.FactoryOpts：BCCSP 配置的指针。
//
// 返回值：
//   - error：初始化过程中的错误，如果没有错误则为 nil。
func InitBCCSPConfig(bccspConfig *factory.FactoryOpts) error {
	// 设置 BCCSP 密钥库的路径
	SetBCCSPKeystorePath()
	// 获取 "peer.BCCSP" 的子配置
	subv := viper.Sub("peer.BCCSP")
	if subv == nil {
		return fmt.Errorf("不能初始化加密, 配置指定 peer.BCCSP 值不能为空")
	}
	// 设置子配置的环境变量前缀为 CmdRootPeerBCCSP
	subv.SetEnvPrefix(CmdRootPeerBCCSP)
	// 自动加载环境变量
	subv.AutomaticEnv()
	// 创建一个新的替换器，将点号替换为下划线，以便在环境变量中使用
	subv.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	// 根据默认值设置配置的类型
	subv.SetTypeByDefaultValue(true)

	// 创建一个解码钩子函数，将字符串转换为时间持续时间、字符串转换为切片，以及字符串转换为密钥标识符
	opts := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		factory.StringToKeyIds(),
	))
	// 将子配置解码为 bccspConfig 结构体
	if err := subv.Unmarshal(&bccspConfig, opts); err != nil {
		return errors.WithMessage(err, "不能初始化加密, 配置指定 peer.BCCSP 值无法解码")
	}

	return nil
}

// InitCrypto 函数用于初始化此对等节点的加密功能。
// 参数：
//   - mspMgrConfigDir string：MSP 管理器的配置目录。
//   - localMSPID string：本地 MSP 的 ID。
//   - localMSPType string：本地 MSP 的类型。
//
// 返回值：
//   - error：初始化过程中的错误，如果没有错误则为 nil。
func InitCrypto(mspMgrConfigDir, localMSPID, localMSPType string) error {
	// 检查 msp 文件夹是否存在
	fi, err := os.Stat(mspMgrConfigDir)
	if err != nil {
		return errors.Errorf("不能初始化加密, 配置指定 peer.mspConfigPath 路径 \"%s\" 不存在或无法访问: %v", mspMgrConfigDir, err)
	} else if !fi.IsDir() {
		return errors.Errorf("不能初始化加密, 配置指定 peer.mspConfigPath 路径 \"%s\" 不是目录", mspMgrConfigDir)
	}
	// 检查 localMSPID 是否存在
	if localMSPID == "" {
		return errors.New("不能初始化加密, 配置指定 peer.localMspId 值不能为空")
	}

	// 初始化 BCCSP , 使用默认hash算法初始化, 读取 peer.BCCSP 配置并赋值到 bccspConfig
	bccspConfig := factory.GetDefaultOpts()
	err = InitBCCSPConfig(bccspConfig)
	if err != nil {
		return err
	}
	// 根据指定的msp目录、bccsp配置、mspID 和 msp类型，返回指定目录中 MSP 的本地配置
	conf, err := msp.GetLocalMspConfigWithType(mspMgrConfigDir, bccspConfig, localMSPID, localMSPType)
	if err != nil {
		return err
	}
	err = mspmgmt.GetLocalMSP(factory.GetDefault()).Setup(conf)
	if err != nil {
		return errors.WithMessagef(err, "从目录 %s 设置MSP时出错", mspMgrConfigDir)
	}

	return nil
}

// SetBCCSPKeystorePath sets the file keystore path for the SW BCCSP provider
// to an absolute path relative to the config file.
func SetBCCSPKeystorePath() {
	key := "peer.BCCSP.SW.FileKeyStore.KeyStore"
	if ksPath := config.GetPath(key); ksPath != "" {
		viper.Set(key, ksPath)
	}
}

// GetDefaultSigner 返回cli的默认签名者 (default/PEER)
func GetDefaultSigner() (msp.SigningIdentity, error) {
	signer, err := mspmgmt.GetLocalMSP(factory.GetDefault()).GetDefaultSigningIdentity()
	if err != nil {
		return nil, errors.WithMessage(err, "获取默认 bccsp 签名标识时出错")
	}

	return signer, err
}

// Signer defines the interface needed for signing messages
type Signer interface {
	Sign(msg []byte) ([]byte, error)
	Serialize() ([]byte, error)
}

// GetOrdererEndpointOfChain 返回给定链的 orderer 端点
func GetOrdererEndpointOfChain(chainID string, signer Signer, endorserClient pb.EndorserClient, cryptoProvider bccsp.BCCSP) ([]string, error) {
	// 查询 cscc 链配置块
	invocation := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			Type:        pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]),
			ChaincodeId: &pb.ChaincodeID{Name: "cscc"},
			Input:       &pb.ChaincodeInput{Args: [][]byte{[]byte(cscc.GetChannelConfig), []byte(chainID)}},
		},
	}

	creator, err := signer.Serialize()
	if err != nil {
		return nil, errors.WithMessage(err, "为签名者序列化标识时出错")
	}

	// 根据序列化的身份和 ChaincodeInvocationSpec 创建一个提案
	prop, _, err := protoutil.CreateProposalFromCIS(pcommon.HeaderType_CONFIG, "", invocation, creator)
	if err != nil {
		return nil, errors.WithMessage(err, "创建 GetChannelConfig 提案时出错")
	}

	// 根据提案消息和签名身份返回一个已签名的提案
	signedProp, err := protoutil.GetSignedProposal(prop, signer)
	if err != nil {
		return nil, errors.WithMessage(err, "创建已签名的 GetChannelConfig 提案时出错")
	}

	proposalResp, err := endorserClient.ProcessProposal(context.Background(), signedProp)
	if err != nil {
		return nil, errors.WithMessage(err, "发送 GetChannelConfig 提案时出错")
	}

	if proposalResp == nil {
		return nil, errors.New("收到零提案回复")
	}

	if proposalResp.Response.Status != 0 && proposalResp.Response.Status != 200 {
		return nil, errors.Errorf("提案响应错误 %d: %s", proposalResp.Response.Status, proposalResp.Response.Message)
	}

	// 解析特定通道的配置
	channelConfig := &pcommon.Config{}
	if err := proto.Unmarshal(proposalResp.Response.Payload, channelConfig); err != nil {
		return nil, errors.WithMessage(err, "反序列化特定通道配置错误")
	}

	// 创建一个新的不可变的通道配置
	bundle, err := channelconfig.NewBundle(chainID, channelConfig, cryptoProvider)
	if err != nil {
		return nil, errors.WithMessage(err, "加载通道配置时出错")
	}

	// 返回 orderer 的地址
	return bundle.ChannelConfig().OrdererAddresses(), nil
}

// CheckLogLevel checks that a given log level string is valid
func CheckLogLevel(level string) error {
	if !flogging.IsValidLevel(level) {
		return errors.Errorf("invalid log level provided - %s", level)
	}
	return nil
}

// 根据 prefix 配置前缀获取配置
func configFromEnv(prefix string) (address string, clientConfig comm.ClientConfig, err error) {
	// peer节点的地址
	address = viper.GetString(prefix + ".address")

	// 定义了配置 GRPCClient 实例的参数。
	clientConfig = comm.ClientConfig{}

	// 建立连接的超时时间
	connTimeout := viper.GetDuration(prefix + ".client.connTimeout")
	if connTimeout == time.Duration(0) {
		connTimeout = defaultConnTimeout
	}
	clientConfig.DialTimeout = connTimeout

	// 结构体定义了GRPCServer或GRPCClient实例的TLS安全参数。
	secOpts := comm.SecureOptions{
		UseTLS:             viper.GetBool(prefix + ".tls.enabled"),                // 是否使用TLS进行通信
		RequireClientCert:  viper.GetBool(prefix + ".tls.clientAuthRequired"),     // 是否要求TLS客户端提供证书进行身份验证
		TimeShift:          viper.GetDuration(prefix + ".tls.handshakeTimeShift"), // 通过给定的持续时间使TLS握手时间采样向过去偏移
		ServerNameOverride: viper.GetString(prefix + ".tls.serverhostoverride"),   // 用于验证返回的证书上的主机名。除非它是IP地址，否则它也包含在客户端的握手中以支持虚拟主机。
	}

	// 是否使用TLS进行通信
	if secOpts.UseTLS {
		caPEM, res := ioutil.ReadFile(config.GetPath(prefix + ".tls.rootcert.file"))
		if res != nil {
			err = errors.WithMessagef(res, "无法加载 %s.tls.rootcert.file", prefix)
			return
		}

		// 用于客户端验证服务器证书的一组PEM编码的X509证书颁发机构
		secOpts.ServerRootCAs = [][]byte{caPEM}
	}

	// 是否要求TLS客户端提供证书进行身份验证
	if secOpts.RequireClientCert {
		// 用于TLS通信的PEM编码的私钥
		// 用于TLS通信的PEM编码的服务器证书
		secOpts.Key, secOpts.Certificate, err = getClientAuthInfoFromEnv(prefix)
		if err != nil {
			return
		}
	}

	clientConfig.SecOpts = secOpts

	// 客户端可以接收的最大消息大小
	clientConfig.MaxRecvMsgSize = comm.DefaultMaxRecvMsgSize
	if viper.IsSet(prefix + ".maxRecvMsgSize") {
		clientConfig.MaxRecvMsgSize = int(viper.GetInt32(prefix + ".maxRecvMsgSize"))
	}

	// 客户端可以发送的最大消息大小
	clientConfig.MaxSendMsgSize = comm.DefaultMaxSendMsgSize
	if viper.IsSet(prefix + ".maxSendMsgSize") {
		clientConfig.MaxSendMsgSize = int(viper.GetInt32(prefix + ".maxSendMsgSize"))
	}
	return
}

// getClientAuthInfoFromEnv 读取客户端tls密钥文件和证书文件并返回文件的字节
func getClientAuthInfoFromEnv(prefix string) ([]byte, []byte, error) {
	keyPEM, err := ioutil.ReadFile(config.GetPath(prefix + ".tls.clientKey.file"))
	if keyPEM == nil {
		keyPEM, err = ioutil.ReadFile(config.GetPath(prefix + ".tls.key.file"))
	}
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "无法加载 %s.tls.clientKey.file", prefix)
	}
	certPEM, err := ioutil.ReadFile(config.GetPath(prefix + ".tls.clientCert.file"))
	if certPEM == nil {
		certPEM, err = ioutil.ReadFile(config.GetPath(prefix + ".tls.cert.file"))
	}
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "无法加载 %s.tls.clientCert.file", prefix)
	}

	return keyPEM, certPEM, nil
}

// InitCmd 函数用于初始化命令。
// 参数：
//   - cmd *cobra.Command：要初始化的命令。
//   - args []string：命令的参数。
func InitCmd(cmd *cobra.Command, args []string) {
	// 初始化 viper 配置，包括路径和 CmdRoot.yaml 配置
	err := InitConfig(CmdRoot)
	// 处理读取配置文件时的错误
	if err != nil {
		mainLogger.Errorf("初始化时出现致命错误 %s.yaml 配置 : %s", CmdRoot, err)
		os.Exit(1)
	}

	// 读取旧的日志级别设置，并在设置了日志级别时通知用户使用 FABRIC_LOGGING_SPEC 环境变量
	var loggingLevel string
	if viper.GetString("logging_level") != "" {
		loggingLevel = viper.GetString("logging_level")
	} else {
		loggingLevel = viper.GetString("logging.level")
	}
	if loggingLevel != "" {
		mainLogger.Warning("CORE_LOGGING_LEVEL 不再支持, 请使用 FABRIC_LOGGING_SPEC 环境变量")
	}
	// FABRIC记录规范
	loggingSpec := os.Getenv("FABRIC_LOGGING_SPEC")
	// FABRIC日志记录格式
	loggingFormat := os.Getenv("FABRIC_LOGGING_FORMAT")

	flogging.Init(flogging.Config{
		Format:  loggingFormat, // 指定日志的格式
		Writer:  logOutput,     // 指定日志的输出位置
		LogSpec: loggingSpec,   // 指定日志的级别和过滤规则
	})

	// 链码打包不需要本地 MSP 的材料
	if cmd.CommandPath() == "peer lifecycle chaincode package" || cmd.CommandPath() == "peer lifecycle chaincode calculatepackageid" {
		mainLogger.Debug("peer lifecycle chaincode package 不需要加密")
		return
	}

	// 初始化 MSP
	// 获取 mspConfigPath(msp目录) 的绝对路径
	mspMgrConfigDir := config.GetPath("peer.mspConfigPath")
	// 获取 mspid
	mspID := viper.GetString("peer.localMspId")
	// 本地MSP的类型-默认情况下，它的类型为bccsp
	// 目前版本已经固定为bccsp, 旧版本才中可使用idemix
	// mspType := viper.GetString("peer.localMspType")
	// 本地MSP的类型-默认情况下，它的类型为bccsp
	mspType := msp.ProviderTypeToString(msp.FABRIC)

	// 函数用于初始化此对等节点的加密功能
	err = InitCrypto(mspMgrConfigDir, mspID, mspType)
	// 处理读取配置文件时的错误
	if err != nil {
		mainLogger.Errorf("初始化此对等节点的加密功能失败, 因为 %s", err.Error())
		os.Exit(1)
	}
}
