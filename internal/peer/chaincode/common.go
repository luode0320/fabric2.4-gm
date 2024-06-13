/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pcommon "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/policydsl"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// checkSpec to see if chaincode resides within current package capture for language.
func checkSpec(spec *pb.ChaincodeSpec) error {
	// Don't allow nil value
	if spec == nil {
		return errors.New("expected chaincode specification, nil received")
	}
	if spec.ChaincodeId == nil {
		return errors.New("expected chaincode ID, nil received")
	}

	return platformRegistry.ValidateSpec(spec.Type.String(), spec.ChaincodeId.Path)
}

// getChaincodeDeploymentSpec get chaincode deployment spec given the chaincode spec
func getChaincodeDeploymentSpec(spec *pb.ChaincodeSpec, crtPkg bool) (*pb.ChaincodeDeploymentSpec, error) {
	var codePackageBytes []byte
	if crtPkg {
		var err error
		if err = checkSpec(spec); err != nil {
			return nil, err
		}

		codePackageBytes, err = platformRegistry.GetDeploymentPayload(spec.Type.String(), spec.ChaincodeId.Path)
		if err != nil {
			return nil, errors.WithMessage(err, "error getting chaincode package bytes")
		}
		chaincodePath, err := platformRegistry.NormalizePath(spec.Type.String(), spec.ChaincodeId.Path)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to normalize chaincode path")
		}
		spec.ChaincodeId.Path = chaincodePath
	}

	return &pb.ChaincodeDeploymentSpec{ChaincodeSpec: spec, CodePackage: codePackageBytes}, nil
}

// getChaincodeSpec 方法用于从命令行参数中获取链码规范。
// 方法接收者：无（全局函数）
// 输入参数：
//   - cmd：*cobra.Command 类型，表示命令对象。
//
// 返回值：
//   - *pb.ChaincodeSpec：表示获取到的链码规范。
//   - error：如果获取链码规范时出现错误，则返回错误。
func getChaincodeSpec(cmd *cobra.Command) (*pb.ChaincodeSpec, error) {
	// 携带链码规范。这是定义链码所需的实际元数据
	spec := &pb.ChaincodeSpec{}

	// 检查链码命令参数, 调用的传参等
	if err := checkChaincodeCmdParams(cmd); err != nil {
		// 取消静默模式，因为这是命令行使用错误
		cmd.SilenceUsage = false
		return spec, err
	}

	// 构建链码规范
	input := chaincodeInput{}
	if err := json.Unmarshal([]byte(chaincodeCtorJSON), &input); err != nil {
		return spec, errors.Wrap(err, "chaincode 参数错误")
	}
	input.IsInit = isInit

	// 链码语言
	chaincodeLang = strings.ToUpper(chaincodeLang)
	spec = &pb.ChaincodeSpec{
		Type:        pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value[chaincodeLang]),                    // 链码语言
		ChaincodeId: &pb.ChaincodeID{Path: chaincodePath, Name: chaincodeName, Version: chaincodeVersion}, // 链码路径,名称.版本
		Input:       &input.ChaincodeInput,                                                                // 链码构造函数的JSON表示
	}

	return spec, nil
}

// chaincodeInput 是proto定义的ChaincodeInput消息的包装，它使用自定义的JSON解组器进行装饰。
type chaincodeInput struct {
	pb.ChaincodeInput // 包含chaincode函数及其参数
}

// UnmarshalJSON converts the string-based REST/JSON input to
// the []byte-based current ChaincodeInput structure.
func (c *chaincodeInput) UnmarshalJSON(b []byte) error {
	sa := struct {
		Function string
		Args     []string
	}{}
	err := json.Unmarshal(b, &sa)
	if err != nil {
		return err
	}
	allArgs := sa.Args
	if sa.Function != "" {
		allArgs = append([]string{sa.Function}, sa.Args...)
	}
	c.Args = util.ToChaincodeArgs(allArgs...)
	return nil
}

// chaincodeInvokeOrQuery 方法用于调用或查询链码。
// 方法接收者：无（全局函数）
// 输入参数：
//   - cmd：*cobra.Command 类型，表示命令对象。
//   - invoke：bool 类型，表示是否为调用操作。
//   - cf：*ChaincodeCmdFactory 类型，表示链码命令工厂。
func chaincodeInvokeOrQuery(cmd *cobra.Command, invoke bool, cf *ChaincodeCmdFactory) (err error) {
	// 方法用于从命令行参数中获取链码规范
	spec, err := getChaincodeSpec(cmd)
	if err != nil {
		return err
	}

	// 使用空的txid调用以确保生产代码生成txid。
	// 否则，测试可以显式设置自己的txid
	txID := ""

	// 方法用于调用或查询链码, 并广播发送到 orderer
	proposalResp, err := ChaincodeInvokeOrQuery(
		spec,               // 表示链码规范
		channelID,          // 通道ID
		txID,               // 交易ID
		invoke,             // 是否为调用操作
		cf.Signer,          // 签名者序列化器
		cf.Certificate,     // TLS证书
		cf.EndorserClients, // 背书者客户端
		cf.DeliverClients,  // 提交客户端
		cf.BroadcastClient, // 广播客户端
	)
	if err != nil {
		return errors.Errorf("%s - 提案响应: %v", err, proposalResp)
	}

	// 是否为调用操作
	if invoke {
		logger.Debugf("ESCC 调用结果: %v", proposalResp)
		pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(proposalResp.Payload)
		if err != nil {
			return errors.WithMessage(err, "反序列化链码调用提案响应 Payload 负载时出错")
		}

		// 字节反序列化到 ChaincodeAction (生成的读取集和写入集, 链码生成的事件, 调用的结果, 调用的ChaincodeID)
		ca, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
		if err != nil {
			return errors.WithMessage(err, "反序列化链码调用提案扩展消息操作时出错")
		}

		// 提案的背书，基本上是背书人在有效载荷上的签名
		if proposalResp.Endorsement == nil {
			return errors.Errorf("背书失败, 背书人在有效载荷上的签名为nil. Message: %s, 响应: %v", proposalResp.Response.Message, proposalResp.Response)
		}
		logger.Infof("链码调用成功. Status: %d, Msg: %s, Payload: %s", ca.Response.Status, ca.Response.Message, string(ca.Response.Payload))
	} else {
		if proposalResp == nil {
			return errors.New("查询过程中出错: 收到链码查询提案响应为nil")
		}
		if proposalResp.Endorsement == nil {
			return errors.Errorf("背书失败, 背书人在有效载荷上的签名为nil. Message: %s, 响应: %v", proposalResp.Response.Message, proposalResp.Response)
		}

		// 是否以原始格式查询链码 && 是否以十六进制格式查询链码
		if chaincodeQueryRaw && chaincodeQueryHex {
			return fmt.Errorf("选项 --raw (-r) 是否以原始格式查询链码和 --hex (-x) 是否以十六进制格式查询链码不兼容, 不能同时为ture")
		}

		// 是否以原始格式查询链码
		if chaincodeQueryRaw {
			fmt.Println(proposalResp.Response.Payload)
			return nil
		}

		// 是否以十六进制格式查询链码
		if chaincodeQueryHex {
			fmt.Printf("%x\n", proposalResp.Response.Payload)
			return nil
		}

		fmt.Println("链码执行结果: ", string(proposalResp.Response.Payload))
	}
	return nil
}

// endorsementPolicy 用于表示背书策略的结构体。
type endorsementPolicy struct {
	ChannelConfigPolicy string `json:"channelConfigPolicy,omitempty"` // 通道配置策略
	SignaturePolicy     string `json:"signaturePolicy,omitempty"`     // 签名策略
}

// collectionConfigJson 用于表示集合配置的结构体。
type collectionConfigJson struct {
	Name              string             `json:"name"`                        // 集合名称
	Policy            string             `json:"policy"`                      // 集合策略
	RequiredPeerCount *int32             `json:"requiredPeerCount"`           // 所需背书节点数量
	MaxPeerCount      *int32             `json:"maxPeerCount"`                // 最大背书节点数量
	BlockToLive       uint64             `json:"blockToLive"`                 // 存活区块数
	MemberOnlyRead    bool               `json:"memberOnlyRead"`              // 仅成员可读
	MemberOnlyWrite   bool               `json:"memberOnlyWrite"`             // 仅成员可写
	EndorsementPolicy *endorsementPolicy `json:"endorsementPolicy,omitempty"` // 背书策略
}

// GetCollectionConfigFromFile 方法从提供的文件中检索集合配置。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - ccFile：string 类型，表示集合配置文件的路径。
//
// 返回值：
//   - *pb.CollectionConfigPackage：表示集合配置包的指针。
//   - []byte：表示文件的字节。
//   - error：如果读取文件时出错，则返回错误。
func GetCollectionConfigFromFile(ccFile string) (*pb.CollectionConfigPackage, []byte, error) {
	// 读取文件的字节
	fileBytes, err := ioutil.ReadFile(ccFile)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "无法读取文件 '%s'", ccFile)
	}

	// 从字节中获取集合配置
	return getCollectionConfigFromBytes(fileBytes)
}

// getCollectionConfigFromBytes 方法从提供的字节数组中检索集合配置。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - cconfBytes：[]byte 类型，表示包含集合配置的字节数组。
//
// 返回值：
//   - *pb.CollectionConfigPackage：表示集合配置包的指针。
//   - []byte：表示字节数组。
//   - error：如果解析集合配置时出错，则返回错误。
func getCollectionConfigFromBytes(cconfBytes []byte) (*pb.CollectionConfigPackage, []byte, error) {
	cconf := &[]collectionConfigJson{}
	err := json.Unmarshal(cconfBytes, cconf)
	if err != nil {
		return nil, nil, errors.Wrap(err, "无法分析收集 '--collectionsConfigFile' json配置")
	}

	// 定义集合对象的配置
	ccarray := make([]*pb.CollectionConfig, 0, len(*cconf))
	for _, cconfitem := range *cconf {
		// 集合策略
		p, err := policydsl.FromString(cconfitem.Policy)
		if err != nil {
			return nil, nil, errors.WithMessagef(err, "无效策略 %s", cconfitem.Policy)
		}

		// 收集策略配置。最初，配置只能包含SignaturePolicy。在未来，SignaturePolicy可能是一个更笼统的政策
		cpc := &pb.CollectionPolicyConfig{
			Payload: &pb.CollectionPolicyConfig_SignaturePolicy{
				SignaturePolicy: p, // 包装SignaturePolicy并包含一个版本以供将来增强
			},
		}

		// 捕获在应用程序级别设置和计算的diffenrent策略类型
		var ep *pb.ApplicationPolicy
		// 背书策略
		if cconfitem.EndorsementPolicy != nil {
			// 签名策略
			signaturePolicy := cconfitem.EndorsementPolicy.SignaturePolicy
			// 通道配置策略
			channelConfigPolicy := cconfitem.EndorsementPolicy.ChannelConfigPolicy
			// 方法用于获取应用程序策略
			ep, err = getApplicationPolicy(signaturePolicy, channelConfigPolicy)
			if err != nil {
				return nil, nil, errors.WithMessagef(err, "无效的背书政策 [%#v]", cconfitem.EndorsementPolicy)
			}
		}

		// 如果未在json中指定，则设置默认 requiredPeerCount=0 和 MaxPeerCount=1
		requiredPeerCount := int32(0)
		maxPeerCount := int32(1)
		if cconfitem.RequiredPeerCount != nil {
			requiredPeerCount = *cconfitem.RequiredPeerCount // 所需背书节点数量
		}
		if cconfitem.MaxPeerCount != nil {
			maxPeerCount = *cconfitem.MaxPeerCount // 最大背书节点数量
		}

		// 定义集合对象的配置
		cc := &pb.CollectionConfig{
			Payload: &pb.CollectionConfig_StaticCollectionConfig{
				StaticCollectionConfig: &pb.StaticCollectionConfig{
					Name:              cconfitem.Name,            // 集合名称
					MemberOrgsPolicy:  cpc,                       // 成员组织策略
					RequiredPeerCount: requiredPeerCount,         // 所需背书节点数量
					MaximumPeerCount:  maxPeerCount,              // 最大背书节点数量
					BlockToLive:       cconfitem.BlockToLive,     // 存活区块数
					MemberOnlyRead:    cconfitem.MemberOnlyRead,  // 仅成员可读
					MemberOnlyWrite:   cconfitem.MemberOnlyWrite, // 仅成员可写
					EndorsementPolicy: ep,                        // 背书策略
				},
			},
		}

		ccarray = append(ccarray, cc)
	}

	// 表示CollectionConfig消息的数组
	ccp := &pb.CollectionConfigPackage{Config: ccarray}
	ccpBytes, err := proto.Marshal(ccp)
	return ccp, ccpBytes, err
}

// getApplicationPolicy 方法用于获取应用程序策略。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - signaturePolicy：string 类型，表示签名策略。
//   - channelConfigPolicy：string 类型，表示通道配置策略。
//
// 返回值：
//   - *pb.ApplicationPolicy：表示应用程序策略的指针。
//   - error：如果获取应用程序策略时出错，则返回错误。
func getApplicationPolicy(signaturePolicy, channelConfigPolicy string) (*pb.ApplicationPolicy, error) {
	if signaturePolicy == "" && channelConfigPolicy == "" {
		// 无签名策略, 无通道配置策略
		return nil, nil
	}

	if signaturePolicy != "" && channelConfigPolicy != "" {
		return nil, errors.New(`不能同时指定 '--signature-policy' 和 '--channel-config-policy'`)
	}

	var applicationPolicy *pb.ApplicationPolicy
	if signaturePolicy != "" {
		signaturePolicyEnvelope, err := policydsl.FromString(signaturePolicy)
		if err != nil {
			return nil, errors.Errorf("无效签名策略: %s", signaturePolicy)
		}

		applicationPolicy = &pb.ApplicationPolicy{
			Type: &pb.ApplicationPolicy_SignaturePolicy{
				SignaturePolicy: signaturePolicyEnvelope,
			},
		}
	}

	if channelConfigPolicy != "" {
		applicationPolicy = &pb.ApplicationPolicy{
			Type: &pb.ApplicationPolicy_ChannelConfigPolicyReference{
				ChannelConfigPolicyReference: channelConfigPolicy,
			},
		}
	}

	return applicationPolicy, nil
}

// 检查链码Cmd参数
func checkChaincodeCmdParams(cmd *cobra.Command) error {
	// 我们需要所有内容的链码名称，包括部署
	if chaincodeName == common.UndefinedParamValue {
		return errors.Errorf("必须为 %s 名称参数提供值. 请使用 --name 手动添加", chainFuncName)
	}

	// 实例化、安装、升级、打包
	if cmd.Name() == instantiateCmdName || cmd.Name() == installCmdName ||
		cmd.Name() == upgradeCmdName || cmd.Name() == packageCmdName {
		if chaincodeVersion == common.UndefinedParamValue {
			return errors.Errorf("未提供 %s 的链码版本", cmd.Name())
		}

		if escc != common.UndefinedParamValue {
			logger.Infof("正在使用 escc 背书系统链码 %s", escc)
		} else {
			logger.Info("使用默认escc 背书系统链码")
			escc = "escc"
		}

		if vscc != common.UndefinedParamValue {
			logger.Infof("正在使用 vscc 验证系统链码 %s", vscc)
		} else {
			logger.Info("使用默认 vscc 验证系统链码")
			vscc = "vscc"
		}

		if policy != common.UndefinedParamValue {
			// 解析策略
			p, err := policydsl.FromString(policy)
			if err != nil {
				return errors.Errorf("无效策略 %s", policy)
			}
			policyMarshalled = protoutil.MarshalOrPanic(p)
		}

		if collectionsConfigFile != common.UndefinedParamValue {
			var err error
			// 方法从提供的文件中检索配置集合
			_, collectionConfigBytes, err = GetCollectionConfigFromFile(collectionsConfigFile)
			if err != nil {
				return errors.WithMessagef(err, "文件 %s 中的配置集合无效", collectionsConfigFile)
			}
		}
	}

	// 检查非空链码参数仅包含 Args 作为键。
	// 类型检查是在 JSON 实际反序列化后进行的
	// 到一个 pb.ChaincodeInput。为了更好地了解发生了什么
	// 在这里使用 JSON 解析，请参阅 http://blog.golang.org/json-and-go
	// 带有接口 {} 的通用JSON
	if chaincodeCtorJSON != "{}" {
		// 如果链码构造函数参数不为空
		var f interface{}
		err := json.Unmarshal([]byte(chaincodeCtorJSON), &f)
		if err != nil {
			// 如果解析 JSON 失败，则返回错误
			return errors.Wrap(err, "chaincode 参数错误")
		}
		m := f.(map[string]interface{})
		sm := make(map[string]interface{})
		for k := range m {
			// 将键转换为小写，并将其存储在新的映射中
			sm[strings.ToLower(k)] = m[k]
		}

		_, argsPresent := sm["args"]
		_, funcPresent := sm["function"]
		if !argsPresent || (len(m) == 2 && !funcPresent) || len(m) > 2 {
			// 如果参数中缺少 'args' 键，或者参数中只有 'args' 键而没有 'function' 键，或者参数中包含超过两个键，则返回错误
			return errors.New("非空 JSON 链码参数必须包含以下键: 'Args' 或者 'Function' 和 'Args'")
		}
	} else {
		if cmd == nil || (cmd != chaincodeInstallCmd && cmd != chaincodePackageCmd) {
			// 如果链码构造函数参数为空，并且命令不是 'chaincodeInstallCmd' 或 'chaincodePackageCmd'，则返回错误
			return errors.New(`空 JSON 链码参数必须包含以下键: 'Args' 或者 'Function' 和 'Args'. 请手动配置 --ctor='{"Args":["a","bb"]}' 链码传参`)
		}
	}

	return nil
}

// validatePeerConnectionParameters 方法用于验证对等节点连接参数。
// 方法接收者：无（全局函数）
// 输入参数：
//   - cmdName：string 类型，表示命令名称。
//
// 返回值：
//   - error：如果验证对等节点连接参数时出现错误，则返回错误。
func validatePeerConnectionParameters(cmdName string) error {
	// 连接配置文件
	if connectionProfile != common.UndefinedParamValue {
		// 方法用于将提供的连接配置文件解析为网络配置结构
		networkConfig, err := common.GetConfig(connectionProfile)
		if err != nil {
			return err
		}
		if len(networkConfig.Channels[channelID].Peers) != 0 {
			peerAddresses = []string{}
			tlsRootCertFiles = []string{}
			for peer, peerChannelConfig := range networkConfig.Channels[channelID].Peers {
				if peerChannelConfig.EndorsingPeer {
					peerConfig, ok := networkConfig.Peers[peer]
					if !ok {
						return errors.Errorf("peer 节点 '%s' 在通道配置中定义，但没有关联的对等方配置", peer)
					}
					peerAddresses = append(peerAddresses, peerConfig.URL)
					tlsRootCertFiles = append(tlsRootCertFiles, peerConfig.TLSCACerts.Path)
				}
			}
		}
	}

	// 当前仅支持多个 peer 地址进行调用
	multiplePeersAllowed := map[string]bool{
		"invoke": true,
	}
	_, ok := multiplePeersAllowed[cmdName]
	if !ok && len(peerAddresses) > 1 {
		return errors.Errorf("'%s' 命令只能对一个 peer 节点执行. 但配置了 %d 个", cmdName, len(peerAddresses))
	}

	if len(tlsRootCertFiles) > len(peerAddresses) {
		logger.Warningf("收到的 TLS 根证书文件 (%d) 个, 多于 peer 节点地址 (%d) 个数", len(tlsRootCertFiles), len(peerAddresses))
	}

	if viper.GetBool("peer.tls.enabled") {
		if len(tlsRootCertFiles) != len(peerAddresses) {
			return errors.Errorf("peer 节点地址数 (%d) 与 TLS 根证书文件数 (%d) 不匹配", len(peerAddresses), len(tlsRootCertFiles))
		}
	} else {
		tlsRootCertFiles = nil
	}

	return nil
}

// ChaincodeCmdFactory 包含ChaincodeCmd使用的客户端
type ChaincodeCmdFactory struct {
	EndorserClients []pb.EndorserClient       // 背书节点客户端列表
	DeliverClients  []pb.DeliverClient        // 提交节点客户端列表
	Certificate     tls.Certificate           // TLS证书
	Signer          identity.SignerSerializer // 签名者
	BroadcastClient common.BroadcastClient    // 广播客户端
}

// InitCmdFactory 方法用于使用默认客户端启动 ChaincodeCmdFactory。
// 方法接收者：无（全局函数）
// 输入参数：
//   - cmdName：string 类型，表示命令名称。
//   - isEndorserRequired：bool 类型，表示是否需要背书节点。
//   - isOrdererRequired：bool 类型，表示是否需要排序节点。
//   - cryptoProvider：bccsp.BCCSP 类型，表示密码提供者。
//
// 返回值：
//   - *ChaincodeCmdFactory：表示初始化的 ChaincodeCmdFactory 对象。
//   - error：如果启动 ChaincodeCmdFactory 时出现错误，则返回错误。
func InitCmdFactory(cmdName string, isEndorserRequired, isOrdererRequired bool, cryptoProvider bccsp.BCCSP) (*ChaincodeCmdFactory, error) {
	var err error
	// 背书客户端
	var endorserClients []pb.EndorserClient
	// 提交客户端
	var deliverClients []pb.DeliverClient

	if isEndorserRequired {
		// 方法用于验证对等节点连接参数,验证 peer 节点数和 tls 证书配置数
		if err = validatePeerConnectionParameters(cmdName); err != nil {
			return nil, errors.WithMessage(err, "验证 peer 节点连接参数时出错")
		}
		for i, address := range peerAddresses {
			var tlsRootCertFile string
			if tlsRootCertFiles != nil {
				tlsRootCertFile = tlsRootCertFiles[i]
			}

			endorserClient, err := common.GetEndorserClientFnc(address, tlsRootCertFile)
			if err != nil {
				return nil, errors.WithMessagef(err, "获取 %s 的背书客户端时出错", cmdName)
			}

			endorserClients = append(endorserClients, endorserClient)
			deliverClient, err := common.GetPeerDeliverClientFnc(address, tlsRootCertFile)
			if err != nil {
				return nil, errors.WithMessagef(err, "获取 %s 的提交客户端时出错", cmdName)
			}

			deliverClients = append(deliverClients, deliverClient)
		}
		if len(endorserClients) == 0 {
			return nil, errors.New("未找到背书客户端-这可能表示存在错误")
		}
	}

	// 是返回客户端TLS证书的函数
	certificate, err := common.GetClientCertificateFnc()
	if err != nil {
		return nil, errors.WithMessage(err, "获取客户端证书时出错")
	}

	// 是一个返回默认签名者的函数
	signer, err := common.GetDefaultSignerFnc()
	if err != nil {
		return nil, errors.WithMessage(err, "获取默认签名者时出错")
	}

	// 广播客户端
	var broadcastClient common.BroadcastClient
	if isOrdererRequired {
		if len(common.OrderingEndpoint) == 0 {
			if len(endorserClients) == 0 {
				return nil, errors.New("未找到背书客户端-这可能表示存在错误")
			}
			endorserClient := endorserClients[0]

			// 返回给定链的 orderer 节点
			orderingEndpoints, err := common.GetOrdererEndpointOfChainFnc(channelID, signer, endorserClient, cryptoProvider)
			if err != nil {
				return nil, errors.WithMessagef(err, "获取通道 (%s) orderer 节点时出错", channelID)
			}
			if len(orderingEndpoints) == 0 {
				return nil, errors.Errorf("没有为通道 %s 检索到 orderer 节点, 请手动配置  -o ", channelID)
			}
			logger.Infof("检索到的通道 (%s) 的 orderer 节点: %s", channelID, orderingEndpoints[0])
			// override viper env
			viper.Set("orderer.address", orderingEndpoints[0])
		}

		// 返回 BroadcastClient 广播客户端接口的实例
		broadcastClient, err = common.GetBroadcastClientFnc()
		if err != nil {
			return nil, errors.WithMessage(err, "获取 orderer 广播客户端时出错")
		}
	}

	// 包含ChaincodeCmd使用的客户端
	return &ChaincodeCmdFactory{
		EndorserClients: endorserClients, // 背书节点客户端列表
		DeliverClients:  deliverClients,  // 提交节点客户端列表
		Signer:          signer,          // 签名者
		BroadcastClient: broadcastClient, // 广播客户端
		Certificate:     certificate,     // TLS证书
	}, nil
}

// processProposals 方法用于向一组背书者发送已签名的提案，并收集所有的响应。
// 方法接收者：无（全局函数）
// 输入参数：
//   - endorserClients：[]pb.EndorserClient 类型，表示背书者客户端。
//   - signedProposal：*pb.SignedProposal 类型，表示已签名的提案。
//
// 返回值：
//   - []*pb.ProposalResponse：表示一组提案响应。
//   - error：如果处理提案时出现错误，则返回错误。
func processProposals(endorserClients []pb.EndorserClient, signedProposal *pb.SignedProposal) ([]*pb.ProposalResponse, error) {
	// 创建用于接收响应和错误的通道, 提案响应从背书人返回给提案提交者
	responsesCh := make(chan *pb.ProposalResponse, len(endorserClients))
	errorCh := make(chan error, len(endorserClients))

	// 创建等待组
	wg := sync.WaitGroup{}

	// 遍历背书者客户端数组
	for _, endorser := range endorserClients {
		wg.Add(1)
		go func(endorser pb.EndorserClient) {
			defer wg.Done()

			// 向背书者发送已签名的提案，并接收响应
			proposalResp, err := endorser.ProcessProposal(context.Background(), signedProposal)
			if err != nil {
				errorCh <- err
				return
			}
			responsesCh <- proposalResp
		}(endorser)
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 关闭通道
	close(responsesCh)
	close(errorCh)

	// 检查是否有错误发生
	for err := range errorCh {
		return nil, err
	}

	// 收集所有的提案响应
	var responses []*pb.ProposalResponse
	for response := range responsesCh {
		responses = append(responses, response)
	}

	return responses, nil
}

// ChaincodeInvokeOrQuery 方法用于调用或查询链码。如果成功，INVOKE形式将ProposalResponse打印到STDOUT，QUERY形式将查询结果打印到STDOUT。
// 命令行标志(-r, --raw)决定查询结果是以原始字节还是可打印字符串的形式输出。可选地，可使用(-x, --hex)以十六进制表示查询响应。
// 如果查询响应为NIL，则不输出任何内容。
//
// 注意 - Query可能会消失，因为与背书者的所有交互都是Proposal和ProposalResponses
//
// 方法接收者：无（全局函数）
// 输入参数：
//   - spec：*pb.ChaincodeSpec 类型，表示链码规范。
//   - cID：string 类型，表示通道ID。
//   - txID：string 类型，表示交易ID。
//   - invoke：bool 类型，表示是否为调用操作。
//   - signer：identity.SignerSerializer 类型，表示签名者序列化器。
//   - certificate：tls.Certificate 类型，表示TLS证书。
//   - endorserClients：[]pb.EndorserClient 类型，表示背书者客户端。
//   - deliverClients：[]pb.DeliverClient 类型，表示提交客户端。
//   - bc：common.BroadcastClient 类型，表示广播客户端。
//
// 返回值：
//   - *pb.ProposalResponse：表示ProposalResponse对象。
//   - error：如果调用或查询链码时出现错误，则返回错误。
func ChaincodeInvokeOrQuery(
	spec *pb.ChaincodeSpec,
	cID string,
	txID string,
	invoke bool,
	signer identity.SignerSerializer,
	certificate tls.Certificate,
	endorserClients []pb.EndorserClient,
	deliverClients []pb.DeliverClient,
	bc common.BroadcastClient,
) (*pb.ProposalResponse, error) {
	// 构建 ChaincodeInvocationSpec 消息
	invocation := &pb.ChaincodeInvocationSpec{ChaincodeSpec: spec}

	// 序列化签名者
	creator, err := signer.Serialize()
	if err != nil {
		return nil, errors.WithMessage(err, "序列化签名者标识时出错")
	}

	funcName := "invoke"
	if !invoke {
		funcName = "query"
	}

	// 提取临时数据，如果它存在
	var tMap map[string][]byte
	if transient != "" {
		if err := json.Unmarshal([]byte(transient), &tMap); err != nil {
			return nil, errors.Wrap(err, "解析 transient 临时数据时出错")
		}
	}

	// 方法根据给定的输入创建一个提案, Header类型背书人交易
	prop, txid, err := protoutil.CreateChaincodeProposalWithTxIDAndTransient(pcommon.HeaderType_ENDORSER_TRANSACTION, cID, invocation, creator, txID, tMap)
	if err != nil {
		return nil, errors.WithMessagef(err, "创建链码调用提案时出错 %s", funcName)
	}

	// 根据提案消息和签名身份返回一个已签名的提案
	signedProp, err := protoutil.GetSignedProposal(prop, signer)
	if err != nil {
		return nil, errors.WithMessagef(err, "创建的签名链码调用提案时出错 %s", funcName)
	}

	// 方法用于向一组背书者发送已签名的提案，并收集所有成功背书的响应
	responses, err := processProposals(endorserClients, signedProp)
	if err != nil {
		return nil, errors.WithMessagef(err, "链码调用提案背书错误 %s", funcName)
	}

	if len(responses) == 0 {
		return nil, errors.New("未收到链码调用提案响应-这可能表示存在错误")
	}

	// 创建签名事务时，将检查所有响应。
	// 现在，只需设置此设置，以便我们检查第一个响应的状态
	proposalResp := responses[0]

	if invoke {
		if proposalResp != nil {
			if proposalResp.Response.Status >= shim.ERRORTHRESHOLD {
				return proposalResp, nil
			}

			// 组装一个签名的交易 (它是一个信封消息), 方法根据提案、背书和签名者组装一个Envelope消息
			env, err := protoutil.CreateSignedTx(prop, signer, responses...)
			if err != nil {
				return proposalResp, errors.WithMessage(err, "无法组装签名交易")
			}

			var dg *DeliverGroup
			var ctx context.Context
			if waitForEvent {
				var cancelFunc context.CancelFunc
				ctx, cancelFunc = context.WithTimeout(context.Background(), waitForEventTimeout)
				defer cancelFunc()

				dg = NewDeliverGroup(
					deliverClients, // DeliverClient 提交客户端实例的切片
					peerAddresses,  // peer 节点的地址
					signer,         // 身份签名者
					certificate,    // TLS证书
					channelID,      // 通道ID
					txid,           // 交易ID
				)

				// 方法等待组中的所有交付客户端连接到对等节点的交付服务
				err := dg.Connect(ctx)
				// 不返回错误, 则标识完成背书, 可以发送到orderer
				if err != nil {
					return nil, err
				}
			}

			// 将数据广播发送到 orderer
			if err = bc.Send(env); err != nil {
				return proposalResp, errors.WithMessagef(err, "将数据发送到 orderer 时出错 %s", funcName)
			}

			if dg != nil && ctx != nil {
				// 等待包含来自所有对等体的txid的事件
				err = dg.Wait(ctx)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return proposalResp, nil
}

// DeliverGroup 是一个结构体，用于保存连接到一组节点并等待感兴趣的交易ID在所有节点的账本中被提交的所有信息。
// 当任何一个节点/传递客户端发生错误时，调用命令将返回一个错误。只会设置第一个发生的错误。
type DeliverGroup struct {
	Clients     []*DeliverClient          // 传递客户端列表
	Certificate tls.Certificate           // TLS证书
	ChannelID   string                    // 通道ID
	TxID        string                    // 交易ID
	Signer      identity.SignerSerializer // 签名者序列化器
	mutex       sync.Mutex                // 互斥锁
	Error       error                     // 错误
	wg          sync.WaitGroup            // 等待组
}

// DeliverClient holds the client/connection related to a specific
// peer. The address is included for logging purposes
type DeliverClient struct {
	Client     pb.DeliverClient
	Connection pb.Deliver_DeliverClient
	Address    string
}

// NewDeliverGroup 方法用于创建一个 DeliverGroup 提交组实例。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - deliverClients：[]pb.DeliverClient 类型，表示 DeliverClient 提交客户端实例的切片。
//   - peerAddresses：[]string 类型，表示 peer 节点的地址。
//   - signer：identity.SignerSerializer 类型，表示身份签名者。
//   - certificate：tls.Certificate 类型，表示TLS证书。
//   - channelID：string 类型，表示通道ID。
//   - txid：string 类型，表示交易ID。
//
// 返回值：
//   - *DeliverGroup：表示创建的 DeliverGroup 实例。
func NewDeliverGroup(
	deliverClients []pb.DeliverClient,
	peerAddresses []string,
	signer identity.SignerSerializer,
	certificate tls.Certificate,
	channelID string,
	txid string,
) *DeliverGroup {
	// 创建一个 DeliverClient 实例的切片
	clients := make([]*DeliverClient, len(deliverClients))

	for i, client := range deliverClients {
		address := peerAddresses[i]
		// 如果地址为空，则使用 viper.GetString("peer.address") 获取默认地址
		if address == "" {
			address = viper.GetString("peer.address")
		}
		// 创建 DeliverClient 实例
		dc := &DeliverClient{
			Client:  client,
			Address: address,
		}
		clients[i] = dc
	}

	// 创建 DeliverGroup 实例
	dg := &DeliverGroup{
		Clients:     clients,
		Certificate: certificate,
		ChannelID:   channelID,
		TxID:        txid,
		Signer:      signer,
	}

	return dg
}

// Connect 方法等待组中的所有交付客户端连接到对等节点的交付服务，
// 接收错误，或等待上下文超时。只要有一个交付客户端无法连接到其对等节点，
// 就会返回错误。
// 方法接收者：*DeliverGroup
// 输入参数：
//   - ctx：context.Context 类型，表示上下文。
//
// 返回值：
//   - error：如果有任何一个交付客户端无法连接到其对等节点，则返回错误。
func (dg *DeliverGroup) Connect(ctx context.Context) error {
	// 使用 WaitGroup 来等待所有客户端连接完成
	dg.wg.Add(len(dg.Clients))

	for _, client := range dg.Clients {
		// 启动一个 goroutine 来连接每个客户端
		go dg.ClientConnect(ctx, client)
	}

	readyCh := make(chan struct{})
	// WaitForWG 方法用于等待 deliverGroup 的等待组完成，并在完成时关闭通道。
	go dg.WaitForWG(readyCh)

	select {
	case <-readyCh: // 关闭通信标识
		// 当所有客户端连接完成时，检查是否有错误发生
		if dg.Error != nil {
			// 如果有错误发生，则返回一个带有错误信息的错误
			err := errors.WithMessage(dg.Error, "无法连接以传递信息到所有 peer 节点")
			return err
		}
	case <-ctx.Done():
		// 如果等待超时，则返回一个超时错误
		err := errors.New("等待在所有 peer 上传递信息连接时超时")
		return err
	}

	return nil
}

// ClientConnect 方法使用提供的交付客户端发送交付 seek info 消息，
// 在任何错误发生时设置 deliverGroup 的 Error 字段。
// 方法接收者：*DeliverGroup
// 输入参数：
//   - ctx：context.Context 类型，表示上下文。
//   - dc：*DeliverClient 类型，表示交付客户端。
func (dg *DeliverGroup) ClientConnect(ctx context.Context, dc *DeliverClient) {
	// 在方法结束时，通过调用 wg.Done() 来标记连接完成
	defer dg.wg.Done()

	// 通过 DeliverFiltered 方法与客户端建立连接
	df, err := dc.Client.DeliverFiltered(ctx)
	if err != nil {
		// 如果连接失败，则返回一个带有错误信息的错误
		err = errors.WithMessagef(err, "连接以传递消息过滤时出错: %s", dc.Address)
		dg.setError(err)
		return
	}
	defer df.CloseSend()

	dc.Connection = df

	// 创建 DeliverEnvelope，方法用于创建交付信封, 包含 tls 的证书hash等消息
	envelope := createDeliverEnvelope(dg.ChannelID, dg.Certificate, dg.Signer)

	// 将 DeliverEnvelope 发送到客户端
	err = df.Send(envelope)
	if err != nil {
		// 如果发送失败，则返回一个带有错误信息的错误
		err = errors.WithMessagef(err, "将传递搜索信息发送到节点 %s 时出错", dc.Address)
		dg.setError(err)
		return
	}
}

// Wait 方法用于等待组中所有交付客户端连接的结果，直到接收到包含交易ID的区块、出现错误或上下文超时。
// 方法接收者：*DeliverGroup
// 输入参数：
//   - ctx：context.Context 类型，表示上下文。
//
// 返回值：
//   - error：如果等待过程中出现错误，则返回错误。
func (dg *DeliverGroup) Wait(ctx context.Context) error {
	// 如果 DeliverGroup 提交组中没有客户端连接，则直接返回
	if len(dg.Clients) == 0 {
		return nil
	}

	// 为每个客户端连接启动一个 goroutine 进行等待
	dg.wg.Add(len(dg.Clients))
	for _, client := range dg.Clients {
		// 等待指定的交付客户端接收包含请求的交易ID的区块事件
		go dg.ClientWait(client)
	}

	// 创建一个用于通知等待组已准备好的通道
	readyCh := make(chan struct{})
	// 等待 deliverGroup 的等待组完成，并在完成时关闭通道
	go dg.WaitForWG(readyCh)

	// 等待结果
	select {
	case <-readyCh:
		// 如果出现错误，则返回错误
		if dg.Error != nil {
			return dg.Error
		}
	case <-ctx.Done():
		// 如果上下文超时，则返回超时错误
		err := errors.New("在所有 peer 上等待 txid 交易超时")
		return err
	}

	return nil
}

// ClientWait 方法用于等待指定的交付客户端接收包含请求的交易ID的区块事件。
// 方法接收者：*DeliverGroup
// 输入参数：
//   - dc：*DeliverClient 类型，表示交付客户端。
func (dg *DeliverGroup) ClientWait(dc *DeliverClient) {
	defer dg.wg.Done()
	for {
		// 从交付客户端接收响应
		resp, err := dc.Connection.Recv()
		if err != nil {
			// 如果接收过程中出现错误，则设置错误并返回
			err = errors.WithMessagef(err, "从交付客户端接收响应时出错 %s", dc.Address)
			dg.setError(err)
			return
		}

		switch r := resp.Type.(type) {
		case *pb.DeliverResponse_FilteredBlock: // 传递响应过滤块
			// 如果接收到的响应是 FilteredBlock 类型，则检查其中的交易是否包含请求的交易ID
			filteredTransactions := r.FilteredBlock.FilteredTransactions
			for _, tx := range filteredTransactions {
				if tx.Txid == dg.TxID {
					// 如果找到了请求的交易ID，则打印日志并检查交易的验证状态
					logger.Infof("txid 交易id: [%s] 已提交状态: (%s) 在节点: %s", dg.TxID, tx.TxValidationCode, dc.Address)
					if tx.TxValidationCode != pb.TxValidationCode_VALID {
						// 如果交易的验证状态不是有效，则设置错误并返回
						err = errors.Errorf("状态无效的交易: (%s)", tx.TxValidationCode)
						dg.setError(err)
					}
					return
				}
			}
		case *pb.DeliverResponse_Status: // 传递响应状态
			// 如果接收到的响应是 Status 类型，则设置错误并返回
			err = errors.Errorf("在收到 txid 交易之前交付已完成, 状态为 (%s) ", r.Status)
			dg.setError(err)
			return
		default:
			// 如果接收到的响应是其他类型，则设置错误并返回
			err = errors.Errorf("收到意外的响应类型 (%T) 从节点: %s", r, dc.Address)
			dg.setError(err)
			return
		}
	}
}

// WaitForWG 方法用于等待 deliverGroup 的等待组完成，并在完成时关闭通道。
// 方法接收者：*DeliverGroup
// 输入参数：
//   - readyCh：chan struct{} 类型，表示用于通知等待组已准备好的通道。
func (dg *DeliverGroup) WaitForWG(readyCh chan struct{}) {
	// 等待 deliverGroup 的等待组完成
	dg.wg.Wait()
	// 关闭通道，通知等待组已准备好
	close(readyCh)
}

// setError serializes an error for the deliverGroup
func (dg *DeliverGroup) setError(err error) {
	dg.mutex.Lock()
	dg.Error = err
	dg.mutex.Unlock()
}

// createDeliverEnvelope 方法用于创建交付信封。
// 方法接收者：无（全局函数）
// 输入参数：
//   - channelID：string 类型，表示通道ID。
//   - certificate：tls.Certificate 类型，表示TLS证书。
//   - signer：identity.SignerSerializer 类型，表示签名者序列化器。
//
// 返回值：
//   - *pcommon.Envelope：表示创建的交付信封。
func createDeliverEnvelope(
	channelID string,
	certificate tls.Certificate,
	signer identity.SignerSerializer,
) *pcommon.Envelope {
	var tlsCertHash []byte

	// 检查客户端证书并创建哈希 (如果存在)
	if len(certificate.Certificate) > 0 {
		tlsCertHash = util.ComputeSHA256(certificate.Certificate[0])
	}

	start := &ab.SeekPosition{
		// 指定起始位置为最新的区块
		Type: &ab.SeekPosition_Newest{
			Newest: &ab.SeekNewest{},
		},
	}

	stop := &ab.SeekPosition{
		// 指定结束位置为一个特定的区块号
		Type: &ab.SeekPosition_Specified{
			Specified: &ab.SeekSpecified{
				// 使用 math.MaxUint64 表示最大的无符号整数，即表示没有限制的最大区块号
				Number: math.MaxUint64,
			},
		},
	}

	seekInfo := &ab.SeekInfo{
		Start:    start,
		Stop:     stop,
		Behavior: ab.SeekInfo_BLOCK_UNTIL_READY, // 搜索信息块，直到准备就绪
	}

	// 函数用于创建一个带有TLS绑定的已签名信封
	env, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		pcommon.HeaderType_DELIVER_SEEK_INFO, // 信封的类型
		channelID,                            // 通道的ID
		signer,                               // 签名者
		seekInfo,                             // 要封装的数据消息
		int32(0),                             // 消息的版本
		uint64(0),                            // 通道的时期
		tlsCertHash,                          // TLS证书的哈希值
	)
	if err != nil {
		logger.Errorf("信封签名时出错: %s", err)
		return nil
	}

	return env
}
