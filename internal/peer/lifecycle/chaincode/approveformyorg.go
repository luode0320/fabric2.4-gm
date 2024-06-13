/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/internal/peer/chaincode"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ApproverForMyOrg 是一个结构体，用于保存批准链码定义所需的依赖项。
type ApproverForMyOrg struct {
	Certificate     tls.Certificate        // TLS证书
	Command         *cobra.Command         // 命令
	BroadcastClient common.BroadcastClient // 广播客户端
	DeliverClients  []pb.DeliverClient     // 提交客户端
	EndorserClients []EndorserClient       // 背书客户端
	Input           *ApproveForMyOrgInput  // 批准链码定义的输入
	Signer          Signer                 // 签名者
}

// ApproveForMyOrgInput 包含了为组织批准链码定义的所有输入参数。
// 当使用默认的背书和验证插件时，ValidationParameterBytes 是背书策略的（序列化）字节。
type ApproveForMyOrgInput struct {
	ChannelID                string                      // 通道ID
	Name                     string                      // 链码名称
	Version                  string                      // 链码版本
	PackageID                string                      // 包ID
	Sequence                 int64                       // 序列号
	EndorsementPlugin        string                      // 背书插件
	ValidationPlugin         string                      // 验证插件
	ValidationParameterBytes []byte                      // 验证参数字节
	CollectionConfigPackage  *pb.CollectionConfigPackage // 集合配置包
	InitRequired             bool                        // 是否需要初始化
	PeerAddresses            []string                    // 背书节点地址
	WaitForEvent             bool                        // 是否等待事件
	WaitForEventTimeout      time.Duration               // 等待事件超时时间
	TxID                     string                      // 交易ID
}

// Validate 方法用于验证 ApproveChaincodeDefinitionForMyOrg 提案的输入参数。
// 方法接收者：a *ApproveForMyOrgInput
// 输入参数：无
// 返回值：error，如果输入参数验证失败，则返回错误。
func (a *ApproveForMyOrgInput) Validate() error {
	// 验证通道ID是否为空
	if a.ChannelID == "" {
		return errors.New("必需的参数 '--channelID' 通道名称为空.")
	}

	// 验证链码名称是否为空
	if a.Name == "" {
		return errors.New("必需的参数 '--name' 链码名称为空.")
	}

	// 验证链码版本是否为空
	if a.Version == "" {
		return errors.New("必须的参数 '--version' 链码版本为空.")
	}

	// 验证链码序列号是否为0
	if a.Sequence == 0 {
		return errors.New("必须的参数 '--sequence' 链码版本序号为空.")
	}

	return nil
}

// ApproveForMyOrgCmd 返回 chaincode ApproveForMyOrg 批准我的组织的cobra命令
func ApproveForMyOrgCmd(a *ApproverForMyOrg, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeApproveForMyOrgCmd := &cobra.Command{
		Use:   "approveformyorg",
		Short: "批准我的组织的链码定义.",
		Long:  "批准我的组织的链码定义.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if a == nil {
				// 从CLI标志设置输入, 组织批准链码定义的所有输入参数
				input, err := a.createInput()
				if err != nil {
					return err
				}

				ccInput := &ClientConnectionsInput{
					CommandName:           cmd.Name(),                        // 命令名称
					EndorserRequired:      true,                              // 是否需要背书节点
					OrdererRequired:       true,                              // 是否需要订购节点
					ChannelID:             channelID,                         // 通道ID
					PeerAddresses:         peerAddresses,                     // 节点地址列表
					TLSRootCertFiles:      tlsRootCertFiles,                  // TLS 根证书文件列表
					ConnectionProfilePath: connectionProfilePath,             // 连接配置文件路径
					TLSEnabled:            viper.GetBool("peer.tls.enabled"), // 是否启用 TLS
				}

				cc, err := NewClientConnections(ccInput, cryptoProvider)
				if err != nil {
					return err
				}

				endorserClients := make([]EndorserClient, len(cc.EndorserClients))
				for i, e := range cc.EndorserClients {
					endorserClients[i] = e
				}

				// 用于保存批准链码定义所需的依赖项
				a = &ApproverForMyOrg{
					Command:         cmd,                // 命令
					Input:           input,              // 批准链码定义的输入
					Certificate:     cc.Certificate,     // TLS证书
					BroadcastClient: cc.BroadcastClient, // 广播客户端
					DeliverClients:  cc.DeliverClients,  // 提交客户端
					EndorserClients: endorserClients,    // 背书客户端
					Signer:          cc.Signer,          // 签名者
				}
			}
			// 提交一个 ApproveChaincodeDefinitionForMyOrg 批准我的组织的链码定义提案
			return a.Approve()
		},
	}
	flagList := []string{
		"channelID",          // 应执行此命令的 channelID 通道
		"name",               // 链码的 name 名称
		"version",            // 链码的 version 版本
		"package-id",         // 链码安装包的 package-id 标识符
		"sequence",           // 通道的链码定义的序列号
		"endorsement-plugin", // 要用于此链码的背书插件的名称
		"validation-plugin",  // 要用于此链码的验证插件的名称
		"signature-policy",
		"channel-config-policy",
		"init-required", // 链码是否需要调用 'init' 初始化函数
		"collections-config",
		"peerAddresses",       // 要连接到的 peer 的地址(数组)
		"tlsRootCertFiles",    // 如果启用了TLS, 则要配置连接的 peer 的TLS根证书文件的路径, 指定的证书的顺序和数量应与 --peerAddresses 标志匹配(数组)
		"connectionProfile",   // 连接的配置文件路径, 该路径为网络提供必要的连接信息. 注意: 目前仅支持提供 peer 的连接信息
		"waitForEvent",        // 是否等待来自每个 peer 的交付服务的事件, 表示事务已成功提交
		"waitForEventTimeout", // 等待来自每个 peer 的交付服务的事件的时间, 该事件表示 '调用' 事务已成功提交
	}
	attachFlags(chaincodeApproveForMyOrgCmd, flagList)

	return chaincodeApproveForMyOrgCmd
}

// Approve 方法用于提交一个 ApproveChaincodeDefinitionForMyOrg 批准我的组织的链码定义提案。
// 方法接收者：a *ApproverForMyOrg
// 输入参数：无
// 返回值：error，如果提交提案时出错，则返回错误。
func (a *ApproverForMyOrg) Approve() error {
	// 验证参数 通道名称、链码名称、链码版本、链码序列号
	err := a.Input.Validate()
	if err != nil {
		return err
	}

	if a.Command != nil {
		// 命令行的解析完成，所以沉默cmd的使用
		a.Command.SilenceUsage = true
	}

	// 方法用于创建一个提案（proposal）
	proposal, txID, err := a.createProposal(a.Input.TxID)
	if err != nil {
		return errors.WithMessage(err, "未能创建批准我的组织的链码定义的提案")
	}

	signedProposal, err := signProposal(proposal, a.Signer)
	if err != nil {
		return errors.WithMessage(err, "创建签名的批准我的组织的链码定义失败")
	}

	logger.Infof("向背书节点发送批准链码提案...")
	var responses []*pb.ProposalResponse
	for _, endorser := range a.EndorserClients {
		// 向背书节点发送提案
		proposalResponse, err := endorser.ProcessProposal(context.Background(), signedProposal)
		if err != nil {
			return errors.WithMessage(err, "发送批准我的组织的链码出错")
		}
		responses = append(responses, proposalResponse)
	}

	if len(responses) == 0 {
		// 由于编程错误，这应该是空的
		return errors.New("批准我的组织未收到提案回复")
	}

	// 创建签名事务时，将检查所有响应。
	// 现在，只需设置此设置，以便我们检查第一个响应的状态
	proposalResponse := responses[0]

	if proposalResponse == nil {
		return errors.New("批准我的组织收到零提案回复")
	}

	if proposalResponse.Response == nil {
		return errors.Errorf("批准我的组织收到提案回复, 无回复")
	}

	if proposalResponse.Response.Status != int32(cb.Status_SUCCESS) {
		return errors.Errorf("批准我的组织提案失败, 状态为: %d - %s", proposalResponse.Response.Status, proposalResponse.Response.Message)
	}

	// 根据提案、背书和签名者组装一个 Envelope(带有签名的有效负载 ，以便可以对消息进行身份验证) 消息
	env, err := protoutil.CreateSignedTx(proposal, a.Signer, responses...)
	if err != nil {
		return errors.WithMessage(err, "无法创建已签名的交易记录")
	}

	// 用于保存连接到 一组节点 并等待感兴趣的交易ID在 所有节点 的账本中被 提交 的信息
	var dg *chaincode.DeliverGroup
	var ctx context.Context

	// 是否等待事件
	if a.Input.WaitForEvent {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(context.Background(), a.Input.WaitForEventTimeout)
		defer cancelFunc()

		// 创建一个 DeliverGroup 提交组实例
		dg = chaincode.NewDeliverGroup(
			a.DeliverClients,      // 提交客户端实例的切片
			a.Input.PeerAddresses, // peer 节点的地址
			a.Signer,              // 身份签名者
			a.Certificate,         // TLS证书
			a.Input.ChannelID,     // 通道ID
			txID,                  // 交易id
		)

		// 连接以在所有对等节点上提供服务
		err := dg.Connect(ctx)
		if err != nil {
			return err
		}
	}

	logger.Infof("向 orderer 发送批准链码广播...")
	// 向 orderer 发送广播
	if err = a.BroadcastClient.Send(env); err != nil {
		return errors.WithMessage(err, "向 orderer 发送广播事务失败")
	}

	if dg != nil && ctx != nil {
		// 等待组中所有交付客户端连接的结果，直到接收到包含交易ID的区块、出现错误或上下文超时
		err = dg.Wait(ctx)
		if err != nil {
			return err
		}
	}

	logger.Infof("批准链码完成...")

	return err
}

// createInput 方法根据 CLI 标志创建输入结构体。
// 方法接收者：a（ApproverForMyOrg类型的指针）
// 返回值：
//   - *ApproveForMyOrgInput：表示为组织批准链码定义的所有输入参数的结构体。
//   - error：如果创建输入时出错，则返回错误。
func (a *ApproverForMyOrg) createInput() (*ApproveForMyOrgInput, error) {
	// 创建策略字节, 签名策略 + 通道配置策略
	policyBytes, err := createPolicyBytes(signaturePolicy, channelConfigPolicy)
	if err != nil {
		return nil, err
	}

	// 创建集合配置包, 集合配置文件
	ccp, err := createCollectionConfigPackage(collectionsConfigFile)
	if err != nil {
		return nil, err
	}

	// ApproveForMyOrgInput 包含了为组织批准链码定义的所有输入参数。
	// 当使用默认的背书和验证插件时，ValidationParameterBytes 是背书策略的（序列化）字节。
	input := &ApproveForMyOrgInput{
		ChannelID:                channelID,           // 通道ID
		Name:                     chaincodeName,       // 链码名称
		Version:                  chaincodeVersion,    // 链码版本
		PackageID:                packageID,           // 包ID
		Sequence:                 int64(sequence),     // 序列号
		EndorsementPlugin:        endorsementPlugin,   // 背书插件
		ValidationPlugin:         validationPlugin,    // 验证插件
		ValidationParameterBytes: policyBytes,         // 验证参数字节
		InitRequired:             initRequired,        // 是否需要初始化
		CollectionConfigPackage:  ccp,                 // 集合配置包
		PeerAddresses:            peerAddresses,       // 背书节点地址
		WaitForEvent:             waitForEvent,        // 是否等待事件
		WaitForEventTimeout:      waitForEventTimeout, // 等待事件超时时间
	}

	return input, nil
}

// createProposal 方法用于创建一个提案（proposal）。
// 方法接收者：a *ApproverForMyOrg
// 输入参数：
//   - inputTxID：string 类型，表示输入的交易ID。
//
// 返回值：
//   - proposal：*pb.Proposal 类型，表示创建的提案。
//   - txID：string 类型，表示交易ID。
//   - err：error 类型，如果创建提案时出错，则返回错误。
func (a *ApproverForMyOrg) createProposal(inputTxID string) (proposal *pb.Proposal, txID string, err error) {
	if a.Signer == nil {
		return nil, "", errors.New("提供的签名者未nil")
	}

	var ccsrc *lb.ChaincodeSource
	if a.Input.PackageID != "" {
		ccsrc = &lb.ChaincodeSource{
			Type: &lb.ChaincodeSource_LocalPackage{ // 可有效分配给类型的类型
				LocalPackage: &lb.ChaincodeSource_Local{
					PackageId: a.Input.PackageID,
				},
			},
		}
	} else {
		ccsrc = &lb.ChaincodeSource{
			Type: &lb.ChaincodeSource_Unavailable_{
				Unavailable: &lb.ChaincodeSource_Unavailable{}, // 不可用
			},
		}
	}

	// 用作 '_lifecycle.ApproveChaincodeDefinitionForMyOrg' 批准我的组织Args的链码定义的参数的消息
	args := &lb.ApproveChaincodeDefinitionForMyOrgArgs{
		Name:                a.Input.Name,                     // 链码名称
		Version:             a.Input.Version,                  // 链码版本
		Sequence:            a.Input.Sequence,                 // 链码序列号
		EndorsementPlugin:   a.Input.EndorsementPlugin,        // 背书插件
		ValidationPlugin:    a.Input.ValidationPlugin,         // 验证插件
		ValidationParameter: a.Input.ValidationParameterBytes, // 验证参数
		InitRequired:        a.Input.InitRequired,             // 是否需要初始化
		Collections:         a.Input.CollectionConfigPackage,  // 集合配置包
		Source:              ccsrc,                            // 链码源代码(PackageID)
	}

	argsBytes, err := proto.Marshal(args)
	if err != nil {
		return nil, "", err
	}

	// 包含chaincode函数及其参数
	ccInput := &pb.ChaincodeInput{Args: [][]byte{[]byte(approveFuncName), argsBytes}}

	// 包含chaincode函数及其参数
	cis := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			ChaincodeId: &pb.ChaincodeID{Name: lifecycleName},
			Input:       ccInput,
		},
	}

	creatorBytes, err := a.Signer.Serialize()
	if err != nil {
		return nil, "", errors.WithMessage(err, "无法序列化标识")
	}

	// 方法根据给定的输入创建一个提案
	proposal, txID, err = protoutil.CreateChaincodeProposalWithTxIDAndTransient(cb.HeaderType_ENDORSER_TRANSACTION, a.Input.ChannelID, cis, creatorBytes, inputTxID, nil)
	if err != nil {
		return nil, "", errors.WithMessage(err, "无法创建 ChaincodeInvocationSpec 提案")
	}

	return proposal, txID, nil
}
