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

// Committer 是一个结构体，用于保存提交链码所需的依赖项。
type Committer struct {
	Certificate     tls.Certificate        // TLS证书
	Command         *cobra.Command         // 命令
	BroadcastClient common.BroadcastClient // 广播客户端
	EndorserClients []EndorserClient       // 背书客户端
	DeliverClients  []pb.DeliverClient     // 提交客户端
	Input           *CommitInput           // 提交链码的输入
	Signer          Signer                 // 签名者
}

// CommitInput 是一个结构体，用于保存提交链码定义的所有输入参数。
// 当使用默认的背书和验证插件时，ValidationParameterBytes 是（序列化的）背书策略。
type CommitInput struct {
	ChannelID                string                      // 通道ID
	Name                     string                      // 链码名称
	Version                  string                      // 链码版本
	Hash                     []byte                      // 链码哈希
	Sequence                 int64                       // 链码序列号
	EndorsementPlugin        string                      // 背书插件
	ValidationPlugin         string                      // 验证插件
	ValidationParameterBytes []byte                      // 验证参数
	CollectionConfigPackage  *pb.CollectionConfigPackage // 集合配置包
	InitRequired             bool                        // 是否需要初始化
	PeerAddresses            []string                    // 节点地址列表
	WaitForEvent             bool                        // 是否等待事件
	WaitForEventTimeout      time.Duration               // 等待事件超时时间
	TxID                     string                      // 交易ID
}

// Validate Commitchaincodefinition 提交链码定义提案的输入
func (c *CommitInput) Validate() error {
	if c.ChannelID == "" {
		return errors.New("所需的参数 '--channelID' 通道名称为空. ")
	}

	if c.Name == "" {
		return errors.New("所需的参数 '--name' 链码名称为空. ")
	}

	if c.Version == "" {
		return errors.New("所需的参数 '--version' 链码版本为空. ")
	}

	if c.Sequence == 0 {
		return errors.New("所需的参数 '--sequence' 链码序列号为空. ")
	}

	return nil
}

// CommitCmd 返回chaincode Commit的cobra命令
func CommitCmd(c *Committer, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeCommitCmd := &cobra.Command{
		Use:   "commit",
		Short: "在通道上提交链码定义.",
		Long:  "在通道上提交链码定义.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if c == nil {
				// 从CLI标志设置输入
				input, err := c.createInput()
				if err != nil {
					return err
				}

				//  保存创建客户端连接的输入参数
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

				// 输入参数创建一个新的客户端连接集合
				cc, err := NewClientConnections(ccInput, cryptoProvider)
				if err != nil {
					return err
				}

				endorserClients := make([]EndorserClient, len(cc.EndorserClients))
				for i, e := range cc.EndorserClients {
					endorserClients[i] = e
				}

				// 保存提交链码所需的依赖项
				c = &Committer{
					Command:         cmd,                // 命令
					Input:           input,              // 提交链码的输入
					Certificate:     cc.Certificate,     // TLS证书
					BroadcastClient: cc.BroadcastClient, // 广播客户端
					DeliverClients:  cc.DeliverClients,  // 提交客户端
					EndorserClients: endorserClients,    // 背书客户端
					Signer:          cc.Signer,          // 签名者
				}
			}
			return c.Commit()
		},
	}
	flagList := []string{
		"channelID",
		"name",
		"version",
		"sequence",
		"endorsement-plugin",
		"validation-plugin",
		"signature-policy",
		"channel-config-policy",
		"init-required",
		"collections-config",
		"peerAddresses",
		"tlsRootCertFiles",
		"connectionProfile",
		"waitForEvent",
		"waitForEventTimeout",
	}
	attachFlags(chaincodeCommitCmd, flagList)

	return chaincodeCommitCmd
}

// Commit 提交 CommitChaincodeDefinition 提交链码定义
func (c *Committer) Commit() error {
	// 检查 通道名称、链码名称、链码版本、链码序列号 参数
	err := c.Input.Validate()
	if err != nil {
		return err
	}

	if c.Command != nil {
		// 命令行的解析完成，所以沉默cmd的使用
		c.Command.SilenceUsage = true
	}

	// 方法用于创建提交链码定义提案
	proposal, txID, err := c.createProposal(c.Input.TxID)
	if err != nil {
		return errors.WithMessage(err, "未能创建提交链码定义提案")
	}

	// 签名提交链码定义提案
	signedProposal, err := signProposal(proposal, c.Signer)
	if err != nil {
		return errors.WithMessage(err, "创建签名的提交链码定义提案失败")
	}

	logger.Infof("向背书节点发送向通道提交更新链码的提案...")
	var responses []*pb.ProposalResponse
	for _, endorser := range c.EndorserClients {
		// 发送提案
		proposalResponse, err := endorser.ProcessProposal(context.Background(), signedProposal)
		if err != nil {
			return errors.WithMessage(err, "提交链码定义提案出错")
		}
		responses = append(responses, proposalResponse)
	}

	if len(responses) == 0 {
		return errors.New("未收到提交链码定义提案回复")
	}

	// 创建签名事务时，将检查所有响应。
	// 现在，只需设置此设置，以便我们检查第一个响应的状态
	proposalResponse := responses[0]

	if proposalResponse == nil {
		return errors.New("收到零个提交链码定义提案回复")
	}

	if proposalResponse.Response == nil {
		return errors.New("收到提交链码定义提案回复, 但回复为nil")
	}

	if proposalResponse.Response.Status != int32(cb.Status_SUCCESS) {
		return errors.Errorf("提交链码定义提案失败，状态为: %d - %s", proposalResponse.Response.Status, proposalResponse.Response.Message)
	}

	// 组装一个签名的交易 (它是一个信封消息)
	env, err := protoutil.CreateSignedTx(proposal, c.Signer, responses...)
	if err != nil {
		return errors.WithMessage(err, "无法签名 提交链码定义提案 + 提案结果")
	}

	var dg *chaincode.DeliverGroup
	var ctx context.Context
	if c.Input.WaitForEvent {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(context.Background(), c.Input.WaitForEventTimeout)
		defer cancelFunc()

		dg = chaincode.NewDeliverGroup(
			c.DeliverClients,      // 提交客户端实例的切片
			c.Input.PeerAddresses, // peer 节点的地址
			c.Signer,              // 身份签名者
			c.Certificate,         // TLS证书
			c.Input.ChannelID,     // 通道ID
			txID,                  // 交易ID
		)
		// 连接以在所有对等节点上提供服务, 等待组中的所有交付客户端连接到对等节点的交付服务
		err := dg.Connect(ctx)
		if err != nil {
			return err
		}
	}

	logger.Infof("向 orderer 发送向通道提交更新链码的广播...")
	// 发送广播
	if err = c.BroadcastClient.Send(env); err != nil {
		return errors.WithMessage(err, "广播 提交链码定义提案 + 提案结果 失败")
	}

	if dg != nil && ctx != nil {
		// 等待包含来自所有对等体的txID的事件
		err = dg.Wait(ctx)
		if err != nil {
			return err
		}
	}

	logger.Infof("向通道提交更新链码完成...")
	return err
}

// createInput 方法根据 CLI 标志创建输入结构体。
// 方法接收者：*Committer
// 返回值：
//   - *CommitInput：表示创建的输入结构体。
//   - error：如果创建输入结构体时出现错误，则返回错误。
func (c *Committer) createInput() (*CommitInput, error) {
	// 创建策略字节码
	policyBytes, err := createPolicyBytes(signaturePolicy, channelConfigPolicy)
	if err != nil {
		return nil, err
	}

	// 创建集合配置包
	ccp, err := createCollectionConfigPackage(collectionsConfigFile)
	if err != nil {
		return nil, err
	}

	// 创建 CommitInput 结构体
	input := &CommitInput{
		ChannelID:                channelID,           // 通道ID
		Name:                     chaincodeName,       // 链码名称
		Version:                  chaincodeVersion,    // 链码版本
		Sequence:                 int64(sequence),     // 链码序列号
		EndorsementPlugin:        endorsementPlugin,   // 背书插件
		ValidationPlugin:         validationPlugin,    // 验证插件
		ValidationParameterBytes: policyBytes,         // 验证参数
		InitRequired:             initRequired,        // 是否需要初始化
		CollectionConfigPackage:  ccp,                 // 集合配置包
		PeerAddresses:            peerAddresses,       // 节点地址列表
		WaitForEvent:             waitForEvent,        // 是否等待事件
		WaitForEventTimeout:      waitForEventTimeout, // 等待事件超时时间
	}

	return input, nil
}

// createProposal 方法用于创建提案（proposal）。
// 方法接收者：*Committer
// 输入参数：
//   - inputTxID：string 类型，表示输入的交易ID。
//
// 返回值：
//   - *pb.Proposal：表示创建的提案。
//   - string：表示交易ID。
//   - error：如果创建提案时出现错误，则返回错误。
func (c *Committer) createProposal(inputTxID string) (proposal *pb.Proposal, txID string, err error) {
	args := &lb.CommitChaincodeDefinitionArgs{
		Name:                c.Input.Name,                     // 链码名称
		Version:             c.Input.Version,                  // 链码版本
		Sequence:            c.Input.Sequence,                 // 链码序列号
		EndorsementPlugin:   c.Input.EndorsementPlugin,        // 背书插件
		ValidationPlugin:    c.Input.ValidationPlugin,         // 验证插件
		ValidationParameter: c.Input.ValidationParameterBytes, // 验证参数
		InitRequired:        c.Input.InitRequired,             // 是否需要初始化
		Collections:         c.Input.CollectionConfigPackage,  // 集合配置包
	}

	argsBytes, err := proto.Marshal(args)
	if err != nil {
		return nil, "", err
	}
	ccInput := &pb.ChaincodeInput{Args: [][]byte{[]byte(commitFuncName), argsBytes}}

	// 包含chaincode函数及其参数
	cis := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			ChaincodeId: &pb.ChaincodeID{Name: lifecycleName}, // 链码名称
			Input:       ccInput,                              // 操作链码参数
		},
	}

	// 获取签名者的序列值
	creatorBytes, err := c.Signer.Serialize()
	if err != nil {
		return nil, "", errors.WithMessage(err, "无法序列化签名者标识")
	}

	// 方法根据给定的输入创建一个提案
	proposal, txID, err = protoutil.CreateChaincodeProposalWithTxIDAndTransient(cb.HeaderType_ENDORSER_TRANSACTION, c.Input.ChannelID, cis, creatorBytes, inputTxID, nil)
	if err != nil {
		return nil, "", errors.WithMessage(err, "无法创建 ChaincodeInvocationSpec 提交链码定义提案")
	}

	return proposal, txID, nil
}
