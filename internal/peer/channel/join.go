/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"context"
	"errors"
	"fmt"
	pcommon "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/scc/cscc"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"path/filepath"
)

const commandDescription = "将 peer 节点加入到 channel 通道."

func joinCmd(cf *ChannelCmdFactory) *cobra.Command {
	// 设置channel start命令上的标志。
	joinCmd := &cobra.Command{
		Use:   "join",
		Short: commandDescription,
		Long:  commandDescription,
		RunE: func(cmd *cobra.Command, args []string) error {
			return join(cmd, args, cf)
		},
	}
	flagList := []string{
		"blockpath",
	}
	attachFlags(joinCmd, flagList)

	return joinCmd
}

// GBFileNotFoundErr genesis block file not found
type GBFileNotFoundErr string

func (e GBFileNotFoundErr) Error() string {
	return fmt.Sprintf("genesis block file not found %s", string(e))
}

// ProposalFailedErr proposal failed
type ProposalFailedErr string

func (e ProposalFailedErr) Error() string {
	return fmt.Sprintf("proposal failed (err: %s)", string(e))
}

// getJoinCCSpec 返回一个用于加入通道的链码规范。
// 返回值：
//   - *pb.ChaincodeSpec：用于加入通道的链码规范。
//   - error：如果获取创世区块文件失败，则返回错误；否则返回nil。
func getJoinCCSpec() (*pb.ChaincodeSpec, error) {
	if genesisBlockPath == common.UndefinedParamValue {
		// 如果创世区块文件路径未定义，则返回错误
		return nil, errors.New("必须提供 -blockpath block块文件的命令参数配置")
	}

	// 读取创世区块文件
	path := config.TranslatePath(filepath.Dir(viper.ConfigFileUsed()), genesisBlockPath)
	gb, err := ioutil.ReadFile(path)
	if err != nil {
		// 如果读取创世区块文件失败，则返回错误
		return nil, GBFileNotFoundErr(err.Error())
	}

	// 构建链码输入
	input := &pb.ChaincodeInput{Args: [][]byte{[]byte(cscc.JoinChain), gb}}

	// 构建链码规范
	// cscc链码的主要作用是提供对网络配置的查询和操作功能。它可以用于执行以下操作：
	//
	// 1. 加入通道：cscc链码允许节点加入一个已存在的通道，以便参与该通道的交易和共识过程。
	// 2. 获取通道配置：cscc链码可以返回指定通道的当前配置信息，包括通道的成员、背书策略、访问控制规则等。
	// 3. 获取通道的锚节点：cscc链码可以返回指定通道的锚节点列表，这些锚节点用于在通道之间进行通信。
	// 4. 获取通道的背书策略：cscc链码可以返回指定通道的背书策略，该策略定义了哪些节点可以对交易进行背书。
	// 5. 获取通道的访问控制规则：cscc链码可以返回指定通道的访问控制规则，该规则定义了哪些身份可以访问通道的资源。
	spec := &pb.ChaincodeSpec{
		Type:        pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]), // 这意味着链码是用Go语言编写的
		ChaincodeId: &pb.ChaincodeID{Name: "cscc"},                                // 设置链码的名称和版本号
		Input:       input,
	}

	return spec, nil
}

// executeJoin 执行加入通道的操作。
// 输入参数：
//   - cf：通道命令工厂。
//   - spec：用于加入通道的链码规范。
//
// 返回值：
//   - error：如果执行加入通道操作过程中出现错误，则返回错误；否则返回nil。
func executeJoin(cf *ChannelCmdFactory, spec *pb.ChaincodeSpec) (err error) {
	// 构建 ChaincodeInvocationSpec 消息, 包含chaincode函数及其参数
	invocation := &pb.ChaincodeInvocationSpec{ChaincodeSpec: spec}

	// 序列化签名者的身份, 转换为字节(mspid + 证书内容)
	creator, err := cf.Signer.Serialize()
	if err != nil {
		return fmt.Errorf("序列化的签名实例时出错 %s: %s", cf.Signer.GetIdentifier(), err)
	}

	// 创建加入通道的提案(提案的头部, 负载数据)
	var prop *pb.Proposal
	prop, _, err = protoutil.CreateProposalFromCIS(pcommon.HeaderType_CONFIG, "", invocation, creator)
	if err != nil {
		return fmt.Errorf("创建加入通道提案时出错 %s", err)
	}

	// 创建带有签名的提案
	var signedProp *pb.SignedProposal
	// 根据提案消息和签名身份返回一个已签名的提案。
	signedProp, err = protoutil.GetSignedProposal(prop, cf.Signer)
	if err != nil {
		return fmt.Errorf("创建已签名的提案时出错 %s", err)
	}

	// 处理提案并获取提案响应
	// 提案响应从背书人返回给提案提交者。
	// 这个想法是，这个消息包含了背书人对客户请求在链码上 (或者更一般地在账本上) 执行操作的响应；
	// 响应可能是成功/错误 (在响应字段中传达) 以及动作的描述和该背书人在其上的签名。
	// 如果有足够数量的不同背书人同意同一动作并为此产生签名，则可以生成交易并发送给排序服务。
	var proposalResp *pb.ProposalResponse
	proposalResp, err = cf.EndorserClient.ProcessProposal(context.Background(), signedProp)
	if err != nil {
		return ProposalFailedErr(err.Error())
	}

	if proposalResp == nil {
		return ProposalFailedErr("执行加入通道提案请求失败")
	}

	if proposalResp.Response.Status != 0 && proposalResp.Response.Status != 200 {
		return ProposalFailedErr(fmt.Sprintf("错误的提案响应 %d: %s", proposalResp.Response.Status, proposalResp.Response.Message))
	}
	logger.Info("已成功提交加入通道的提案")
	logger.Infof("加入通道完成")
	return nil
}

// join 用于执行加入通道的操作。
// 方法接收者：无（函数）
// 输入参数：
//   - cmd：命令对象。
//   - args：命令行参数。
//   - cf：通道命令工厂对象。
//
// 返回值：
//   - error：执行过程中的错误，如果没有错误则为nil。
func join(cmd *cobra.Command, args []string, cf *ChannelCmdFactory) error {
	// 检查是否提供了创世区块路径
	if genesisBlockPath == common.UndefinedParamValue {
		return errors.New("必须提供创世区块 -blockpath block块文件配置")
	}
	// 解析命令行参数完成后，静默显示命令的使用信息
	cmd.SilenceUsage = true

	var err error
	// 如果通道命令工厂对象为空，则初始化一个默认的通道命令工厂对象
	if cf == nil {
		// 初始化 背书/提交/排序 节点连接
		cf, err = InitCmdFactory(EndorserRequired, PeerDeliverNotRequired, OrdererNotRequired)
		if err != nil {
			return err
		}
	}

	// 返回一个用于加入通道的链码规范。(使用 cscc 加入通道链码, 指定 cscc 的链码类型是go语言)
	spec, err := getJoinCCSpec()
	if err != nil {
		return err
	}

	// 执行加入通道的操作
	return executeJoin(cf, spec)
}
