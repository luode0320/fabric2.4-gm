/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"context"
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/core/chaincode/persistence"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"path/filepath"
)

// Reader defines the interface needed for reading a file.
type Reader interface {
	ReadFile(string) ([]byte, error)
}

// Installer 保存安装链码所需的依赖项。
type Installer struct {
	Command        *cobra.Command // 命令
	EndorserClient EndorserClient // 背书客户端
	Input          *InstallInput  // 安装输入参数
	Reader         Reader         // 读取器
	Signer         Signer         // 签名者
}

// InstallInput 安装包的文件路径
type InstallInput struct {
	PackageFile string
}

// Validate 验证检查所需的安装参数
func (i *InstallInput) Validate() error {
	if i.PackageFile == "" {
		return errors.New("必须提供链码安装包")
	}

	return nil
}

// InstallCmd 返回链码安装 chaincode install 的cobra命令。
func InstallCmd(i *Installer, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeInstallCmd := &cobra.Command{
		Use:       "install",
		Short:     "安装链码.",
		Long:      "在peer上安装链码.",
		ValidArgs: []string{"1"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if i == nil {
				// 保存创建客户端连接的输入参数
				ccInput := &ClientConnectionsInput{
					CommandName:           cmd.Name(),                        // 命令名称
					EndorserRequired:      true,                              // 需要背书节点
					PeerAddresses:         peerAddresses,                     // 节点地址列表
					TLSRootCertFiles:      tlsRootCertFiles,                  // TLS 根证书文件列表
					ConnectionProfilePath: connectionProfilePath,             // 连接配置文件路径
					TargetPeer:            targetPeer,                        // 目标节点
					TLSEnabled:            viper.GetBool("peer.tls.enabled"), // 是否启用 TLS
				}

				// 根据输入参数创建一个新的客户端连接集合
				c, err := NewClientConnections(ccInput, cryptoProvider)
				if err != nil {
					return err
				}

				// install目前仅支持一个对等方，因此只需使用
				// 保存安装链码所需的依赖项, 第一个背书人客户端
				i = &Installer{
					Command:        cmd,                         // 命令
					EndorserClient: c.EndorserClients[0],        // 背书客户端
					Reader:         &persistence.FilesystemIO{}, // 读取器
					Signer:         c.Signer,                    // 签名者
				}
			}

			// 安装链码
			return i.InstallChaincode(args)
		},
	}
	flagList := []string{
		"peerAddresses",
		"tlsRootCertFiles",
		"connectionProfile",
		"targetPeer",
	}
	attachFlags(chaincodeInstallCmd, flagList)

	return chaincodeInstallCmd
}

// InstallChaincode 安装链码。
// 方法接收者：i（Installer类型的指针）
// 输入参数：
//   - args：命令行参数。
//
// 返回值：
//   - error：如果在安装链码时出错，则返回错误。
func (i *Installer) InstallChaincode(args []string) error {
	// 如果命令不为空，则静默使用命令的用法
	if i.Command != nil {
		i.Command.SilenceUsage = true
	}

	// 设置输入参数
	i.setInput(args)

	// 执行安装操作, 使用_lifecycle安装链码
	return i.Install()
}

func (i *Installer) setInput(args []string) {
	i.Input = &InstallInput{}

	if len(args) > 0 {
		i.Input.PackageFile = args[0]
	}
}

// Install 使用_lifecycle安装链码。
// 方法接收者：i（Installer类型的指针）
// 输入参数：无
// 返回值：
//   - error：如果在安装链码时出错，则返回错误。
func (i *Installer) Install() error {
	// 验证输入参数的有效性
	err := i.Input.Validate()
	if err != nil {
		return err
	}

	// 读取链码包的字节数据
	path := config.TranslatePath(filepath.Dir(viper.ConfigFileUsed()), i.Input.PackageFile)
	pkgBytes, err := i.Reader.ReadFile(path)
	if err != nil {
		return errors.WithMessagef(err, "无法读取位于 '%s' 的链码包, 链码包使用绝对路径", i.Input.PackageFile)
	}

	// 序列化签名者
	serializedSigner, err := i.Signer.Serialize()
	if err != nil {
		return errors.Wrap(err, "无法序列化签名者")
	}

	// 创建安装链码提案
	proposal, err := i.createInstallProposal(pkgBytes, serializedSigner)
	if err != nil {
		return err
	}

	// 对提案进行签名
	signedProposal, err := signProposal(proposal, i.Signer)
	if err != nil {
		return errors.WithMessage(err, "链码安装对提案解析签名是出错")
	}

	// 提交安装提案
	return i.submitInstallProposal(signedProposal)
}

// submitInstallProposal 提交安装提案。
// 方法接收者：i（Installer类型的指针）
// 输入参数：
//   - signedProposal：签名后的提案。
//
// 返回值：
//   - error：如果在提交安装提案时出错，则返回错误。
func (i *Installer) submitInstallProposal(signedProposal *pb.SignedProposal) error {
	// 使用EndorserClient处理提案, 向背书节点发送提案
	proposalResponse, err := i.EndorserClient.ProcessProposal(context.Background(), signedProposal)
	if err != nil {
		return errors.WithMessage(err, "向背书节点发送提案出错, 无法认可链码安装")
	}

	// 检查提案响应是否为nil
	if proposalResponse == nil {
		return errors.New("链码安装失败: 收到提案的响应 proposalResponse 为nil")
	}

	// 检查提案响应的响应是否为nil
	if proposalResponse.Response == nil {
		return errors.New("链码安装失败: 收到提案的响应 proposalResponse.Response 为nil")
	}

	// 检查提案响应的状态是否为成功
	if proposalResponse.Response.Status != int32(cb.Status_SUCCESS) {
		return errors.Errorf("链码安装失败, 状态为: %d - %s", proposalResponse.Response.Status, proposalResponse.Response.Message)
	}
	logger.Infof("安装链码: %v", proposalResponse)

	// 反序列化提案响应的响应负载, 是 '_lifecycle.Installchaincode' 返回的消息
	icr := &lb.InstallChaincodeResult{}
	err = proto.Unmarshal(proposalResponse.Response.Payload, icr)
	if err != nil {
		return errors.Wrap(err, "无法反序列化提案响应 lb.InstallChaincodeResult 有效负载")
	}
	logger.Infof("链码包标识PackageId: %s", icr.PackageId)

	return nil
}

// createInstallProposal 创建安装提案。
// 方法接收者：i（Installer类型的指针）
// 输入参数：
//   - pkgBytes：链码包的字节数据。
//   - creatorBytes：签名者的字节数据。
//
// 返回值：
//   - *pb.Proposal：安装提案。
//   - error：如果在创建安装提案时出错，则返回错误。
func (i *Installer) createInstallProposal(pkgBytes []byte, creatorBytes []byte) (*pb.Proposal, error) {
	// 创建 InstallChaincodeArgs , InstallChaincodeArgs是用作 “_lifecycle.Installchaincode” 参数的消息
	installChaincodeArgs := &lb.InstallChaincodeArgs{
		ChaincodeInstallPackage: pkgBytes, // 链码安装包
	}

	// 序列化 InstallChaincodeArgs
	installChaincodeArgsBytes, err := proto.Marshal(installChaincodeArgs)
	if err != nil {
		return nil, errors.Wrap(err, "无法序列化 InstallChaincodeArgs 安装链码参数")
	}

	// 创建 ChaincodeInput , 包含chaincode函数及其参数
	ccInput := &pb.ChaincodeInput{Args: [][]byte{[]byte("InstallChaincode"), installChaincodeArgsBytes}}

	// 创建 ChaincodeInvocationSpec , 包含chaincode函数及其参数
	cis := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{ // 携带链码规范。这是定义链码所需的实际元数据。
			ChaincodeId: &pb.ChaincodeID{Name: lifecycleName}, // 链码id, 这里使用lifecycle, 操作系统链码
			Input:       ccInput,
		},
	}

	// 创建提案
	proposal, _, err := protoutil.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, "", cis, creatorBytes)
	if err != nil {
		return nil, errors.WithMessage(err, "无法为安装链码规则 ChaincodeInvocationSpec 创建提案")
	}

	return proposal, nil
}
