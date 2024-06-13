/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/policydsl"
	"github.com/hyperledger/fabric/internal/peer/chaincode"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// EndorserClient 定义了向背书节点发送提案的接口。
type EndorserClient interface {
	ProcessProposal(ctx context.Context, in *pb.SignedProposal, opts ...grpc.CallOption) (*pb.ProposalResponse, error)
}

// PeerDeliverClient defines the interface for a peer deliver client
type PeerDeliverClient interface {
	Deliver(ctx context.Context, opts ...grpc.CallOption) (pb.Deliver_DeliverClient, error)
	DeliverFiltered(ctx context.Context, opts ...grpc.CallOption) (pb.Deliver_DeliverClient, error)
}

// Signer 定义了签名消息所需的接口。
type Signer interface {
	Sign(msg []byte) ([]byte, error) // 对消息进行签名
	Serialize() ([]byte, error)      // 序列化签名器
}

// Writer defines the interface needed for writing a file
type Writer interface {
	WriteFile(string, string, []byte) error
}

// signProposal 对提案进行签名。
// 方法接收者：无
// 输入参数：
//   - proposal：待签名的提案。
//   - signer：签名者。
//
// 返回值：
//   - *pb.SignedProposal：签名后的提案。
//   - error：如果在签名提案时出错，则返回错误。
func signProposal(proposal *pb.Proposal, signer Signer) (*pb.SignedProposal, error) {
	// 检查参数是否为nil
	if proposal == nil {
		return nil, errors.New("提案内容不能为nil")
	}

	if signer == nil {
		return nil, errors.New("签名者不存在")
	}

	// 将提案序列化为字节数据
	proposalBytes, err := proto.Marshal(proposal)
	if err != nil {
		return nil, errors.Wrap(err, "提案序列化为字节数据出错")
	}

	// 对提案字节数据进行签名
	signature, err := signer.Sign(proposalBytes)
	if err != nil {
		return nil, err
	}

	// 创建签名后的提案, 该结构对于签署包含报头和有效载荷
	return &pb.SignedProposal{
		ProposalBytes: proposalBytes, // 待签名的原始数据
		Signature:     signature,     // 签名后的签名数据
	}, nil
}

// createPolicyBytes 方法用于创建策略字节。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - signaturePolicy：string 类型，表示签名策略。
//   - channelConfigPolicy：string 类型，表示通道配置策略。
//
// 返回值：
//   - []byte：表示策略的字节。
//   - error：如果创建策略字节时出错，则返回错误。
func createPolicyBytes(signaturePolicy, channelConfigPolicy string) ([]byte, error) {
	if signaturePolicy == "" && channelConfigPolicy == "" {
		// 没有主动配置策略，没有问题
		return nil, nil
	}

	if signaturePolicy != "" && channelConfigPolicy != "" {
		return nil, errors.New("不能同时指定 '--signature-policy' 和 '--channel-config-policy'")
	}

	var applicationPolicy *pb.ApplicationPolicy
	if signaturePolicy != "" {
		signaturePolicyEnvelope, err := policydsl.FromString(signaturePolicy)
		if err != nil {
			return nil, errors.Errorf("无效 --signaturePolicy 签名策略: %s", signaturePolicy)
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

	policyBytes := protoutil.MarshalOrPanic(applicationPolicy)
	return policyBytes, nil
}

// createCollectionConfigPackage 方法用于创建集合配置包。
// 方法接收者：无（该函数是一个独立的函数）
// 输入参数：
//   - collectionsConfigFile：string 类型，表示集合配置文件的路径。
//
// 返回值：
//   - *pb.CollectionConfigPackage：表示集合配置包的指针。
//   - error：如果创建集合配置包时出错，则返回错误。
func createCollectionConfigPackage(collectionsConfigFile string) (*pb.CollectionConfigPackage, error) {
	var ccp *pb.CollectionConfigPackage

	// 检查是否提供了集合配置文件
	if collectionsConfigFile != "" {
		// 从文件中获取集合配置
		var err error
		// 方法从提供的文件中检索集合配置
		ccp, _, err = chaincode.GetCollectionConfigFromFile(collectionsConfigFile)
		if err != nil {
			return nil, errors.WithMessagef(err, "文件中的 '--collectionsConfigFile' 集合配置无效 %s", collectionsConfigFile)
		}
	}

	return ccp, nil
}

func printResponseAsJSON(proposalResponse *pb.ProposalResponse, msg proto.Message, out io.Writer) error {
	err := proto.Unmarshal(proposalResponse.Response.Payload, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal proposal response's response payload as type %T", msg)
	}

	bytes, err := json.MarshalIndent(msg, "", "\t")
	if err != nil {
		return errors.Wrap(err, "failed to marshal output")
	}

	fmt.Fprintf(out, "%s\n", string(bytes))

	return nil
}
