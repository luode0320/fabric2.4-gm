/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"

	peerproto "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/scc"
	gdiscovery "github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit"
	"github.com/hyperledger/fabric/internal/pkg/gateway/config"
	"github.com/hyperledger/fabric/internal/pkg/gateway/ledger"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"
	"google.golang.org/grpc"
)

var logger = flogging.MustGetLogger("gateway")

// Server 表示网关的 GRPC 服务器。
type Server struct {
	registry       *registry              // 注册表
	commitFinder   CommitFinder           // 提交查找器
	policy         ACLChecker             // ACL 检查器
	options        config.Options         // 配置选项
	logger         *flogging.FabricLogger // 日志记录器
	ledgerProvider ledger.Provider        // 账本提供者
}

type EndorserServerAdapter struct {
	Server peerproto.EndorserServer
}

func (e *EndorserServerAdapter) ProcessProposal(ctx context.Context, req *peerproto.SignedProposal, _ ...grpc.CallOption) (*peerproto.ProposalResponse, error) {
	return e.Server.ProcessProposal(ctx, req)
}

type CommitFinder interface {
	TransactionStatus(ctx context.Context, channelName string, transactionID string) (*commit.Status, error)
}

type ACLChecker interface {
	CheckACL(policyName string, channelName string, data interface{}) error
}

// CreateServer 创建一个嵌入式的 Gateway 实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - localEndorser：peerproto.EndorserServer，表示本地背书服务器
//   - discovery：Discovery，表示发现服务
//   - peerInstance：*peer.Peer，表示 Peer 实例
//   - secureOptions：*comm.SecureOptions，表示安全选项
//   - policy：ACLChecker，表示 ACL 检查器
//   - localMSPID：string，表示本地 MSP ID
//   - options：config.Options，表示配置选项
//   - systemChaincodes：scc.BuiltinSCCs，表示系统链码
//
// 返回值：
//   - *Server：表示 Server 实例
func CreateServer(
	localEndorser peerproto.EndorserServer,
	discovery Discovery,
	peerInstance *peer.Peer,
	secureOptions *comm.SecureOptions,
	policy ACLChecker,
	localMSPID string,
	options config.Options,
	systemChaincodes scc.BuiltinSCCs,
) *Server {
	// peer节点适配器
	adapter := &ledger.PeerAdapter{
		Peer: peerInstance,
	}
	// NewNotifier 提供事务提交的通知。
	notifier := commit.NewNotifier(adapter)

	// 创建一个新的服务器实例
	server := newServer(
		&EndorserServerAdapter{ // 表示本地背书客户端
			Server: localEndorser,
		},
		discovery,                           // 表示发现服务
		commit.NewFinder(adapter, notifier), // 提交查找器
		policy,                              // ACL 检查器
		adapter,                             // 表示账本提供器
		peerInstance.GossipService.SelfMembershipInfo(), // 表示本地网络成员信息
		localMSPID,                            // 表示本地 MSP ID
		secureOptions,                         // 表示安全选项
		options,                               // 表示配置选项
		systemChaincodes,                      // 表示系统链码
		peerInstance.OrdererEndpointOverrides, // 表示 Orderer 节点的覆盖配置
	)

	// 将服务器的配置更新回调函数添加到 peerInstance 的配置回调列表中
	peerInstance.AddConfigCallbacks(server.registry.configUpdate)

	return server
}

// newServer 创建一个新的 Server 实例。
// 输入参数：
//   - localEndorser：peerproto.EndorserClient，表示本地背书客户端
//   - discovery：Discovery，表示发现服务
//   - finder：CommitFinder，表示提交查找器
//   - policy：ACLChecker，表示 ACL 检查器
//   - ledgerProvider：ledger.Provider，表示账本提供器
//   - localInfo：gdiscovery.NetworkMember，表示本地网络成员信息
//   - localMSPID：string，表示本地 MSP ID
//   - secureOptions：*comm.SecureOptions，表示安全选项
//   - options：config.Options，表示配置选项
//   - systemChaincodes：scc.BuiltinSCCs，表示系统链码
//   - ordererEndpointOverrides：map[string]*orderers.Endpoint，表示 Orderer 节点的覆盖配置
//
// 返回值：
//   - *Server：表示 Server 实例
func newServer(localEndorser peerproto.EndorserClient,
	discovery Discovery,
	finder CommitFinder,
	policy ACLChecker,
	ledgerProvider ledger.Provider,
	localInfo gdiscovery.NetworkMember,
	localMSPID string,
	secureOptions *comm.SecureOptions,
	options config.Options,
	systemChaincodes scc.BuiltinSCCs,
	ordererEndpointOverrides map[string]*orderers.Endpoint,
) *Server {
	return &Server{
		registry: &registry{ //是一个注册表，用于管理背书节点和相关的连接信息。
			localEndorser: &endorser{ // 本地背书节点
				client: localEndorser, // 背书节点的客户端
				endpointConfig: &endpointConfig{ //封装了一个节点的配置信息。
					pkiid:      localInfo.PKIid,    // 节点的 PKI ID
					address:    localInfo.Endpoint, // 节点的地址
					logAddress: localInfo.Endpoint, // 节点的日志地址
					mspid:      localMSPID,         // 节点所属的 MSP ID
				},
			},
			discovery: discovery, // 发现服务
			logger:    logger,    // 日志记录器
			endpointFactory: &endpointFactory{ // 端点工厂
				timeout:                  options.DialTimeout,       // 连接超时时间
				clientCert:               secureOptions.Certificate, // 客户端证书
				clientKey:                secureOptions.Key,         // 客户端私钥
				ordererEndpointOverrides: ordererEndpointOverrides,  // 排序节点端点的覆盖映射
			},
			remoteEndorsers:    map[string]*endorser{}, //封装了一个背书节点的客户端和相关的配置信息。
			channelInitialized: map[string]bool{},      // 通道初始化状态映射
			systemChaincodes:   systemChaincodes,       // 系统链码
			localProvider:      ledgerProvider,         // 本地账本提供者
		},
		commitFinder:   finder,         // 提交查找器
		policy:         policy,         // ACL 检查器
		options:        options,        // 配置选项
		logger:         logger,         // 日志记录器
		ledgerProvider: ledgerProvider, // 账本提供者
	}
}
