/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"crypto/tls"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/internal/peer/common"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// ClientConnections holds the clients for connecting to the various
// endpoints in a Fabric network.
type ClientConnections struct {
	BroadcastClient common.BroadcastClient
	DeliverClients  []pb.DeliverClient
	EndorserClients []pb.EndorserClient
	Certificate     tls.Certificate
	Signer          identity.SignerSerializer
	CryptoProvider  bccsp.BCCSP
}

// ClientConnectionsInput 保存创建客户端连接的输入参数。
type ClientConnectionsInput struct {
	CommandName           string   // 命令名称
	EndorserRequired      bool     // 是否需要背书节点
	OrdererRequired       bool     // 是否需要订购节点
	OrderingEndpoint      string   // 订购节点的终端点
	ChannelID             string   // 通道ID
	PeerAddresses         []string // 节点地址列表
	TLSRootCertFiles      []string // TLS 根证书文件列表
	ConnectionProfilePath string   // 连接配置文件路径
	TargetPeer            string   // 目标节点
	TLSEnabled            bool     // 是否启用 TLS
}

// NewClientConnections 根据输入参数创建一个新的客户端连接集合。
// 方法接收者：无
// 输入参数：
//   - input：客户端连接的输入参数。
//   - cryptoProvider：加密提供者。
//
// 返回值：
//   - *ClientConnections：客户端连接集合。
//   - error：如果在创建客户端连接时出错，则返回错误。
func NewClientConnections(input *ClientConnectionsInput, cryptoProvider bccsp.BCCSP) (*ClientConnections, error) {
	// 获取默认的签名者
	signer, err := common.GetDefaultSigner()
	if err != nil {
		return nil, errors.WithMessage(err, "获取默认签名者失败")
	}

	// 创建ClientConnections实例
	c := &ClientConnections{
		Signer:         signer,
		CryptoProvider: cryptoProvider,
	}

	// 如果需要Endorser，则设置Peer客户端
	if input.EndorserRequired {
		err := c.setPeerClients(input)
		if err != nil {
			return nil, err
		}
	}

	// 如果需要Endorser，则设置orderer客户端
	if input.OrdererRequired {
		err := c.setOrdererClient()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// setPeerClients 设置对等节点客户端连接。
// 方法接收者：c（ClientConnections类型的指针）
// 输入参数：
//   - input：客户端连接的输入参数。
//
// 返回值：
//   - error：如果在设置对等节点客户端连接时出错，则返回错误。
func (c *ClientConnections) setPeerClients(input *ClientConnectionsInput) error {
	var endorserClients []pb.EndorserClient
	var deliverClients []pb.DeliverClient

	// 验证 peer 节点连接参数
	if err := c.validatePeerConnectionParameters(input); err != nil {
		return errors.WithMessage(err, "无法验证 peer 连接参数")
	}

	for i, address := range input.PeerAddresses {
		var tlsRootCertFile string
		if input.TLSRootCertFiles != nil {
			tlsRootCertFile = input.TLSRootCertFiles[i]
		}

		// 获取背书节点连接
		endorserClient, err := common.GetEndorserClient(address, tlsRootCertFile)
		if err != nil {
			return errors.WithMessagef(err, "连接 %s 的背书节点失败", input.CommandName)
		}
		endorserClients = append(endorserClients, endorserClient)

		// 获取提交节点连接
		deliverClient, err := common.GetPeerDeliverClient(address, tlsRootCertFile)
		if err != nil {
			return errors.WithMessagef(err, "连接 %s 的提交节点失败", input.CommandName)
		}
		deliverClients = append(deliverClients, deliverClient)
	}
	if len(endorserClients) == 0 {
		return errors.New("core.yaml/环境变量 缺失 peer 节点的配置")
	}

	// 配置客户端tls证书
	err := c.setCertificate()
	if err != nil {
		return err
	}

	// 配置背书和提交节点
	c.EndorserClients = endorserClients
	c.DeliverClients = deliverClients

	return nil
}

// validatePeerConnectionParameters 验证 peer 节点连接参数。
// 方法接收者：c（ClientConnections类型的指针）
// 输入参数：
//   - input：客户端连接的输入参数。
//
// 返回值：
//   - error：如果对等节点连接参数无效，则返回错误。
func (c *ClientConnections) validatePeerConnectionParameters(input *ClientConnectionsInput) error {
	// 如果连接配置文件路径
	if input.ConnectionProfilePath != "" {
		err := input.parseConnectionProfile()
		if err != nil {
			return err
		}
	}

	// 对于批准和提交, 当前仅支持多个对等地址用于 _lifecycle
	multiplePeersAllowed := map[string]bool{
		"approveformyorg": true, // 批准
		"commit":          true, // 提交
	}

	// 如果不是批准/提交, 并且 peer 地址最多只能有一个
	if !multiplePeersAllowed[input.CommandName] && len(input.PeerAddresses) > 1 {
		return errors.Errorf("'%s' 命令支持一个 peer 节点, 已提供 %d 个同伴", input.CommandName, len(input.PeerAddresses))
	}

	// 如果关闭tls, 则把 TLSRootCertFiles = nil
	if !input.TLSEnabled {
		input.TLSRootCertFiles = nil
		return nil
	}

	// 如果 tls 根证书数量不等于 peer 节点的配置数量
	if len(input.TLSRootCertFiles) != len(input.PeerAddresses) {
		return errors.Errorf("peer 地址数 (%d) 与TLS根证书文件数 (%d) 不匹配", len(input.PeerAddresses), len(input.TLSRootCertFiles))
	}

	return nil
}

func (c *ClientConnectionsInput) parseConnectionProfile() error {
	networkConfig, err := common.GetConfig(c.ConnectionProfilePath)
	if err != nil {
		return err
	}

	c.PeerAddresses = []string{}
	c.TLSRootCertFiles = []string{}

	if c.ChannelID == "" {
		if c.TargetPeer == "" {
			return errors.New("--targetPeer must be specified for channel-less operation using connection profile")
		}
		return c.appendPeerConfig(networkConfig, c.TargetPeer)
	}

	if len(networkConfig.Channels[c.ChannelID].Peers) == 0 {
		return nil
	}

	for peer, peerChannelConfig := range networkConfig.Channels[c.ChannelID].Peers {
		if peerChannelConfig.EndorsingPeer {
			err := c.appendPeerConfig(networkConfig, peer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *ClientConnectionsInput) appendPeerConfig(n *common.NetworkConfig, peer string) error {
	peerConfig, ok := n.Peers[peer]
	if !ok {
		return errors.Errorf("peer '%s' doesn't have associated peer config", peer)
	}
	c.PeerAddresses = append(c.PeerAddresses, peerConfig.URL)
	c.TLSRootCertFiles = append(c.TLSRootCertFiles, peerConfig.TLSCACerts.Path)

	return nil
}

// 配置客户端tls证书
func (c *ClientConnections) setCertificate() error {
	// 返回客户端的TLS证书
	certificate, err := common.GetClientCertificate()
	if err != nil {
		return errors.WithMessage(err, "获取客户端 client cerificate 证书失败")
	}

	c.Certificate = certificate

	return nil
}

// setOrdererClient 方法用于设置Orderer客户端。
// 方法接收者：c *ClientConnections
// 输入参数：无
// 返回值：error，如果设置Orderer客户端时出错，则返回错误。
func (c *ClientConnections) setOrdererClient() error {
	// 从配置文件中获取Orderer的地址
	oe := viper.GetString("orderer.address")
	if oe == "" {
		// 如果没有从命令行获取 Orderer 的地址，则尝试从 cscc 获取
		if c.Signer == nil {
			return errors.New("无法获取 orderer 端点, 并且未配置任何 Signer 签名者")
		}

		if len(c.EndorserClients) == 0 {
			return errors.New("无法获取orderer端点, 并且背书节点列表为空")
		}

		// 从 cscc 获取通道的 Orderer 节点地址
		orderingEndpoints, err := common.GetOrdererEndpointOfChainFnc(channelID, c.Signer, c.EndorserClients[0], c.CryptoProvider)
		if err != nil {
			return errors.WithMessagef(err, "从 cscc 系统链码中获取通道 (%s) orderer 节点时出错", channelID)
		}
		if len(orderingEndpoints) == 0 {
			return errors.Errorf("cscc 系统链码中没有找到通道 %s 的 orderer 端点, 请用 -o 标志主动传入 orderer 端点(ip:port)", channelID)
		}

		logger.Infof("cscc 系统链码自动检索到的通道 (%s) orderer 节点: %s, (自动获取的 orderer 配置无 tls 配置)", channelID, orderingEndpoints[0])
		// 覆盖viper环境变量
		viper.Set("orderer.address", orderingEndpoints[0])
	}

	// 获取广播客户端(实际就是连接的 orderer 节点)
	broadcastClient, err := common.GetBroadcastClient()
	if err != nil {
		return errors.WithMessage(err, "没有找到 BroadcastClient 广播客户端")
	}

	c.BroadcastClient = broadcastClient

	return nil
}
