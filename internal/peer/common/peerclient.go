/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"context"
	"crypto/tls"
	"github.com/hyperledger/fabric/bccsp/common"
	"io/ioutil"
	"time"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// PeerClient represents a client for communicating with a peer
type PeerClient struct {
	*CommonClient
}

// NewPeerClientFromEnv 从全局Viper实例创建PeerClient的实例
func NewPeerClientFromEnv() (*PeerClient, error) {
	address, clientConfig, err := configFromEnv("peer")
	if err != nil {
		return nil, errors.WithMessage(err, "无法加载 PeerClient 的配置")
	}

	//  用于根据客户端配置创建一个 PeerClient 对象。
	return newPeerClientForClientConfig(address, clientConfig)
}

// NewPeerClientForAddress creates an instance of a PeerClient using the
// provided peer address and, if TLS is enabled, the TLS root cert file
func NewPeerClientForAddress(address, tlsRootCertFile string) (*PeerClient, error) {
	if address == "" {
		return nil, errors.New("peer address must be set")
	}

	clientConfig := comm.ClientConfig{}
	clientConfig.DialTimeout = viper.GetDuration("peer.client.connTimeout")
	if clientConfig.DialTimeout == time.Duration(0) {
		clientConfig.DialTimeout = defaultConnTimeout
	}

	secOpts := comm.SecureOptions{
		UseTLS:             viper.GetBool("peer.tls.enabled"),
		RequireClientCert:  viper.GetBool("peer.tls.clientAuthRequired"),
		ServerNameOverride: viper.GetString("peer.tls.serverhostoverride"),
	}

	if secOpts.RequireClientCert {
		var err error
		secOpts.Key, secOpts.Certificate, err = getClientAuthInfoFromEnv("peer")
		if err != nil {
			return nil, err
		}

	}
	clientConfig.SecOpts = secOpts

	if clientConfig.SecOpts.UseTLS {
		if tlsRootCertFile == "" {
			return nil, errors.New("tls root cert file must be set")
		}
		caPEM, res := ioutil.ReadFile(tlsRootCertFile)
		if res != nil {
			return nil, errors.WithMessagef(res, "unable to load TLS root cert file from %s", tlsRootCertFile)
		}
		clientConfig.SecOpts.ServerRootCAs = [][]byte{caPEM}
	}

	clientConfig.MaxRecvMsgSize = comm.DefaultMaxRecvMsgSize
	if viper.IsSet("peer.maxRecvMsgSize") {
		clientConfig.MaxRecvMsgSize = int(viper.GetInt32("peer.maxRecvMsgSize"))
	}
	clientConfig.MaxSendMsgSize = comm.DefaultMaxSendMsgSize
	if viper.IsSet("peer.maxSendMsgSize") {
		clientConfig.MaxSendMsgSize = int(viper.GetInt32("peer.maxSendMsgSize"))
	}

	//  用于根据客户端配置创建一个 PeerClient 对象。
	return newPeerClientForClientConfig(address, clientConfig)
}

// newPeerClientForClientConfig 用于根据客户端配置创建一个 PeerClient 对象。
// 输入参数：
//   - address：Peer 节点的地址。
//   - clientConfig：客户端配置。
//
// 返回值：
//   - *PeerClient：创建的 PeerClient 对象。
//   - error：创建过程中的错误，如果没有错误则为 nil。
func newPeerClientForClientConfig(address string, clientConfig comm.ClientConfig) (*PeerClient, error) {
	// 将默认的保持活动选项设置为与服务器匹配
	clientConfig.KaOpts = comm.DefaultKeepaliveOptions
	// 一个通用的客户端结构体
	cc, err := newCommonClient(address, clientConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "无法从配置创建客户端结构体 PeerClient")
	}
	// 创建并返回一个 PeerClient 对象
	return &PeerClient{CommonClient: cc}, nil
}

// Endorser 返回一个用于 Endorser 背书服务的客户端。
// 方法接收者：PeerClient
// 返回值：
//   - pb.EndorserClient：Endorser 客户端。
//   - error：连接过程中的错误，如果没有错误则为 nil。
func (pc *PeerClient) Endorser() (pb.EndorserClient, error) {
	// 使用客户端配置中的地址进行连接
	conn, err := pc.CommonClient.clientConfig.Dial(pc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "背书客户端无法连接到 %s", pc.address)
	}

	// 创建并返回一个 EndorserClient 对象
	return pb.NewEndorserClient(conn), nil
}

// Deliver returns a client for the Deliver service
func (pc *PeerClient) Deliver() (pb.Deliver_DeliverClient, error) {
	conn, err := pc.CommonClient.clientConfig.Dial(pc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "deliver client failed to connect to %s", pc.address)
	}
	return pb.NewDeliverClient(conn).Deliver(context.TODO())
}

// PeerDeliver returns a client for the Deliver service for peer-specific use
// cases (i.e. DeliverFiltered)
func (pc *PeerClient) PeerDeliver() (pb.DeliverClient, error) {
	conn, err := pc.CommonClient.clientConfig.Dial(pc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "deliver client failed to connect to %s", pc.address)
	}
	return pb.NewDeliverClient(conn), nil
}

// Certificate returns the TLS client certificate (if available)
func (pc *PeerClient) Certificate() tls.Certificate {
	return pc.CommonClient.Certificate()
}

// GetEndorserClient 返回一个新的背书服务客户端。
// 方法接收者：无（全局函数）
// 输入参数：
//   - address：背书服务的地址。
//   - tlsRootCertFile：TLS根证书文件的路径。
//
// 返回值：
//   - pb.EndorserClient：背书服务客户端。
//   - error：如果创建客户端过程中出现错误，则返回错误；否则返回nil。
func GetEndorserClient(address, tlsRootCertFile string) (pb.EndorserClient, error) {
	// 使用给定的地址和TLS根证书文件创建一个新的对等节点客户端
	peerClient, err := newPeerClient(address, tlsRootCertFile)
	if err != nil {
		return nil, err
	}

	// 返回一个用于背书服务的客户端
	return peerClient.Endorser()
}

// GetClientCertificate 返回客户端的TLS证书。
// 方法接收者：无
// 输入参数：无
// 返回值：
//   - tls.Certificate：客户端的TLS证书。
//   - error：如果获取客户端证书时出错，则返回错误。
func GetClientCertificate() (tls.Certificate, error) {
	// 检查是否需要客户端认证
	if !viper.GetBool("peer.tls.clientAuthRequired") {
		return tls.Certificate{}, nil
	}

	// 从环境变量中获取客户端认证信息
	key, certificate, err := getClientAuthInfoFromEnv("peer")
	if err != nil {
		return tls.Certificate{}, err
	}

	// 加载客户端证书
	cert, err := common.X509KeyPair(certificate, key)
	if err != nil {
		return tls.Certificate{}, errors.WithMessage(err, "无法加载客户端tls证书")
	}
	return cert, nil
}

// GetDeliverClient 返回一个新的交付（Deliver）客户端。如果未提供地址（address）和
// TLS根证书文件（tlsRootCertFile），则客户端的目标地址值将从配置设置中获取，
// 分别对应于"peer.address"和"peer.tls.rootcert.file"。
func GetDeliverClient(address, tlsRootCertFile string) (pb.Deliver_DeliverClient, error) {
	// 根据提供的地址和TLS根证书文件创建一个新的Peer客户端，如果创建过程中出错，则返回错误
	peerClient, err := newPeerClient(address, tlsRootCertFile)
	if err != nil {
		return nil, err
	}
	// 通过Peer客户端获取Deliver服务客户端
	return peerClient.Deliver()
}

// GetPeerDeliverClient returns a new deliver client. If both the address and
// tlsRootCertFile are not provided, the target values for the client are taken
// from the configuration settings for "peer.address" and
// "peer.tls.rootcert.file"
func GetPeerDeliverClient(address, tlsRootCertFile string) (pb.DeliverClient, error) {
	peerClient, err := newPeerClient(address, tlsRootCertFile)
	if err != nil {
		return nil, err
	}
	return peerClient.PeerDeliver()
}

// SnapshotClient returns a client for the snapshot service
func (pc *PeerClient) SnapshotClient() (pb.SnapshotClient, error) {
	conn, err := pc.CommonClient.clientConfig.Dial(pc.address)
	if err != nil {
		return nil, errors.WithMessagef(err, "snapshot client failed to connect to %s", pc.address)
	}
	return pb.NewSnapshotClient(conn), nil
}

// GetSnapshotClient returns a new snapshot client. If both the address and
// tlsRootCertFile are not provided, the target values for the client are taken
// from the configuration settings for "peer.address" and
// "peer.tls.rootcert.file"
func GetSnapshotClient(address, tlsRootCertFile string) (pb.SnapshotClient, error) {
	peerClient, err := newPeerClient(address, tlsRootCertFile)
	if err != nil {
		return nil, err
	}
	return peerClient.SnapshotClient()
}

// newPeerClient 根据给定的地址和TLS根证书文件创建一个新的对等节点客户端。
// 方法接收者：无（全局函数）
// 输入参数：
//   - address：对等节点的地址。
//   - tlsRootCertFile：TLS根证书文件的路径。
//
// 返回值：
//   - *PeerClient：对等节点客户端。
//   - error：如果创建客户端过程中出现错误，则返回错误；否则返回nil。
func newPeerClient(address, tlsRootCertFile string) (*PeerClient, error) {
	if address != "" {
		// 如果地址不为空，则使用给定的地址和TLS根证书文件创建一个新的对等节点客户端
		return NewPeerClientForAddress(address, tlsRootCertFile)
	}

	// 如果地址为空，则从全局Viper实例创建一个新的对等节点客户端
	return NewPeerClientFromEnv()
}
