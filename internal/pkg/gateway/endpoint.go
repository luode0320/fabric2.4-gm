/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"
	"fmt"
	"time"

	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// endorser 封装了一个背书节点的客户端和相关的配置信息。
type endorser struct {
	client          peer.EndorserClient // 背书节点的客户端
	closeConnection func() error        // 关闭连接的函数
	*endpointConfig                     // 节点配置信息
}

type orderer struct {
	client          ab.AtomicBroadcastClient
	closeConnection func() error
	*endpointConfig
}

// endpointConfig 封装了一个节点的配置信息。
type endpointConfig struct {
	pkiid        common.PKIidType // 节点的 PKI ID
	address      string           // 节点的地址
	logAddress   string           // 节点的日志地址
	mspid        string           // 节点所属的 MSP ID
	tlsRootCerts [][]byte         // TLS 根证书列表
}

type (
	endorserConnector func(*grpc.ClientConn) peer.EndorserClient
	ordererConnector  func(*grpc.ClientConn) ab.AtomicBroadcastClient
)

//go:generate counterfeiter -o mocks/dialer.go --fake-name Dialer . dialer
type dialer func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

// endpointFactory 是一个用于创建端点的工厂。
type endpointFactory struct {
	timeout                  time.Duration                 // 连接超时时间
	connectEndorser          endorserConnector             // 背书节点连接器
	connectOrderer           ordererConnector              // 排序节点连接器
	dialer                   dialer                        // 拨号器
	clientCert               []byte                        // 客户端证书
	clientKey                []byte                        // 客户端私钥
	ordererEndpointOverrides map[string]*orderers.Endpoint // 排序节点端点的覆盖映射
}

func (ef *endpointFactory) newEndorser(pkiid common.PKIidType, address, mspid string, tlsRootCerts [][]byte) (*endorser, error) {
	conn, err := ef.newConnection(address, tlsRootCerts)
	if err != nil {
		return nil, err
	}
	connectEndorser := ef.connectEndorser
	if connectEndorser == nil {
		connectEndorser = peer.NewEndorserClient
	}
	close := func() error {
		if conn != nil && conn.GetState() != connectivity.Shutdown {
			logger.Infow("Closing connection to remote endorser", "address", address, "mspid", mspid)
			return conn.Close()
		}
		return nil
	}
	return &endorser{
		client:          connectEndorser(conn),
		closeConnection: close,
		endpointConfig:  &endpointConfig{pkiid: pkiid, address: address, logAddress: address, mspid: mspid, tlsRootCerts: tlsRootCerts},
	}, nil
}

func (ef *endpointFactory) newOrderer(address, mspid string, tlsRootCerts [][]byte) (*orderer, error) {
	connAddress := address
	logAddess := address
	connCerts := tlsRootCerts
	if override, ok := ef.ordererEndpointOverrides[address]; ok {
		connAddress = override.Address
		connCerts = override.RootCerts
		logAddess = fmt.Sprintf("%s (mapped from %s)", connAddress, address)
		logger.Debugw("Overriding orderer endpoint address", "from", address, "to", connAddress)
	}
	conn, err := ef.newConnection(connAddress, connCerts)
	if err != nil {
		return nil, err
	}
	connectOrderer := ef.connectOrderer
	if connectOrderer == nil {
		connectOrderer = ab.NewAtomicBroadcastClient
	}
	return &orderer{
		client:          connectOrderer(conn),
		closeConnection: conn.Close,
		endpointConfig:  &endpointConfig{address: address, logAddress: logAddess, mspid: mspid, tlsRootCerts: tlsRootCerts},
	}, nil
}

func (ef *endpointFactory) newConnection(address string, tlsRootCerts [][]byte) (*grpc.ClientConn, error) {
	config := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            len(tlsRootCerts) > 0,
			ServerRootCAs:     tlsRootCerts,
			RequireClientCert: true,
			Certificate:       ef.clientCert,
			Key:               ef.clientKey,
		},
		DialTimeout:  ef.timeout,
		AsyncConnect: true,
	}
	dialOpts, err := config.DialOptions()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ef.timeout)
	defer cancel()

	dialer := ef.dialer
	if dialer == nil {
		dialer = grpc.DialContext
	}
	conn, err := dialer(ctx, address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create new connection: %w", err)
	}
	return conn, nil
}
