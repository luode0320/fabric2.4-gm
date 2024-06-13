/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliverservice

import (
	"encoding/pem"
	"io/ioutil"
	"time"

	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/peer/orderers"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	DefaultReConnectBackoffThreshold   = time.Hour * 1
	DefaultReConnectTotalTimeThreshold = time.Second * 60 * 60
	DefaultConnectionTimeout           = time.Second * 3
)

// DeliverServiceConfig 是定义deliverservice提交服务配置的结构。
type DeliverServiceConfig struct {
	// PeerTLSEnabled 对等TLS启用/禁用。
	PeerTLSEnabled bool
	// BlockGossipEnabled 启用通过gossip转发提交block
	BlockGossipEnabled bool
	// ReConnectBackoffThreshold 设置连续重试之间的传递服务最大延迟。
	ReConnectBackoffThreshold time.Duration
	// ReconnectTotalTimeThreshold 设置交付服务可能花费在重新连接尝试上的总时间, 直到其重试逻辑放弃并返回错误。
	ReconnectTotalTimeThreshold time.Duration
	// ConnectionTimeout 设置交付服务 <-> 订购服务节点连接超时
	ConnectionTimeout time.Duration
	// KeepaliveOptions 长连接
	KeepaliveOptions comm.KeepaliveOptions
	// SecOpts 提供连接的TLS证书信息
	SecOpts comm.SecureOptions

	// OrdererEndpointOverrides 是一个应该是重新映射到不同的orderer端点。
	OrdererEndpointOverrides map[string]*orderers.Endpoint
}

type AddressOverride struct {
	From        string
	To          string
	CACertsFile string
}

// GlobalConfig 从viper获取一组配置，构建并返回配置结构。
func GlobalConfig() *DeliverServiceConfig {
	c := &DeliverServiceConfig{}
	c.loadDeliverServiceConfig()
	return c
}

// LoadOverridesMap 加载覆盖的排序节点msp配置
func LoadOverridesMap() (map[string]*orderers.Endpoint, error) {
	var overrides []AddressOverride
	err := viper.UnmarshalKey("peer.deliveryclient.addressOverrides", &overrides)
	if err != nil {
		return nil, errors.WithMessage(err, "无法解析数组 peer.deliveryclient.addressOverrides")
	}

	if len(overrides) == 0 {
		return nil, nil
	}

	overrideMap := map[string]*orderers.Endpoint{}
	for _, override := range overrides {
		var rootCerts [][]byte
		if override.CACertsFile != "" {
			pem, err := ioutil.ReadFile(override.CACertsFile)
			if err != nil {
				logger.Warningf("无法从指定caCertsFile的读取文件 '%s' ,覆盖Orders节点: '%s' to '%s': %s", override.CACertsFile, override.From, override.To, err)
				continue
			}
			rootCerts = extractCerts(pem)
			if len(rootCerts) == 0 {
				logger.Warningf("尝试创建证书池以覆盖orderer地址从 '%s' 至 '%s', 但没有找到任何有效的证书 '%s'", override.From, override.To, override.CACertsFile)
				continue
			}
		}
		overrideMap[override.From] = &orderers.Endpoint{
			Address:   override.To,
			RootCerts: rootCerts,
		}
	}

	return overrideMap, nil
}

// extractCerts 是一种破解的方式，用于拆分一组 PEM 编码的证书。
// 这用于在从 orderers.Endpoint 中删除 CertPool 后保留 x509.CertPool#AppendCertsFromPEM 的语义。
//
// 参数：
//   - pemCerts: []byte 类型，表示 PEM 编码的证书集合。
//
// 返回值：
//   - [][]byte: 表示拆分后的证书集合。
func extractCerts(pemCerts []byte) [][]byte {
	var certs [][]byte
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		certs = append(certs, pem.EncodeToMemory(block))
	}
	return certs
}

// 加载交付/提交服务配置
func (c *DeliverServiceConfig) loadDeliverServiceConfig() {
	enabledKey := "peer.deliveryclient.blockGossipEnabled"
	enabledConfigOptionMissing := !viper.IsSet(enabledKey)
	if enabledConfigOptionMissing {
		logger.Infof("peer.deliveryclient.blockGossipEnabled 未设置，默认为true.")
	}

	// 是否启用
	c.BlockGossipEnabled = enabledConfigOptionMissing || viper.GetBool(enabledKey)
	// 开启tls验证
	c.PeerTLSEnabled = viper.GetBool("peer.tls.enabled")
	// 设置连续重试之间的传递服务最大延迟。
	c.ReConnectBackoffThreshold = viper.GetDuration("peer.deliveryclient.reConnectBackoffThreshold")
	if c.ReConnectBackoffThreshold == 0 {
		// 默认延迟1小时
		c.ReConnectBackoffThreshold = DefaultReConnectBackoffThreshold
	}

	// 设置交付服务可能花费在重新连接尝试上的总时间
	c.ReconnectTotalTimeThreshold = viper.GetDuration("peer.deliveryclient.reconnectTotalTimeThreshold")
	if c.ReconnectTotalTimeThreshold == 0 {
		// 默认1小时重连
		c.ReconnectTotalTimeThreshold = DefaultReConnectTotalTimeThreshold
	}

	// 设置交付服务 <-> 订购服务节点连接超时
	c.ConnectionTimeout = viper.GetDuration("peer.deliveryclient.connTimeout")
	if c.ConnectionTimeout == 0 {
		// 默认3秒
		c.ConnectionTimeout = DefaultConnectionTimeout
	}

	// 设置长连接
	c.KeepaliveOptions = comm.DefaultKeepaliveOptions
	if viper.IsSet("peer.keepalive.deliveryClient.interval") {
		c.KeepaliveOptions.ClientInterval = viper.GetDuration("peer.keepalive.deliveryClient.interval")
	}
	if viper.IsSet("peer.keepalive.deliveryClient.timeout") {
		c.KeepaliveOptions.ClientTimeout = viper.GetDuration("peer.keepalive.deliveryClient.timeout")
	}

	c.SecOpts = comm.SecureOptions{
		UseTLS:            viper.GetBool("peer.tls.enabled"),
		RequireClientCert: viper.GetBool("peer.tls.clientAuthRequired"),
	}

	// 读取tls证书配置
	if c.SecOpts.RequireClientCert {
		certFile := config.GetPath("peer.tls.clientCert.file")
		if certFile == "" {
			certFile = config.GetPath("peer.tls.cert.file")
		}

		keyFile := config.GetPath("peer.tls.clientKey.file")
		if keyFile == "" {
			keyFile = config.GetPath("peer.tls.key.file")
		}

		keyPEM, err := ioutil.ReadFile(keyFile)
		if err != nil {
			panic(errors.WithMessagef(err, "无法读取 '%s'", keyFile))
		}
		c.SecOpts.Key = keyPEM
		certPEM, err := ioutil.ReadFile(certFile)
		if err != nil {
			panic(errors.WithMessagef(err, "无法读取 '%s'", certFile))
		}
		c.SecOpts.Certificate = certPEM
	}

	// 尝试配置覆盖的orderer证书, orderer节点覆盖, 包括需要覆盖的节点addr, 证书
	overridesMap, err := LoadOverridesMap()
	if err != nil {
		panic(err)
	}

	c.OrdererEndpointOverrides = overridesMap
}
