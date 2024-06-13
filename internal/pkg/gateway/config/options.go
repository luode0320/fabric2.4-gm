/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"time"

	"github.com/spf13/viper"
)

// GatewayOptions is used to configure the gateway settings.
type Options struct {
	// GatewayEnabled is used to enable the gateway service.
	Enabled bool
	// EndorsementTimeout is used to specify the maximum time to wait for endorsement responses from external peers.
	EndorsementTimeout time.Duration
	// BroadcastTimeout is used to specify the maximum time to wait for responses from ordering nodes.
	BroadcastTimeout time.Duration
	// DialTimeout is used to specify the maximum time to wait for connecting to external peers and orderer nodes.
	DialTimeout time.Duration
}

// 默认的网关配置
var defaultOptions = Options{
	Enabled:            true,
	EndorsementTimeout: 10 * time.Second, // 指定背书操作的超时时间。在这个例子中，设置为 10 秒
	BroadcastTimeout:   10 * time.Second, // 指定广播操作的超时时间。在这个例子中，设置为 10 秒
	DialTimeout:        30 * time.Second, // 指定建立连接的超时时间。在这个例子中，设置为 30 秒
}

// GetOptions 函数获取默认的网关配置选项。
//
// 参数：
//   - v: *viper.Viper: 用于获取配置值的viper实例
//
// 返回值：
//   - Options: 网关配置选项
func GetOptions(v *viper.Viper) Options {
	options := defaultOptions
	if v.IsSet("peer.gateway.enabled") {
		options.Enabled = v.GetBool("peer.gateway.enabled")
	}
	if v.IsSet("peer.gateway.endorsementTimeout") {
		options.EndorsementTimeout = v.GetDuration("peer.gateway.endorsementTimeout")
	}
	if v.IsSet("peer.gateway.broadcastTimeout") {
		options.BroadcastTimeout = v.GetDuration("peer.gateway.broadcastTimeout")
	}
	if v.IsSet("peer.gateway.dialTimeout") {
		options.DialTimeout = v.GetDuration("peer.gateway.dialTimeout")
	}

	return options
}
