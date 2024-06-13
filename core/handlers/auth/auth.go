/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package auth

import (
	"github.com/hyperledger/fabric-protos-go/peer"
)

// Filter 定义了一个背书过滤器，用于拦截 ProcessProposal 背书方法。
type Filter interface {
	peer.EndorserServer // 继承自 peer.EndorserServer 接口
	// Init 用于初始化过滤器，并设置下一个 EndorserServer
	Init(next peer.EndorserServer)
}

// ChainFilters 将给定的认证过滤器按照提供的顺序进行链式连接。
// 输入参数：
//   - endorser：peer.EndorserServer，表示背书服务器
//   - filters：[]Filter，表示认证过滤器列表
//
// 返回值：
//   - peer.EndorserServer：表示链式连接后的背书服务器
func ChainFilters(endorser peer.EndorserServer, filters ...Filter) peer.EndorserServer {
	// 如果过滤器列表为空，则直接返回背书服务器
	if len(filters) == 0 {
		return endorser
	}

	// 每个过滤器都将请求转发给下一个过滤器
	for i := 0; i < len(filters)-1; i++ {
		filters[i].Init(filters[i+1])
	}

	// 最后一个过滤器将请求转发给背书服务器
	filters[len(filters)-1].Init(endorser)

	return filters[0]
}
