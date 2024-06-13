/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"github.com/hyperledger/fabric/common/metrics"
)

var (
	streamsOpened = metrics.CounterOpts{
		Namespace: "deliver",
		Name:      "streams_opened",
		Help:      "The number of GRPC streams that have been opened for the deliver service.",
	}
	streamsClosed = metrics.CounterOpts{
		Namespace: "deliver",
		Name:      "streams_closed",
		Help:      "The number of GRPC streams that have been closed for the deliver service.",
	}

	requestsReceived = metrics.CounterOpts{
		Namespace:    "deliver",
		Name:         "requests_received",
		Help:         "The number of deliver requests that have been received.",
		LabelNames:   []string{"channel", "filtered", "data_type"},
		StatsdFormat: "%{#fqname}.%{channel}.%{filtered}.%{data_type}",
	}
	requestsCompleted = metrics.CounterOpts{
		Namespace:    "deliver",
		Name:         "requests_completed",
		Help:         "The number of deliver requests that have been completed.",
		LabelNames:   []string{"channel", "filtered", "data_type", "success"},
		StatsdFormat: "%{#fqname}.%{channel}.%{filtered}.%{data_type}.%{success}",
	}

	blocksSent = metrics.CounterOpts{
		Namespace:    "deliver",
		Name:         "blocks_sent",
		Help:         "The number of blocks sent by the deliver service.",
		LabelNames:   []string{"channel", "filtered", "data_type"},
		StatsdFormat: "%{#fqname}.%{channel}.%{filtered}.%{data_type}",
	}
)

// Metrics 结构体用于记录各种指标的计数器。
type Metrics struct {
	StreamsOpened     metrics.Counter // 记录已打开的流的数量
	StreamsClosed     metrics.Counter // 记录已关闭的流的数量
	RequestsReceived  metrics.Counter // 记录已接收的请求的数量
	RequestsCompleted metrics.Counter // 记录已完成的请求的数量
	BlocksSent        metrics.Counter // 记录已发送的区块的数量
}

// NewMetrics 结构体用于记录各种指标的计数器。
func NewMetrics(p metrics.Provider) *Metrics {
	return &Metrics{
		StreamsOpened:     p.NewCounter(streamsOpened),     // 记录已打开的流的数量
		StreamsClosed:     p.NewCounter(streamsClosed),     // 记录已关闭的流的数量
		RequestsReceived:  p.NewCounter(requestsReceived),  // 记录已接收的请求的数量
		RequestsCompleted: p.NewCounter(requestsCompleted), // 记录已完成的请求的数量
		BlocksSent:        p.NewCounter(blocksSent),        // 记录已发送的区块的数量
	}
}
