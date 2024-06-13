/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package operations

import (
	"context"
	"net"
	"strings"
	"time"

	kitstatsd "github.com/go-kit/kit/metrics/statsd"
	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/hyperledger/fabric/common/fabhttp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/flogging/httpadmin"
	"github.com/hyperledger/fabric/common/metadata"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/common/metrics/prometheus"
	"github.com/hyperledger/fabric/common/metrics/statsd"
	"github.com/hyperledger/fabric/common/metrics/statsd/goruntime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:generate counterfeiter -o fakes/logger.go -fake-name Logger . Logger

type Logger interface {
	Warn(args ...interface{})
	Warnf(template string, args ...interface{})
}

type Statsd struct {
	Network       string
	Address       string
	WriteInterval time.Duration
	Prefix        string
}

type MetricsOptions struct {
	Provider string
	Statsd   *Statsd
}

// Options 结构体定义了operations.System的选项配置。
type Options struct {
	fabhttp.Options                // 继承自fabhttp.Options，表示fabhttp.Server的选项配置
	Metrics         MetricsOptions // MetricsOptions结构体，表示指标配置
	Version         string         // string，表示版本号
}

type System struct {
	*fabhttp.Server
	metrics.Provider

	logger          Logger
	healthHandler   *healthz.HealthHandler
	options         Options
	statsd          *kitstatsd.Statsd
	collectorTicker *time.Ticker
	sendTicker      *time.Ticker
	versionGauge    metrics.Gauge
}

// NewSystem 函数创建一个新的operations.System实例。
//
// 参数：
//   - o: Options，表示operations.System的选项配置
//
// 返回值：
//   - *System: System结构体指针，表示operations.System实例
func NewSystem(o Options) *System {
	logger := o.Logger
	if logger == nil {
		logger = flogging.MustGetLogger("operations.runner") // 创建一个名为"operations.runner"的日志记录器
	}

	// 创建一个新的fabhttp.Server实例
	s := fabhttp.NewServer(o.Options)

	system := &System{
		Server:  s,      // 设置System的Server字段为新创建的fabhttp.Server实例
		logger:  logger, // 设置System的logger字段为日志记录器
		options: o,      // 设置System的options字段为传入的选项配置
	}

	// 初始化健康检查处理程序
	system.initializeHealthCheckHandler()
	// 初始化日志处理程序
	system.initializeLoggingHandler()
	// 初始化指标提供者
	system.initializeMetricsProvider()
	// 初始化版本信息处理程序
	system.initializeVersionInfoHandler()

	return system
}

// Start 方法用于启动System实例。
//
// 返回值：
//   - error: 如果启动过程中发生错误，则返回非nil的错误对象；否则返回nil。
func (s *System) Start() error {
	// 启动指标计时器
	err := s.startMetricsTickers()
	if err != nil {
		return err
	}

	// 设置版本号指标为1
	s.versionGauge.With("version", s.options.Version).Set(1)

	// 启动HTTP服务器
	return s.Server.Start()
}

// Stop 方法用于停止System实例。
//
// 返回值：
//   - error: 如果停止过程中发生错误，则返回非nil的错误对象；否则返回nil。
func (s *System) Stop() error {
	if s.collectorTicker != nil {
		s.collectorTicker.Stop() // 停止指标收集计时器
		s.collectorTicker = nil
	}
	if s.sendTicker != nil {
		s.sendTicker.Stop() // 停止发送计时器
		s.sendTicker = nil
	}
	return s.Server.Stop() // 停止HTTP服务器
}

func (s *System) RegisterChecker(component string, checker healthz.HealthChecker) error {
	return s.healthHandler.RegisterChecker(component, checker)
}

// 初始化指标提供者
func (s *System) initializeMetricsProvider() error {
	m := s.options.Metrics
	providerType := m.Provider
	switch providerType {
	case "statsd":
		prefix := m.Statsd.Prefix
		if prefix != "" && !strings.HasSuffix(prefix, ".") {
			prefix = prefix + "."
		}

		ks := kitstatsd.New(prefix, s)
		s.Provider = &statsd.Provider{Statsd: ks}
		s.statsd = ks
		s.versionGauge = versionGauge(s.Provider)
		return nil

	case "prometheus":
		s.Provider = &prometheus.Provider{}
		s.versionGauge = versionGauge(s.Provider)
		// swagger:operation GET /metrics operations metrics
		// ---
		// responses:
		//     '200':
		//        description: Ok.
		s.RegisterHandler("/metrics", promhttp.Handler(), s.options.TLS.Enabled)
		return nil

	default:
		if providerType != "disabled" {
			s.logger.Warnf("Unknown provider type: %s; metrics disabled", providerType)
		}

		s.Provider = &disabled.Provider{}
		s.versionGauge = versionGauge(s.Provider)
		return nil
	}
}

// 初始化日志处理程序
func (s *System) initializeLoggingHandler() {
	// swagger:operation GET /logspec operations logspecget
	// ---
	// summary: Retrieves the active logging spec for a peer or orderer.
	// responses:
	//     '200':
	//        description: Ok.

	// swagger:operation PUT /logspec operations logspecput
	// ---
	// summary: Updates the active logging spec for a peer or orderer.
	//
	// parameters:
	// - name: payload
	//   in: formData
	//   type: string
	//   description: The payload must consist of a single attribute named spec.
	//   required: true
	// responses:
	//     '204':
	//        description: No content.
	//     '400':
	//        description: Bad request.
	// consumes:
	//   - multipart/form-data
	s.RegisterHandler("/logspec", httpadmin.NewSpecHandler(), s.options.TLS.Enabled)
}

// 初始化健康检查处理程序
func (s *System) initializeHealthCheckHandler() {
	s.healthHandler = healthz.NewHealthHandler()
	// swagger:operation GET /healthz operations healthz
	// ---
	// summary: Retrieves all registered health checkers for the process.
	// responses:
	//     '200':
	//        description: Ok.
	//     '503':
	//        description: Service unavailable.
	s.RegisterHandler("/healthz", s.healthHandler, false)
}

// 初始化版本信息处理程序
func (s *System) initializeVersionInfoHandler() {
	versionInfo := &VersionInfoHandler{
		CommitSHA: metadata.CommitSHA,
		Version:   metadata.Version,
	}
	// swagger:operation GET /version operations version
	// ---
	// summary: Returns the orderer or peer version and the commit SHA on which the release was created.
	// responses:
	//     '200':
	//        description: Ok.
	s.RegisterHandler("/version", versionInfo, false)
}

// startMetricsTickers 方法用于启动指标计时器。
//
// 返回值：
//   - error: 如果启动过程中发生错误，则返回非nil的错误对象；否则返回nil。
func (s *System) startMetricsTickers() error {
	m := s.options.Metrics // 获取Metrics配置
	if s.statsd != nil {
		network := m.Statsd.Network          // 获取Statsd的网络类型
		address := m.Statsd.Address          // 获取Statsd的地址
		c, err := net.Dial(network, address) // 创建与Statsd服务器的连接
		if err != nil {
			return err
		}
		c.Close()

		opts := s.options.Metrics.Statsd    // 获取Statsd的配置选项
		writeInterval := opts.WriteInterval // 获取写入间隔

		s.collectorTicker = time.NewTicker(writeInterval / 2) // 创建指标收集计时器
		goCollector := goruntime.NewCollector(s.Provider)     // 创建goruntime.Collector实例
		go goCollector.CollectAndPublish(s.collectorTicker.C) // 启动指标收集和发布

		s.sendTicker = time.NewTicker(writeInterval)                           // 创建发送计时器
		go s.statsd.SendLoop(context.TODO(), s.sendTicker.C, network, address) // 启动发送循环
	}

	return nil
}
