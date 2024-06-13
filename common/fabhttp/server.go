/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fabhttp

import (
	"context"
	"crypto/tls"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/middleware"
)

//go:generate counterfeiter -o fakes/logger.go -fake-name Logger . Logger

type Logger interface {
	Warn(args ...interface{})
	Warnf(template string, args ...interface{})
}

// Options 结构体定义了operations.System的选项配置。
type Options struct {
	Logger        Logger // Logger接口，表示日志记录器
	ListenAddress string // string，表示监听地址
	TLS           TLS    // TLS结构体，表示TLS配置
}

// Server 结构体定义了fabhttp.Server的实例。
type Server struct {
	logger     Logger         // Logger接口，表示日志记录器
	options    Options        // Options结构体，表示fabhttp.Server的选项配置
	httpServer *http.Server   // http.Server实例，表示HTTP服务器
	mux        *http.ServeMux // http.ServeMux实例，用于路由请求
	addr       string         // string，表示服务器地址
}

// NewServer 函数创建一个新的fabhttp.Server实例。
//
// 参数：
//   - o: Options，表示fabhttp.Server的选项配置
//
// 返回值：
//   - *Server: Server结构体指针，表示fabhttp.Server实例
func NewServer(o Options) *Server {
	logger := o.Logger
	if logger == nil {
		// 创建一个名为"fabhttp"的日志记录器
		logger = flogging.MustGetLogger("fabhttp")
	}

	server := &Server{
		logger:  logger, // 设置Server的logger字段为日志记录器
		options: o,      // 设置Server的options字段为传入的选项配置
	}
	// 初始化服务器
	server.initializeServer()

	return server
}

func (s *Server) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err := s.Start()
	if err != nil {
		return err
	}

	close(ready)

	<-signals
	return s.Stop()
}

func (s *Server) Start() error {
	// 创建监听
	listener, err := s.Listen()
	if err != nil {
		return err
	}

	s.addr = listener.Addr().String()

	// 接受侦听器上的传入连接，为每个连接创建一个新的服务goroutine。服务goroutines读取请求，然后调用srv.Handler来回复它们。
	go s.httpServer.Serve(listener)

	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}

// initializeServer 方法用于初始化HTTP服务器。
//
// 参数：
//   - s: *Server，表示fabhttp.Server实例
func (s *Server) initializeServer() {
	// 创建一个新的http.ServeMux实例
	// 创建一个新的 HTTP 服务器多路复用器（ServeMux）
	// HTTP 服务器多路复用器是一个用于处理 HTTP 请求的路由器。它可以根据请求的路径和方法将请求分发到不同的处理程序（handler）上。
	s.mux = http.NewServeMux()
	s.httpServer = &http.Server{
		Addr:         s.options.ListenAddress, // 设置HTTP服务器的监听地址
		Handler:      s.mux,                   // 设置HTTP服务器的处理程序为ServeMux实例
		ReadTimeout:  10 * time.Second,        // 设置HTTP服务器的读取超时时间为10秒
		WriteTimeout: 2 * time.Minute,         // 设置HTTP服务器的写入超时时间为2分钟
		TLSConfig: &tls.Config{
			CipherSuites: comm.DefaultTLSCipherSuites},
	}
}

func (s *Server) HandlerChain(h http.Handler, secure bool) http.Handler {
	if secure {
		return middleware.NewChain(middleware.RequireCert(), middleware.WithRequestID(util.GenerateUUID)).Handler(h)
	}
	return middleware.NewChain(middleware.WithRequestID(util.GenerateUUID)).Handler(h)
}

// RegisterHandler registers into the ServeMux a handler chain that borrows
// its security properties from the fabhttp.Server. This method is thread
// safe because ServeMux.Handle() is thread safe, and options are immutable.
// This method can be called either before or after Server.Start(). If the
// pattern exists the method panics.
func (s *Server) RegisterHandler(pattern string, handler http.Handler, secure bool) {
	s.mux.Handle(
		pattern,
		s.HandlerChain(
			handler,
			secure,
		),
	)
}

// Listen 方法用于监听服务器地址并返回一个net.Listener实例。
//
// 返回值：
//   - net.Listener: 表示服务器监听的net.Listener实例
//   - error: 如果监听过程中发生错误，则返回非nil的错误对象；否则返回nil。
func (s *Server) Listen() (net.Listener, error) {
	// 监听TCP地址
	listener, err := net.Listen("tcp", s.options.ListenAddress)
	if err != nil {
		return nil, err
	}

	// 获取TLS证书配置
	tlsConfig, err := s.options.TLS.Config()
	if err != nil {
		return nil, err
	}

	// 使用TLS配置创建TLS监听器
	if tlsConfig != nil {
		listener = gmtls.NewListener(listener, tlsConfig)
	}

	return listener, nil
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Log(keyvals ...interface{}) error {
	s.logger.Warn(keyvals...)
	return nil
}
