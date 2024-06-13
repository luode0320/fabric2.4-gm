/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/accesscontrol"
	"github.com/hyperledger/fabric/core/chaincode/extcc"
	"github.com/hyperledger/fabric/core/container/ccintf"
	"github.com/pkg/errors"
)

// LaunchRegistry tracks launching chaincode instances.
type LaunchRegistry interface {
	Launching(ccid string) (launchState *LaunchState, started bool)
	Deregister(ccid string) error
}

// ConnectionHandler 处理 “chaincode” 客户端连接
type ConnectionHandler interface {
	Stream(ccid string, ccinfo *ccintf.ChaincodeServerInfo, sHandler extcc.StreamHandler) error
}

// RuntimeLauncher 负责启动链码运行时。
type RuntimeLauncher struct {
	Runtime           Runtime           // 运行时
	Registry          LaunchRegistry    // 启动注册表
	StartupTimeout    time.Duration     // 启动超时时间
	Metrics           *LaunchMetrics    // 启动指标
	PeerAddress       string            // 对等节点地址
	CACert            []byte            // CA 证书
	CertGenerator     CertGenerator     // 证书生成器
	ConnectionHandler ConnectionHandler // 连接处理器
}

// CertGenerator generates client certificates for chaincode.
type CertGenerator interface {
	// Generate returns a certificate and private key and associates
	// the hash of the certificates with the given chaincode name
	Generate(ccName string) (*accesscontrol.CertAndPrivKeyPair, error)
}

func (r *RuntimeLauncher) ChaincodeClientInfo(ccid string) (*ccintf.PeerConnection, error) {
	var tlsConfig *ccintf.TLSConfig
	if r.CertGenerator != nil {
		certKeyPair, err := r.CertGenerator.Generate(string(ccid))
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to generate TLS certificates for %s", ccid)
		}

		tlsConfig = &ccintf.TLSConfig{
			ClientCert: certKeyPair.Cert,
			ClientKey:  certKeyPair.Key,
			RootCert:   r.CACert,
		}
	}

	return &ccintf.PeerConnection{
		Address:   r.PeerAddress,
		TLSConfig: tlsConfig,
	}, nil
}

// Launch 方法用于启动链码。
// 方法接收者：*RuntimeLauncher
// 输入参数：
//   - ccid：string 类型，表示链码ID。
//   - streamHandler：extcc.StreamHandler 类型，表示流处理程序。
//
// 返回值：
//   - error：如果启动运行时时出现错误，则返回错误。
func (r *RuntimeLauncher) Launch(ccid string, streamHandler extcc.StreamHandler) error {
	var startFailCh chan error
	var timeoutCh <-chan time.Time

	startTime := time.Now()
	launchState, alreadyStarted := r.Registry.Launching(ccid)
	if !alreadyStarted {
		startFailCh = make(chan error, 1)
		timeoutCh = time.NewTimer(r.StartupTimeout).C

		go func() {
			// 完成构建过程以获取 connecion 信息
			ccservinfo, err := r.Runtime.Build(ccid)
			if err != nil {
				startFailCh <- errors.WithMessage(err, "构建链码时出错")
				return
			}

			// 链码服务器型号表示... 继续连接到CC
			if ccservinfo != nil {
				if err = r.ConnectionHandler.Stream(ccid, ccservinfo, streamHandler); err != nil {
					startFailCh <- errors.WithMessagef(err, "%s 连接失败", ccid)
					return
				}

				launchState.Notify(errors.Errorf("与 %s 的连接已终止", ccid))
				return
			}

			// 默认的对等即服务器模型...计算CC回调的连接信息
			// 并继续启动链码
			ccinfo, err := r.ChaincodeClientInfo(ccid)
			if err != nil {
				startFailCh <- errors.WithMessage(err, "无法获取链码连接信息")
				return
			}
			if ccinfo == nil {
				startFailCh <- errors.New("无法获取链码连接信息")
				return
			}
			if err = r.Runtime.Start(ccid, ccinfo); err != nil {
				startFailCh <- errors.WithMessage(err, "启动链码容器时出错")
				return
			}
			exitCode, err := r.Runtime.Wait(ccid)
			if err != nil {
				launchState.Notify(errors.Wrap(err, "等待链码容器退出失败"))
			}
			launchState.Notify(errors.Errorf("链码容器已退出 %d", exitCode))
		}()
	}

	var err error
	select {
	case <-launchState.Done():
		err = errors.WithMessage(launchState.Err(), "链码注册失败")
	case err = <-startFailCh:
		launchState.Notify(err)
		r.Metrics.LaunchFailures.With("chaincode", ccid).Add(1)
	case <-timeoutCh:
		err = errors.Errorf("启动链码 %s 时超时过期", ccid)
		launchState.Notify(err)
		r.Metrics.LaunchTimeouts.With("chaincode", ccid).Add(1)
	}

	success := true
	if err != nil && !alreadyStarted {
		success = false
		chaincodeLogger.Debugf("链码启动时由于错误而停止: %+v", err)
		defer r.Registry.Deregister(ccid)
	}

	r.Metrics.LaunchDuration.With(
		"chaincode", ccid,
		"success", strconv.FormatBool(success),
	).Observe(time.Since(startTime).Seconds())

	chaincodeLogger.Debug("链码启动完成")
	return err
}

func (r *RuntimeLauncher) Stop(ccid string) error {
	err := r.Runtime.Stop(ccid)
	if err != nil {
		return errors.WithMessagef(err, "failed to stop chaincode %s", ccid)
	}

	return nil
}
