/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcc

import (
	"context"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/container/ccintf"
	"github.com/pkg/errors"

	pb "github.com/hyperledger/fabric-protos-go/peer"

	"google.golang.org/grpc"
)

var extccLogger = flogging.MustGetLogger("extcc")

// StreamHandler handles the `Chaincode` gRPC service with peer as client
type StreamHandler interface {
	HandleChaincodeStream(stream ccintf.ChaincodeStream) error
}

type ExternalChaincodeRuntime struct{}

// createConnection 方法用于创建标准的gRPC客户端连接，使用ClientConfig信息。
// 方法接收者：*ExternalChaincodeRuntime
// 输入参数：
//   - ccid：string 类型，表示链码ID。
//   - ccinfo：*ccintf.ChaincodeServerInfo 类型，表示链码服务器信息。
//
// 返回值：
//   - *grpc.ClientConn：表示创建的gRPC客户端连接。
//   - error：如果创建gRPC连接时出现错误，则返回错误。
func (i *ExternalChaincodeRuntime) createConnection(ccid string, ccinfo *ccintf.ChaincodeServerInfo) (*grpc.ClientConn, error) {
	// 使用ClientConfig信息创建gRPC客户端连接
	conn, err := ccinfo.ClientConfig.Dial(ccinfo.Address)
	if err != nil {
		return nil, errors.WithMessagef(err, "创建到 %s 的grpc连接时出错", ccinfo.Address)
	}

	extccLogger.Infof("外部链码 %s 连接成功: %s", ccinfo.Address, ccid)

	return conn, nil
}

func (i *ExternalChaincodeRuntime) Stream(ccid string, ccinfo *ccintf.ChaincodeServerInfo, sHandler StreamHandler) error {
	extccLogger.Infof("正在启动连接外部链码 %s: %s", ccinfo.Address, ccid)
	conn, err := i.createConnection(ccid, ccinfo)
	if err != nil {
		return errors.WithMessagef(err, "错误: 无法为 %s 创建连接", ccid)
	}

	defer conn.Close()

	// 创建客户端并开始流式处理
	client := pb.NewChaincodeClient(conn)

	stream, err := client.Connect(context.Background())
	if err != nil {
		return errors.WithMessagef(err, "创建到 %s 的grpc客户端连接时出错", ccid)
	}

	// 作为客户端的 peer 必须启动流。进程的其余部分不变
	sHandler.HandleChaincodeStream(stream)

	extccLogger.Debugf("外部链码 %s 客户端已退出", ccid)

	return nil
}
