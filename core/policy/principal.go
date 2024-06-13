/*
Copyright IBM Corp. 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package policy

import (
	"github.com/golang/protobuf/proto"
	protomsp "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
)

const (
	// Admins is the label for the local MSP admins
	Admins = "Admins"
	// Members is the label for the local MSP members
	Members = "Members"
)

type MSPPrincipalGetter interface {
	// Get returns an MSP principal for the given role
	Get(role string) (*protomsp.MSPPrincipal, error)
}

func NewLocalMSPPrincipalGetter(localMSP msp.MSP) MSPPrincipalGetter {
	return &localMSPPrincipalGetter{
		localMSP: localMSP,
	}
}

type localMSPPrincipalGetter struct {
	localMSP msp.MSP
}

// Get 根据角色获取本地MSP的 MSP 主体负责人。
// 参数：
//   - role: string 类型，表示角色。
//
// 返回值：
//   - *protomsp.MSPPrincipal: 表示MSPPrincipal。
//   - error: 如果提取本地MSP标识符失败、编组失败或角色未被识别，则返回错误。
func (m *localMSPPrincipalGetter) Get(role string) (*protomsp.MSPPrincipal, error) {
	mspid, err := m.localMSP.GetIdentifier()
	if err != nil {
		return nil, errors.WithMessage(err, "无法提取本地mspid标识符")
	}

	switch role {
	case Admins:
		// 构建MSPRole结构体
		principalBytes, err := proto.Marshal(&protomsp.MSPRole{Role: protomsp.MSPRole_ADMIN, MspIdentifier: mspid})
		if err != nil {
			return nil, errors.Wrap(err, "构建 MSPRole Admins 结构体出错")
		}

		// 构建MSPPrincipal结构体
		return &protomsp.MSPPrincipal{
			PrincipalClassification: protomsp.MSPPrincipal_ROLE,
			Principal:               principalBytes,
		}, nil
	case Members:
		// 构建MSPRole结构体
		principalBytes, err := proto.Marshal(&protomsp.MSPRole{Role: protomsp.MSPRole_MEMBER, MspIdentifier: mspid})
		if err != nil {
			return nil, errors.Wrap(err, "构建 MSPRole Members 结构体")
		}

		// 构建MSPPrincipal结构体
		return &protomsp.MSPPrincipal{
			PrincipalClassification: protomsp.MSPPrincipal_ROLE,
			Principal:               principalBytes,
		}, nil
	default:
		return nil, errors.Errorf("MSP主体角色 [%s] 未识别, 只能是 Admins/Members", role)
	}
}
