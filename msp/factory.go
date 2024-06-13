/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"github.com/IBM/idemix"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/pkg/errors"
)

type MSPVersion int

const (
	MSPv1_0 = iota
	MSPv1_1
	MSPv1_3
	MSPv1_4_3
)

// NewOpts 主要是选择msp的版本
type NewOpts interface {
	// GetVersion 返回要实例化的MSP的版本
	GetVersion() MSPVersion
}

// NewBaseOpts 是所有MSP实例化选项的默认基本类型
type NewBaseOpts struct {
	Version MSPVersion
}

func (o *NewBaseOpts) GetVersion() MSPVersion {
	return o.Version
}

// BCCSPNewOpts 包含用于实例化新的基于BCCSP的 (X509) MSP的选项
type BCCSPNewOpts struct {
	NewBaseOpts
}

// IdemixNewOpts 包含用于实例化基于Idemix的新MSP的选项
type IdemixNewOpts struct {
	NewBaseOpts
}

// New 函数根据传入的 Opts 创建一个新的 MSP 实例。
// 参数：
//   - opts NewOpts：创建 MSP 实例所需的选项。
//   - cryptoProvider bccsp.BCCSP：加密服务提供者。
//
// 返回值：
//   - MSP：创建的 MSP 实例。
//   - error：如果创建过程中出现错误，则返回相应的错误信息。
func New(opts NewOpts, cryptoProvider bccsp.BCCSP) (MSP, error) {
	// 目前默认使用BCCSPNewOpts
	switch opts.(type) {
	case *BCCSPNewOpts:
		switch opts.GetVersion() {
		case MSPv1_0:
			return newBccspMsp(MSPv1_0, cryptoProvider)
		case MSPv1_1:
			return newBccspMsp(MSPv1_1, cryptoProvider)
		case MSPv1_3:
			return newBccspMsp(MSPv1_3, cryptoProvider)
		case MSPv1_4_3:
			return newBccspMsp(MSPv1_4_3, cryptoProvider)
		default:
			return nil, errors.Errorf("无效 *BCCSPNewOpts. 版本无法识别 [%v]", opts.GetVersion())
		}
	case *IdemixNewOpts:
		switch opts.GetVersion() {
		case MSPv1_4_3:
			fallthrough
		case MSPv1_3:
			msp, err := idemix.NewIdemixMsp(MSPv1_3)
			if err != nil {
				return nil, err
			}

			return &idemixMSPWrapper{msp.(*idemix.Idemixmsp)}, nil
		case MSPv1_1:
			msp, err := idemix.NewIdemixMsp(MSPv1_1)
			if err != nil {
				return nil, err
			}

			return &idemixMSPWrapper{msp.(*idemix.Idemixmsp)}, nil
		default:
			return nil, errors.Errorf("无效 *IdemixNewOpts. 版本无法识别 [%v]", opts.GetVersion())
		}
	default:
		return nil, errors.Errorf("无效 msp.NewOpts 实例. 它必须是要么 *BCCSPNewOpts 或者 *IdemixNewOpts. 当前: [%v]", opts)
	}
}
