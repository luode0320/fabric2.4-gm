package gm

import (
	"errors"

	"github.com/hyperledger/fabric/bccsp"
)

// 定义国密派生器，实现 KeyDeriv 接口
type smPublicKeyKeyDerive struct{}

func (kd *smPublicKeyKeyDerive) KeyDeriv(k bccsp.Key, opts bccsp.KeyDerivOpts) (dk bccsp.Key, err error) {
	return nil, errors.New("国密无派生器")
}
