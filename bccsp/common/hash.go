package common

import (
	"github.com/hyperledger/fabric/bccsp/factory"
)

// ComputeHASH 函数用于计算给定数据的哈希值。
// 输入参数：
//   - data：要计算哈希值的数据。
//
// 返回值：
//   - []byte：计算得到的哈希值。
//   - error：如果计算哈希值过程中出现错误，则返回相应的错误信息。
//
// @Author: 罗德
// @Date: 2023/10/24
func ComputeHASH(data []byte) ([]byte, error) {
	// todo luode 进行国密sm3的改造
	hash, err := factory.GetDefault().Hash(data, Hash())
	if err != nil {
		return nil, err
	}
	return hash, nil
}
