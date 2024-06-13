/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"crypto/rand"
	"fmt"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/bccsp/factory"
	"io"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

// ComputeSHA256 返回对数据的hash
// 统一调整为 common.ComputeHASH(data)
func ComputeSHA256(data []byte) (hash []byte) {
	hash, err := common.ComputeHASH(data)
	if err != nil {
		panic(fmt.Errorf("计算失败 hash on [% x]", data))
	}
	return
}

func ComputeGMSM3(data []byte) (hash []byte) {
	hash, err := factory.GetDefault().Hash(data, &bccsp.GMSM3Opts{})
	if err != nil {
		panic(fmt.Errorf("计算失败 hash on [% x]", data))
	}
	return
}

// ComputeSHA3256 返回对数据的hash
func ComputeSHA3256(data []byte) (hash []byte) {
	hash, err := common.ComputeHASH(data)
	if err != nil {
		panic(fmt.Errorf("计算失败 hash on [% x]", data))
	}
	return
}

// GenerateBytesUUID 返回基于RFC的UUID，4122返回生成的字节
func GenerateBytesUUID() []byte {
	uuid := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, uuid)
	if err != nil {
		panic(fmt.Sprintf("错误生成 UUID: %s", err))
	}

	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80

	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40

	return uuid
}

// GenerateUUID 返回基于RFC 4122的UUID
func GenerateUUID() string {
	uuid := GenerateBytesUUID()
	return idBytesToStr(uuid)
}

// CreateUtcTimestamp 返回UTC格式的google/protobuf/时间戳
func CreateUtcTimestamp() *timestamp.Timestamp {
	now := time.Now().UTC()
	secs := now.Unix()
	nanos := int32(now.UnixNano() - (secs * 1000000000))
	return &(timestamp.Timestamp{Seconds: secs, Nanos: nanos})
}

func idBytesToStr(id []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:])
}

// ToChaincodeArgs 将字符串args转换为 [] 字节args
func ToChaincodeArgs(args ...string) [][]byte {
	bargs := make([][]byte, len(args))
	for i, arg := range args {
		bargs[i] = []byte(arg)
	}
	return bargs
}

// ConcatenateBytes 函数用于将多个字节切片连接成一个字节切片。
// 输入参数：
//   - data：要连接的多个字节切片。
//
// 返回值：
//   - []byte：连接后的字节切片。
func ConcatenateBytes(data ...[]byte) []byte {
	finalLength := 0
	for _, slice := range data {
		finalLength += len(slice)
	}
	result := make([]byte, finalLength)
	last := 0
	for _, slice := range data {
		for i := range slice {
			result[i+last] = slice[i]
		}
		last += len(slice)
	}
	return result
}
