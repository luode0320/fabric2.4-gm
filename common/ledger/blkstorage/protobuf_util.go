/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// buffer provides a wrapper on top of proto.Buffer.
// The purpose of this wrapper is to get to know the current position in the []byte
type buffer struct {
	buf      *proto.Buffer
	position int
}

// newBuffer constructs a new instance of Buffer
func newBuffer(b []byte) *buffer {
	return &buffer{proto.NewBuffer(b), 0}
}

// DecodeVarint 包装了实际的方法并更新位置。
// 方法接收者：b（buffer类型的指针）
// 输入参数：无
// 返回值：
//   - uint64：解码得到的变长整数值。
//   - error：如果解码变长整数时出错，则返回错误。
func (b *buffer) DecodeVarint() (uint64, error) {
	// 调用buf.DecodeVarint方法解码变长整数值
	val, err := b.buf.DecodeVarint()
	// 如果解码成功，则更新位置
	if err == nil {
		b.position += proto.SizeVarint(val)
	} else {
		err = errors.Wrap(err, "解码时出错 proto.Buffer")
	}
	// 返回解码得到的变长整数值和错误
	return val, err
}

// DecodeRawBytes wraps the actual method and updates the position
func (b *buffer) DecodeRawBytes(alloc bool) ([]byte, error) {
	val, err := b.buf.DecodeRawBytes(alloc)
	if err == nil {
		b.position += proto.SizeVarint(uint64(len(val))) + len(val)
	} else {
		err = errors.Wrap(err, "error decoding raw bytes with proto.Buffer")
	}
	return val, err
}

// GetBytesConsumed 返回基础 [] 字节中当前位置的偏移量
func (b *buffer) GetBytesConsumed() int {
	return b.position
}
