package gm

import (
	"hash"

	"github.com/hyperledger/fabric/bccsp"
)

// hasher 定义hasher 结构体，实现内部的一个 Hasher 接口
type hasher struct {
	hash func() hash.Hash
}

// Hash 接受一个消息（msg），并返回对消息进行哈希后的结果(参数opts无用)
func (c *hasher) Hash(msg []byte, opts bccsp.HashOpts) (hash []byte, err error) {
	// 获取一个哈希实例（hash.Hash）
	h := c.hash()
	// 将消息写入哈希实例
	h.Write(msg)
	// 调用Sum(nil)方法获取哈希结果
	return h.Sum(nil), nil
}

// GetHash 返回一个哈希实例(参数opts无用)
func (c *hasher) GetHash(opts bccsp.HashOpts) (h hash.Hash, err error) {
	return c.hash(), nil
}
