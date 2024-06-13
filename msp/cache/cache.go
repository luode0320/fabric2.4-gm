/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cache

import (
	pmsp "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/msp"
	"github.com/pkg/errors"
)

const (
	deserializeIdentityCacheSize = 100
	validateIdentityCacheSize    = 100
	satisfiesPrincipalCacheSize  = 100
)

var mspLogger = flogging.MustGetLogger("msp")

// New 函数用于创建一个 Cache-MSP 实例。
//
// 参数：
//   - o msp.MSP：要缓存的 MSP 实例。
//
// 返回值：
//   - msp.MSP：创建的 Cache-MSP 实例。
//   - error：如果发生错误，则返回错误信息。
func New(o msp.MSP) (msp.MSP, error) {
	mspLogger.Debugf("正在创建 Cache-MSP 实例")

	// 检查传入的 MSP 实例是否为 nil
	if o == nil {
		return nil, errors.Errorf("无效的传递MSP. 它不能为nil.")
	}

	// 创建 Cache-MSP 实例
	theMsp := &cachedMSP{MSP: o}
	theMsp.deserializeIdentityCache = newSecondChanceCache(deserializeIdentityCacheSize)
	theMsp.satisfiesPrincipalCache = newSecondChanceCache(satisfiesPrincipalCacheSize)
	theMsp.validateIdentityCache = newSecondChanceCache(validateIdentityCacheSize)

	return theMsp, nil
}

// cachedMSP 结构体表示一个带有缓存的 MSP 实例。
type cachedMSP struct {
	msp.MSP

	// 缓存用于 DeserializeIdentity.
	deserializeIdentityCache *secondChanceCache

	// 缓存用于 validateIdentity
	validateIdentityCache *secondChanceCache

	// 基本上主体的映射 =>identities =>stringified到布尔值指定此身份是否满足此主体
	satisfiesPrincipalCache *secondChanceCache
}

type cachedIdentity struct {
	msp.Identity
	cache *cachedMSP
}

func (id *cachedIdentity) SatisfiesPrincipal(principal *pmsp.MSPPrincipal) error {
	return id.cache.SatisfiesPrincipal(id.Identity, principal)
}

// Validate 验证 id 的 msp.Identity。
func (id *cachedIdentity) Validate() error {
	// 验证给定的 msp.Identity。
	return id.cache.Validate(id.Identity)
}

// DeserializeIdentity 反序列化给定的序列化身份标识符，并返回对应的 msp.Identity 实例。
//
// 输入参数：
//   - serializedIdentity：要反序列化的身份标识符的字节流。
//
// 返回值：
//   - msp.Identity：msp.Identity 实例，表示反序列化后的身份。
//   - error：如果反序列化过程中发生错误，则返回非空的错误。
func (c *cachedMSP) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	// 从缓存中获取反序列化后的身份标识符
	id, ok := c.deserializeIdentityCache.get(string(serializedIdentity))
	if ok {
		return &cachedIdentity{
			cache:    c,
			Identity: id.(msp.Identity),
		}, nil
	}

	// 如果缓存中不存在，则进行反序列化操作
	id, err := c.MSP.DeserializeIdentity(serializedIdentity)
	if err == nil {
		// 将反序列化后的身份标识符添加到缓存中
		c.deserializeIdentityCache.add(string(serializedIdentity), id)
		return &cachedIdentity{
			cache:    c,
			Identity: id.(msp.Identity),
		}, nil
	}
	return nil, err
}

// Setup 方法用于设置 MSP 实例。
//
// 参数：
//   - config *pmsp.MSPConfig：MSP 配置。
//
// 返回值：
//   - error：如果发生错误，则返回错误信息。
func (c *cachedMSP) Setup(config *pmsp.MSPConfig) error {
	// 清空缓存
	c.cleanCache()

	// 调用原始 MSP 实例的 Setup 方法
	return c.MSP.Setup(config)
}

// Validate 验证给定的 msp.Identity。
//
// 输入参数：
//   - id：要验证的 msp.Identity。
//
// 返回值：
//   - error：如果验证成功，则返回 nil；否则返回非空的错误。
func (c *cachedMSP) Validate(id msp.Identity) error {
	// 获取 msp.Identity 的标识符
	identifier := id.GetIdentifier()
	key := identifier.Mspid + ":" + identifier.Id

	// 检查缓存中是否存在已验证的身份
	_, ok := c.validateIdentityCache.get(key)
	if ok {
		// 缓存只存储已验证的身份
		return nil
	}

	// 调用 MSP 的 Validate 方法进行验证
	err := c.MSP.Validate(id)
	if err == nil {
		// 将已验证的身份添加到缓存中
		c.validateIdentityCache.add(key, true)
	}

	return err
}

// SatisfiesPrincipal 方法用于判断给定的身份是否满足指定的 MSPPrincipal。
// 方法接收者：c *cachedMSP，表示 cachedMSP 结构体的指针。
// 输入参数：
//   - id msp.Identity，表示身份对象。
//   - principal *pmsp.MSPPrincipal，表示要满足的 MSPPrincipal 对象。
//
// 返回值：
//   - error，表示判断过程中的错误，如果身份满足指定的 MSPPrincipal 则返回nil。
func (c *cachedMSP) SatisfiesPrincipal(id msp.Identity, principal *pmsp.MSPPrincipal) error {
	// 获取身份的标识符
	identifier := id.GetIdentifier()
	// 构建身份键，格式为 Mspid:Id
	identityKey := identifier.Mspid + ":" + identifier.Id
	// 构建 MSPPrincipal 键，格式为 PrincipalClassificationPrincipal
	principalKey := string(principal.PrincipalClassification) + string(principal.Principal)
	// 构建缓存键，格式为 身份键 + MSPPrincipal 键
	key := identityKey + principalKey

	// 从缓存中获取结果
	v, ok := c.satisfiesPrincipalCache.get(key)
	if ok {
		if v == nil {
			return nil
		}

		return v.(error)
	}

	// 调用 MSP 的 SatisfiesPrincipal 方法判断身份是否满足指定的 MSPPrincipal
	err := c.MSP.SatisfiesPrincipal(id, principal)

	// 将结果添加到缓存中
	c.satisfiesPrincipalCache.add(key, err)
	return err
}

// cleanCache 方法用于清空缓存。
func (c *cachedMSP) cleanCache() {
	// 重新创建 DeserializeIdentity 缓存
	c.deserializeIdentityCache = newSecondChanceCache(deserializeIdentityCacheSize)

	// 重新创建 SatisfiesPrincipal 缓存
	c.satisfiesPrincipalCache = newSecondChanceCache(satisfiesPrincipalCacheSize)

	// 重新创建 ValidateIdentity 缓存
	c.validateIdentityCache = newSecondChanceCache(validateIdentityCacheSize)
}
