/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package identity

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	errors "github.com/pkg/errors"
)

// identityUsageThreshold sets the maximum time that an identity
// can not be used to verify some signature before it will be deleted
var usageThreshold = time.Hour

// Mapper 保存pkiID到对等体的证书 (标识) 之间的映射
type Mapper interface {
	// Put 将身份与其给定的pkiID相关联，并在给定的pkiID与身份不匹配的情况下返回错误
	Put(pkiID common.PKIidType, identity api.PeerIdentityType) error

	// Get 返回给定pkiID的标识，如果找不到此类标识，则返回错误
	Get(pkiID common.PKIidType) (api.PeerIdentityType, error)

	// Sign 对消息进行签名，在成功时返回签名消息或在失败时返回错误
	Sign(msg []byte) ([]byte, error)

	// Verify 验证已签名的消息
	Verify(vkID, signature, message []byte) error

	// GetPKIidOfCert 返回证书的pki-id
	GetPKIidOfCert(api.PeerIdentityType) common.PKIidType

	// SuspectPeers 重新验证与给定谓词匹配的所有对等方
	SuspectPeers(isSuspected api.PeerSuspector)

	// IdentityInfo 返回已知对等身份信息
	IdentityInfo() api.PeerIdentitySet

	// Stop 停止映射器的所有背景计算
	Stop()
}

type purgeTrigger func(pkiID common.PKIidType, identity api.PeerIdentityType)

// identityMapperImpl 是一个实现Mapper的结构
type identityMapperImpl struct {
	onPurge      purgeTrigger
	mcs          api.MessageCryptoService   // gossip组件与对等方的密码层之间的契约，用于gossip组件验证
	sa           api.SecurityAdvisor        // 定义提供安全和身份相关功能的外部辅助对象
	pkiID2Cert   map[string]*storedIdentity // 存储身份
	sync.RWMutex                            // 读写锁
	stopChan     chan struct{}              //停止标识
	once         sync.Once                  // 只执行一次的方法
	selfPKIID    string                     // pki-id
}

// NewIdentityMapper 我们所需要的只是对MessageCryptoService的引用
func NewIdentityMapper(mcs api.MessageCryptoService, selfIdentity api.PeerIdentityType, onPurge purgeTrigger, sa api.SecurityAdvisor) Mapper {
	// 根据证书返回对等方身份的pki-id
	selfPKIID := mcs.GetPKIidOfCert(selfIdentity)
	// 一个实现Mapper的结构, 存储身份相关的数据
	idMapper := &identityMapperImpl{
		onPurge:    onPurge,
		mcs:        mcs,                              // gossip组件与对等方的密码层之间的契约，用于gossip组件验证
		pkiID2Cert: make(map[string]*storedIdentity), // 存储身份
		stopChan:   make(chan struct{}),              //停止标识
		selfPKIID:  string(selfPKIID),                // pki-id
		sa:         sa,                               // 定义提供安全和身份相关功能的外部辅助对象
	}
	if err := idMapper.Put(selfPKIID, selfIdentity); err != nil {
		panic(errors.Wrap(err, "未能将我们自己的身份放入身份映射器"))
	}

	// 定期清除未使用的标识
	go idMapper.periodicalPurgeUnusedIdentities()
	return idMapper
}

func (is *identityMapperImpl) periodicalPurgeUnusedIdentities() {
	usageTh := GetIdentityUsageThreshold()
	for {
		select {
		case <-is.stopChan:
			return
		case <-time.After(usageTh / 10):
			is.SuspectPeers(func(_ api.PeerIdentityType) bool {
				return false
			})
		}
	}
}

// Put put将一个身份与其给定的pkiID相关联，并在给定的pkiID与身份不匹配的情况下返回错误
func (is *identityMapperImpl) Put(pkiID common.PKIidType, identity api.PeerIdentityType) error {
	if pkiID == nil {
		return errors.New("PKIID 证书hash值 为nil")
	}
	if identity == nil {
		return errors.New("identity证书 为nil")
	}

	// 获取证书的过期时间
	expirationDate, err := is.mcs.Expiration(identity)
	if err != nil {
		return errors.Wrap(err, "获取身份证书的过期时间失败")
	}

	// 验证远程对等方的身份.
	if err := is.mcs.ValidateIdentity(identity); err != nil {
		return err
	}

	// 根据证书返回对等方身份的pki-id(mspid + 证书 的 哈希值)
	id := is.mcs.GetPKIidOfCert(identity)
	if !bytes.Equal(pkiID, id) {
		return errors.New("身份证书与计算出的 pkiID(mspid + 证书 的 哈希值) 不匹配")
	}

	is.Lock()
	defer is.Unlock()
	// 检查身份是否已存在。
	// 如果是这样，则无需覆盖它。
	if _, exists := is.pkiID2Cert[string(pkiID)]; exists {
		return nil
	}

	var expirationTimer *time.Timer
	if !expirationDate.IsZero() {
		if time.Now().After(expirationDate) {
			return errors.New("gossip 身份已过期")
		}
		// 身份信息将在过期后一毫秒被清除
		timeToLive := time.Until(expirationDate.Add(time.Millisecond))
		expirationTimer = time.AfterFunc(timeToLive, func() {
			is.delete(pkiID, identity)
		})
	}

	// 保存一个身份
	is.pkiID2Cert[string(id)] = newStoredIdentity(pkiID, identity, expirationTimer, is.sa.OrgByPeerIdentity(identity))
	return nil
}

// get returns the identity of a given pkiID, or error if such an identity
// isn't found
func (is *identityMapperImpl) Get(pkiID common.PKIidType) (api.PeerIdentityType, error) {
	is.RLock()
	defer is.RUnlock()
	storedIdentity, exists := is.pkiID2Cert[string(pkiID)]
	if !exists {
		return nil, errors.New("PKIID wasn't found")
	}
	return storedIdentity.fetchIdentity(), nil
}

// Sign signs a message, returns a signed message on success
// or an error on failure
func (is *identityMapperImpl) Sign(msg []byte) ([]byte, error) {
	return is.mcs.Sign(msg)
}

func (is *identityMapperImpl) Stop() {
	is.once.Do(func() {
		close(is.stopChan)
	})
}

// Verify verifies a signed message
func (is *identityMapperImpl) Verify(vkID, signature, message []byte) error {
	cert, err := is.Get(vkID)
	if err != nil {
		return err
	}
	return is.mcs.Verify(cert, signature, message)
}

// GetPKIidOfCert returns the PKI-ID of a certificate
func (is *identityMapperImpl) GetPKIidOfCert(identity api.PeerIdentityType) common.PKIidType {
	return is.mcs.GetPKIidOfCert(identity)
}

// SuspectPeers re-validates all peers that match the given predicate
func (is *identityMapperImpl) SuspectPeers(isSuspected api.PeerSuspector) {
	for _, identity := range is.validateIdentities(isSuspected) {
		identity.cancelExpirationTimer()
		is.delete(identity.pkiID, identity.peerIdentity)
	}
}

// validateIdentities returns a list of identities that have been revoked, expired or haven't been
// used for a long time
func (is *identityMapperImpl) validateIdentities(isSuspected api.PeerSuspector) []*storedIdentity {
	now := time.Now()
	usageTh := GetIdentityUsageThreshold()
	is.RLock()
	defer is.RUnlock()
	var revokedIdentities []*storedIdentity
	for pkiID, storedIdentity := range is.pkiID2Cert {
		if pkiID != is.selfPKIID && storedIdentity.fetchLastAccessTime().Add(usageTh).Before(now) {
			revokedIdentities = append(revokedIdentities, storedIdentity)
			continue
		}
		if !isSuspected(storedIdentity.peerIdentity) {
			continue
		}
		if err := is.mcs.ValidateIdentity(storedIdentity.fetchIdentity()); err != nil {
			revokedIdentities = append(revokedIdentities, storedIdentity)
		}
	}
	return revokedIdentities
}

// IdentityInfo returns information known peer identities
func (is *identityMapperImpl) IdentityInfo() api.PeerIdentitySet {
	var res api.PeerIdentitySet
	is.RLock()
	defer is.RUnlock()
	for _, storedIdentity := range is.pkiID2Cert {
		res = append(res, api.PeerIdentityInfo{
			Identity:     storedIdentity.peerIdentity,
			PKIId:        storedIdentity.pkiID,
			Organization: storedIdentity.orgId,
		})
	}
	return res
}

func (is *identityMapperImpl) delete(pkiID common.PKIidType, identity api.PeerIdentityType) {
	is.Lock()
	defer is.Unlock()
	is.onPurge(pkiID, identity)
	delete(is.pkiID2Cert, string(pkiID))
}

type storedIdentity struct {
	pkiID           common.PKIidType
	lastAccessTime  int64
	peerIdentity    api.PeerIdentityType
	orgId           api.OrgIdentityType
	expirationTimer *time.Timer
}

func newStoredIdentity(pkiID common.PKIidType, identity api.PeerIdentityType, expirationTimer *time.Timer, org api.OrgIdentityType) *storedIdentity {
	return &storedIdentity{
		pkiID:           pkiID,
		lastAccessTime:  time.Now().UnixNano(),
		peerIdentity:    identity,
		expirationTimer: expirationTimer,
		orgId:           org,
	}
}

func (si *storedIdentity) fetchIdentity() api.PeerIdentityType {
	atomic.StoreInt64(&si.lastAccessTime, time.Now().UnixNano())
	return si.peerIdentity
}

func (si *storedIdentity) fetchLastAccessTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&si.lastAccessTime))
}

func (si *storedIdentity) cancelExpirationTimer() {
	if si.expirationTimer == nil {
		return
	}
	si.expirationTimer.Stop()
}

// SetIdentityUsageThreshold sets the usage threshold of identities.
// Identities that are not used at least once during the given time
// are purged
func SetIdentityUsageThreshold(duration time.Duration) {
	atomic.StoreInt64((*int64)(&usageThreshold), int64(duration))
}

// GetIdentityUsageThreshold returns the usage threshold of identities.
// Identities that are not used at least once during the usage threshold
// duration are purged.
func GetIdentityUsageThreshold() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(&usageThreshold)))
}
