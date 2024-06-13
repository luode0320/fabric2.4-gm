/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cceventmgmt

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
)

var logger = flogging.MustGetLogger("cceventmgmt")

var mgr *Mgr

// Initialize 初始化事件管理
func Initialize(ccInfoProvider ChaincodeInfoProvider) {
	initialize(ccInfoProvider)
}

func initialize(ccInfoProvider ChaincodeInfoProvider) {
	mgr = newMgr(ccInfoProvider)
}

// GetMgr returns the reference to singleton event manager
func GetMgr() *Mgr {
	return mgr
}

// Mgr 封装了与账本相关的事件的重要交互。
type Mgr struct {
	// rwlock 主要用于在部署交易、链码安装和通道创建之间进行同步。
	// 理想情况下，对等节点中的不同服务应该被设计为在不同的重要事件上公开锁，
	// 以便在需要时可以进行同步。然而，在缺乏任何这样的系统级设计的情况下，
	// 我们使用此锁进行上下文使用。
	rwlock               sync.RWMutex
	infoProvider         ChaincodeInfoProvider // 使事件管理器能够为给定的链码检索链码信息。
	ccLifecycleListeners map[string][]ChaincodeLifecycleEventListener
	callbackStatus       *callbackStatus
}

func newMgr(chaincodeInfoProvider ChaincodeInfoProvider) *Mgr {
	return &Mgr{
		infoProvider:         chaincodeInfoProvider,                              // 使事件管理器能够为给定的链码检索链码信息。
		ccLifecycleListeners: make(map[string][]ChaincodeLifecycleEventListener), // 账本组件（主要是状态数据库）能够监听链码生命周期事件
		callbackStatus:       newCallbackStatus(),                                // 回调状态
	}
}

// Register registers a ChaincodeLifecycleEventListener for given ledgerid
// Since, `Register` is expected to be invoked when creating/opening a ledger instance
func (m *Mgr) Register(ledgerid string, l ChaincodeLifecycleEventListener) {
	// write lock to synchronize concurrent 'chaincode install' operations with ledger creation/open
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	m.ccLifecycleListeners[ledgerid] = append(m.ccLifecycleListeners[ledgerid], l)
}

// RegisterAndInvokeFor registers the listener and in addition invokes the listener for each chaincode that is present in the supplied
// list of legacyChaincodes and is installed on the peer
func (m *Mgr) RegisterAndInvokeFor(legacyChaincodes []*ChaincodeDefinition, ledgerid string, l ChaincodeLifecycleEventListener) error {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	m.ccLifecycleListeners[ledgerid] = append(m.ccLifecycleListeners[ledgerid], l)
	for _, chaincodeDefinition := range legacyChaincodes {
		installed, dbArtifacts, err := m.infoProvider.RetrieveChaincodeArtifacts(chaincodeDefinition)
		if err != nil {
			return err
		}
		if !installed {
			continue
		}
		if err := l.HandleChaincodeDeploy(chaincodeDefinition, dbArtifacts); err != nil {
			return err
		}
		l.ChaincodeDeployDone(true)
	}
	return nil
}

// HandleChaincodeDeploy is expected to be invoked when a chaincode is deployed via a deploy transaction
// The `chaincodeDefinitions` parameter contains all the chaincodes deployed in a block
// We need to store the last received `chaincodeDefinitions` because this function is expected to be invoked
// after the deploy transactions validation is performed but not committed yet to the ledger. Further, we
// release the read lock after this function. This leaves a small window when a `chaincode install` can happen
// before the deploy transaction is committed and hence the function `HandleChaincodeInstall` may miss finding
// the deployed chaincode. So, in function `HandleChaincodeInstall`, we explicitly check for chaincode deployed
// in this stored `chaincodeDefinitions`
func (m *Mgr) HandleChaincodeDeploy(chainid string, chaincodeDefinitions []*ChaincodeDefinition) error {
	logger.Debugf("Channel [%s]: Handling chaincode deploy event for chaincode [%s]", chainid, chaincodeDefinitions)
	// Read lock to allow concurrent deploy on multiple channels but to synchronize concurrent `chaincode install` operation
	m.rwlock.RLock()
	for _, chaincodeDefinition := range chaincodeDefinitions {
		installed, dbArtifacts, err := m.infoProvider.RetrieveChaincodeArtifacts(chaincodeDefinition)
		if err != nil {
			return err
		}
		if !installed {
			logger.Infof("Channel [%s]: Chaincode [%s] is not installed hence no need to create chaincode artifacts for endorsement",
				chainid, chaincodeDefinition)
			continue
		}
		m.callbackStatus.setDeployPending(chainid)
		if err := m.invokeHandler(chainid, chaincodeDefinition, dbArtifacts); err != nil {
			logger.Warningf("Channel [%s]: Error while invoking a listener for handling chaincode install event: %s", chainid, err)
			return err
		}
		logger.Debugf("Channel [%s]: Handled chaincode deploy event for chaincode [%s]", chainid, chaincodeDefinitions)
	}
	return nil
}

// ChaincodeDeployDone is expected to be called when the deploy transaction state is committed
func (m *Mgr) ChaincodeDeployDone(chainid string) {
	// release the lock acquired in function `HandleChaincodeDeploy`
	defer m.rwlock.RUnlock()
	if m.callbackStatus.isDeployPending(chainid) {
		m.invokeDoneOnHandlers(chainid, true)
		m.callbackStatus.unsetDeployPending(chainid)
	}
}

// HandleChaincodeInstall is expected to get invoked during installation of a chaincode package
func (m *Mgr) HandleChaincodeInstall(chaincodeDefinition *ChaincodeDefinition, dbArtifacts []byte) error {
	logger.Debugf("HandleChaincodeInstall() - chaincodeDefinition=%#v", chaincodeDefinition)
	// Write lock prevents concurrent deploy operations
	m.rwlock.Lock()
	for chainid := range m.ccLifecycleListeners {
		logger.Debugf("Channel [%s]: Handling chaincode install event for chaincode [%s]", chainid, chaincodeDefinition)
		var deployedCCInfo *ledger.DeployedChaincodeInfo
		var err error
		if deployedCCInfo, err = m.infoProvider.GetDeployedChaincodeInfo(chainid, chaincodeDefinition); err != nil {
			logger.Warningf("Channel [%s]: Error while getting the deployment status of chaincode: %s", chainid, err)
			return err
		}
		if deployedCCInfo == nil {
			logger.Debugf("Channel [%s]: Chaincode [%s] is not deployed on channel hence not creating chaincode artifacts.",
				chainid, chaincodeDefinition)
			continue
		}
		if !deployedCCInfo.IsLegacy {
			// the chaincode has already been defined via new lifecycle, we reach here because of a subsequent
			// install of chaincode using legacy package. So, ignoring this event
			logger.Debugf("Channel [%s]: Chaincode [%s] is already defined in new lifecycle hence not creating chaincode artifacts.",
				chainid, chaincodeDefinition)
			continue
		}
		m.callbackStatus.setInstallPending(chainid)
		chaincodeDefinition.CollectionConfigs = deployedCCInfo.ExplicitCollectionConfigPkg
		if err := m.invokeHandler(chainid, chaincodeDefinition, dbArtifacts); err != nil {
			logger.Warningf("Channel [%s]: Error while invoking a listener for handling chaincode install event: %s", chainid, err)
			return err
		}
		logger.Debugf("Channel [%s]: Handled chaincode install event for chaincode [%s]", chainid, chaincodeDefinition)
	}
	return nil
}

// ChaincodeInstallDone is expected to get invoked when chaincode install finishes
func (m *Mgr) ChaincodeInstallDone(succeeded bool) {
	// release the lock acquired in function `HandleChaincodeInstall`
	defer m.rwlock.Unlock()
	for chainid := range m.callbackStatus.installPending {
		m.invokeDoneOnHandlers(chainid, succeeded)
		m.callbackStatus.unsetInstallPending(chainid)
	}
}

func (m *Mgr) invokeHandler(chainid string, chaincodeDefinition *ChaincodeDefinition, dbArtifactsTar []byte) error {
	listeners := m.ccLifecycleListeners[chainid]
	for _, listener := range listeners {
		if err := listener.HandleChaincodeDeploy(chaincodeDefinition, dbArtifactsTar); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mgr) invokeDoneOnHandlers(chainid string, succeeded bool) {
	listeners := m.ccLifecycleListeners[chainid]
	for _, listener := range listeners {
		listener.ChaincodeDeployDone(succeeded)
	}
}

type callbackStatus struct {
	l              sync.Mutex
	deployPending  map[string]bool
	installPending map[string]bool
}

func newCallbackStatus() *callbackStatus {
	return &callbackStatus{
		deployPending:  make(map[string]bool),
		installPending: make(map[string]bool),
	}
}

func (s *callbackStatus) setDeployPending(channelID string) {
	s.l.Lock()
	defer s.l.Unlock()
	s.deployPending[channelID] = true
}

func (s *callbackStatus) unsetDeployPending(channelID string) {
	s.l.Lock()
	defer s.l.Unlock()
	delete(s.deployPending, channelID)
}

func (s *callbackStatus) isDeployPending(channelID string) bool {
	s.l.Lock()
	defer s.l.Unlock()
	return s.deployPending[channelID]
}

func (s *callbackStatus) setInstallPending(channelID string) {
	s.l.Lock()
	defer s.l.Unlock()
	s.installPending[channelID] = true
}

func (s *callbackStatus) unsetInstallPending(channelID string) {
	s.l.Lock()
	defer s.l.Unlock()
	delete(s.installPending, channelID)
}
