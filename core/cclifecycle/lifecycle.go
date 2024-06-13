/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cclifecycle

import (
	"sync"

	"github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

// Logger is the logging instance for this package.
// It's exported because the tests override its backend
var Logger = flogging.MustGetLogger("discovery.lifecycle")

// MetadataManager 管理有关 lscc 链码的信息。
type MetadataManager struct {
	sync.RWMutex
	listeners              []MetadataChangeListener              // 监听器列表，用于监听元数据的变化
	installedCCs           []chaincode.InstalledChaincode        // 已安装链码的列表
	deployedCCsByChannel   map[string]*chaincode.MetadataMapping // 按通道存储的已部署链码的元数据映射
	queryCreatorsByChannel map[string]QueryCreator               // 按通道存储的查询创建器映射
}

//go:generate mockery -dir . -name MetadataChangeListener -case underscore  -output mocks/

// MetadataChangeListener 每当在特定通道的上下文中更改链码的元数据时运行
type MetadataChangeListener interface {
	HandleMetadataUpdate(channel string, chaincodes chaincode.MetadataSet)
}

// HandleMetadataUpdateFunc is triggered upon a change in the chaincode lifecycle
type HandleMetadataUpdateFunc func(channel string, chaincodes chaincode.MetadataSet)

// HandleMetadataUpdate runs whenever there is a change to the metadata
// of a chaincode in the context of a specific channel
func (handleMetadataUpdate HandleMetadataUpdateFunc) HandleMetadataUpdate(channel string, chaincodes chaincode.MetadataSet) {
	handleMetadataUpdate(channel, chaincodes)
}

//go:generate mockery -dir . -name Enumerator -case underscore  -output mocks/

// Enumerator enumerates chaincodes
type Enumerator interface {
	// Enumerate returns the installed chaincodes
	Enumerate() ([]chaincode.InstalledChaincode, error)
}

// EnumerateFunc 枚举已安装的链代码
type EnumerateFunc func() ([]chaincode.InstalledChaincode, error)

// Enumerate enumerates chaincodes
func (enumerate EnumerateFunc) Enumerate() ([]chaincode.InstalledChaincode, error) {
	return enumerate()
}

//go:generate mockery -dir . -name Query -case underscore  -output mocks/

// Query 查询状态
type Query interface {
	// GetState 获取给定命名空间和键的值。对于链码，命名空间对应于chaincodeId
	GetState(namespace string, key string) ([]byte, error)

	// Done 释放QueryExecutor占用的资源
	Done()
}

//go:generate mockery -dir . -name QueryCreator -case underscore  -output mocks/

// QueryCreator 创建查询
type QueryCreator interface {
	// NewQuery 创建新查询，或失败时出错
	NewQuery() (Query, error)
}

// QueryCreatorFunc creates a new query
type QueryCreatorFunc func() (Query, error)

// NewQuery creates a new Query, or error on failure
func (queryCreator QueryCreatorFunc) NewQuery() (Query, error) {
	return queryCreator()
}

// NewMetadataManager 为 lscc 链码创建一个元数据管理器。
// 输入参数：
//   - installedChaincodes：已安装链码的枚举器。
//
// 返回值：
//   - *MetadataManager：元数据管理器实例。
//   - error：如果创建元数据管理器过程中出现错误，则返回相应的错误信息。
func NewMetadataManager(installedChaincodes Enumerator) (*MetadataManager, error) {
	// 枚举已安装的链码
	installedCCs, err := installedChaincodes.Enumerate()
	if err != nil {
		return nil, errors.Wrap(err, "未能列出已安装的链码")
	}

	// 创建元数据管理器实例，并初始化相关字段
	metadataManager := &MetadataManager{
		installedCCs:           installedCCs,                            // 已安装链码的列表
		deployedCCsByChannel:   map[string]*chaincode.MetadataMapping{}, // 按通道存储的已部署链码的元数据映射
		queryCreatorsByChannel: map[string]QueryCreator{},               // 按通道存储的查询创建器映射
	}

	return metadataManager, nil
}

// Metadata returns the metadata of the chaincode on the given channel,
// or nil if not found or an error occurred at retrieving it
func (m *MetadataManager) Metadata(channel string, cc string, collections ...string) *chaincode.Metadata {
	queryCreator := m.queryCreatorsByChannel[channel]
	if queryCreator == nil {
		Logger.Warning("Requested Metadata for non-existent channel", channel)
		return nil
	}
	// Search the metadata in our local cache, and if it exists - return it, but only if
	// no collections were specified in the invocation.
	if md, found := m.deployedCCsByChannel[channel].Lookup(cc); found && len(collections) == 0 {
		Logger.Debug("Returning metadata for channel", channel, ", chaincode", cc, ":", md)
		return &md
	}
	query, err := queryCreator.NewQuery()
	if err != nil {
		Logger.Error("Failed obtaining new query for channel", channel, ":", err)
		return nil
	}
	md, err := DeployedChaincodes(query, AcceptAll, len(collections) > 0, cc)
	if err != nil {
		Logger.Error("Failed querying LSCC for channel", channel, ":", err)
		return nil
	}
	if len(md) == 0 {
		Logger.Warn("Chaincode", cc, "isn't defined in channel", channel)
		return nil
	}

	return &md[0]
}

func (m *MetadataManager) initMetadataForChannel(channel string, queryCreator QueryCreator) error {
	if m.isChannelMetadataInitialized(channel) {
		return nil
	}
	// Create a new metadata mapping for the channel
	query, err := queryCreator.NewQuery()
	if err != nil {
		return errors.WithStack(err)
	}
	ccs, err := queryChaincodeDefinitions(query, m.installedCCs, DeployedChaincodes)
	if err != nil {
		return errors.WithStack(err)
	}
	m.createMetadataForChannel(channel, queryCreator)
	m.updateState(channel, ccs)
	return nil
}

func (m *MetadataManager) createMetadataForChannel(channel string, newQuery QueryCreator) {
	m.Lock()
	defer m.Unlock()
	m.deployedCCsByChannel[channel] = chaincode.NewMetadataMapping()
	m.queryCreatorsByChannel[channel] = newQuery
}

func (m *MetadataManager) isChannelMetadataInitialized(channel string) bool {
	m.RLock()
	defer m.RUnlock()
	_, exists := m.deployedCCsByChannel[channel]
	return exists
}

func (m *MetadataManager) updateState(channel string, ccUpdate chaincode.MetadataSet) {
	m.RLock()
	defer m.RUnlock()
	for _, cc := range ccUpdate {
		m.deployedCCsByChannel[channel].Update(cc)
	}
}

func (m *MetadataManager) fireChangeListeners(channel string) {
	m.RLock()
	md := m.deployedCCsByChannel[channel]
	m.RUnlock()
	for _, listener := range m.listeners {
		aggregatedMD := md.Aggregate()
		listener.HandleMetadataUpdate(channel, aggregatedMD)
	}
	Logger.Debug("Listeners for channel", channel, "invoked")
}

// NewChannelSubscription subscribes to a channel
func (m *MetadataManager) NewChannelSubscription(channel string, queryCreator QueryCreator) (*Subscription, error) {
	sub := &Subscription{
		metadataManager: m,
		channel:         channel,
		queryCreator:    queryCreator,
	}
	// Initialize metadata for the channel.
	// This loads metadata about all installed chaincodes
	if err := m.initMetadataForChannel(channel, queryCreator); err != nil {
		return nil, errors.WithStack(err)
	}
	m.fireChangeListeners(channel)
	return sub, nil
}

// AddListener 注册要在生命周期更改时触发的给定侦听器
func (m *MetadataManager) AddListener(listener MetadataChangeListener) {
	m.Lock()
	defer m.Unlock()
	m.listeners = append(m.listeners, listener)
}
