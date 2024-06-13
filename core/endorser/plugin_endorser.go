/*
Copyright IBM Corp. 2018 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"fmt"
	"sync"

	pb "github.com/hyperledger/fabric-protos-go/peer"
	endorsement "github.com/hyperledger/fabric/core/handlers/endorsement/api"
	endorsement3 "github.com/hyperledger/fabric/core/handlers/endorsement/api/identities"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/pkg/errors"
)

//go:generate mockery -dir . -name TransientStoreRetriever -case underscore -output mocks/

// TransientStoreRetriever retrieves transient stores
type TransientStoreRetriever interface {
	// StoreForChannel returns the transient store for the given channel
	StoreForChannel(channel string) *transientstore.Store
}

//go:generate mockery -dir . -name ChannelStateRetriever -case underscore -output mocks/

// ChannelStateRetriever 检索通道状态的接口
type ChannelStateRetriever interface {
	// NewQueryCreator 返回给定通道的 QueryCreator
	NewQueryCreator(channel string) (QueryCreator, error)
}

//go:generate mockery -dir . -name PluginMapper -case underscore -output mocks/

// PluginMapper maps plugin names to their corresponding factories
type PluginMapper interface {
	PluginFactoryByName(name PluginName) endorsement.PluginFactory
}

// MapBasedPluginMapper 将插件名称映射到其相应的工厂
type MapBasedPluginMapper map[string]endorsement.PluginFactory

// PluginFactoryByName returns a plugin factory for the given plugin name, or nil if not found
func (m MapBasedPluginMapper) PluginFactoryByName(name PluginName) endorsement.PluginFactory {
	return m[string(name)]
}

// Context defines the data that is related to an in-flight endorsement
type Context struct {
	PluginName     string
	Channel        string
	TxID           string
	Proposal       *pb.Proposal
	SignedProposal *pb.SignedProposal
	Visibility     []byte
	Response       *pb.Response
	Event          []byte
	ChaincodeID    *pb.ChaincodeID
	SimRes         []byte
}

// String returns a text representation of this context
func (c Context) String() string {
	return fmt.Sprintf("{plugin: %s, channel: %s, tx: %s, chaincode: %s}", c.PluginName, c.Channel, c.TxID, c.ChaincodeID.Name)
}

// PluginSupport 是插件背书者操作所需的支持接口的聚合体
type PluginSupport struct {
	ChannelStateRetriever               // ChannelStateRetriever 用于检索通道状态
	endorsement3.SigningIdentityFetcher // SigningIdentityFetcher 用于获取签名身份
	PluginMapper                        // PluginMapper 用于管理插件
	TransientStoreRetriever             // TransientStoreRetriever 用于检索瞬态存储
}

// NewPluginEndorser endorses with using a plugin
func NewPluginEndorser(ps *PluginSupport) *PluginEndorser {
	return &PluginEndorser{
		SigningIdentityFetcher:  ps.SigningIdentityFetcher,
		PluginMapper:            ps.PluginMapper,
		pluginChannelMapping:    make(map[PluginName]*pluginsByChannel),
		ChannelStateRetriever:   ps.ChannelStateRetriever,
		TransientStoreRetriever: ps.TransientStoreRetriever,
	}
}

// PluginName defines the name of the plugin as it appears in the configuration
type PluginName string

type pluginsByChannel struct {
	sync.RWMutex
	pluginFactory    endorsement.PluginFactory
	channels2Plugins map[string]endorsement.Plugin
	pe               *PluginEndorser
}

func (pbc *pluginsByChannel) createPluginIfAbsent(channel string) (endorsement.Plugin, error) {
	pbc.RLock()
	plugin, exists := pbc.channels2Plugins[channel]
	pbc.RUnlock()
	if exists {
		return plugin, nil
	}

	pbc.Lock()
	defer pbc.Unlock()
	plugin, exists = pbc.channels2Plugins[channel]
	if exists {
		return plugin, nil
	}

	pluginInstance := pbc.pluginFactory.New()
	plugin, err := pbc.initPlugin(pluginInstance, channel)
	if err != nil {
		return nil, err
	}
	pbc.channels2Plugins[channel] = plugin
	return plugin, nil
}

func (pbc *pluginsByChannel) initPlugin(plugin endorsement.Plugin, channel string) (endorsement.Plugin, error) {
	var dependencies []endorsement.Dependency
	var err error
	// If this is a channel endorsement, add the channel state as a dependency
	if channel != "" {
		query, err := pbc.pe.NewQueryCreator(channel)
		if err != nil {
			return nil, errors.Wrap(err, "failed obtaining channel state")
		}
		store := pbc.pe.TransientStoreRetriever.StoreForChannel(channel)
		if store == nil {
			return nil, errors.Errorf("transient store for channel %s was not initialized", channel)
		}
		dependencies = append(dependencies, &ChannelState{QueryCreator: query, Store: store})
	}
	// Add the SigningIdentityFetcher as a dependency
	dependencies = append(dependencies, pbc.pe.SigningIdentityFetcher)
	err = plugin.Init(dependencies...)
	if err != nil {
		return nil, err
	}
	return plugin, nil
}

// PluginEndorser 是使用插件对提案响应进行背书的结构体。
type PluginEndorser struct {
	sync.Mutex                                                           // 互斥锁，用于保护并发访问
	PluginMapper                                                         // 插件映射器，用于管理插件
	pluginChannelMapping                map[PluginName]*pluginsByChannel // 插件和通道之间的映射关系
	ChannelStateRetriever                                                // 通道状态检索器，用于检索通道的状态
	endorsement3.SigningIdentityFetcher                                  // 签名身份获取器，用于获取签名身份
	TransientStoreRetriever                                              // 瞬态存储检索器，用于检索瞬态存储
}

// EndorseWithPlugin endorses the response with a plugin
func (pe *PluginEndorser) EndorseWithPlugin(pluginName, channelID string, prpBytes []byte, signedProposal *pb.SignedProposal) (*pb.Endorsement, []byte, error) {
	plugin, err := pe.getOrCreatePlugin(PluginName(pluginName), channelID)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "plugin with name %s could not be used", pluginName)
	}

	return plugin.Endorse(prpBytes, signedProposal)
}

// getOrCreatePlugin returns a plugin instance for the given plugin name and channel
func (pe *PluginEndorser) getOrCreatePlugin(plugin PluginName, channel string) (endorsement.Plugin, error) {
	pluginFactory := pe.PluginFactoryByName(plugin)
	if pluginFactory == nil {
		return nil, errors.Errorf("plugin with name %s wasn't found", plugin)
	}

	pluginsByChannel := pe.getOrCreatePluginChannelMapping(PluginName(plugin), pluginFactory)
	return pluginsByChannel.createPluginIfAbsent(channel)
}

func (pe *PluginEndorser) getOrCreatePluginChannelMapping(plugin PluginName, pf endorsement.PluginFactory) *pluginsByChannel {
	pe.Lock()
	defer pe.Unlock()
	endorserChannelMapping, exists := pe.pluginChannelMapping[PluginName(plugin)]
	if !exists {
		endorserChannelMapping = &pluginsByChannel{
			pluginFactory:    pf,
			channels2Plugins: make(map[string]endorsement.Plugin),
			pe:               pe,
		}
		pe.pluginChannelMapping[PluginName(plugin)] = endorserChannelMapping
	}
	return endorserChannelMapping
}
