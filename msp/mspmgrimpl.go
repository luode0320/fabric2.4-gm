/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
)

var mspLogger = flogging.MustGetLogger("msp")

// mspManagerImpl 结构体实现了MSPManager接口，用于管理一系列MSP实例。
type mspManagerImpl struct {
	// mspsMap 存储所有已经设置或添加的MSP实例，键为MSPID。
	mspsMap map[string]MSP

	// mspsByProviders 按照MSP的提供者类型对MSP实例进行分类存储。
	mspsByProviders map[ProviderType][]MSP

	// up 标记表示MSPManager实例是否已成功初始化。
	up bool
}

// NewMSPManager returns a new MSP manager instance;
// note that this instance is not initialized until
// the Setup method is called
func NewMSPManager() MSPManager {
	return &mspManagerImpl{}
}

// Setup initializes the internal data structures of this manager and creates MSPs
func (mgr *mspManagerImpl) Setup(msps []MSP) error {
	if mgr.up {
		mspLogger.Infof("MSP manager already up")
		return nil
	}

	mspLogger.Debugf("Setting up the MSP manager (%d msps)", len(msps))

	// create the map that assigns MSP IDs to their manager instance - once
	mgr.mspsMap = make(map[string]MSP)

	// create the map that sorts MSPs by their provider types
	mgr.mspsByProviders = make(map[ProviderType][]MSP)

	for _, msp := range msps {
		// add the MSP to the map of active MSPs
		mspID, err := msp.GetIdentifier()
		if err != nil {
			return errors.WithMessage(err, "could not extract msp identifier")
		}
		mgr.mspsMap[mspID] = msp
		providerType := msp.GetType()
		mgr.mspsByProviders[providerType] = append(mgr.mspsByProviders[providerType], msp)
	}

	mgr.up = true

	mspLogger.Debugf("MSP manager setup complete, setup %d msps", len(msps))

	return nil
}

// GetMSPs 返回此管理器管理的msp
func (mgr *mspManagerImpl) GetMSPs() (map[string]MSP, error) {
	return mgr.mspsMap, nil
}

// DeserializeIdentity returns an identity given its serialized version supplied as argument
func (mgr *mspManagerImpl) DeserializeIdentity(serializedID []byte) (Identity, error) {
	if !mgr.up {
		return nil, errors.New("channel doesn't exist")
	}
	// We first deserialize to a SerializedIdentity to get the MSP ID
	sId := &msp.SerializedIdentity{}
	err := proto.Unmarshal(serializedID, sId)
	if err != nil {
		return nil, errors.Wrap(err, "could not deserialize a SerializedIdentity")
	}

	// we can now attempt to obtain the MSP
	msp := mgr.mspsMap[sId.Mspid]
	if msp == nil {
		return nil, errors.Errorf("MSP %s is not defined on channel", sId.Mspid)
	}

	switch t := msp.(type) {
	case *bccspmsp:
		return t.deserializeIdentityInternal(sId.IdBytes)
	case *idemixMSPWrapper:
		return t.deserializeIdentityInternal(sId.IdBytes)
	default:
		return t.DeserializeIdentity(serializedID)
	}
}

func (mgr *mspManagerImpl) IsWellFormed(identity *msp.SerializedIdentity) error {
	// Iterate over all the MSPs by their providers, and find at least 1 MSP that can attest
	// that this identity is well formed
	for _, mspList := range mgr.mspsByProviders {
		// We are guaranteed to have at least 1 MSP in each list from the initialization at Setup()
		msp := mspList[0]
		if err := msp.IsWellFormed(identity); err == nil {
			return nil
		}
	}
	return errors.New("no MSP provider recognizes the identity")
}
