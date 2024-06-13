/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorsement

import (
	"github.com/hyperledger/fabric-protos-go/peer"
	endorsement "github.com/hyperledger/fabric/core/handlers/endorsement/api"
)

// SigningIdentity signs messages and serializes its public identity to bytes
type SigningIdentity interface {
	// Serialize returns a byte representation of this identity which is used to verify
	// messages signed by this SigningIdentity
	Serialize() ([]byte, error)

	// Sign signs the given payload and returns a signature
	Sign([]byte) ([]byte, error)
}

// SigningIdentityFetcher 根据提案获取签名身份的接口
type SigningIdentityFetcher interface {
	endorsement.Dependency
	// SigningIdentityForRequest 根据给定的提案返回一个签名身份
	SigningIdentityForRequest(*peer.SignedProposal) (SigningIdentity, error)
}
