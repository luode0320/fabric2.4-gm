/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package api

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/gossip/common"
	"google.golang.org/grpc"
)

// MessageCryptoService 是gossip组件与对等方的密码层之间的契约，用于gossip组件验证，
// 并对远程对等体及其发送的数据进行身份验证，并验证从订购服务接收到的块。
type MessageCryptoService interface {
	// GetPKIidOfCert 根据证书返回对等方身份的pki-id(mspid + 证书 的 哈希值)
	// 如果发生任何错误，该方法返回nil
	// 此方法不验证peer Identity。 这个验证应该在执行流程中适当地完成。
	GetPKIidOfCert(peerIdentity PeerIdentityType) common.PKIidType

	// VerifyBlock 如果块已正确签名，则返回nil，并且声明的seqNum是块的标头包含的序列号。
	VerifyBlock(channelID common.ChannelID, seqNum uint64, block *cb.Block) error

	// Sign 使用此对等体的签名密钥对msg进行签名，如果没有发生错误，则输出签名。
	Sign(msg []byte) ([]byte, error)

	// Verify 检查签名是否为对等方验证密钥下的消息的有效签名。
	// 如果验证成功，则Verify返回nil，表示没有发生错误。
	// 如果peerIdentity为nil，则验证失败。
	Verify(peerIdentity PeerIdentityType, signature, message []byte) error

	// VerifyByChannel 检查签名是否为对等方验证密钥下的消息的有效签名， 而且在特定通道的上下文中。
	// 如果验证成功，则Verify返回nil，表示没有发生错误。
	// 如果peerIdentity为nil，则验证失败。
	VerifyByChannel(channelID common.ChannelID, peerIdentity PeerIdentityType, signature, message []byte) error

	// ValidateIdentity 验证远程对等方的身份.
	// 如果身份无效、已撤销、已过期，则返回错误。
	// 否则，返回nil
	ValidateIdentity(peerIdentity PeerIdentityType) error

	// Expiration returns:
	// - 身份过期的时间，nil
	//   以防它过期
	// - 零值 time.Time, nil
	//   以防它不能过期
	// - 零值，在无法确定身份是否过期时出错
	Expiration(peerIdentity PeerIdentityType) (time.Time, error)
}

// PeerIdentityInfo aggregates a peer's identity,
// and also additional metadata about it
type PeerIdentityInfo struct {
	PKIId        common.PKIidType
	Identity     PeerIdentityType
	Organization OrgIdentityType
}

// PeerIdentitySet aggregates a PeerIdentityInfo slice
type PeerIdentitySet []PeerIdentityInfo

// PeerIdentityFilter defines predicate function used to filter
// peer identities
type PeerIdentityFilter func(info PeerIdentityInfo) bool

// ByOrg sorts the PeerIdentitySet by organizations of its peers
func (pis PeerIdentitySet) ByOrg() map[string]PeerIdentitySet {
	m := make(map[string]PeerIdentitySet)
	for _, id := range pis {
		m[string(id.Organization)] = append(m[string(id.Organization)], id)
	}
	return m
}

// ByID sorts the PeerIdentitySet by PKI-IDs of its peers
func (pis PeerIdentitySet) ByID() map[string]PeerIdentityInfo {
	m := make(map[string]PeerIdentityInfo)
	for _, id := range pis {
		m[string(id.PKIId)] = id
	}
	return m
}

// Filter filters identities based on predicate, returns new  PeerIdentitySet
// with filtered ids.
func (pis PeerIdentitySet) Filter(filter PeerIdentityFilter) PeerIdentitySet {
	var result PeerIdentitySet
	for _, id := range pis {
		if filter(id) {
			result = append(result, id)
		}
	}
	return result
}

// PeerIdentityType 是同行的证书
type PeerIdentityType []byte

// String returns a string representation of this PeerIdentityType
func (pit PeerIdentityType) String() string {
	base64Representation := base64.StdEncoding.EncodeToString(pit)
	sID := &msp.SerializedIdentity{}
	err := proto.Unmarshal(pit, sID)
	if err != nil {
		return fmt.Sprintf("non SerializedIdentity: %s", base64Representation)
	}

	bl, _ := pem.Decode(sID.IdBytes)
	if bl == nil {
		return fmt.Sprintf("non PEM encoded identity: %s", base64Representation)
	}

	cert, _ := x509.ParseCertificate(bl.Bytes)
	if cert == nil {
		return fmt.Sprintf("non x509 identity: %s", base64Representation)
	}
	m := make(map[string]interface{})
	m["MSP"] = sID.Mspid
	s := cert.Subject
	m["CN"] = s.CommonName
	m["OU"] = s.OrganizationalUnit
	m["L-ST-C"] = fmt.Sprintf("%s-%s-%s", s.Locality, s.StreetAddress, s.Country)
	i := cert.Issuer
	m["Issuer-CN"] = i.CommonName
	m["Issuer-OU"] = i.OrganizationalUnit
	m["Issuer-L-ST-C"] = fmt.Sprintf("%s-%s-%s", i.Locality, i.StreetAddress, i.Country)

	rawJSON, err := json.Marshal(m)
	if err != nil {
		return base64Representation
	}
	return string(rawJSON)
}

// PeerSuspector returns whether a peer with a given identity is suspected
// as being revoked, or its CA is revoked
type PeerSuspector func(identity PeerIdentityType) bool

// PeerSecureDialOpts 返回与远程对等端点通信时用于连接级别安全性的gRPC 拨号选项
type PeerSecureDialOpts func() []grpc.DialOption

// PeerSignature defines a signature of a peer
// on a given message
type PeerSignature struct {
	Signature    []byte
	Message      []byte
	PeerIdentity PeerIdentityType
}
