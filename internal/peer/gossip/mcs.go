/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"fmt"
	"time"

	pcommon "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	common2 "github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var mcsLogger = flogging.MustGetLogger("peer.gossip.mcs")

// Hasher 是接口提供的散列函数应该用于所有gossip组件。
type Hasher interface {
	Hash(msg []byte, opts bccsp.HashOpts) (hash []byte, err error)
}

// MSPMessageCryptoService 使用对等msp (本地和信道相关) 实现MessageCryptoService接口
//
// 为了使系统安全，让msp保持最新是至关重要的。
// 渠道的msp通过排序服务分发的配置事务进行更新。
//
// 也需要类似的机制来更新本地MSP。
// 此实现假定这些机制都已到位并正常工作。
type MSPMessageCryptoService struct {
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter // 用于访问给定渠道的策略管理器
	localSigner                identity.SignerSerializer           // 对Sign和Serialize方法进行分组。
	deserializer               DeserializersManager                // 访问本地和通道反序列化器的支持接口
	hasher                     Hasher                              // hash器
}

// NewMCS 创建一个实现了 MessageCryptoService 接口的 MSPMessageCryptoService 实例。
//
// 输入参数：
//   - channelPolicyManagerGetter：通过 Manager 方法获取给定通道的策略管理器的通道策略管理器获取器。
//   - localSigner：身份签名者序列化器的实例。
//   - deserializer：身份反序列化器管理器。
//   - hasher：哈希器。
//
// 返回值：
//   - *MSPMessageCryptoService：MSPMessageCryptoService 实例。
func NewMCS(
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter,
	localSigner identity.SignerSerializer,
	deserializer DeserializersManager,
	hasher Hasher,
) *MSPMessageCryptoService {
	return &MSPMessageCryptoService{
		channelPolicyManagerGetter: channelPolicyManagerGetter, // 通过 Manager 方法获取给定通道的策略管理器的通道策略管理器获取器。
		localSigner:                localSigner,                // 身份签名者序列化器的实例。
		deserializer:               deserializer,               // 身份反序列化器管理器。
		hasher:                     hasher,                     // 哈希器。
	}
}

// ValidateIdentity 验证远程对等方的身份。
// 如果身份无效、已撤销、已过期，则返回错误。
// 否则，返回nil
func (s *MSPMessageCryptoService) ValidateIdentity(peerIdentity api.PeerIdentityType) error {
	// 根据方法契约的规定，
	// 下面我们仅检查 peerIdentity 是否无效、撤销或过期。
	_, _, err := s.getValidatedIdentity(peerIdentity)
	return err
}

// GetPKIidOfCert 返回对等方身份的pki-id
// 如果发生任何错误，该方法返回nil
// 对等体的PKid被计算为peerIdentity的hash，它应该是MSP标识的序列化版本。
// 此方法不验证peerIdentity。 这个验证应该在执行流程中适当地完成。
func (s *MSPMessageCryptoService) GetPKIidOfCert(peerIdentity api.PeerIdentityType) common.PKIidType {
	// 验证参数
	if len(peerIdentity) == 0 {
		mcsLogger.Error("对等身份证书无效. 它必须不等于nil.")
		return nil
	}

	// 接收SerializedIdentity字节并反序列化为 mspproto.SerializedIdentity
	sid, err := s.deserializer.Deserialize(peerIdentity)
	if err != nil {
		mcsLogger.Errorf("从对等身份证书获取已验证的身份失败 %s: [%s]", peerIdentity, err)

		return nil
	}

	// 连接msp-id和idbytes(证书的pem格式)
	// idbytes是标识的低级表示形式。
	// 它应该已经处于其最小表示形式

	mspIDRaw := []byte(sid.Mspid)
	raw := append(mspIDRaw, sid.IdBytes...)

	// todo luode 进行国密sm3的改造
	digest, err := s.hasher.Hash(raw, common2.Hash())
	if err != nil {
		mcsLogger.Errorf("无法计算序列化身份的摘要 %s: [%s]", string(peerIdentity), err)
		return nil
	}

	return digest
}

// VerifyBlock returns nil if the block is properly signed, and the claimed seqNum is the
// sequence number that the block's header contains.
// else returns error
func (s *MSPMessageCryptoService) VerifyBlock(chainID common.ChannelID, seqNum uint64, block *pcommon.Block) error {
	if block.Header == nil {
		return fmt.Errorf("Invalid Block on channel [%s]. Header must be different from nil.", chainID)
	}

	blockSeqNum := block.Header.Number
	if seqNum != blockSeqNum {
		return fmt.Errorf("Claimed seqNum is [%d] but actual seqNum inside block is [%d]", seqNum, blockSeqNum)
	}

	// - Extract channelID and compare with chainID
	channelID, err := protoutil.GetChannelIDFromBlock(block)
	if err != nil {
		return fmt.Errorf("Failed getting channel id from block with id [%d] on channel [%s]: [%s]", block.Header.Number, chainID, err)
	}

	if channelID != string(chainID) {
		return fmt.Errorf("Invalid block's channel id. Expected [%s]. Given [%s]", chainID, channelID)
	}

	// - Unmarshal medatada
	if block.Metadata == nil || len(block.Metadata.Metadata) == 0 {
		return fmt.Errorf("Block with id [%d] on channel [%s] does not have metadata. Block not valid.", block.Header.Number, chainID)
	}

	metadata, err := protoutil.GetMetadataFromBlock(block, pcommon.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return fmt.Errorf("Failed unmarshalling medatata for signatures [%s]", err)
	}

	// - Verify that Header.DataHash is equal to the hash of block.Data
	// This is to ensure that the header is consistent with the data carried by this block
	if !bytes.Equal(protoutil.BlockDataHash(block.Data), block.Header.DataHash) {
		return fmt.Errorf("Header.DataHash is different from Hash(block.Data) for block with id [%d] on channel [%s]", block.Header.Number, chainID)
	}

	// - Get Policy for block validation

	// Get the policy manager for channelID
	cpm := s.channelPolicyManagerGetter.Manager(channelID)
	if cpm == nil {
		return fmt.Errorf("Could not acquire policy manager for channel %s", channelID)
	}
	mcsLogger.Debugf("Got policy manager for channel [%s]", channelID)

	// Get block validation policy
	policy, ok := cpm.GetPolicy(policies.BlockValidation)
	// ok is true if it was the policy requested, or false if it is the default policy
	mcsLogger.Debugf("Got block validation policy for channel [%s] with flag [%t]", channelID, ok)

	// - Prepare SignedData
	signatureSet := []*protoutil.SignedData{}
	for _, metadataSignature := range metadata.Signatures {
		shdr, err := protoutil.UnmarshalSignatureHeader(metadataSignature.SignatureHeader)
		if err != nil {
			return fmt.Errorf("Failed unmarshalling signature header for block with id [%d] on channel [%s]: [%s]", block.Header.Number, chainID, err)
		}
		signatureSet = append(
			signatureSet,
			&protoutil.SignedData{
				Identity:  shdr.Creator,
				Data:      util.ConcatenateBytes(metadata.Value, metadataSignature.SignatureHeader, protoutil.BlockHeaderBytes(block.Header)),
				Signature: metadataSignature.Signature,
			},
		)
	}

	// - Evaluate policy
	return policy.EvaluateSignedData(signatureSet)
}

// Sign signs msg with this peer's signing key and outputs
// the signature if no error occurred.
func (s *MSPMessageCryptoService) Sign(msg []byte) ([]byte, error) {
	return s.localSigner.Sign(msg)
}

// Verify checks that signature is a valid signature of message under a peer's verification key.
// If the verification succeeded, Verify returns nil meaning no error occurred.
// If peerIdentity is nil, then the verification fails.
func (s *MSPMessageCryptoService) Verify(peerIdentity api.PeerIdentityType, signature, message []byte) error {
	identity, chainID, err := s.getValidatedIdentity(peerIdentity)
	if err != nil {
		mcsLogger.Errorf("Failed getting validated identity from peer identity [%s]", err)

		return err
	}

	if len(chainID) == 0 {
		// At this stage, this means that peerIdentity
		// belongs to this peer's LocalMSP.
		// The signature is validated directly
		return identity.Verify(message, signature)
	}

	// At this stage, the signature must be validated
	// against the reader policy of the channel
	// identified by chainID

	return s.VerifyByChannel(chainID, peerIdentity, signature, message)
}

// VerifyByChannel checks that signature is a valid signature of message
// under a peer's verification key, but also in the context of a specific channel.
// If the verification succeeded, Verify returns nil meaning no error occurred.
// If peerIdentity is nil, then the verification fails.
func (s *MSPMessageCryptoService) VerifyByChannel(chainID common.ChannelID, peerIdentity api.PeerIdentityType, signature, message []byte) error {
	// Validate arguments
	if len(peerIdentity) == 0 {
		return errors.New("Invalid Peer Identity. It must be different from nil.")
	}

	// Get the policy manager for channel chainID
	cpm := s.channelPolicyManagerGetter.Manager(string(chainID))
	if cpm == nil {
		return fmt.Errorf("Could not acquire policy manager for channel %s", string(chainID))
	}
	mcsLogger.Debugf("Got policy manager for channel [%s]", string(chainID))

	// Get channel reader policy
	policy, flag := cpm.GetPolicy(policies.ChannelApplicationReaders)
	mcsLogger.Debugf("Got reader policy for channel [%s] with flag [%t]", string(chainID), flag)

	return policy.EvaluateSignedData(
		[]*protoutil.SignedData{{
			Data:      message,
			Identity:  []byte(peerIdentity),
			Signature: signature,
		}},
	)
}

// Expiration 返回给定对等身份的过期时间。
//
// 输入参数：
//   - peerIdentity：要获取过期时间的对等身份。
//
// 返回值：
//   - time.Time：对等身份的过期时间。
//   - error：如果无法从对等身份证书提取 msp.Identity 或发生其他错误，则返回非空的错误。
func (s *MSPMessageCryptoService) Expiration(peerIdentity api.PeerIdentityType) (time.Time, error) {
	// 从对等身份证书中提取 msp.Identity
	id, _, err := s.getValidatedIdentity(peerIdentity)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "无法从对等身份证书提取 msp.Identity")
	}

	// 返回标识过期的时间。
	return id.ExpiresAt(), nil
}

// getValidatedIdentity 从给定的对等身份证书中提取并验证 msp.Identity。
//
// 输入参数：
//   - peerIdentity：要提取和验证的对等身份证书。
//
// 返回值：
//   - msp.Identity：从对等身份证书中提取并验证的 msp.Identity。
//   - common.ChannelID：对等身份所属的通道 ID。
//   - error：如果无法提取和验证对等身份证书的 msp.Identity 或发生其他错误，则返回非空的错误。
func (s *MSPMessageCryptoService) getValidatedIdentity(peerIdentity api.PeerIdentityType) (msp.Identity, common.ChannelID, error) {
	// 验证参数
	if len(peerIdentity) == 0 {
		return nil, nil, errors.New("对等身份证书无效. 它必须不能为nil.")
	}

	// 接收SerializedIdentity字节并反序列化为 mspproto.SerializedIdentity
	sId, err := s.deserializer.Deserialize(peerIdentity)
	if err != nil {
		mcsLogger.Error("对身份证书进行反序列化失败", err)
		return nil, nil, err
	}

	// 请注意，假定peerIdentity是标识的序列化。
	// 所以，第一步是身份反序列化，然后验证它。

	// 首先检查本地MSP。
	// 如果peerIdentity在该节点的同一组织中，则要求本地MSP对签名的有效性做出最终决定。
	lDes := s.deserializer.GetLocalDeserializer()
	// 反序列化给定的序列化身份标识符，并返回对应的 msp.Identity 实例。
	identity, err := lDes.DeserializeIdentity([]byte(peerIdentity))
	if err == nil {
		// 没有错误意味着本地MSP成功地反序列化了标识。
		// 我们现在检查其他属性。检查给定的标识是否可以反序列化为其提供程序特定的形式。
		if err := lDes.IsWellFormed(sId); err != nil {
			return nil, nil, errors.Wrap(err, "检查身份的其他属性失败")
		}
		// TODO: 以下检查将替换为对组织单位的检查
		// 当我们允许gossip网络具有组织单元 (MSP细分) 范围内的消息时。
		// 以下检查与 SecurityAdvisor # OrgByPeerIdentity实现一致。
		// TODO: 请注意，以下检查使我们避免了 DeserializeIdentity 尚未强制 msp-ids 一致性的事实。
		// 一旦DeserializeIdentity将被修复，就可以删除此检查。
		if identity.GetMSPIdentifier() == s.deserializer.GetLocalMSPIdentifier() {
			// 检查身份有效性

			// 请注意，在此阶段，我们不必根据任何渠道的策略检查身份。
			// 如果需要，这将由调用函数完成。
			return identity, nil, identity.Validate()
		}
	}

	// Check against managers
	for chainID, mspManager := range s.deserializer.GetChannelDeserializers() {
		// Deserialize identity
		identity, err := mspManager.DeserializeIdentity([]byte(peerIdentity))
		if err != nil {
			mcsLogger.Debugf("Failed deserialization identity %s on [%s]: [%s]", peerIdentity, chainID, err)
			continue
		}

		// We managed deserializing the identity with this MSP manager. Now we check if it's well formed.
		if err := mspManager.IsWellFormed(sId); err != nil {
			return nil, nil, errors.Wrap(err, "identity is not well formed")
		}

		// Check identity validity
		// Notice that at this stage we don't have to check the identity
		// against any channel's policies.
		// This will be done by the caller function, if needed.

		if err := identity.Validate(); err != nil {
			mcsLogger.Debugf("Failed validating identity %s on [%s]: [%s]", peerIdentity, chainID, err)
			continue
		}

		mcsLogger.Debugf("Validation succeeded %s on [%s]", peerIdentity, chainID)

		return identity, common.ChannelID(chainID), nil
	}

	return nil, nil, fmt.Errorf("Peer Identity %s cannot be validated. No MSP found able to do that.", peerIdentity)
}
