/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"crypto/x509"
	"encoding/pem"
	x509GM "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/bccsp/gm"
	"github.com/hyperledger/fabric/bccsp/verify"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/orderer/etcdraft"
	"github.com/hyperledger/fabric/bccsp"
	common2 "github.com/hyperledger/fabric/bccsp/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// RaftNodes 将共识者ID映射为raft.Peer切片(这个不是peer节点, 这里的peer代表节点的意思, 实际上是排序节点)
func RaftNodes(consenterIDs []uint64) []raft.Peer {
	var orderers []raft.Peer // 初始化一个空的raft.Peer切片用于存放Peer对象

	// 遍历所有共识者ID
	for _, raftID := range consenterIDs {
		// 为每个共识者ID创建一个新的raft.Peer实例并将其追加到peers切片中
		orderers = append(orderers, raft.Peer{ID: raftID})
	}

	// 返回包含所有raft.Peer实例的切片
	return orderers
}

type ConsentersMap map[string]struct{}

func (c ConsentersMap) Exists(consenter *etcdraft.Consenter) bool {
	_, exists := c[string(consenter.ClientTlsCert)]
	return exists
}

// ConsentersToMap 函数将共识者映射为以客户端 TLS 证书为键的集合。
func ConsentersToMap(consenters []*etcdraft.Consenter) ConsentersMap {
	set := map[string]struct{}{}

	// 遍历共识者列表，将客户端 TLS 证书作为键存入集合中
	for _, c := range consenters {
		set[string(c.ClientTlsCert)] = struct{}{}
	}

	return set
}

// MetadataHasDuplication returns an error if the metadata has duplication of consenters.
// A duplication is defined by having a server or a client TLS certificate that is found
// in two different consenters, regardless of the type of certificate (client/server).
func MetadataHasDuplication(md *etcdraft.ConfigMetadata) error {
	if md == nil {
		return errors.New("nil metadata")
	}

	for _, consenter := range md.Consenters {
		if consenter == nil {
			return errors.New("nil consenter in metadata")
		}
	}

	seen := make(map[string]struct{})
	for _, consenter := range md.Consenters {
		serverKey := string(consenter.ServerTlsCert)
		clientKey := string(consenter.ClientTlsCert)
		_, duplicateServerCert := seen[serverKey]
		_, duplicateClientCert := seen[clientKey]
		if duplicateServerCert || duplicateClientCert {
			return errors.Errorf("duplicate consenter: server cert: %s, client cert: %s", serverKey, clientKey)
		}

		seen[serverKey] = struct{}{}
		seen[clientKey] = struct{}{}
	}
	return nil
}

// MetadataFromConfigValue 函数从配置值中读取并将配置更新转换为 Raft 元数据。
func MetadataFromConfigValue(configValue *common.ConfigValue) (*etcdraft.ConfigMetadata, error) {
	// 解码共识类型配置更新
	consensusTypeValue := &orderer.ConsensusType{}
	if err := proto.Unmarshal(configValue.Value, consensusTypeValue); err != nil {
		return nil, errors.Wrap(err, "无法解码共识类型配置更新")
	}

	// 解码更新后的 Raft 元数据配置
	updatedMetadata := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(consensusTypeValue.Metadata, updatedMetadata); err != nil {
		return nil, errors.Wrap(err, "无法解码更新后的 etcdraft 元数据配置")
	}

	return updatedMetadata, nil
}

// MetadataFromConfigUpdate 函数从配置更新中提取共识元数据。
func MetadataFromConfigUpdate(update *common.ConfigUpdate) (*etcdraft.ConfigMetadata, error) {
	var baseVersion uint64

	// 从读集中提取基础版本号
	if update.ReadSet != nil && update.ReadSet.Groups != nil {
		if ordererConfigGroup, ok := update.ReadSet.Groups["Orderer"]; ok {
			if val, ok := ordererConfigGroup.Values["ConsensusType"]; ok {
				baseVersion = val.Version
			}
		}
	}

	// 检查写集中是否有更新共识类型的操作
	if update.WriteSet != nil && update.WriteSet.Groups != nil {
		if ordererConfigGroup, ok := update.WriteSet.Groups["Orderer"]; ok {
			if val, ok := ordererConfigGroup.Values["ConsensusType"]; ok {
				if baseVersion == val.Version {
					// 只有在写集中的版本与读集中的版本不同的情况下，才认为这是对共识类型的更新
					return nil, nil
				}
				return MetadataFromConfigValue(val)
			}
		}
	}

	return nil, nil
}

// ConfigChannelHeader 一个配置区块，并返回其中包含的配置信封的头类型，
// 例如 HeaderType_ORDERER_TRANSACTION
func ConfigChannelHeader(block *common.Block) (hdr *common.ChannelHeader, err error) {
	envelope, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, errors.Wrap(err, "无法从区块中提取信封")
	}

	channelHeader, err := protoutil.ChannelHeader(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "无法提取通道头")
	}

	return channelHeader, nil
}

// ConfigEnvelopeFromBlock extracts configuration envelope from the block based on the
// config type, i.e. HeaderType_ORDERER_TRANSACTION or HeaderType_CONFIG
func ConfigEnvelopeFromBlock(block *common.Block) (*common.Envelope, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}

	envelope, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to extract envelope from the block")
	}

	channelHeader, err := protoutil.ChannelHeader(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "cannot extract channel header")
	}

	switch channelHeader.Type {
	case int32(common.HeaderType_ORDERER_TRANSACTION):
		payload, err := protoutil.UnmarshalPayload(envelope.Payload)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal envelope to extract config payload for orderer transaction")
		}
		configEnvelop, err := protoutil.UnmarshalEnvelope(payload.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal config envelope for orderer type transaction")
		}

		return configEnvelop, nil
	case int32(common.HeaderType_CONFIG):
		return envelope, nil
	default:
		return nil, errors.Errorf("unexpected header type: %v", channelHeader.Type)
	}
}

// ConsensusMetadataFromConfigBlock reads consensus metadata updates from the configuration block
func ConsensusMetadataFromConfigBlock(block *common.Block) (*etcdraft.ConfigMetadata, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}

	if !protoutil.IsConfigBlock(block) {
		return nil, errors.New("not a config block")
	}

	configEnvelope, err := ConfigEnvelopeFromBlock(block)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read config update")
	}

	payload, err := protoutil.UnmarshalPayload(configEnvelope.Payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract payload from config envelope")
	}
	// get config update
	configUpdate, err := configtx.UnmarshalConfigUpdateFromPayload(payload)
	if err != nil {
		return nil, errors.Wrap(err, "could not read config update")
	}

	return MetadataFromConfigUpdate(configUpdate)
}

// VerifyConfigMetadata validates Raft config metadata.
// Note: ignores certificates expiration.
func VerifyConfigMetadata(metadata *etcdraft.ConfigMetadata, verifyOpts interface{}) error {
	if metadata == nil {
		// defensive check. this should not happen as CheckConfigMetadata
		// should always be called with non-nil config metadata
		return errors.Errorf("nil Raft config metadata")
	}

	if metadata.Options == nil {
		return errors.Errorf("nil Raft config metadata options")
	}

	if metadata.Options.HeartbeatTick == 0 ||
		metadata.Options.ElectionTick == 0 ||
		metadata.Options.MaxInflightBlocks == 0 {
		// if SnapshotIntervalSize is zero, DefaultSnapshotIntervalSize is used
		return errors.Errorf("none of HeartbeatTick (%d), ElectionTick (%d) and MaxInflightBlocks (%d) can be zero",
			metadata.Options.HeartbeatTick, metadata.Options.ElectionTick, metadata.Options.MaxInflightBlocks)
	}

	// check Raft options
	if metadata.Options.ElectionTick <= metadata.Options.HeartbeatTick {
		return errors.Errorf("ElectionTick (%d) must be greater than HeartbeatTick (%d)",
			metadata.Options.ElectionTick, metadata.Options.HeartbeatTick)
	}

	if d, err := time.ParseDuration(metadata.Options.TickInterval); err != nil {
		return errors.Errorf("failed to parse TickInterval (%s) to time duration: %s", metadata.Options.TickInterval, err)
	} else if d == 0 {
		return errors.Errorf("TickInterval cannot be zero")
	}

	if len(metadata.Consenters) == 0 {
		return errors.Errorf("empty consenter set")
	}

	// 验证证书是否由CA签名，过期被忽略
	for _, consenter := range metadata.Consenters {
		if consenter == nil {
			return errors.Errorf("共识证书配置 consenter 为nil")
		}
		if err := validateConsenterTLSCerts(consenter, verifyOpts, true); err != nil {
			return err
			return errors.WithMessagef(err, "consenter %s:%d has invalid certificate", consenter.Host, consenter.Port)
		}
	}

	if err := MetadataHasDuplication(metadata); err != nil {
		return err
	}

	return nil
}

func parseCertificateFromBytes(cert []byte) (*x509.Certificate, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return &x509.Certificate{}, errors.Errorf("no PEM data found in cert[% x]", cert)
	}

	certificate, err := common2.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, errors.Errorf("%s TLS certificate has invalid ASN1 structure %s", err, string(pemBlock.Bytes))
	}

	return certificate, nil
}

func parseCertificateListFromBytes(certs [][]byte) ([]*x509.Certificate, error) {
	var certificateList []*x509.Certificate

	for _, cert := range certs {
		certificate, err := parseCertificateFromBytes(cert)
		if err != nil {
			return certificateList, err
		}

		certificateList = append(certificateList, certificate)
	}

	return certificateList, nil
}

func createX509VerifyOptions(ordererConfig channelconfig.Orderer) (interface{}, error) {
	// 根据工厂名称适配国密或软件证书
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		tlsRoots := x509GM.NewCertPool()
		tlsIntermediates := x509GM.NewCertPool()

		for _, org := range ordererConfig.Organizations() {
			rootCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSRootCerts())
			if err != nil {
				return x509GM.VerifyOptions{}, errors.Wrap(err, "解析tls根证书出错")
			}
			intermediateCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSIntermediateCerts())
			if err != nil {
				return x509GM.VerifyOptions{}, errors.Wrap(err, "解析tls中间证书出错")
			}

			for _, cert := range rootCerts {
				tlsRoots.AddCert(gm.ParseX509Certificate2Sm2(cert))
			}

			for _, cert := range intermediateCerts {
				tlsIntermediates.AddCert(gm.ParseX509Certificate2Sm2(cert))
			}
		}

		return x509GM.VerifyOptions{
			Roots:         tlsRoots,
			Intermediates: tlsIntermediates,
			KeyUsages: []x509GM.ExtKeyUsage{
				x509GM.ExtKeyUsageClientAuth,
				x509GM.ExtKeyUsageServerAuth,
			},
		}, nil
	case factory.SoftwareBasedFactoryName:
		tlsRoots := x509.NewCertPool()
		tlsIntermediates := x509.NewCertPool()

		for _, org := range ordererConfig.Organizations() {
			rootCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSRootCerts())
			if err != nil {
				return x509.VerifyOptions{}, errors.Wrap(err, "解析tls根证书出错")
			}
			intermediateCerts, err := parseCertificateListFromBytes(org.MSP().GetTLSIntermediateCerts())
			if err != nil {
				return x509.VerifyOptions{}, errors.Wrap(err, "解析tls中间证书出错")
			}

			for _, cert := range rootCerts {
				tlsRoots.AddCert(cert)
			}

			for _, cert := range intermediateCerts {
				tlsIntermediates.AddCert(cert)
			}
		}

		return x509.VerifyOptions{
			Roots:         tlsRoots,
			Intermediates: tlsIntermediates,
			KeyUsages: []x509.ExtKeyUsage{
				x509.ExtKeyUsageClientAuth,
				x509.ExtKeyUsageServerAuth,
			},
		}, nil
	}

	return nil, errors.Errorf("根据工厂名称适配国密或软件证书出错")
}

// validateConsenterTLSCerts decodes PEM cert, parses and validates it.
func validateConsenterTLSCerts(c *etcdraft.Consenter, opts interface{}, ignoreExpiration bool) error {
	clientCert, err := parseCertificateFromBytes(c.ClientTlsCert)
	if err != nil {
		return errors.Wrapf(err, "parsing tls client cert of %s:%d", c.Host, c.Port)
	}

	serverCert, err := parseCertificateFromBytes(c.ServerTlsCert)
	if err != nil {
		return errors.Wrapf(err, "parsing tls server cert of %s:%d", c.Host, c.Port)
	}

	verifyFunc := func(certType string, cert *x509.Certificate, opts interface{}) error {
		if _, err := verify.VerifyByOpts(cert, opts); err != nil {
			if validationRes, ok := err.(x509.CertificateInvalidError); !ok || (!ignoreExpiration || validationRes.Reason != x509.Expired) {
				return errors.Wrapf(err, "正在验证序列号为 %d 的tls %s 证书", cert.SerialNumber, certType)
			}
		}
		return nil
	}

	if err := verifyFunc("client", clientCert, opts); err != nil {
		return err
	}
	if err := verifyFunc("server", serverCert, opts); err != nil {
		return err
	}

	return nil
}

// ConsenterCertificate denotes a TLS certificate of a consenter
type ConsenterCertificate struct {
	ConsenterCertificate []byte
	CryptoProvider       bccsp.BCCSP
	Logger               *flogging.FabricLogger
}

// IsConsenterOfChannel returns whether the caller is a consenter of a channel
// by inspecting the given configuration block.
// It returns nil if true, else returns an error.
func (conCert ConsenterCertificate) IsConsenterOfChannel(configBlock *common.Block) error {
	if configBlock == nil || configBlock.Header == nil {
		return errors.New("nil block or nil header")
	}
	envelopeConfig, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, conCert.CryptoProvider)
	if err != nil {
		return err
	}
	oc, exists := bundle.OrdererConfig()
	if !exists {
		return errors.New("no orderer config in bundle")
	}
	m := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), m); err != nil {
		return err
	}

	bl, _ := pem.Decode(conCert.ConsenterCertificate)
	if bl == nil {
		return errors.Errorf("my consenter certificate %s is not a valid PEM", string(conCert.ConsenterCertificate))
	}

	myCertDER := bl.Bytes

	var failedMatches []string
	for _, consenter := range m.Consenters {
		candidateBlock, _ := pem.Decode(consenter.ServerTlsCert)
		if candidateBlock == nil {
			return errors.Errorf("candidate server certificate %s is not a valid PEM", string(consenter.ServerTlsCert))
		}
		sameServerCertErr := crypto.CertificatesWithSamePublicKey(myCertDER, candidateBlock.Bytes)

		candidateBlock, _ = pem.Decode(consenter.ClientTlsCert)
		if candidateBlock == nil {
			return errors.Errorf("candidate client certificate %s is not a valid PEM", string(consenter.ClientTlsCert))
		}
		sameClientCertErr := crypto.CertificatesWithSamePublicKey(myCertDER, candidateBlock.Bytes)

		if sameServerCertErr == nil || sameClientCertErr == nil {
			return nil
		}
		conCert.Logger.Debugf("I am not %s:%d because %s, %s", consenter.Host, consenter.Port, sameServerCertErr, sameClientCertErr)
		failedMatches = append(failedMatches, string(consenter.ClientTlsCert))
	}
	conCert.Logger.Debugf("Failed matching our certificate %s against certificates encoded in config block %d: %v",
		string(conCert.ConsenterCertificate),
		configBlock.Header.Number,
		failedMatches)

	return cluster.ErrNotInChannel
}

// NodeExists returns trues if node id exists in the slice
// and false otherwise
func NodeExists(id uint64, nodes []uint64) bool {
	for _, nodeID := range nodes {
		if nodeID == id {
			return true
		}
	}
	return false
}

// ConfChange 根据当前的Raft配置状态（confState）和存储在RaftMetadata中的ConsenterIds计算Raft配置变更。
func ConfChange(blockMetadata *etcdraft.BlockMetadata, confState *raftpb.ConfState) *raftpb.ConfChange {
	// 初始化一个Raft配置变更对象
	raftConfChange := &raftpb.ConfChange{}

	// 需要计算需要提出的配置变更
	if len(confState.Nodes) < len(blockMetadata.ConsenterIds) {
		// 如果Raft配置中的节点数量少于区块元数据中的ConsenterIds数量，说明需要添加新节点
		raftConfChange.Type = raftpb.ConfChangeAddNode
		// 遍历区块元数据中的ConsenterIds，查找尚未存在于当前Raft配置中的节点ID
		for _, consenterID := range blockMetadata.ConsenterIds {
			if NodeExists(consenterID, confState.Nodes) {
				continue // 如果节点已存在，则跳过
			}
			raftConfChange.NodeID = consenterID // 找到需添加的节点ID
		}
	} else {
		// 如果Raft配置中的节点数量多于或等于区块元数据中的ConsenterIds数量，说明需要移除节点
		raftConfChange.Type = raftpb.ConfChangeRemoveNode
		// 遍历当前Raft配置中的节点ID，查找不在区块元数据ConsenterIds中的节点
		for _, nodeID := range confState.Nodes {
			if NodeExists(nodeID, blockMetadata.ConsenterIds) {
				continue // 如果节点仍然有效（存在于ConsenterIds中），则跳过
			}
			raftConfChange.NodeID = nodeID // 找到需移除的节点ID
		}
	}

	// 返回计算好的配置变更对象
	return raftConfChange
}

// CreateConsentersMap 根据区块元数据和配置元数据创建一个从Raft节点ID到Consenter的映射。
func CreateConsentersMap(blockMetadata *etcdraft.BlockMetadata, configMetadata *etcdraft.ConfigMetadata) map[uint64]*etcdraft.Consenter {
	// 初始化共识者映射
	consenters := map[uint64]*etcdraft.Consenter{}

	// 遍历配置元数据中的共识者列表
	for i, consenter := range configMetadata.Consenters {
		// 使用区块元数据中的共识者ID作为键，将共识者信息添加到映射中
		consenters[blockMetadata.ConsenterIds[i]] = consenter
	}

	// 返回填充后的共识者映射
	return consenters
}
