/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster

import (
	"bytes"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-config/protolator"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// ConnByCertMap maps certificates represented as strings
// to gRPC connections
type ConnByCertMap map[string]*grpc.ClientConn

// Lookup looks up a certificate and returns the connection that was mapped
// to the certificate, and whether it was found or not
func (cbc ConnByCertMap) Lookup(cert []byte) (*grpc.ClientConn, bool) {
	conn, ok := cbc[string(cert)]
	return conn, ok
}

// Put associates the given connection to the certificate
func (cbc ConnByCertMap) Put(cert []byte, conn *grpc.ClientConn) {
	cbc[string(cert)] = conn
}

// Remove removes the connection that is associated to the given certificate
func (cbc ConnByCertMap) Remove(cert []byte) {
	delete(cbc, string(cert))
}

// Size returns the size of the connections by certificate mapping
func (cbc ConnByCertMap) Size() int {
	return len(cbc)
}

// CertificateComparator 是一个函数类型，用于比较两个给定的证书字节切片，
// 并返回一个布尔值表示两者之间的某种关系是否成立。
type CertificateComparator func([]byte, []byte) bool

// MemberMapping 结构体定义了一个网络成员的映射，其中成员通过其ID进行标识，
// 并且允许通过证书查找对应的 Stub（即远程节点的代理对象）。
// 它还包含了一个用于比较证书公钥是否相同的 CertificateComparator 函数。
type MemberMapping struct {
	id2stub       map[uint64]*Stub      // ID 到 Stub 的映射，用于快速查找特定ID的成员
	SamePublicKey CertificateComparator // 用于比较两个证书的公钥是否相同，以确定证书的等价性
}

// Foreach 方法遍历 MemberMapping 中的所有 Stub，并将每个 Stub 的 ID 和 Stub 本身传递给给定的函数 f。
func (mp *MemberMapping) Foreach(f func(id uint64, stub *Stub)) {
	for id, stub := range mp.id2stub {
		f(id, stub)
	}
}

// Put 方法将给定的 Stub 插入到 MemberMapping 中，使用 Stub 的 ID 作为键。
func (mp *MemberMapping) Put(stub *Stub) {
	mp.id2stub[stub.ID] = stub
}

// Remove 方法从 MemberMapping 中移除具有给定 ID 的 Stub。
func (mp *MemberMapping) Remove(ID uint64) {
	delete(mp.id2stub, ID)
}

// ByID 方法从 MemberMapping 中通过给定的 ID 检索 Stub。
func (mp MemberMapping) ByID(ID uint64) *Stub {
	return mp.id2stub[ID]
}

// LookupByClientCert 方法通过给定的客户端证书查找 MemberMapping 中的 Stub。
// 如果找到了具有相同公钥的 Stub，则返回该 Stub；否则返回 nil。
func (mp MemberMapping) LookupByClientCert(cert []byte) *Stub {
	for _, stub := range mp.id2stub {
		if mp.SamePublicKey(stub.ClientTLSCert, cert) {
			return stub
		}
	}
	return nil
}

// ServerCertificates 方法返回 MemberMapping 中所有成员的服务器证书集合，
// 证书以字符串形式表示，用于后续的比较或处理。
func (mp MemberMapping) ServerCertificates() StringSet {
	res := make(StringSet)
	for _, member := range mp.id2stub {
		res[string(member.ServerTLSCert)] = struct{}{}
	}
	return res
}

// StringSet 是一组字符串
type StringSet map[string]struct{}

// union 将给定集的元素添加到StringSet
func (ss StringSet) union(set StringSet) {
	for k := range set {
		ss[k] = struct{}{}
	}
}

// subtract removes all elements in the given set from the StringSet
func (ss StringSet) subtract(set StringSet) {
	for k := range set {
		delete(ss, k)
	}
}

// PredicateDialer 结构体用于创建gRPC连接，但这些连接的建立是受制于特定条件（predicate）的。
// 当给定的条件判断函数返回true时，连接才会真正建立。
type PredicateDialer struct {
	lock   sync.RWMutex      // 读写锁，用于保护内部状态的安全访问，确保线程安全。
	Config comm.ClientConfig // 客户端配置信息，包含了创建gRPC连接所需的各项配置，如地址、认证方式等。
}

func (dialer *PredicateDialer) UpdateRootCAs(serverRootCAs [][]byte) {
	dialer.lock.Lock()
	defer dialer.lock.Unlock()
	dialer.Config.SecOpts.ServerRootCAs = serverRootCAs
}

// Dial creates a new gRPC connection that can only be established, if the remote node's
// certificate chain satisfy verifyFunc
func (dialer *PredicateDialer) Dial(address string, verifyFunc RemoteVerifier) (*grpc.ClientConn, error) {
	dialer.lock.RLock()
	clientConfigCopy := dialer.Config
	dialer.lock.RUnlock()

	clientConfigCopy.SecOpts.VerifyCertificate = verifyFunc
	return clientConfigCopy.Dial(address)
}

// DERtoPEM returns a PEM representation of the DER
// encoded certificate
func DERtoPEM(der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	}))
}

// StandardDialer wraps an ClientConfig, and provides
// a means to connect according to given EndpointCriteria.
type StandardDialer struct {
	Config comm.ClientConfig
}

// Dial 方法根据给定的 EndpointCriteria 拨打指定的地址。
// 它会使用 StandardDialer 的配置副本，并将 EndpointCriteria 中的 TLSRootCAs 字段赋值给配置副本的 SecOpts.ServerRootCAs 字段，
// 然后使用配置副本拨打 EndpointCriteria.Endpoint 指定的地址。
// 参数：
// endpointCriteria EndpointCriteria // 包含远程排序节点的网络地址和 TLS 根证书的端点标准或条件
// 返回值：
// *grpc.ClientConn, error // 返回一个指向 gRPC 客户端连接的指针，以及可能的错误
func (dialer *StandardDialer) Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error) {
	// 创建配置副本，确保不会修改原始配置
	clientConfigCopy := dialer.Config
	// 将 EndpointCriteria 中的 TLSRootCAs 字段赋值给配置副本的 SecOpts.ServerRootCAs 字段
	clientConfigCopy.SecOpts.ServerRootCAs = endpointCriteria.TLSRootCAs

	// 使用配置副本拨打指定的地址
	return clientConfigCopy.Dial(endpointCriteria.Endpoint)
}

//go:generate mockery -dir . -name BlockVerifier -case underscore -output ./mocks/

// BlockVerifier 接口定义了区块签名验证的行为规范。
// 功能描述：
// VerifyBlockSignature 方法用于验证一组区块签名数据（SignedData 列表）的正确性。
// 可选参数为配置信封（ConfigEnvelope），如果提供，验证规则将基于此配置信封中的配置信息执行。
// 如果未提供配置信封（即为 nil），则使用之前区块提交时所应用的验证规则进行签名验证。
type BlockVerifier interface {
	// VerifyBlockSignature 方法用于验证一组区块签名数据的正确性。
	// 参数：
	// sd []*protoutil.SignedData // 包含签名者身份、签名数据和签名的签名数据集合
	// config *common.ConfigEnvelope // 可选配置信封，包含用于验证的配置信息
	// 返回值：
	// error // 如果签名验证失败，返回错误；否则，返回 nil 表示签名验证成功
	VerifyBlockSignature(sd []*protoutil.SignedData, config *common.ConfigEnvelope) error
}

// BlockSequenceVerifier 验证给定的连续块序列是否有效。
type BlockSequenceVerifier func(blocks []*common.Block, channel string) error

// Dialer 创建到远程地址的gRPC连接
type Dialer interface {
	Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error)
}

// VerifyBlocks 方法用于验证给定的一系列连续区块的合法性。
// 参数：
// blockBuff []*common.Block // 需要验证的区块序列
// signatureVerifier BlockVerifier // 用于验证区块签名的接口实现
// 功能描述：
// 验证区块序列的非空性。
// 验证每个区块的哈希值与其头部的哈希值相匹配，并且是前一个区块的后继哈希值。
// 验证所有配置区块，使用提交的配置或在迭代区块批处理过程中捕获的配置。
// 如果最后一个区块是配置区块，它已经在先前的验证中被确认，直接返回无误。
// 否则，验证最后一个区块的签名。
func VerifyBlocks(blockBuff []*common.Block, signatureVerifier BlockVerifier) error {
	// 如果区块序列为空，返回错误
	if len(blockBuff) == 0 {
		return errors.New("缓冲区为空")
	}

	// 第一步：验证每个区块的内部哈希一致性
	// 确保区块的哈希值等于头部的哈希值，且等于后继区块的前一个哈希值
	for i := range blockBuff {
		if err := VerifyBlockHash(i, blockBuff); err != nil {
			return err
		}
	}

	// 初始化配置变量和标记，用于跟踪是否最后一个区块是配置区块
	var config *common.ConfigEnvelope
	var isLastBlockConfigBlock bool

	// 第二步：验证所有配置区块
	// 使用已提交的配置或在迭代区块批处理过程中找到的配置
	for _, block := range blockBuff {
		// 方法用于从给定的区块中解析配置信封（ConfigEnvelope），如果存在的话。
		configFromBlock, err := ConfigFromBlock(block)
		if err == errNotAConfig {
			isLastBlockConfigBlock = false
			continue
		}
		if err != nil {
			return err
		}
		// 如果区块是配置区块，则验证它
		if err := VerifyBlockSignature(block, signatureVerifier, config); err != nil {
			return err
		}
		config = configFromBlock
		isLastBlockConfigBlock = true
	}

	// 第三步：验证最后一个区块的签名
	lastBlock := blockBuff[len(blockBuff)-1]

	// 如果最后一个区块是配置区块，它已在先前的验证中被确认，直接返回无误
	if isLastBlockConfigBlock {
		return nil
	}

	// 否则，验证最后一个区块的签名
	return VerifyBlockSignature(lastBlock, signatureVerifier, config)
}

var errNotAConfig = errors.New("不是配置块")

// ConfigFromBlock 方法用于从给定的区块中解析配置信封（ConfigEnvelope），如果存在的话。
// 参数：
// block *common.Block // 需要解析的区块
// 功能描述：
// 检查区块是否为空或无效。
// 解析区块中的第一个交易数据为信封。
// 解析信封的负载。
// 如果区块序号为0，直接解析负载数据为配置信封。
// 如果区块序号大于0，检查负载头部的类型是否为 CONFIG 类型。
// 解析配置信封并返回。
func ConfigFromBlock(block *common.Block) (*common.ConfigEnvelope, error) {
	// 检查区块是否为空或无效
	if block == nil || block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("空块")
	}

	// 解析区块中的第一个交易数据为信封
	txn := block.Data.Data[0]
	env, err := protoutil.GetEnvelopeFromBlock(txn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 解析信封的负载
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 如果区块序号为0，直接解析负载数据为配置信封
	if block.Header.Number == 0 {
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			return nil, errors.Wrap(err, "无效的配置信封")
		}
		return configEnvelope, nil
	}

	// 如果区块序号大于0，检查负载头部的类型是否为 CONFIG 类型
	if payload.Header == nil {
		return nil, errors.New("有效负载中的nil标头")
	}
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if common.HeaderType(chdr.Type) != common.HeaderType_CONFIG {
		return nil, errNotAConfig
	}

	// 解析配置信封并返回
	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.Wrap(err, "无效的配置信封")
	}
	return configEnvelope, nil
}

// VerifyBlockHash 方法用于验证区块缓冲区中特定索引位置的区块的哈希链。
// 参数：
// indexInBuffer int // 区块在缓冲区中的索引位置
// blockBuff []*common.Block // 区块缓冲区
// 功能描述：
// 检查区块索引是否超出缓冲区范围。
// 检查区块头部是否存在。
// 验证区块数据哈希与头部声明的哈希一致。
// 若当前区块非首个区块，验证当前区块的前一个区块哈希与实际前一个区块的哈希一致。
func VerifyBlockHash(indexInBuffer int, blockBuff []*common.Block) error {
	// 检查区块索引是否超出缓冲区范围
	if len(blockBuff) <= indexInBuffer {
		return errors.Errorf("索引 %d 超出界限 (总计 %d块)", indexInBuffer, len(blockBuff))
	}

	// 获取待验证的区块
	block := blockBuff[indexInBuffer]

	// 检查区块头部是否存在
	if block.Header == nil {
		return errors.New("缺少块标头")
	}

	// 获取区块序列号
	seq := block.Header.Number

	// 计算区块数据哈希
	dataHash := protoutil.BlockDataHash(block.Data)

	// 验证区块数据哈希与头部声明的哈希一致
	if !bytes.Equal(dataHash, block.Header.DataHash) {
		computedHash := hex.EncodeToString(dataHash)
		claimedHash := hex.EncodeToString(block.Header.DataHash)
		return errors.Errorf("块的计算哈希 (%d) (%s) 与声明的哈希值不匹配(%s)",
			seq, computedHash, claimedHash)
	}

	// 若当前区块非首个区块，验证当前区块的前一个区块哈希与实际前一个区块的哈希一致
	if indexInBuffer > 0 {
		// 获取前一个区块
		prevBlock := blockBuff[indexInBuffer-1]

		// 检查前一个区块头部是否存在
		if prevBlock.Header == nil {
			return errors.New("上一个块标头为零")
		}

		// 获取前一个区块序列号
		prevSeq := prevBlock.Header.Number

		// 验证当前区块序列号是否为前一个区块序列号的下一序号
		if prevSeq+1 != seq {
			return errors.Errorf("序列 %d 和 %d 不连续", prevSeq, seq)
		}

		// 验证当前区块的前一个区块哈希与实际前一个区块的哈希一致
		if !bytes.Equal(block.Header.PreviousHash, protoutil.BlockHeaderHash(prevBlock.Header)) {
			claimedPrevHash := hex.EncodeToString(block.Header.PreviousHash)
			actualPrevHash := hex.EncodeToString(protoutil.BlockHeaderHash(prevBlock.Header))
			return errors.Errorf("块 [%d] 的哈希 (%s) 不匹配块 [%d] 的前一个块哈希 (%s)",
				prevSeq, actualPrevHash, seq, claimedPrevHash)
		}
	}

	// 如果所有验证均通过，返回无误
	return nil
}

// SignatureSetFromBlock 方法用于从给定的区块中创建一个签名集，该签名集包含了区块的多个签名及其元数据。
// 参数：
// block *common.Block // 需要创建签名集的区块
// 功能描述：
// 检查区块的元数据是否存在，以及元数据是否包含足够的条目。
// 解析区块中与签名相关的元数据。
// 遍历元数据中的签名信息，反序列化签名头，并创建签名集中的每个 SignedData 对象，其中包含了签名者的身份信息、签名数据（由元数据值、签名头和区块头部组成的字节串）以及签名本身。
// 返回创建好的签名集。
func SignatureSetFromBlock(block *common.Block) ([]*protoutil.SignedData, error) {
	// 检查区块的元数据是否存在，以及元数据是否包含足够的条目
	if block.Metadata == nil || len(block.Metadata.Metadata) <= int(common.BlockMetadataIndex_SIGNATURES) {
		return nil, errors.New("块中没有元数据")
	}

	// 解析区块中与签名相关的元数据
	metadata, err := protoutil.GetMetadataFromBlock(block, common.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return nil, errors.Errorf("对签名的元数据进行取消编组失败: %v", err)
	}

	// 创建签名集
	var signatureSet []*protoutil.SignedData

	// 遍历元数据中的签名信息
	for _, metadataSignature := range metadata.Signatures {
		// 反序列化签名头
		sigHdr, err := protoutil.UnmarshalSignatureHeader(metadataSignature.SignatureHeader)
		if err != nil {
			return nil, errors.Errorf("无法对id为的块的签名标头进行编组 %d: %v",
				block.Header.Number, err)
		}

		// 创建签名集中的每个 SignedData 对象
		signatureSet = append(signatureSet,
			&protoutil.SignedData{
				Identity: sigHdr.Creator, // 签名者的身份信息
				Data: util.ConcatenateBytes(
					metadata.Value,                            // 签名数据的一部分
					metadataSignature.SignatureHeader,         // 签名数据的一部分
					protoutil.BlockHeaderBytes(block.Header)), // 签名数据的一部分
				Signature: metadataSignature.Signature, // 签名本身
			},
		)
	}

	// 返回创建好的签名集
	return signatureSet, nil
}

// VerifyBlockSignature 方法用于验证给定区块的签名，确保区块的完整性和来源的可靠性。
// 参数：
// block *common.Block // 需要验证签名的区块
// verifier BlockVerifier // 用于验证区块签名的接口实现
// config *common.ConfigEnvelope // 包含验证签名所需配置信息的配置信封
// 功能描述：
// 从给定区块中提取签名集。
// 使用提供的 BlockVerifier 接口实现来验证签名集的正确性，基于配置信封中的信息。
func VerifyBlockSignature(block *common.Block, verifier BlockVerifier, config *common.ConfigEnvelope) error {
	// 从给定区块中提取签名集
	signatureSet, err := SignatureSetFromBlock(block)
	if err != nil {
		return err
	}

	// 使用提供的 BlockVerifier 接口实现来验证签名集的正确性
	return verifier.VerifyBlockSignature(signatureSet, config)
}

// EndpointCriteria 定义了如何连接到远程排序节点的标准或条件。
type EndpointCriteria struct {
	// 远程排序节点的网络地址，格式为 host:port
	Endpoint string
	// TLS 根证书，PEM 编码格式的字节切片，用于验证远程节点的身份
	TLSRootCAs [][]byte
}

// String returns a string representation of this EndpointCriteria
func (ep EndpointCriteria) String() string {
	var formattedCAs []interface{}
	for _, rawCAFile := range ep.TLSRootCAs {
		var bl *pem.Block
		pemContent := rawCAFile
		for {
			bl, pemContent = pem.Decode(pemContent)
			if bl == nil {
				break
			}
			cert, err := x509.ParseCertificate(bl.Bytes)
			if err != nil {
				break
			}

			issuedBy := cert.Issuer.String()
			if cert.Issuer.String() == cert.Subject.String() {
				issuedBy = "self"
			}

			info := make(map[string]interface{})
			info["Expired"] = time.Now().After(cert.NotAfter)
			info["Subject"] = cert.Subject.String()
			info["Issuer"] = issuedBy
			formattedCAs = append(formattedCAs, info)
		}
	}

	formattedEndpointCriteria := make(map[string]interface{})
	formattedEndpointCriteria["Endpoint"] = ep.Endpoint
	formattedEndpointCriteria["CAs"] = formattedCAs

	rawJSON, err := json.Marshal(formattedEndpointCriteria)
	if err != nil {
		return fmt.Sprintf("{\"Endpoint\": \"%s\"}", ep.Endpoint)
	}

	return string(rawJSON)
}

// EndpointconfigFromConfigBlock 函数从配置区块中提取 TLS CA 证书和端点信息。
// 参数:
// block *common.Block       // 区块，包含配置信息
// bccsp bccsp.BCCSP         // 加密服务提供者，用于处理加密和解密操作
//
// 返回值:
// []EndpointCriteria        // 端点配置信息列表
// error                     // 错误信息，如果出现任何问题则返回非空错误
func EndpointconfigFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) ([]EndpointCriteria, error) {
	// 检查输入的区块是否为 nil
	if block == nil {
		return nil, errors.New("块为空") // 如果区块为 nil，返回错误
	}

	// 从区块中提取配置信封
	envelopeConfig, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, err // 如果提取失败，返回错误
	}

	// 从配置信封中创建配置束
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, bccsp)
	if err != nil {
		return nil, errors.Wrap(err, "从信封中提取包失败") // 如果创建失败，返回错误
	}

	// 从配置束中获取 MSP 管理器中的所有 MSP 实例
	msps, err := bundle.MSPManager().GetMSPs()
	if err != nil {
		return nil, errors.Wrap(err, "从MSPManager获取MSPs失败") // 如果获取失败，返回错误
	}

	// 从配置束中获取排序服务配置
	ordererConfig, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.New("从包中获取orderer配置失败") // 如果获取失败，返回错误
	}

	// 构建一个映射，将每个组织的 MSPID 映射到其 TLS CA 证书
	mspIDsToCACerts := make(map[string][][]byte)
	var aggregatedTLSCerts [][]byte // 聚合所有 TLS CA 证书

	// 遍历排序服务配置中的所有组织
	for _, org := range ordererConfig.Organizations() {
		// 验证每个排序服务组织都有一个对应的 MSP 实例在 MSP 管理器中
		msp, exists := msps[org.MSPID()]
		if !exists {
			return nil, errors.Errorf("未找到ID为的MSP %s", org.MSPID()) // 如果找不到对应的 MSP，返回错误
		}

		// 构建一个按组织划分的 TLS CA 证书映射，
		// 并将所有 TLS CA 证书聚合到 aggregatedTLSCerts 中以供后续使用
		var caCerts [][]byte
		caCerts = append(caCerts, msp.GetTLSIntermediateCerts()...)
		caCerts = append(caCerts, msp.GetTLSRootCerts()...)
		mspIDsToCACerts[org.MSPID()] = caCerts
		aggregatedTLSCerts = append(aggregatedTLSCerts, caCerts...)
	}

	// 从排序服务配置中提取每个组织的端点信息，使用之前构建的 TLS CA 证书映射
	endpointsPerOrg := perOrgEndpoints(ordererConfig, mspIDsToCACerts)
	if len(endpointsPerOrg) > 0 {
		return endpointsPerOrg, nil // 如果每个组织的端点信息存在，直接返回
	}

	// 如果每个组织的端点信息不存在，从配置中提取全局端点信息，使用聚合的 TLS CA 证书
	return globalEndpointsFromConfig(aggregatedTLSCerts, bundle), nil
}

// perOrgEndpoints 函数接收一个排序服务的配置对象和一个映射，该映射关联组织标识符 (MSPID) 到对应的 TLS 根证书。
// 函数的目的是为了每个组织的每一个端点创建并返回一系列的 EndpointCriteria 对象数组，
// 这些对象包含了访问排序服务所需的 TLS 根证书和具体的端点 URL。
func perOrgEndpoints(ordererConfig channelconfig.Orderer, mspIDsToCerts map[string][][]byte) []EndpointCriteria {
	var endpointsPerOrg []EndpointCriteria

	// 遍历排序服务配置中的所有组织信息
	for _, org := range ordererConfig.Organizations() {
		// 对于每个组织，遍历其所有的端点信息
		for _, endpoint := range org.Endpoints() {
			// 使用组织的 MSPID 在 mspIDsToCerts 映射中查找对应的 TLS 根证书
			// 创建一个 EndpointCriteria 对象，其中包含 TLS 根证书和端点 URL
			endpointsPerOrg = append(endpointsPerOrg, EndpointCriteria{
				TLSRootCAs: mspIDsToCerts[org.MSPID()], // TLS 根证书
				Endpoint:   endpoint,                   // 具体的端点 URL
			})
		}
	}

	// 返回包含所有 EndpointCriteria 对象的数组
	return endpointsPerOrg
}

// globalEndpointsFromConfig 函数接收聚合的 TLS 根证书和一个通道配置束（bundle）。
// 它的目的是创建并返回一个 EndpointCriteria 对象数组，
// 这些对象包含了全局排序服务的端点 URL 和用于 TLS 验证的根证书，
// 使得客户端可以安全地与这些全局排序服务进行通信。
func globalEndpointsFromConfig(aggregatedTLSCerts [][]byte, bundle *channelconfig.Bundle) []EndpointCriteria {
	var globalEndpoints []EndpointCriteria

	// 遍历通道配置束中的所有排序服务地址
	for _, endpoint := range bundle.ChannelConfig().OrdererAddresses() {
		// 对于每个地址，创建一个 EndpointCriteria 对象，包含地址和聚合的 TLS 根证书
		globalEndpoints = append(globalEndpoints, EndpointCriteria{
			Endpoint:   endpoint,           // 排序服务的端点 URL
			TLSRootCAs: aggregatedTLSCerts, // 聚合的 TLS 根证书
		})
	}

	// 返回包含所有 EndpointCriteria 对象的数组
	return globalEndpoints
}

//go:generate mockery -dir . -name VerifierFactory -case underscore -output ./mocks/

// VerifierFactory creates BlockVerifiers.
type VerifierFactory interface {
	// VerifierFromConfig creates a BlockVerifier from the given configuration.
	VerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (BlockVerifier, error)
}

// VerificationRegistry registers verifiers and retrieves them.
type VerificationRegistry struct {
	LoadVerifier       func(chain string) BlockVerifier
	Logger             *flogging.FabricLogger
	VerifierFactory    VerifierFactory
	VerifiersByChannel map[string]BlockVerifier
}

// RegisterVerifier adds a verifier into the registry if applicable.
func (vr *VerificationRegistry) RegisterVerifier(chain string) {
	if _, exists := vr.VerifiersByChannel[chain]; exists {
		vr.Logger.Debugf("No need to register verifier for chain %s", chain)
		return
	}

	v := vr.LoadVerifier(chain)
	if v == nil {
		vr.Logger.Errorf("Failed loading verifier for chain %s", chain)
		return
	}

	vr.VerifiersByChannel[chain] = v
	vr.Logger.Infof("Registered verifier for chain %s", chain)
}

// RetrieveVerifier returns a BlockVerifier for the given channel, or nil if not found.
func (vr *VerificationRegistry) RetrieveVerifier(channel string) BlockVerifier {
	verifier, exists := vr.VerifiersByChannel[channel]
	if exists {
		return verifier
	}
	vr.Logger.Errorf("No verifier for channel %s exists", channel)
	return nil
}

// BlockCommitted notifies the VerificationRegistry upon a block commit, which may
// trigger a registration of a verifier out of the block in case the block is a config block.
func (vr *VerificationRegistry) BlockCommitted(block *common.Block, channel string) {
	conf, err := ConfigFromBlock(block)
	// The block doesn't contain a config block, but is a valid block
	if err == errNotAConfig {
		vr.Logger.Debugf("Committed block [%d] for channel %s that is not a config block",
			block.Header.Number, channel)
		return
	}
	// The block isn't a valid block
	if err != nil {
		vr.Logger.Errorf("Failed parsing block of channel %s: %v, content: %s",
			channel, err, BlockToString(block))
		return
	}

	// The block contains a config block
	verifier, err := vr.VerifierFactory.VerifierFromConfig(conf, channel)
	if err != nil {
		vr.Logger.Errorf("Failed creating a verifier from a config block for channel %s: %v, content: %s",
			channel, err, BlockToString(block))
		return
	}

	vr.VerifiersByChannel[channel] = verifier

	vr.Logger.Debugf("Committed config block [%d] for channel %s", block.Header.Number, channel)
}

// BlockToString returns a string representation of this block.
func BlockToString(block *common.Block) string {
	buff := &bytes.Buffer{}
	protolator.DeepMarshalJSON(buff, block)
	return buff.String()
}

// BlockCommitFunc signals a block commit.
type BlockCommitFunc func(block *common.Block, channel string)

// LedgerInterceptor intercepts block commits.
type LedgerInterceptor struct {
	Channel              string
	InterceptBlockCommit BlockCommitFunc
	LedgerWriter
}

// Append commits a block into the ledger, and also fires the configured callback.
func (interceptor *LedgerInterceptor) Append(block *common.Block) error {
	defer interceptor.InterceptBlockCommit(block, interceptor.Channel)
	return interceptor.LedgerWriter.Append(block)
}

// BlockVerifierAssembler creates a BlockVerifier out of a config envelope
type BlockVerifierAssembler struct {
	Logger *flogging.FabricLogger
	BCCSP  bccsp.BCCSP
}

// VerifierFromConfig creates a BlockVerifier from the given configuration.
func (bva *BlockVerifierAssembler) VerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (BlockVerifier, error) {
	bundle, err := channelconfig.NewBundle(channel, configuration.Config, bva.BCCSP)
	if err != nil {
		return nil, errors.Wrap(err, "failed extracting bundle from envelope")
	}
	policyMgr := bundle.PolicyManager()

	return &BlockValidationPolicyVerifier{
		Logger:    bva.Logger,
		PolicyMgr: policyMgr,
		Channel:   channel,
		BCCSP:     bva.BCCSP,
	}, nil
}

// BlockValidationPolicyVerifier verifies signatures based on the block validation policy.
type BlockValidationPolicyVerifier struct {
	Logger    *flogging.FabricLogger
	Channel   string
	PolicyMgr policies.Manager
	BCCSP     bccsp.BCCSP
}

// VerifyBlockSignature verifies the signed data associated to a block, optionally with the given config envelope.
func (bv *BlockValidationPolicyVerifier) VerifyBlockSignature(sd []*protoutil.SignedData, envelope *common.ConfigEnvelope) error {
	policyMgr := bv.PolicyMgr
	// If the envelope passed isn't nil, we should use a different policy manager.
	if envelope != nil {
		bundle, err := channelconfig.NewBundle(bv.Channel, envelope.Config, bv.BCCSP)
		if err != nil {
			buff := &bytes.Buffer{}
			protolator.DeepMarshalJSON(buff, envelope.Config)
			bv.Logger.Errorf("Failed creating a new bundle for channel %s, Config content is: %s", bv.Channel, buff.String())
			return err
		}
		bv.Logger.Infof("Initializing new PolicyManager for channel %s", bv.Channel)
		policyMgr = bundle.PolicyManager()
	}
	policy, exists := policyMgr.GetPolicy(policies.BlockValidation)
	if !exists {
		return errors.Errorf("policy %s wasn't found", policies.BlockValidation)
	}
	return policy.EvaluateSignedData(sd)
}

//go:generate mockery -dir . -name BlockRetriever -case underscore -output ./mocks/

// BlockRetriever 检索块
type BlockRetriever interface {
	// Block返回一个具有给定数字的块，
	// 如果这样的块不存在，则为nil。
	Block(number uint64) *common.Block
}

// LastConfigBlock 函数返回相对于给定区块的最后一个配置区块。
// 参数:
// block *common.Block          // 当前区块，从其中解析出最后一次配置变更的索引
// blockRetriever BlockRetriever // 区块检索器，用于根据区块序号获取区块实例
//
// 返回值:
// *common.Block                // 最后一个配置区块
// error                        // 错误信息，如果出现任何问题则返回非空错误
func LastConfigBlock(block *common.Block, blockRetriever BlockRetriever) (*common.Block, error) {
	// 检查输入参数是否为 nil
	if block == nil {
		return nil, errors.New("块为空") // 如果 block 为 nil，返回错误
	}
	if blockRetriever == nil {
		return nil, errors.New("块检索器为空") // 如果 blockRetriever 为 nil，返回错误
	}

	// 从当前区块中解析出最后一次配置变更的索引
	lastConfigBlockNum, err := protoutil.GetLastConfigIndexFromBlock(block)
	if err != nil {
		return nil, err // 如果解析失败，返回错误
	}

	// 使用区块检索器根据最后一次配置变更的索引获取区块实例
	lastConfigBlock := blockRetriever.Block(lastConfigBlockNum)
	if lastConfigBlock == nil {
		// 如果未能成功获取区块，返回错误
		return nil, errors.Errorf("unable to retrieve last config block [%d]", lastConfigBlockNum)
	}

	// 成功获取到最后一个配置区块，返回该区块
	return lastConfigBlock, nil
}

// StreamCountReporter reports the number of streams currently connected to this node
type StreamCountReporter struct {
	Metrics *Metrics
	count   uint32
}

func (scr *StreamCountReporter) Increment() {
	count := atomic.AddUint32(&scr.count, 1)
	scr.Metrics.reportStreamCount(count)
}

func (scr *StreamCountReporter) Decrement() {
	count := atomic.AddUint32(&scr.count, ^uint32(0))
	scr.Metrics.reportStreamCount(count)
}

type certificateExpirationCheck struct {
	minimumExpirationWarningInterval time.Duration
	expiresAt                        time.Time
	expirationWarningThreshold       time.Duration
	lastWarning                      time.Time
	nodeName                         string
	endpoint                         string
	alert                            func(string, ...interface{})
}

// 检查证书过期
func (exp *certificateExpirationCheck) checkExpiration(currentTime time.Time, channel string) {
	// 计算证书过期剩余时间
	timeLeft := exp.expiresAt.Sub(currentTime)
	// 如果剩余时间大于过期警告阈值，则不进行警告
	if timeLeft > exp.expirationWarningThreshold {
		return
	}

	// 计算距上次警告的时间间隔
	timeSinceLastWarning := currentTime.Sub(exp.lastWarning)
	// 如果距上次警告的时间小于最小过期警告间隔，则不进行警告
	if timeSinceLastWarning < exp.minimumExpirationWarningInterval {
		return
	}

	// 发出证书即将过期的警告
	exp.alert("节点 %s 的证书来自 %s，用于通道 %s 的证书将在 %v 后过期",
		exp.nodeName, exp.endpoint, channel, timeLeft)
	exp.lastWarning = currentTime // 更新上次警告时间
}

// CachePublicKeyComparisons creates CertificateComparator that caches invocations based on input arguments.
// The given CertificateComparator must be a stateless function.
func CachePublicKeyComparisons(f CertificateComparator) CertificateComparator {
	m := &ComparisonMemoizer{
		MaxEntries: 4096,
		F:          f,
	}
	return m.Compare
}

// ComparisonMemoizer speeds up comparison computations by caching past invocations of a stateless function
type ComparisonMemoizer struct {
	// Configuration
	F          func(a, b []byte) bool
	MaxEntries uint16
	// Internal state
	cache map[arguments]bool
	lock  sync.RWMutex
	once  sync.Once
	rand  *rand.Rand
}

type arguments struct {
	a, b string
}

// Size returns the number of computations the ComparisonMemoizer currently caches.
func (cm *ComparisonMemoizer) Size() int {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return len(cm.cache)
}

// Compare compares the given two byte slices.
// It may return previous computations for the given two arguments,
// otherwise it will compute the function F and cache the result.
func (cm *ComparisonMemoizer) Compare(a, b []byte) bool {
	cm.once.Do(cm.setup)
	key := arguments{
		a: string(a),
		b: string(b),
	}

	cm.lock.RLock()
	result, exists := cm.cache[key]
	cm.lock.RUnlock()

	if exists {
		return result
	}

	result = cm.F(a, b)

	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.shrinkIfNeeded()
	cm.cache[key] = result

	return result
}

func (cm *ComparisonMemoizer) shrinkIfNeeded() {
	for {
		currentSize := uint16(len(cm.cache))
		if currentSize < cm.MaxEntries {
			return
		}
		cm.shrink()
	}
}

func (cm *ComparisonMemoizer) shrink() {
	// Shrink the cache by 25% by removing every fourth element (on average)
	for key := range cm.cache {
		if cm.rand.Int()%4 != 0 {
			continue
		}
		delete(cm.cache, key)
	}
}

func (cm *ComparisonMemoizer) setup() {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	cm.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	cm.cache = make(map[arguments]bool)
}
