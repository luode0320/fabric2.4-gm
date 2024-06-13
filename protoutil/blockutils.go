/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm3"
	"github.com/hyperledger/fabric/bccsp/factory"
	"math/big"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/pkg/errors"
)

// NewBlock 构造一个没有数据和元数据的块。
// 输入参数：
//   - seqNum：块的序列号。
//   - previousHash：前一个块的哈希值。
//
// 返回值：
//   - *cb.Block：构造的块。
func NewBlock(seqNum uint64, previousHash []byte) *cb.Block {
	// 创建一个新的块
	block := &cb.Block{}
	// 创建块头部
	block.Header = &cb.BlockHeader{}
	// 设置块的序列号
	block.Header.Number = seqNum
	// 设置块的前一个块的哈希值
	block.Header.PreviousHash = previousHash
	// 设置块的数据哈希值为空
	block.Header.DataHash = []byte{}
	// 创建块的数据
	block.Data = &cb.BlockData{}

	// 创建元数据内容的切片
	var metadataContents [][]byte
	// 遍历块元数据的索引
	for i := 0; i < len(cb.BlockMetadataIndex_name); i++ {
		// 将空的元数据内容添加到切片中
		metadataContents = append(metadataContents, []byte{})
	}
	// 创建块的元数据
	block.Metadata = &cb.BlockMetadata{Metadata: metadataContents}

	return block
}

type asn1Header struct {
	Number       *big.Int
	PreviousHash []byte
	DataHash     []byte
}

func BlockHeaderBytes(b *cb.BlockHeader) []byte {
	asn1Header := asn1Header{
		PreviousHash: b.PreviousHash,
		DataHash:     b.DataHash,
		Number:       new(big.Int).SetUint64(b.Number),
	}
	result, err := asn1.Marshal(asn1Header)
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BlockHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propagated
		panic(err)
	}
	return result
}

func BlockHeaderHash(b *cb.BlockHeader) []byte {
	// todo luode 进行国密sm3的改造
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		sum := sm3.Sm3Sum(BlockHeaderBytes(b))
		return sum[:]
	default:
		sum := sha256.Sum256(BlockHeaderBytes(b))
		return sum[:]
	}
}

func BlockDataHash(b *cb.BlockData) []byte {
	// todo luode 进行国密sm3的改造
	switch factory.FactoryName {
	case factory.GuomiBasedFactoryName:
		sum := sm3.Sm3Sum(bytes.Join(b.Data, nil))
		return sum[:]
	default:
		sum := sha256.Sum256(bytes.Join(b.Data, nil))
		return sum[:]
	}
}

// GetChannelIDFromBlockBytes returns channel ID given byte array which represents
// the block
func GetChannelIDFromBlockBytes(bytes []byte) (string, error) {
	block, err := UnmarshalBlock(bytes)
	if err != nil {
		return "", err
	}

	return GetChannelIDFromBlock(block)
}

// GetChannelIDFromBlock 从区块中获取通道的ID，并返回
func GetChannelIDFromBlock(block *cb.Block) (string, error) {
	if block == nil || block.Data == nil || block.Data.Data == nil || len(block.Data.Data) == 0 {
		return "", errors.New("检索通道id失败 block 为空")
	}
	var err error
	// 从块的Data字段中获取一个envelope带有签名的有效负载,以便可以对消息进行身份验证。
	envelope, err := GetEnvelopeFromBlock(block.Data.Data[0])
	if err != nil {
		return "", err
	}
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return "", err
	}

	if payload.Header == nil {
		return "", errors.New("无法检索通道id -payload 标头Header为空")
	}
	chdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}

	return chdr.ChannelId, nil
}

// GetMetadataFromBlock 函数从指定索引处检索块的元数据。
// 方法接收者：无。
// 输入参数：
//   - block *cb.Block，表示块对象。
//   - index cb.BlockMetadataIndex，表示元数据索引。
//
// 返回值：
//   - *cb.Metadata，表示检索到的元数据。
//   - error，表示可能发生的错误。
func GetMetadataFromBlock(block *cb.Block, index cb.BlockMetadataIndex) (*cb.Metadata, error) {
	// 检查块是否包含元数据
	if block.Metadata == nil {
		return nil, errors.New("块中没有元数据")
	}

	// 检查指定索引是否超出元数据切片的长度
	if len(block.Metadata.Metadata) <= int(index) {
		return nil, errors.Errorf("索引处没有元数据: [%s]", index)
	}

	// 创建 Metadata 对象
	md := &cb.Metadata{}

	// 反序列化元数据
	err := proto.Unmarshal(block.Metadata.Metadata[index], md)
	if err != nil {
		return nil, errors.Wrapf(err, "在索引处反序列化元数据时出错 [%s]", index)
	}

	// 返回检索到的元数据
	return md, nil
}

// GetMetadataFromBlockOrPanic retrieves metadata at the specified index, or
// panics on error
func GetMetadataFromBlockOrPanic(block *cb.Block, index cb.BlockMetadataIndex) *cb.Metadata {
	md, err := GetMetadataFromBlock(block, index)
	if err != nil {
		panic(err)
	}
	return md
}

// GetConsenterMetadataFromBlock attempts to retrieve consenter metadata from the value
// stored in block metadata at index SIGNATURES (first field). If no consenter metadata
// is found there, it falls back to index ORDERER (third field).
func GetConsenterMetadataFromBlock(block *cb.Block) (*cb.Metadata, error) {
	m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve metadata")
	}

	// TODO FAB-15864 Remove this fallback when we can stop supporting upgrade from pre-1.4.1 orderer
	if len(m.Value) == 0 {
		return GetMetadataFromBlock(block, cb.BlockMetadataIndex_ORDERER)
	}

	obm := &cb.OrdererBlockMetadata{}
	err = proto.Unmarshal(m.Value, obm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal orderer block metadata")
	}

	res := &cb.Metadata{}
	err = proto.Unmarshal(obm.ConsenterMetadata, res)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consenter metadata")
	}

	return res, nil
}

// GetLastConfigIndexFromBlock 从块元数据中检索最后一个配置块的索引。
// 方法接收者：无。
// 输入参数：
//   - block *cb.Block，表示块对象。
//
// 返回值：
//   - uint64，表示最后一个配置块的索引。
//   - error，表示可能发生的错误。
func GetLastConfigIndexFromBlock(block *cb.Block) (uint64, error) {
	// 从块元数据中获取签名的元数据, 块元数据索引签名
	m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return 0, errors.WithMessage(err, "无法检索元数据")
	}

	// 如果签名的元数据为空，则尝试从块元数据中获取最后一个配置块的元数据
	if len(m.Value) == 0 {
		m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_LAST_CONFIG)
		if err != nil {
			return 0, errors.WithMessage(err, "无法检索元数据")
		}

		// 反序列化 LastConfig 对象
		lc := &cb.LastConfig{}
		err = proto.Unmarshal(m.Value, lc)
		if err != nil {
			return 0, errors.Wrap(err, "反序列化 LastConfig 时出错")
		}

		// 返回最后一个配置块的索引
		return lc.Index, nil
	}

	// 反序列化 OrdererBlockMetadata 对象
	obm := &cb.OrdererBlockMetadata{}
	err = proto.Unmarshal(m.Value, obm)
	if err != nil {
		return 0, errors.Wrap(err, "无法反序列化 orderer 块元数据")
	}

	// 返回最后一个配置块的索引
	return obm.LastConfig.Index, nil
}

// GetLastConfigIndexFromBlockOrPanic retrieves the index of the last config
// block as encoded in the block metadata, or panics on error
func GetLastConfigIndexFromBlockOrPanic(block *cb.Block) uint64 {
	index, err := GetLastConfigIndexFromBlock(block)
	if err != nil {
		panic(err)
	}
	return index
}

// CopyBlockMetadata copies metadata from one block into another
func CopyBlockMetadata(src *cb.Block, dst *cb.Block) {
	dst.Metadata = src.Metadata
	// Once copied initialize with rest of the
	// required metadata positions.
	InitBlockMetadata(dst)
}

// InitBlockMetadata initializes metadata structure
func InitBlockMetadata(block *cb.Block) {
	if block.Metadata == nil {
		block.Metadata = &cb.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}}
	} else if len(block.Metadata.Metadata) < int(cb.BlockMetadataIndex_COMMIT_HASH+1) {
		for i := int(len(block.Metadata.Metadata)); i <= int(cb.BlockMetadataIndex_COMMIT_HASH); i++ {
			block.Metadata.Metadata = append(block.Metadata.Metadata, []byte{})
		}
	}
}
