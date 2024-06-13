/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// serializedBlockInfo 是一个序列化的区块信息的结构体。
type serializedBlockInfo struct {
	blockHeader *common.BlockHeader   // 区块头
	txOffsets   []*txindexInfo        // 交易偏移量信息列表
	metadata    *common.BlockMetadata // 区块元数据
}

// txindexInfo 是一个交易索引信息的结构体。
type txindexInfo struct {
	txID string      // 交易ID
	loc  *locPointer // 位置指针
}

func serializeBlock(block *common.Block) ([]byte, *serializedBlockInfo, error) {
	buf := proto.NewBuffer(nil)
	var err error
	info := &serializedBlockInfo{}
	info.blockHeader = block.Header
	info.metadata = block.Metadata
	if err = addHeaderBytes(block.Header, buf); err != nil {
		return nil, nil, err
	}
	if info.txOffsets, err = addDataBytesAndConstructTxIndexInfo(block.Data, buf); err != nil {
		return nil, nil, err
	}
	if err = addMetadataBytes(block.Metadata, buf); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), info, nil
}

// deserializeBlock 将序列化的区块字节数据反序列化为区块对象。
// 方法接收者：无（全局函数）
// 输入参数：
//   - serializedBlockBytes：序列化的区块字节数据。
//
// 返回值：
//   - *common.Block：反序列化后的区块对象。
//   - error：如果反序列化时出错，则返回错误。
func deserializeBlock(serializedBlockBytes []byte) (*common.Block, error) {
	// 创建一个空的区块对象
	block := &common.Block{}
	var err error
	// 创建一个新的缓冲区，并将序列化的区块字节数据传入
	b := newBuffer(serializedBlockBytes)
	// 提取区块头部，并将提取的结果赋值给区块的Header字段
	if block.Header, err = extractHeader(b); err != nil {
		return nil, err
	}
	// 提取区块数据，并将提取的结果赋值给区块的Data字段
	if block.Data, _, err = extractData(b); err != nil {
		return nil, err
	}
	// 提取区块元数据，并将提取的结果赋值给区块的Metadata字段
	if block.Metadata, err = extractMetadata(b); err != nil {
		return nil, err
	}
	return block, nil
}

// extractSerializedBlockInfo 提取序列化的块信息。头信息(区块号、数据哈希、上一个哈希), 提取交易偏移量信息, 提取元数据信息
// 方法接收者：无（函数）
// 输入参数：
//   - serializedBlockBytes：序列化的块字节数据。
//
// 返回值：
//   - *serializedBlockInfo：提取到的序列化的块信息。
//   - error：如果提取块信息时出错，则返回错误。
func extractSerializedBlockInfo(serializedBlockBytes []byte) (*serializedBlockInfo, error) {
	// 创建一个serializedBlockInfo实例, 一个序列化的区块信息的结构体(区块头、交易偏移量信息列表、区块元数据)
	info := &serializedBlockInfo{}
	var err error
	// 创建一个新的缓冲区，并将序列化的块字节数据传入
	b := newBuffer(serializedBlockBytes)

	// 提取块头信息(区块号、数据哈希、上一个哈希)
	info.blockHeader, err = extractHeader(b)
	if err != nil {
		return nil, err
	}

	// 提取交易偏移量信息
	_, info.txOffsets, err = extractData(b)
	if err != nil {
		return nil, err
	}

	// 提取元数据信息
	info.metadata, err = extractMetadata(b)
	if err != nil {
		return nil, err
	}

	// 返回提取到的序列化的块信息
	return info, nil
}

func addHeaderBytes(blockHeader *common.BlockHeader, buf *proto.Buffer) error {
	if err := buf.EncodeVarint(blockHeader.Number); err != nil {
		return errors.Wrapf(err, "error encoding the block number [%d]", blockHeader.Number)
	}
	if err := buf.EncodeRawBytes(blockHeader.DataHash); err != nil {
		return errors.Wrapf(err, "error encoding the data hash [%v]", blockHeader.DataHash)
	}
	if err := buf.EncodeRawBytes(blockHeader.PreviousHash); err != nil {
		return errors.Wrapf(err, "error encoding the previous hash [%v]", blockHeader.PreviousHash)
	}
	return nil
}

func addDataBytesAndConstructTxIndexInfo(blockData *common.BlockData, buf *proto.Buffer) ([]*txindexInfo, error) {
	var txOffsets []*txindexInfo

	if err := buf.EncodeVarint(uint64(len(blockData.Data))); err != nil {
		return nil, errors.Wrap(err, "error encoding the length of block data")
	}
	for _, txEnvelopeBytes := range blockData.Data {
		offset := len(buf.Bytes())
		txid, err := protoutil.GetOrComputeTxIDFromEnvelope(txEnvelopeBytes)
		if err != nil {
			logger.Warningf("error while extracting txid from tx envelope bytes during serialization of block. Ignoring this error as this is caused by a malformed transaction. Error:%s",
				err)
		}
		if err := buf.EncodeRawBytes(txEnvelopeBytes); err != nil {
			return nil, errors.Wrap(err, "error encoding the transaction envelope")
		}
		idxInfo := &txindexInfo{txID: txid, loc: &locPointer{offset, len(buf.Bytes()) - offset}}
		txOffsets = append(txOffsets, idxInfo)
	}
	return txOffsets, nil
}

func addMetadataBytes(blockMetadata *common.BlockMetadata, buf *proto.Buffer) error {
	numItems := uint64(0)
	if blockMetadata != nil {
		numItems = uint64(len(blockMetadata.Metadata))
	}
	if err := buf.EncodeVarint(numItems); err != nil {
		return errors.Wrap(err, "error encoding the length of metadata")
	}
	for _, b := range blockMetadata.Metadata {
		if err := buf.EncodeRawBytes(b); err != nil {
			return errors.Wrap(err, "error encoding the block metadata")
		}
	}
	return nil
}

// extractHeader 提取块头信息(区块号、数据哈希、上一个哈希)。
// 方法接收者：无（函数）
// 输入参数：
//   - buf：缓冲区。
//
// 返回值：
//   - *common.BlockHeader：提取到的块头信息。
//   - error：如果提取块头信息时出错，则返回错误。
func extractHeader(buf *buffer) (*common.BlockHeader, error) {
	// 创建一个common.BlockHeader实例(区块号、数据哈希、上一个哈希)
	header := &common.BlockHeader{}
	var err error
	// 解码块编号
	if header.Number, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "解码区块号时出错")
	}
	// 解码数据哈希
	if header.DataHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "解码数据哈希时出错")
	}
	// 解码前一个块的哈希
	if header.PreviousHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "解码上一个哈希时出错")
	}
	// 如果前一个块的哈希长度为0，则将其设置为nil
	if len(header.PreviousHash) == 0 {
		header.PreviousHash = nil
	}
	// 返回提取到的块头信息
	return header, nil
}

// extractData 提取块数据。
// 方法接收者：无（函数）
// 输入参数：
//   - buf：缓冲区。
//
// 返回值：
//   - *common.BlockData：提取到的块数据。
//   - []*txindexInfo：提取到的交易索引信息。
//   - error：如果提取块数据时出错，则返回错误。
func extractData(buf *buffer) (*common.BlockData, []*txindexInfo, error) {
	// 创建一个common.BlockData实例
	data := &common.BlockData{}
	var txOffsets []*txindexInfo
	var numItems uint64
	var err error

	// 解码块数据长度
	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, nil, errors.Wrap(err, "解码块数据长度时出错")
	}

	// 循环提取每个交易的数据
	for i := uint64(0); i < numItems; i++ {
		var txEnvBytes []byte
		var txid string
		// 返回基础 [] 字节中当前位置的偏移量
		txOffset := buf.GetBytesConsumed()
		if txEnvBytes, err = buf.DecodeRawBytes(false); err != nil {
			return nil, nil, errors.Wrap(err, "解码交易数据时出现了错误")
		}
		if txid, err = protoutil.GetOrComputeTxIDFromEnvelope(txEnvBytes); err != nil {
			logger.Warningf("在块反序列化期间从 tx 交易数据提取 txid 交易id时出错, 忽略此错误, 因为这是由格式不正确的事务引起的, 错误:%s",
				err)
		}
		data.Data = append(data.Data, txEnvBytes)
		idxInfo := &txindexInfo{txID: txid, loc: &locPointer{txOffset, buf.GetBytesConsumed() - txOffset}}
		txOffsets = append(txOffsets, idxInfo)
	}
	return data, txOffsets, nil
}

// extractMetadata 提取块元数据。
// 方法接收者：无（函数）
// 输入参数：
//   - buf：缓冲区。
//
// 返回值：
//   - *common.BlockMetadata：提取到的块元数据。
//   - error：如果提取块元数据时出错，则返回错误。
func extractMetadata(buf *buffer) (*common.BlockMetadata, error) {
	// 创建一个common.BlockMetadata实例
	metadata := &common.BlockMetadata{}
	var numItems uint64
	var metadataEntry []byte
	var err error
	// 解码块元数据的长度
	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "解码块元数据的长度时出错")
	}

	// 循环提取每个块元数据项
	for i := uint64(0); i < numItems; i++ {
		// 解码块元数据项
		if metadataEntry, err = buf.DecodeRawBytes(false); err != nil {
			return nil, errors.Wrap(err, "解码块元数据时出错")
		}
		// 将块元数据项添加到块元数据中
		metadata.Metadata = append(metadata.Metadata, metadataEntry)
	}
	// 返回提取到的块元数据
	return metadata, nil
}
