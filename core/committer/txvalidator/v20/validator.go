/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	mspprotos "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	commonerrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/core/committer/txvalidator/plugin"
	"github.com/hyperledger/fabric/core/committer/txvalidator/v20/plugindispatcher"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// Semaphore provides to the validator means for synchronisation
type Semaphore interface {
	// Acquire implements semaphore-like acquire semantics
	Acquire(ctx context.Context) error

	// Release implements semaphore-like release semantics
	Release()
}

// ChannelResources provides access to channel artefacts or
// functions to interact with them
type ChannelResources interface {
	// MSPManager returns the MSP manager for this channel
	MSPManager() msp.MSPManager

	// Apply attempts to apply a configtx to become the new config
	Apply(configtx *common.ConfigEnvelope) error

	// GetMSPIDs returns the IDs for the application MSPs
	// that have been defined in the channel
	GetMSPIDs() []string

	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

// LedgerResources provides access to ledger artefacts or
// functions to interact with them
type LedgerResources interface {
	// TxIDExists returns true if the specified txID is already present in one of the already committed blocks
	TxIDExists(txID string) (bool, error)

	// NewQueryExecutor gives handle to a query executor.
	// A client can obtain more than one 'QueryExecutor's for parallel execution.
	// Any synchronization should be performed at the implementation level if required
	NewQueryExecutor() (ledger.QueryExecutor, error)
}

// Dispatcher is an interface to decouple tx validator
// and plugin dispatcher
type Dispatcher interface {
	// Dispatch invokes the appropriate validation plugin for the supplied transaction in the block
	Dispatch(seq int, payload *common.Payload, envBytes []byte, block *common.Block) (peer.TxValidationCode, error)
}

//go:generate mockery -dir . -name ChannelResources -case underscore -output mocks/
//go:generate mockery -dir . -name LedgerResources -case underscore -output mocks/
//go:generate mockery -dir . -name Dispatcher -case underscore -output mocks/

//go:generate mockery -dir . -name QueryExecutor -case underscore -output mocks/

// QueryExecutor is the local interface that used to generate mocks for foreign interface.
type QueryExecutor interface {
	ledger.QueryExecutor
}

//go:generate mockery -dir . -name ChannelPolicyManagerGetter -case underscore -output mocks/

// ChannelPolicyManagerGetter is the local interface that used to generate mocks for foreign interface.
type ChannelPolicyManagerGetter interface {
	policies.ChannelPolicyManagerGetter
}

//go:generate mockery -dir . -name PolicyManager -case underscore -output mocks/

type PolicyManager interface {
	policies.Manager
}

//go:generate mockery -dir plugindispatcher/ -name CollectionResources -case underscore -output mocks/

// TxValidator is the implementation of Validator interface, keeps
// reference to the ledger to enable tx simulation
// and execution of plugins
type TxValidator struct {
	ChannelID        string
	Semaphore        Semaphore
	ChannelResources ChannelResources
	LedgerResources  LedgerResources
	Dispatcher       Dispatcher
	CryptoProvider   bccsp.BCCSP
}

var logger = flogging.MustGetLogger("committer.txvalidator")

type blockValidationRequest struct {
	block *common.Block
	d     []byte
	tIdx  int
}

type blockValidationResult struct {
	tIdx           int
	validationCode peer.TxValidationCode
	err            error
	txid           string
}

// NewTxValidator creates new transactions validator
func NewTxValidator(
	channelID string,
	sem Semaphore,
	cr ChannelResources,
	ler LedgerResources,
	lcr plugindispatcher.LifecycleResources,
	cor plugindispatcher.CollectionResources,
	pm plugin.Mapper,
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter,
	cryptoProvider bccsp.BCCSP,
) *TxValidator {
	// Encapsulates interface implementation
	pluginValidator := plugindispatcher.NewPluginValidator(pm, ler, &dynamicDeserializer{cr: cr}, &dynamicCapabilities{cr: cr}, channelPolicyManagerGetter, cor)
	return &TxValidator{
		ChannelID:        channelID,
		Semaphore:        sem,
		ChannelResources: cr,
		LedgerResources:  ler,
		Dispatcher:       plugindispatcher.New(channelID, cr, ler, lcr, pluginValidator),
		CryptoProvider:   cryptoProvider,
	}
}

func (v *TxValidator) chainExists(chain string) bool {
	// TODO: implement this function!
	return true
}

// Validate 方法执行区块的验证工作。每个区块中的交易将并行地进行验证。
// 验证流程如下：提交者线程在协程中启动交易验证函数（使用信号量限制并发验证协程的数量）。
// 提交者线程随后从结果通道读取验证结果（按照协程完成的顺序）。协程负责验证区块中的交易，
// 并将验证结果推送到结果通道。值得注意的几点：
//  1. 为了保持方法简单，提交者线程会将区块中的所有交易加入验证队列，然后再读取结果。
//  2. 并行验证能够正常工作的重要前提是，验证函数不应改变系统的状态。
//     否则，验证的顺序将变得重要，我们不得不退回到顺序验证（或使用某种锁定机制）。
//     目前这一假设成立，因为唯一影响状态的功能是在接收到配置交易时，但这类交易在区块中是单独存在的。
//     如果/当这一假设被打破，代码必须相应地调整。
func (v *TxValidator) Validate(block *common.Block) error {
	var err error
	var errPos int

	startValidation := time.Now() // 记录验证区块开始的时间点
	logger.Debugf("[%s] 开始验证区块 [%d]", v.ChannelID, block.Header.Number)

	// 初始化交易标记为有效，在下面的验证过程中如果交易无效将设置其无效原因代码
	txsfltr := txflags.New(len(block.Data.Data))
	// 创建一个交易ID数组
	txidArray := make([]string, len(block.Data.Data))

	// 创建结果通道，用于接收验证结果
	results := make(chan *blockValidationResult)
	go func() {
		for tIdx, d := range block.Data.Data {
			// 确保并发验证工作者不超过设定的限制
			v.Semaphore.Acquire(context.Background())

			// 在一个新的协程中验证交易
			go func(index int, data []byte) {
				defer v.Semaphore.Release()

				v.validateTx(&blockValidationRequest{
					d:     data,
					block: block,
					tIdx:  index,
				}, results)
			}(tIdx, d)
		}
	}()

	// 日志记录期待的验证响应数量
	logger.Debugf("期待 %d 条块验证响应", len(block.Data.Data))

	// 以协程完成的顺序读取响应
	for i := 0; i < len(block.Data.Data); i++ {
		res := <-results

		if res.err != nil {
			// 如果有错误，缓冲错误值，等待所有工作者完成验证，
			// 然后返回第一个在区块中返回错误的交易产生的错误
			logger.Debugf("对于索引 %d 的交易，收到终止错误 %s", res.tIdx, res.err)

			if err == nil || res.tIdx < errPos {
				err = res.err
				errPos = res.tIdx
			}
		} else {
			// 如果没有错误，设置交易标记并更新交易链码名和升级链码映射
			logger.Debugf("对于索引 %d 的交易，收到验证结果，代码 %d", res.tIdx, res.validationCode)

			txsfltr.SetFlag(res.tIdx, res.validationCode)

			if res.validationCode == peer.TxValidationCode_VALID {
				// 将有效交易的交易ID存入数组
				txidArray[res.tIdx] = res.txid
			}
		}
	}

	// 当所有工作者完成验证后，如果存在错误，
	// 返回区块中第一个返回错误的交易产生的错误
	if err != nil {
		return err
	}

	// 标记交易ID重复的交易为无效
	markTXIdDuplicates(txidArray, txsfltr)

	// 确保所有交易都已完成验证
	err = v.allValidated(txsfltr, block)
	if err != nil {
		return err
	}

	// 初始化元数据结构
	protoutil.InitBlockMetadata(block)

	// 设置交易过滤器到元数据中
	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsfltr

	// 计算验证耗时并日志记录
	elapsedValidation := time.Since(startValidation) / time.Millisecond // duration in ms
	logger.Debugf("[%s] 验证区块 [%d] 耗时 %d ms", v.ChannelID, block.Header.Number, elapsedValidation)

	return nil
}

// allValidated returns error if some of the validation flags have not been set
// during validation
func (v *TxValidator) allValidated(txsfltr txflags.ValidationFlags, block *common.Block) error {
	for id, f := range txsfltr {
		if peer.TxValidationCode(f) == peer.TxValidationCode_NOT_VALIDATED {
			return errors.Errorf("transaction %d in block %d has skipped validation", id, block.Header.Number)
		}
	}

	return nil
}

func markTXIdDuplicates(txids []string, txsfltr txflags.ValidationFlags) {
	txidMap := make(map[string]struct{})

	for id, txid := range txids {
		if txid == "" {
			continue
		}

		_, in := txidMap[txid]
		if in {
			logger.Error("Duplicate txid", txid, "found, skipping")
			txsfltr.SetFlag(id, peer.TxValidationCode_DUPLICATE_TXID)
		} else {
			txidMap[txid] = struct{}{}
		}
	}
}

func (v *TxValidator) validateTx(req *blockValidationRequest, results chan<- *blockValidationResult) {
	block := req.block
	d := req.d
	tIdx := req.tIdx
	txID := ""

	if d == nil {
		results <- &blockValidationResult{
			tIdx: tIdx,
		}
		return
	}

	if env, err := protoutil.GetEnvelopeFromBlock(d); err != nil {
		logger.Warningf("Error getting tx from block: %+v", err)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
		}
		return
	} else if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for the validation plugins
		logger.Debugf("[%s] validateTx starts for block %p env %p txn %d", v.ChannelID, block, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %p env %p txn %d", v.ChannelID, block, env, tIdx)
		var payload *common.Payload
		var err error
		var txResult peer.TxValidationCode

		if payload, txResult = validation.ValidateTransaction(env, v.CryptoProvider); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			}
			return
		}

		channel := chdr.ChannelId
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			txID = chdr.TxId

			// Check duplicate transactions
			erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.LedgerResources)
			if erroneousResultEntry != nil {
				results <- erroneousResultEntry
				return
			}

			// Validate tx with plugins
			logger.Debug("Validating transaction with plugins")
			cde, err := v.Dispatcher.Dispatch(tIdx, payload, d, block)
			if err != nil {
				logger.Errorf("Dispatch for transaction txId = %s returned error: %s", txID, err)
				switch err.(type) {
				case *commonerrors.VSCCExecutionFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				case *commonerrors.VSCCInfoLookupFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				default:
					results <- &blockValidationResult{
						tIdx:           tIdx,
						validationCode: cde,
					}
					return
				}
			}
		} else if common.HeaderType(chdr.Type) == common.HeaderType_CONFIG {
			configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
			if err != nil {
				err = errors.WithMessage(err, "error unmarshalling config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			if err := v.ChannelResources.Apply(configEnvelope); err != nil {
				err = errors.WithMessage(err, "error validating config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}
			logger.Debugf("config transaction received for chain %s", channel)
		} else {
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(chdr.Type), block.Header.Number, tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_UNKNOWN_TX_TYPE,
			}
			return
		}

		if _, err := proto.Marshal(env); err != nil {
			logger.Warningf("Cannot marshal transaction: %s", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_MARSHAL_TX_ERROR,
			}
			return
		}
		// Succeeded to pass down here, transaction is valid
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_VALID,
			txid:           txID,
		}
		return
	} else {
		logger.Warning("Nil tx from block")
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_NIL_ENVELOPE,
		}
		return
	}
}

// CheckTxIdDupsLedger returns a vlockValidationResult enhanced with the respective
// error codes if and only if there is transaction with the same transaction identifier
// in the ledger or no decision can be made for whether such transaction exists;
// the function returns nil if it has ensured that there is no such duplicate, such
// that its consumer can proceed with the transaction processing
func (v *TxValidator) checkTxIdDupsLedger(tIdx int, chdr *common.ChannelHeader, ldgr LedgerResources) *blockValidationResult {
	// Retrieve the transaction identifier of the input header
	txID := chdr.TxId

	// Look for a transaction with the same identifier inside the ledger
	exists, err := ldgr.TxIDExists(txID)
	if err != nil {
		logger.Errorf("Ledger failure while attempting to detect duplicate status for txid %s: %s", txID, err)
		return &blockValidationResult{
			tIdx: tIdx,
			err:  err,
		}
	}
	if exists {
		logger.Error("Duplicate transaction found, ", txID, ", skipping")
		return &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_DUPLICATE_TXID,
		}
	}
	return nil
}

type dynamicDeserializer struct {
	cr ChannelResources
}

func (ds *dynamicDeserializer) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	return ds.cr.MSPManager().DeserializeIdentity(serializedIdentity)
}

func (ds *dynamicDeserializer) IsWellFormed(identity *mspprotos.SerializedIdentity) error {
	return ds.cr.MSPManager().IsWellFormed(identity)
}

type dynamicCapabilities struct {
	cr ChannelResources
}

func (ds *dynamicCapabilities) ACLs() bool {
	return ds.cr.Capabilities().ACLs()
}

func (ds *dynamicCapabilities) CollectionUpgrade() bool {
	return ds.cr.Capabilities().CollectionUpgrade()
}

func (ds *dynamicCapabilities) ForbidDuplicateTXIdInBlock() bool {
	return ds.cr.Capabilities().ForbidDuplicateTXIdInBlock()
}

func (ds *dynamicCapabilities) KeyLevelEndorsement() bool {
	return ds.cr.Capabilities().KeyLevelEndorsement()
}

func (ds *dynamicCapabilities) MetadataLifecycle() bool {
	// This capability no longer exists and should not be referenced in validation anyway
	return false
}

func (ds *dynamicCapabilities) PrivateChannelData() bool {
	return ds.cr.Capabilities().PrivateChannelData()
}

func (ds *dynamicCapabilities) StorePvtDataOfInvalidTx() bool {
	return ds.cr.Capabilities().StorePvtDataOfInvalidTx()
}

func (ds *dynamicCapabilities) Supported() error {
	return ds.cr.Capabilities().Supported()
}

func (ds *dynamicCapabilities) V1_1Validation() bool {
	return ds.cr.Capabilities().V1_1Validation()
}

func (ds *dynamicCapabilities) V1_2Validation() bool {
	return ds.cr.Capabilities().V1_2Validation()
}

func (ds *dynamicCapabilities) V1_3Validation() bool {
	return ds.cr.Capabilities().V1_3Validation()
}

func (ds *dynamicCapabilities) V2_0Validation() bool {
	return ds.cr.Capabilities().V2_0Validation()
}
