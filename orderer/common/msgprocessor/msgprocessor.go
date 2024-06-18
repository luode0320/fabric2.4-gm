/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package msgprocessor provides the implementations for processing of the assorted message
// types which may arrive in the system through Broadcast.
package msgprocessor

import (
	"errors"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
)

const (
	// These should eventually be derived from the channel support once enabled
	msgVersion = int32(0)
	epoch      = 0
)

var logger = flogging.MustGetLogger("orderer.common.msgprocessor")

// ErrChannelDoesNotExist is returned by the system channel for transactions which
// are not for the system channel ID and are not attempting to create a new channel
var ErrChannelDoesNotExist = errors.New("channel does not exist")

// ErrPermissionDenied is returned by errors which are caused by transactions
// which are not permitted due to an authorization failure.
var ErrPermissionDenied = errors.New("permission denied")

// ErrMaintenanceMode is returned when transactions are rejected because the orderer is in "maintenance mode",
// as defined by ConsensusType.State != NORMAL. This typically happens during consensus-type migration.
var ErrMaintenanceMode = errors.New("maintenance mode")

// Classification represents the possible message types for the system.
type Classification int

const (
	// NormalMsg 是标准消息（背书或其他非配置类型）的类别。
	// 此类别的消息应由ProcessNormalMsg方法处理。
	NormalMsg Classification = iota

	// ConfigUpdateMsg 表示类型为CONFIG_UPDATE的消息。
	// 此类别的消息应由ProcessConfigUpdateMsg方法处理。
	ConfigUpdateMsg

	// ConfigMsg 表示类型为ORDERER_TRANSACTION或CONFIG的消息。
	// 此类别的消息应由ProcessConfigMsg方法处理。
	ConfigMsg
)

// Processor 提供了一组方法，这些方法对于通过Broadcast接口接收的任何消息进行分类和处理都是必需的。
type Processor interface {

	// ClassifyMsg 检查消息头以确定需要进行哪种类型的处理
	ClassifyMsg(chdr *cb.ChannelHeader) Classification

	// ProcessNormalMsg 将根据当前配置检查消息的有效性。成功时返回当前配置的序列号和nil，如果消息无效则返回错误。
	ProcessNormalMsg(env *cb.Envelope) (configSeq uint64, err error)

	// ProcessConfigUpdateMsg 尝试将配置更新应用到当前配置中，如果成功则返回生成的配置消息及用于计算此配置的配置序列号。
	// 如果配置更新消息无效，则返回错误。
	ProcessConfigUpdateMsg(env *cb.Envelope) (config *cb.Envelope, configSeq uint64, err error)

	// ProcessConfigMsg 接收类型为`ORDERER_TX`或`CONFIG`的消息，解包其中嵌入的ConfigUpdate信封，
	// 然后调用`ProcessConfigUpdateMsg`以生成与原始消息相同类型的新配置消息。
	// 当原先的消息被认为不再有效时，使用此方法重新验证并重新生成配置消息。
	ProcessConfigMsg(env *cb.Envelope) (*cb.Envelope, uint64, error)
}
