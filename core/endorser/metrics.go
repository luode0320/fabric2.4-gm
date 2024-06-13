/*
Copyright State Street Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import "github.com/hyperledger/fabric/common/metrics"

var (
	proposalDurationHistogramOpts = metrics.HistogramOpts{
		Namespace:    "endorser",
		Name:         "proposal_duration",
		Help:         "The time to complete a proposal.",
		LabelNames:   []string{"channel", "chaincode", "success"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}.%{success}",
	}

	receivedProposalsCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "proposals_received",
		Help:      "The number of proposals received.",
	}

	successfulProposalsCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "successful_proposals",
		Help:      "The number of successful proposals.",
	}

	proposalValidationFailureCounterOpts = metrics.CounterOpts{
		Namespace: "endorser",
		Name:      "proposal_validation_failures",
		Help:      "The number of proposals that have failed initial validation.",
	}

	proposalChannelACLFailureOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "proposal_acl_failures",
		Help:         "The number of proposals that failed ACL checks.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	initFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "chaincode_instantiation_failures",
		Help:         "The number of chaincode instantiations or upgrade that have failed.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	endorsementFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "endorsement_failures",
		Help:         "The number of failed endorsements.",
		LabelNames:   []string{"channel", "chaincode", "chaincodeerror"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}.%{chaincodeerror}",
	}

	duplicateTxsFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "duplicate_transaction_failures",
		Help:         "The number of failed proposals due to duplicate transaction ID.",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}

	simulationFailureCounterOpts = metrics.CounterOpts{
		Namespace:    "endorser",
		Name:         "proposal_simulation_failures",
		Help:         "The number of failed proposal simulations",
		LabelNames:   []string{"channel", "chaincode"},
		StatsdFormat: "%{#fqname}.%{channel}.%{chaincode}",
	}
)

// Metrics 策略
type Metrics struct {
	ProposalDuration         metrics.Histogram // 提案持续时间的直方图
	ProposalsReceived        metrics.Counter   // 收到的提案计数器
	SuccessfulProposals      metrics.Counter   // 成功的提案计数器
	ProposalValidationFailed metrics.Counter   // 提案验证失败的计数器
	ProposalACLCheckFailed   metrics.Counter   // 提案 ACL 检查失败的计数器
	InitFailed               metrics.Counter   // 初始化失败的计数器
	EndorsementsFailed       metrics.Counter   // 背书失败的计数器
	DuplicateTxsFailure      metrics.Counter   // 重复交易失败的计数器
	SimulationFailure        metrics.Counter   // 模拟失败的计数器
}

func NewMetrics(p metrics.Provider) *Metrics {
	return &Metrics{
		ProposalDuration:         p.NewHistogram(proposalDurationHistogramOpts),
		ProposalsReceived:        p.NewCounter(receivedProposalsCounterOpts),
		SuccessfulProposals:      p.NewCounter(successfulProposalsCounterOpts),
		ProposalValidationFailed: p.NewCounter(proposalValidationFailureCounterOpts),
		ProposalACLCheckFailed:   p.NewCounter(proposalChannelACLFailureOpts),
		InitFailed:               p.NewCounter(initFailureCounterOpts),
		EndorsementsFailed:       p.NewCounter(endorsementFailureCounterOpts),
		DuplicateTxsFailure:      p.NewCounter(duplicateTxsFailureCounterOpts),
		SimulationFailure:        p.NewCounter(simulationFailureCounterOpts),
	}
}
