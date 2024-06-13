/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/hyperledger/fabric-protos-go/discovery"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/discovery/protoext"
	common2 "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("discovery")

var accessDenied = wrapError(errors.New("access denied"))

// certHashExtractor extracts the TLS certificate from a given context
// and returns its hash
type certHashExtractor func(ctx context.Context) []byte

// dispatcher defines a function that dispatches a query
type dispatcher func(q *discovery.Query) *discovery.QueryResult

type Service struct {
	config             Config
	channelDispatchers map[protoext.QueryType]dispatcher
	localDispatchers   map[protoext.QueryType]dispatcher
	auth               *authCache
	Support
}

// Config 定义了发现服务的配置
type Config struct {
	TLS                          bool    // 是否启用 TLS
	AuthCacheEnabled             bool    // 是否启用身份验证缓存
	AuthCacheMaxSize             int     // 身份验证缓存的最大大小
	AuthCachePurgeRetentionRatio float64 // 身份验证缓存清理保留比例
}

// String 返回此配置的字符串表示形式
func (c Config) String() string {
	if c.AuthCacheEnabled {
		return fmt.Sprintf("发现服务的配置 -> 是否启用TLS: %t, 身份验证缓存的最大大小: %d, 身份验证缓存清理保留比例: %f", c.TLS, c.AuthCacheMaxSize, c.AuthCachePurgeRetentionRatio)
	}
	return fmt.Sprintf("TLS: %t, 身份验证缓存发现", c.TLS)
}

// peerMapping maps PKI-IDs to Peers
type peerMapping map[string]*discovery.Peer

// NewService 创建一个新的 Discovery Service 实例。
// 方法接收者：
//   - 无
//
// 输入参数：
//   - config：Config，表示配置信息
//   - sup：Support，表示支持实例
//
// 返回值：
//   - *Service：表示 Service 实例
func NewService(config Config, sup Support) *Service {
	// 创建一个新的 Service 实例
	s := &Service{
		auth: newAuthCache(sup, authCacheConfig{
			enabled:             config.AuthCacheEnabled,
			maxCacheSize:        config.AuthCacheMaxSize,
			purgeRetentionRatio: config.AuthCachePurgeRetentionRatio,
		}),
		Support: sup,
	}

	// 初始化 channelDispatchers 和 localDispatchers
	s.channelDispatchers = map[protoext.QueryType]dispatcher{
		protoext.ConfigQueryType:         s.configQuery,
		protoext.ChaincodeQueryType:      s.chaincodeQuery,
		protoext.PeerMembershipQueryType: s.channelMembershipResponse,
	}
	s.localDispatchers = map[protoext.QueryType]dispatcher{
		protoext.LocalMembershipQueryType: s.localMembershipResponse,
	}

	// 输出日志信息
	logger.Info("使用config创建", config)

	return s
}

func (s *Service) Discover(ctx context.Context, request *discovery.SignedRequest) (*discovery.Response, error) {
	addr := util.ExtractRemoteAddress(ctx)
	req, err := validateStructure(ctx, request, s.config.TLS, util.ExtractCertificateHashFromContext)
	if err != nil {
		logger.Warningf("Request from %s is malformed or invalid: %v", addr, err)
		return nil, err
	}
	logger.Debugf("Processing request from %s: %v", addr, req)
	var res []*discovery.QueryResult
	for _, q := range req.Queries {
		res = append(res, s.processQuery(q, request, req.Authentication.ClientIdentity, addr))
	}
	logger.Debugf("Returning to %s a response containing: %v", addr, res)
	return &discovery.Response{
		Results: res,
	}, nil
}

func (s *Service) processQuery(query *discovery.Query, request *discovery.SignedRequest, identity []byte, addr string) *discovery.QueryResult {
	if query.Channel != "" && !s.ChannelExists(query.Channel) {
		logger.Warning("got query for channel", query.Channel, "from", addr, "but it doesn't exist")
		return accessDenied
	}
	if err := s.auth.EligibleForService(query.Channel, protoutil.SignedData{
		Data:      request.Payload,
		Signature: request.Signature,
		Identity:  identity,
	}); err != nil {
		logger.Warning("got query for channel", query.Channel, "from", addr, "but it isn't eligible:", err)
		return accessDenied
	}
	return s.dispatch(query)
}

func (s *Service) dispatch(q *discovery.Query) *discovery.QueryResult {
	dispatchers := s.channelDispatchers
	// Ensure local queries are routed only to channel-less dispatchers
	if q.Channel == "" {
		dispatchers = s.localDispatchers
	}
	dispatchQuery, exists := dispatchers[protoext.GetQueryType(q)]
	if !exists {
		return wrapError(errors.New("unknown or missing request type"))
	}
	return dispatchQuery(q)
}

func (s *Service) chaincodeQuery(q *discovery.Query) *discovery.QueryResult {
	if err := validateCCQuery(q.GetCcQuery()); err != nil {
		return wrapError(err)
	}
	var descriptors []*discovery.EndorsementDescriptor
	for _, interest := range q.GetCcQuery().Interests {
		desc, err := s.PeersForEndorsement(common2.ChannelID(q.Channel), interest)
		if err != nil {
			logger.Errorf("Failed constructing descriptor for chaincode %s: %v", interest, err)
			return wrapError(errors.Errorf("failed constructing descriptor for %v", interest))
		}
		descriptors = append(descriptors, desc)
	}

	return &discovery.QueryResult{
		Result: &discovery.QueryResult_CcQueryRes{
			CcQueryRes: &discovery.ChaincodeQueryResult{
				Content: descriptors,
			},
		},
	}
}

func (s *Service) configQuery(q *discovery.Query) *discovery.QueryResult {
	conf, err := s.Config(q.Channel)
	if err != nil {
		logger.Errorf("Failed fetching config for channel %s: %v", q.Channel, err)
		return wrapError(errors.Errorf("failed fetching config for channel %s", q.Channel))
	}
	return &discovery.QueryResult{
		Result: &discovery.QueryResult_ConfigResult{
			ConfigResult: conf,
		},
	}
}

func wrapPeerResponse(peersByOrg map[string]*discovery.Peers) *discovery.QueryResult {
	return &discovery.QueryResult{
		Result: &discovery.QueryResult_Members{
			Members: &discovery.PeerMembershipResult{
				PeersByOrg: peersByOrg,
			},
		},
	}
}

func (s *Service) channelMembershipResponse(q *discovery.Query) *discovery.QueryResult {
	chanPeers, err := s.PeersAuthorizedByCriteria(common2.ChannelID(q.Channel), q.GetPeerQuery().Filter)
	if err != nil {
		return wrapError(err)
	}
	membersByOrgs := make(map[string]*discovery.Peers)
	chanPeerByID := chanPeers.ByID()
	for org, ids2Peers := range s.computeMembership(q) {
		membersByOrgs[org] = &discovery.Peers{}
		for id, peer := range ids2Peers {
			// Check if the peer is in the channel view
			stateInfoMsg, exists := chanPeerByID[string(id)]
			// If the peer isn't in the channel view, skip it and don't include it in the response
			if !exists {
				continue
			}
			peer.StateInfo = stateInfoMsg.Envelope
			membersByOrgs[org].Peers = append(membersByOrgs[org].Peers, peer)
		}
	}
	return wrapPeerResponse(membersByOrgs)
}

func (s *Service) localMembershipResponse(q *discovery.Query) *discovery.QueryResult {
	membersByOrgs := make(map[string]*discovery.Peers)
	for org, ids2Peers := range s.computeMembership(q) {
		membersByOrgs[org] = &discovery.Peers{}
		for _, peer := range ids2Peers {
			membersByOrgs[org].Peers = append(membersByOrgs[org].Peers, peer)
		}
	}
	return wrapPeerResponse(membersByOrgs)
}

func (s *Service) computeMembership(_ *discovery.Query) map[string]peerMapping {
	peersByOrg := make(map[string]peerMapping)
	peerAliveInfo := s.Peers().ByID()
	for org, peerIdentities := range s.IdentityInfo().ByOrg() {
		peersForCurrentOrg := make(peerMapping)
		peersByOrg[org] = peersForCurrentOrg
		for _, id := range peerIdentities {
			// Check peer exists in alive membership view
			aliveInfo, exists := peerAliveInfo[string(id.PKIId)]
			if !exists {
				continue
			}
			peersForCurrentOrg[string(id.PKIId)] = &discovery.Peer{
				Identity:       id.Identity,
				MembershipInfo: aliveInfo.Envelope,
			}
		}
	}
	return peersByOrg
}

// validateStructure validates that the request contains all the needed fields and that they are computed correctly
func validateStructure(ctx context.Context, request *discovery.SignedRequest, tlsEnabled bool, certHashFromContext certHashExtractor) (*discovery.Request, error) {
	if request == nil {
		return nil, errors.New("nil request")
	}
	req, err := protoext.SignedRequestToRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "failed parsing request")
	}
	if req.Authentication == nil {
		return nil, errors.New("access denied, no authentication info in request")
	}
	if len(req.Authentication.ClientIdentity) == 0 {
		return nil, errors.New("access denied, client identity wasn't supplied")
	}
	if !tlsEnabled {
		return req, nil
	}
	computedHash := certHashFromContext(ctx)
	if len(computedHash) == 0 {
		return nil, errors.New("client didn't send a TLS certificate")
	}
	if !bytes.Equal(computedHash, req.Authentication.ClientTlsCertHash) {
		claimed := hex.EncodeToString(req.Authentication.ClientTlsCertHash)
		logger.Warningf("client claimed TLS hash %s doesn't match computed TLS hash from gRPC stream %s", claimed, hex.EncodeToString(computedHash))
		return nil, errors.New("client claimed TLS hash doesn't match computed TLS hash from gRPC stream")
	}
	return req, nil
}

func validateCCQuery(ccQuery *discovery.ChaincodeQuery) error {
	if len(ccQuery.Interests) == 0 {
		return errors.New("chaincode query must have at least one chaincode interest")
	}
	for _, interest := range ccQuery.Interests {
		if interest == nil {
			return errors.New("chaincode interest is nil")
		}
		if len(interest.Chaincodes) == 0 {
			return errors.New("chaincode interest must contain at least one chaincode")
		}
		for _, cc := range interest.Chaincodes {
			if cc.Name == "" {
				return errors.New("chaincode name in interest cannot be empty")
			}
		}
	}
	return nil
}

func wrapError(err error) *discovery.QueryResult {
	return &discovery.QueryResult{
		Result: &discovery.QueryResult_Error{
			Error: &discovery.Error{
				Content: err.Error(),
			},
		},
	}
}
