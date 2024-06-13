/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package osnadmin

import (
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	gmx509 "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"net/http"
)

// ListAllChannels 列出OSN所属的通道。
//
// @Author: zhaoruobo
// @Date: 2023/11/7
func ListAllChannels(osnURL string, orderer string, gmCaCertPool *gmx509.CertPool, gmTlsClientCert gmtls.Certificate) (*http.Response, error) {
	url := fmt.Sprintf("%s/participation/v1/channels", osnURL)

	return httpGet(url, orderer, gmCaCertPool, gmTlsClientCert)
}

// ListSingleChannel 列出OSN所属的单个通道。
//
// @Author: zhaoruobo
// @Date: 2023/11/7
func ListSingleChannel(osnURL, orderer string, channelID string, gmCaCertPool *gmx509.CertPool, gmTlsClientCert gmtls.Certificate) (*http.Response, error) {
	url := fmt.Sprintf("%s/participation/v1/channels/%s", osnURL, channelID)

	return httpGet(url, orderer, gmCaCertPool, gmTlsClientCert)
}
