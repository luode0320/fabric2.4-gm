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

// 从一个存在的通道中移除指定的 orderer服务节点(OSN)
func Remove(osnURL, channelID, ordererHost string, caCertPool *gmx509.CertPool, gmTlsClientCert gmtls.Certificate) (*http.Response, error) {
	url := fmt.Sprintf("%s/participation/v1/channels/%s", osnURL, channelID)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	return httpDo(req, ordererHost, caCertPool, gmTlsClientCert)
}
