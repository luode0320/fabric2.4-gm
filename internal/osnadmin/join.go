/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package osnadmin

import (
	"bytes"
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	gmx509 "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"mime/multipart"
	"net/http"
)

// Join 将 orderer服务节点(OSN) 加入到一个新的或者已存在的通道
//
// @Author: zhaoruobo
// @Date: 2023/11/7
func Join(osnURL string, ordererHost string, blockBytes []byte, caCertPool *gmx509.CertPool, gmTlsClientCert gmtls.Certificate) (*http.Response, error) {
	url := fmt.Sprintf("%s/participation/v1/channels", osnURL)
	req, err := createJoinRequest(url, blockBytes)
	if err != nil {
		return nil, err
	}

	return httpDo(req, ordererHost, caCertPool, gmTlsClientCert)
}

// 创建一个将orderer节点加入通道的 http 请求
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func createJoinRequest(url string, blockBytes []byte) (*http.Request, error) {
	joinBody := new(bytes.Buffer)
	writer := multipart.NewWriter(joinBody)
	part, err := writer.CreateFormFile("config-block", "config.block")
	if err != nil {
		return nil, err
	}
	part.Write(blockBytes)
	err = writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, joinBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req, nil
}
