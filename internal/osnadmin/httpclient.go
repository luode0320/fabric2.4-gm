/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package osnadmin

import (
	"context"
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	gmx509 "github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"net"
	"net/http"
)

// 新建一个http请求的客户端，该客户端中包含了tls链接的配置
// 参数： ordererHost : orderer节点的host路径
//
//	gmCacertPool : 包含国密证书的证书池
//	gmTlsClientCert : 国密tls证书
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func httpClient(ordererHost string, gmCaCertPool *gmx509.CertPool, gmTlsClientCert gmtls.Certificate) (*http.Client, error) {
	conn, err := tlsConn(gmCaCertPool, gmTlsClientCert, ordererHost)
	if err != nil {
		fmt.Printf("newTlsConn error : %s\n", err.Error())
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return conn, nil
			},
		},
	}

	return client, nil
}

// 通过创建的http.client以及http.Request 做post/delete 请求
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func httpDo(req *http.Request, ordererHost string, caCertPool *gmx509.CertPool, tlsClientCert gmtls.Certificate) (*http.Response, error) {
	client, err := httpClient(ordererHost, caCertPool, tlsClientCert)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// 通过创建的http.client 对指定url做get请求
func httpGet(url, orderer string, caCertPool *gmx509.CertPool, tlsClientCert gmtls.Certificate) (*http.Response, error) {
	client, err := httpClient(orderer, caCertPool, tlsClientCert)
	if err != nil {
		return nil, err
	}
	return client.Get(url)
}

// 创建一个tls的链接，该链接中兼容了国密算法
//
// @Author: zhaoruobo
// @Date: 2023/11/15
func tlsConn(caCertPool *gmx509.CertPool, tlsClientCert gmtls.Certificate, ordererHost string) (*gmtls.Conn, error) {
	conn, err := gmtls.Dial("tcp",
		ordererHost,
		&gmtls.Config{
			GMSupport:                new(gmtls.GMSupport),
			RootCAs:                  caCertPool,
			Certificates:             []gmtls.Certificate{tlsClientCert},
			MinVersion:               gmtls.VersionGMSSL,
			MaxVersion:               gmtls.VersionTLS12,
			PreferServerCipherSuites: true,
			CipherSuites:             []uint16{gmtls.GMTLS_SM2_WITH_SM4_SM3, gmtls.GMTLS_ECDHE_SM2_WITH_SM4_SM3}, // 设置TLS密码套件
		})
	if err != nil {
		fmt.Println("无法建立TLS连接:", err)
		return conn, err
	}
	return conn, nil
}
