/*
# Copyright State Street Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
*/
package ccprovider

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"strings"
)

// The targz metadata provider is reference for other providers (such as what CAR would
// implement). Currently it treats only statedb metadata but will be generalized in future
// to allow for arbitrary metadata to be packaged with the chaincode.
const (
	ccPackageStatedbDir = "META-INF/statedb/"
)

type PersistenceAdapter func([]byte) ([]byte, error)

func (pa PersistenceAdapter) GetDBArtifacts(codePackage []byte) ([]byte, error) {
	return pa(codePackage)
}

// MetadataAsTarEntries 函数从链码包中提取元数据。
//
// 参数：
//   - code: 字节数组，表示链码包的内容。
//
// 返回值：
//   - []byte: 表示元数据的字节数组。
//   - error: 如果提取过程中发生错误，则返回非nil的错误对象；否则返回nil。
func MetadataAsTarEntries(code []byte) ([]byte, error) {
	is := bytes.NewReader(code)
	// 创建gzip读取器
	gr, err := gzip.NewReader(is)
	if err != nil {
		ccproviderLogger.Errorf("打开链码包 gzip 流失败: %s", err)
		return nil, err
	}

	statedbTarBuffer := bytes.NewBuffer(nil)
	// 创建tar写入器
	tw := tar.NewWriter(statedbTarBuffer)
	// 创建tar读取器
	tr := tar.NewReader(gr)

	// 对于链码包中的每个文件，如果路径中包含 "statedb"，则将其添加到statedb artifact tar中
	for {
		header, err := tr.Next() // 获取下一个文件头
		if err == io.EOF {
			// 只有当没有更多的条目可扫描时才会到达这里
			break
		}
		if err != nil {
			return nil, err
		}

		if !strings.HasPrefix(header.Name, ccPackageStatedbDir) {
			continue
		}

		if err = tw.WriteHeader(header); err != nil {
			ccproviderLogger.Error("将标头添加到时出错 statedb tar:", err, header.Name)
			return nil, err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			ccproviderLogger.Error("将文件复制到 statedb tar:", err, header.Name)
			return nil, err
		}
		ccproviderLogger.Debug("已将文件写入 statedb tar:", header.Name)
	}

	if err = tw.Close(); err != nil {
		return nil, err
	}

	ccproviderLogger.Debug("创建的元数据 tar")

	return statedbTarBuffer.Bytes(), nil
}
