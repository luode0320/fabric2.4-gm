/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package persistence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	pb "github.com/hyperledger/fabric-protos-go/peer"

	"github.com/pkg/errors"
)

// The chaincode package is simply a .tar.gz file.  For the time being, we
// assume that the package contains a metadata.json file which contains a
// 'type', a 'path', and a 'label'.  In the future, it would be nice if we
// move to a more buildpack type system, rather than the below presented
// JAR+manifest type system, but for expediency and incremental changes,
// moving to a tar format over the proto format for a user-inspectable
// artifact seems like a good step.

const (
	// MetadataFile is the expected location of the metadata json document
	// in the top level of the chaincode package.
	MetadataFile = "metadata.json"

	// CodePackageFile is the expected location of the code package in the
	// top level of the chaincode package
	CodePackageFile = "code.tar.gz"
)

//go:generate counterfeiter -o mock/legacy_cc_package_locator.go --fake-name LegacyCCPackageLocator . LegacyCCPackageLocator

type LegacyCCPackageLocator interface {
	GetChaincodeDepSpec(nameVersion string) (*pb.ChaincodeDeploymentSpec, error)
}

// FallbackPackageLocator 是一个回退的链码包定位器。
type FallbackPackageLocator struct {
	ChaincodePackageLocator *ChaincodePackageLocator // 链码包定位器
	LegacyCCPackageLocator  LegacyCCPackageLocator   // 旧版链码包定位器
}

func (fpl *FallbackPackageLocator) GetChaincodePackage(packageID string) (*ChaincodePackageMetadata, []byte, io.ReadCloser, error) {
	// XXX，此路径的返回参数太多。我们可以把它分成两个电话，
	// 或者，我们可以在需要的地方反序列化元数据。但是，正如所写的是
	// 修复元数据突变错误的最快路径。
	streamer := fpl.ChaincodePackageLocator.ChaincodePackageStreamer(packageID)
	if streamer.Exists() {
		metadata, err := streamer.Metadata()
		if err != nil {
			return nil, nil, nil, errors.WithMessagef(err, "检索链码包元数据时出错 '%s'", packageID)
		}

		mdBytes, err := streamer.MetadataBytes()
		if err != nil {
			return nil, nil, nil, errors.WithMessagef(err, "检索链码包元数据字节时出错 '%s'", packageID)
		}

		tarStream, err := streamer.Code()
		if err != nil {
			return nil, nil, nil, errors.WithMessagef(err, "检索链码包代码时出错 '%s'", packageID)
		}

		return metadata, mdBytes, tarStream, nil
	}

	cds, err := fpl.LegacyCCPackageLocator.GetChaincodeDepSpec(string(packageID))
	if err != nil {
		return nil, nil, nil, errors.WithMessagef(err, "无法获取旧版链代码包 '%s'", packageID)
	}

	md := &ChaincodePackageMetadata{
		Path:  cds.ChaincodeSpec.ChaincodeId.Path,
		Type:  cds.ChaincodeSpec.Type.String(),
		Label: cds.ChaincodeSpec.ChaincodeId.Name,
	}

	mdBytes, err := json.Marshal(md)
	if err != nil {
		return nil, nil, nil, errors.WithMessagef(err, "无法序列化链码包的元数据: '%s'", packageID)
	}

	return md,
		mdBytes,
		ioutil.NopCloser(bytes.NewBuffer(cds.CodePackage)),
		nil
}

type ChaincodePackageLocator struct {
	ChaincodeDir string
}

func (cpl *ChaincodePackageLocator) ChaincodePackageStreamer(packageID string) *ChaincodePackageStreamer {
	return &ChaincodePackageStreamer{
		PackagePath: filepath.Join(cpl.ChaincodeDir, CCFileName(packageID)),
	}
}

type ChaincodePackageStreamer struct {
	PackagePath string
}

func (cps *ChaincodePackageStreamer) Exists() bool {
	_, err := os.Stat(cps.PackagePath)
	return err == nil
}

func (cps *ChaincodePackageStreamer) Metadata() (*ChaincodePackageMetadata, error) {
	tarFileStream, err := cps.File(MetadataFile)
	if err != nil {
		return nil, errors.WithMessage(err, "could not get metadata file")
	}

	defer tarFileStream.Close()

	metadata := &ChaincodePackageMetadata{}
	err = json.NewDecoder(tarFileStream).Decode(metadata)
	if err != nil {
		return nil, errors.WithMessage(err, "could not parse metadata file")
	}

	return metadata, nil
}

func (cps *ChaincodePackageStreamer) MetadataBytes() ([]byte, error) {
	tarFileStream, err := cps.File(MetadataFile)
	if err != nil {
		return nil, errors.WithMessage(err, "could not get metadata file")
	}

	defer tarFileStream.Close()

	md, err := ioutil.ReadAll(tarFileStream)
	if err != nil {
		return nil, errors.WithMessage(err, "could read metadata file")
	}

	return md, nil
}

func (cps *ChaincodePackageStreamer) Code() (*TarFileStream, error) {
	tarFileStream, err := cps.File(CodePackageFile)
	if err != nil {
		return nil, errors.WithMessage(err, "could not get code package")
	}

	return tarFileStream, nil
}

func (cps *ChaincodePackageStreamer) File(name string) (tarFileStream *TarFileStream, err error) {
	file, err := os.Open(cps.PackagePath)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not open chaincode package at '%s'", cps.PackagePath)
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading as gzip stream")
	}

	tarReader := tar.NewReader(gzReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Wrapf(err, "error inspecting next tar header")
		}

		if header.Name != name {
			continue
		}

		if header.Typeflag != tar.TypeReg {
			return nil, errors.Errorf("tar entry %s is not a regular file, type %v", header.Name, header.Typeflag)
		}

		return &TarFileStream{
			TarFile:    tarReader,
			FileStream: file,
		}, nil
	}

	return nil, errors.Errorf("did not find file '%s' in package", name)
}

type TarFileStream struct {
	TarFile    io.Reader
	FileStream io.Closer
}

func (tfs *TarFileStream) Read(p []byte) (int, error) {
	return tfs.TarFile.Read(p)
}

func (tfs *TarFileStream) Close() error {
	return tfs.FileStream.Close()
}

// ChaincodePackage 表示链码包的未解压格式。
type ChaincodePackage struct {
	Metadata    *ChaincodePackageMetadata // 链码包元数据
	CodePackage []byte                    // 代码包
	DBArtifacts []byte                    // 数据库构件
}

// ChaincodePackageMetadata contains the information necessary to understand
// the embedded code package.
type ChaincodePackageMetadata struct {
	Type  string `json:"type"`
	Path  string `json:"path"`
	Label string `json:"label"`
}

// MetadataProvider 提供从代码包中检索元数据信息 (例如DB索引) 的方法。
type MetadataProvider interface {
	GetDBArtifacts(codePackage []byte) ([]byte, error)
}

// ChaincodePackageParser 提供解析链代码包的能力。
type ChaincodePackageParser struct {
	MetadataProvider MetadataProvider
}

// LabelRegexp is the regular expression controlling the allowed characters
// for the package label.
var LabelRegexp = regexp.MustCompile(`^[[:alnum:]][[:alnum:]_.+-]*$`)

// ValidateLabel return an error if the provided label contains any invalid
// characters, as determined by LabelRegexp.
func ValidateLabel(label string) error {
	if !LabelRegexp.MatchString(label) {
		return errors.Errorf("invalid label '%s'. Label must be non-empty, can only consist of alphanumerics, symbols from '.+-_', and can only begin with alphanumerics", label)
	}

	return nil
}

// Parse 将一组字节解析为链码包，并将解析后的包返回为结构体。
// 方法接收者：ccpp（ChaincodePackageParser类型）
// 输入参数：
//   - source：[]byte类型，表示要解析的字节数据。
//
// 返回值：
//   - *ChaincodePackage：表示解析后的链码包。
//   - error：如果解析过程中出错，则返回错误。
func (ccpp ChaincodePackageParser) Parse(source []byte) (*ChaincodePackage, error) {
	// 解析链码包的元数据和代码包
	ccPackageMetadata, codePackage, err := ParseChaincodePackage(source)
	if err != nil {
		return nil, err
	}

	// 从代码包中获取数据库相关的信息
	dbArtifacts, err := ccpp.MetadataProvider.GetDBArtifacts(codePackage)
	if err != nil {
		return nil, errors.WithMessage(err, "从代码包中检索DB工件时出错")
	}

	// 创建并返回解析后的链码包, 表示链码包的未解压格式
	return &ChaincodePackage{
		Metadata:    ccPackageMetadata, // 链码包元数据
		CodePackage: codePackage,       // 代码包
		DBArtifacts: dbArtifacts,       // 数据库构件
	}, nil
}

// ParseChaincodePackage 将一组字节解析为链码包
// 并返回解析后的包作为元数据结构和代码包
func ParseChaincodePackage(source []byte) (*ChaincodePackageMetadata, []byte, error) {
	gzReader, err := gzip.NewReader(bytes.NewBuffer(source))
	if err != nil {
		return &ChaincodePackageMetadata{}, nil, errors.Wrapf(err, "读取为 gzip 流时出错")
	}

	tarReader := tar.NewReader(gzReader)

	var codePackage []byte
	var ccPackageMetadata *ChaincodePackageMetadata
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return ccPackageMetadata, nil, errors.Wrapf(err, "检查下一个 tar 标头时出错")
		}

		if header.Typeflag != tar.TypeReg {
			return ccPackageMetadata, nil, errors.Errorf("tar 条目 %s 不是常规文件, 类型为 %v", header.Name, header.Typeflag)
		}

		fileBytes, err := ioutil.ReadAll(tarReader)
		if err != nil {
			return ccPackageMetadata, nil, errors.Wrapf(err, "无法从tar读取 %s", header.Name)
		}

		switch header.Name {

		case MetadataFile:
			ccPackageMetadata = &ChaincodePackageMetadata{}
			err := json.Unmarshal(fileBytes, ccPackageMetadata)
			if err != nil {
				return ccPackageMetadata, nil, errors.Wrapf(err, "无法将 %s 反序列化为json", MetadataFile)
			}

		case CodePackageFile:
			codePackage = fileBytes
		default:
			logger.Warningf("在链码包的顶级中遇到意外的文件 '%s'", header.Name)
		}
	}

	if codePackage == nil {
		return ccPackageMetadata, nil, errors.Errorf("在包内未找到代码包")
	}

	if ccPackageMetadata == nil {
		return ccPackageMetadata, nil, errors.Errorf("未找到任何包元数据 (缺少 %s)", MetadataFile)
	}

	if err := ValidateLabel(ccPackageMetadata.Label); err != nil {
		return ccPackageMetadata, nil, err
	}

	return ccPackageMetadata, codePackage, nil
}
