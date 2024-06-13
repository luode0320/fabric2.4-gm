/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ccprovider

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/golang/protobuf/proto"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
)

var ccproviderLogger = flogging.MustGetLogger("ccprovider")

var chaincodeInstallPath string

// CCPackage 封装了一个链码包，可以是
//
//	raw ChaincodeDeploymentSpec
//	SignedChaincodeDeploymentSpec
//
// 尝试保持接口在最小化的级别上，以便可能进行一般化。
type CCPackage interface {
	// InitFromBuffer 从字节初始化链码包。
	InitFromBuffer(buf []byte) (*ChaincodeData, error)

	// PutChaincodeToFS 将链码写入文件系统。
	PutChaincodeToFS() error

	// GetDepSpec 从包中获取 ChaincodeDeploymentSpec。
	GetDepSpec() *pb.ChaincodeDeploymentSpec

	// GetDepSpecBytes 从包中获取序列化的 ChaincodeDeploymentSpec。
	GetDepSpecBytes() []byte

	// ValidateCC 验证并返回与 ChaincodeData 对应的链码部署规范。
	// 验证基于 ChaincodeData 的元数据。
	// 此方法的一个用途是在启动之前验证链码。
	ValidateCC(ccdata *ChaincodeData) error

	// GetPackageObject 获取对象作为 proto.Message。
	GetPackageObject() proto.Message

	// GetChaincodeData 获取 ChaincodeData。
	GetChaincodeData() *ChaincodeData

	// GetId 获取基于包计算的链码指纹。
	GetId() []byte
}

// SetChaincodesPath 设置此对等方的链码路径
func SetChaincodesPath(path string) {
	if s, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, 0o755); err != nil {
				panic(fmt.Sprintf("无法创建链码安装路径: %s", err))
			}
		} else {
			panic(fmt.Sprintf("不能识别链码安装路径: %s", err))
		}
	} else if !s.IsDir() {
		panic(fmt.Errorf("链码路径存在，但没有dir目录: %s", path))
	}

	chaincodeInstallPath = path
}

// isPrintable is used by CDSPackage and SignedCDSPackage validation to
// detect garbage strings in unmarshaled proto fields where printable
// characters are expected.
func isPrintable(name string) bool {
	notASCII := func(r rune) bool {
		return !unicode.IsPrint(r)
	}
	return strings.IndexFunc(name, notASCII) == -1
}

// GetChaincodePackageFromPath 从文件系统中获取链码包
func GetChaincodePackageFromPath(ccNameVersion string, ccInstallPath string) ([]byte, error) {
	// 构建链码包的路径
	path := fmt.Sprintf("%s/%s", ccInstallPath, strings.ReplaceAll(ccNameVersion, ":", "."))

	// 读取链码包的内容
	ccbytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return ccbytes, nil
}

// ChaincodePackageExists returns whether the chaincode package exists in the file system
func ChaincodePackageExists(ccname string, ccversion string) (bool, error) {
	path := filepath.Join(chaincodeInstallPath, ccname+"."+ccversion)
	_, err := os.Stat(path)
	if err == nil {
		// chaincodepackage already exists
		return true, nil
	}
	return false, err
}

type CCCacheSupport interface {
	// GetChaincode is needed by the cache to get chaincode data
	GetChaincode(ccNameVersion string) (CCPackage, error)
}

// CCInfoFSImpl provides the implementation for CC on the FS and the access to it
// It implements CCCacheSupport
type CCInfoFSImpl struct {
	GetHasher GetHasher
}

// GetChaincodeFromFS this is a wrapper for hiding package implementation.
// It calls GetChaincodeFromPath with the chaincodeInstallPath
func (cifs *CCInfoFSImpl) GetChaincode(ccNameVersion string) (CCPackage, error) {
	return cifs.GetChaincodeFromPath(ccNameVersion, chaincodeInstallPath)
}

func (cifs *CCInfoFSImpl) GetChaincodeCodePackage(ccNameVersion string) ([]byte, error) {
	ccpack, err := cifs.GetChaincode(ccNameVersion)
	if err != nil {
		return nil, err
	}
	return ccpack.GetDepSpec().CodePackage, nil
}

func (cifs *CCInfoFSImpl) GetChaincodeDepSpec(ccNameVersion string) (*pb.ChaincodeDeploymentSpec, error) {
	ccpack, err := cifs.GetChaincode(ccNameVersion)
	if err != nil {
		return nil, err
	}
	return ccpack.GetDepSpec(), nil
}

// GetChaincodeFromPath 是隐藏包实现的包装器函数。
// 输入参数：
//   - ccNameVersion：链码的名称和版本号。
//   - path：链码的路径。
//
// 返回值：
//   - CCPackage：链码包对象。
//   - error：如果获取链码包过程中出现错误，则返回相应的错误信息。
func (cifs *CCInfoFSImpl) GetChaincodeFromPath(ccNameVersion string, path string) (CCPackage, error) {
	// 尝试使用原始的CDS包
	cccdspack := &CDSPackage{GetHasher: cifs.GetHasher}
	_, _, err := cccdspack.InitFromPath(ccNameVersion, path)
	if err != nil {
		// 尝试使用签名的CDS包
		ccscdspack := &SignedCDSPackage{GetHasher: cifs.GetHasher}
		_, _, err = ccscdspack.InitFromPath(ccNameVersion, path)
		if err != nil {
			// 如果签名的CDS包初始化失败，则返回错误信息
			return nil, err
		}
		// 如果签名的CDS包初始化成功，则返回签名的CDS包对象
		return ccscdspack, nil
	}
	// 如果原始的CDS包初始化成功，则返回原始的CDS包对象
	return cccdspack, nil
}

// GetChaincodeInstallPath 返回已安装的链代码的路径
func (*CCInfoFSImpl) GetChaincodeInstallPath() string {
	return chaincodeInstallPath
}

// PutChaincode is a wrapper for putting raw ChaincodeDeploymentSpec
// using CDSPackage. This is only used in UTs
func (cifs *CCInfoFSImpl) PutChaincode(depSpec *pb.ChaincodeDeploymentSpec) (CCPackage, error) {
	buf, err := proto.Marshal(depSpec)
	if err != nil {
		return nil, err
	}
	cccdspack := &CDSPackage{GetHasher: cifs.GetHasher}
	if _, err := cccdspack.InitFromBuffer(buf); err != nil {
		return nil, err
	}
	err = cccdspack.PutChaincodeToFS()
	if err != nil {
		return nil, err
	}

	return cccdspack, nil
}

// DirEnumerator enumerates directories
type DirEnumerator func(string) ([]os.FileInfo, error)

// ChaincodeExtractor extracts chaincode from a given path
type ChaincodeExtractor func(ccNameVersion string, path string, getHasher GetHasher) (CCPackage, error)

// ListInstalledChaincodes 用于检索已安装的链码。
// 输入参数：
//   - dir：链码所在的目录路径。
//   - ls：用于枚举目录的函数。
//   - ccFromPath：从链码路径中提取链码信息的函数。
//
// 返回值：
//   - []chaincode.InstalledChaincode：已安装的链码列表。
//   - error：如果在检索过程中出现错误，则返回相应的错误信息。
func (cifs *CCInfoFSImpl) ListInstalledChaincodes(dir string, ls DirEnumerator, ccFromPath ChaincodeExtractor) ([]chaincode.InstalledChaincode, error) {
	var chaincodes []chaincode.InstalledChaincode
	if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
		return nil, nil
	}
	files, err := ls(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "读取目录失败 %s", dir)
	}

	// 遍历已安装链码目录
	for _, f := range files {
		// 跳过目录，我们只关注普通文件
		if f.IsDir() {
			continue
		}
		// 链码文件名的格式为 "name.version"
		// 我们只关注名称，跳过不符合文件命名规范 "A.B" 的文件
		i := strings.Index(f.Name(), ".")
		if i == -1 {
			ccproviderLogger.Info("正在跳过", f.Name(), "由于缺少分隔符 '.'")
			continue
		}
		ccName := f.Name()[:i]      // 分隔符之前的部分
		ccVersion := f.Name()[i+1:] // 分隔符之后的部分

		// 从链码路径中提取链码信息
		ccPackage, err := ccFromPath(ccName+":"+ccVersion, dir, cifs.GetHasher)
		if err != nil {
			ccproviderLogger.Warning("获取链码信息失败", ccName, ccVersion, ":", err)
			return nil, errors.Wrapf(err, "获取有关信息失败 %s, version %s", ccName, ccVersion)
		}

		// 构建已安装链码的结构体，并添加到链码列表中
		chaincodes = append(chaincodes, chaincode.InstalledChaincode{
			Name:    ccName,    // 链码名称
			Version: ccVersion, // 链码版本
			Hash:    ccPackage.GetId(),
		})
	}
	ccproviderLogger.Debug("Returning", chaincodes)
	return chaincodes, nil
}

// ccInfoFSStorageMgr is the storage manager used either by the cache or if the
// cache is bypassed
var ccInfoFSProvider = &CCInfoFSImpl{GetHasher: factory.GetDefault()}

// ccInfoCache is the cache instance itself
var ccInfoCache = NewCCInfoCache(ccInfoFSProvider)

// GetChaincodeFromFS retrieves chaincode information from the file system
func GetChaincodeFromFS(ccNameVersion string) (CCPackage, error) {
	return ccInfoFSProvider.GetChaincode(ccNameVersion)
}

// GetChaincodeData gets chaincode data from cache if there's one
func GetChaincodeData(ccNameVersion string) (*ChaincodeData, error) {
	ccproviderLogger.Debugf("Getting chaincode data for <%s> from cache", ccNameVersion)
	return ccInfoCache.GetChaincodeData(ccNameVersion)
}

// GetCCPackage tries each known package implementation one by one
// till the right package is found
func GetCCPackage(buf []byte, bccsp bccsp.BCCSP) (CCPackage, error) {
	// try raw CDS
	cds := &CDSPackage{GetHasher: bccsp}
	if ccdata, err := cds.InitFromBuffer(buf); err != nil {
		cds = nil
	} else {
		err = cds.ValidateCC(ccdata)
		if err != nil {
			cds = nil
		}
	}

	// try signed CDS
	scds := &SignedCDSPackage{GetHasher: bccsp}
	if ccdata, err := scds.InitFromBuffer(buf); err != nil {
		scds = nil
	} else {
		err = scds.ValidateCC(ccdata)
		if err != nil {
			scds = nil
		}
	}

	if cds != nil && scds != nil {
		// Both were unmarshaled successfully, this is exactly why the approach of
		// hoping proto fails for bad inputs is fatally flawed.
		ccproviderLogger.Errorf("Could not determine chaincode package type, guessing SignedCDS")
		return scds, nil
	}

	if cds != nil {
		return cds, nil
	}

	if scds != nil {
		return scds, nil
	}

	return nil, errors.New("could not unmarshal chaincode package to CDS or SignedCDS")
}

// GetInstalledChaincodes returns a map whose key is the chaincode id and
// value is the ChaincodeDeploymentSpec struct for that chaincodes that have
// been installed (but not necessarily instantiated) on the peer by searching
// the chaincode install path
func GetInstalledChaincodes() (*pb.ChaincodeQueryResponse, error) {
	files, err := ioutil.ReadDir(chaincodeInstallPath)
	if err != nil {
		return nil, err
	}

	// array to store info for all chaincode entries from LSCC
	var ccInfoArray []*pb.ChaincodeInfo

	for _, file := range files {
		// split at first period as chaincode versions can contain periods while
		// chaincode names cannot
		fileNameArray := strings.SplitN(file.Name(), ".", 2)

		// check that length is 2 as expected, otherwise skip to next cc file
		if len(fileNameArray) == 2 {
			ccname := fileNameArray[0]
			ccversion := fileNameArray[1]
			ccpack, err := GetChaincodeFromFS(ccname + ":" + ccversion)
			if err != nil {
				// either chaincode on filesystem has been tampered with or
				// _lifecycle chaincode files exist in the chaincodes directory.
				continue
			}

			cdsfs := ccpack.GetDepSpec()

			name := cdsfs.GetChaincodeSpec().GetChaincodeId().Name
			version := cdsfs.GetChaincodeSpec().GetChaincodeId().Version
			if name != ccname || version != ccversion {
				// chaincode name/version in the chaincode file name has been modified
				// by an external entity
				ccproviderLogger.Errorf("Chaincode file's name/version has been modified on the filesystem: %s", file.Name())
				continue
			}

			path := cdsfs.GetChaincodeSpec().ChaincodeId.Path
			// since this is just an installed chaincode these should be blank
			input, escc, vscc := "", "", ""

			ccInfo := &pb.ChaincodeInfo{Name: name, Version: version, Path: path, Input: input, Escc: escc, Vscc: vscc, Id: ccpack.GetId()}

			// add this specific chaincode's metadata to the array of all chaincodes
			ccInfoArray = append(ccInfoArray, ccInfo)
		}
	}
	// add array with info about all instantiated chaincodes to the query
	// response proto
	cqr := &pb.ChaincodeQueryResponse{Chaincodes: ccInfoArray}

	return cqr, nil
}

//-------- ChaincodeData is stored on the LSCC -------

// ChaincodeData defines the datastructure for chaincodes to be serialized by proto
// Type provides an additional check by directing to use a specific package after instantiation
// Data is Type specific (see CDSPackage and SignedCDSPackage)
type ChaincodeData struct {
	// Name of the chaincode
	Name string `protobuf:"bytes,1,opt,name=name"`

	// Version of the chaincode
	Version string `protobuf:"bytes,2,opt,name=version"`

	// Escc for the chaincode instance
	Escc string `protobuf:"bytes,3,opt,name=escc"`

	// Vscc for the chaincode instance
	Vscc string `protobuf:"bytes,4,opt,name=vscc"`

	// Policy endorsement policy for the chaincode instance
	Policy []byte `protobuf:"bytes,5,opt,name=policy,proto3"`

	// Data data specific to the package
	Data []byte `protobuf:"bytes,6,opt,name=data,proto3"`

	// Id of the chaincode that's the unique fingerprint for the CC This is not
	// currently used anywhere but serves as a good eyecatcher
	Id []byte `protobuf:"bytes,7,opt,name=id,proto3"`

	// InstantiationPolicy for the chaincode
	InstantiationPolicy []byte `protobuf:"bytes,8,opt,name=instantiation_policy,proto3"`
}

// ChaincodeID is the name by which the chaincode will register itself.
func (cd *ChaincodeData) ChaincodeID() string {
	return cd.Name + ":" + cd.Version
}

// implement functions needed from proto.Message for proto's mar/unmarshal functions

// Reset resets
func (cd *ChaincodeData) Reset() { *cd = ChaincodeData{} }

// String converts to string
func (cd *ChaincodeData) String() string { return proto.CompactTextString(cd) }

// ProtoMessage just exists to make proto happy
func (*ChaincodeData) ProtoMessage() {}

// TransactionParams 是与特定交易相关联的参数，用于调用链码。
type TransactionParams struct {
	TxID                 string                      // 交易ID
	ChannelID            string                      // 通道ID
	NamespaceID          string                      // 命名空间ID
	SignedProp           *pb.SignedProposal          // 签名的提案
	Proposal             *pb.Proposal                // 原始提案
	TXSimulator          ledger.TxSimulator          // 交易模拟器
	HistoryQueryExecutor ledger.HistoryQueryExecutor // 历史查询执行器
	CollectionStore      privdata.CollectionStore    // 集合存储
	IsInitTransaction    bool                        // 是否为初始化交易
	ProposalDecorations  map[string][]byte           // 传递给链码的额外数据
}
