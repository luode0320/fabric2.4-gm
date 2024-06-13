package gm

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm4"
	"github.com/Hyperledger-TWGC/tjfoc-gm/x509"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/utils"
)

// NewFileBasedKeyStore 在给定位置实例化基于文件的密钥存储。
// 如果非空密码为specifiec，则可以加密密钥存储。
// 也可以设置为只读。在这种情况下，任何存储操作
// 将被禁止
func NewFileBasedKeyStore(pwd []byte, path string, readOnly bool) (bccsp.KeyStore, error) {
	ks := &fileBasedKeyStore{}
	return ks, ks.Init(pwd, path, readOnly)
}

// fileBasedKeyStore 是一个基于文件夹的密钥库。
// 每个密钥都存储在一个单独的文件中，该文件的名称包含密钥的 SKI 以及标识密钥类型的标志。
// 所有的密钥都存储在其路径在初始化时 提供的 文件夹下。
// KeyStore可以用密码初始化，这个密码用于加密和解密存储密钥的文件(给文件加密)。
// KeyStore 只能读取，以避免密钥被覆盖。
type fileBasedKeyStore struct {
	path string // 密钥存储文件夹

	readOnly bool // 是否只读, 密钥文件生成后不可更改, 避免修改出错。
	isOpen   bool // 初始化完成标志

	pwd []byte // 加密密钥文件的密码, 可以为空

	// 同步锁
	m sync.Mutex
}

// Init 使用密码、文件夹路径初始化此 KeyStore 存储密钥和只读标志的位置(只创建目录, 不创建密钥文件)。
// 每个密钥都存储在一个单独的文件中，该文件的名称包含密钥的 SKI 以及标识密钥类型的标志。
// 如果KeyStore是用密码初始化的，则该密码用于加密和解密存储密钥的文件。
// 对于非加密的 KeyStore，pwd 可以为零。如果一个加密的密钥存储在没有密码的情况下初始化，然后密钥库将失败。
// KeyStore 只能读取，以避免密钥被覆盖。
func (ks *fileBasedKeyStore) Init(pwd []byte, path string, readOnly bool) error {
	// Validate inputs
	// pwd can be nil

	if len(path) == 0 {
		return errors.New("An invalid KeyStore path provided. Path cannot be an empty string.")
	}

	ks.m.Lock()
	defer ks.m.Unlock()

	if ks.isOpen {
		return errors.New("KeyStore already initilized.")
	}
	ks.path = path
	ks.pwd = utils.Clone(pwd)
	err := ks.createKeyStoreIfNotExists()
	if err != nil {
		return err
	}

	err = ks.openKeyStore()
	if err != nil {
		return err
	}

	ks.readOnly = readOnly

	return nil
}

// ReadOnly 如果此密钥库是只读的，则返回true，否则返回false。
// 如果ReadOnly为true，则StoreKey将失败。
func (ks *fileBasedKeyStore) ReadOnly() bool {
	return ks.readOnly
}

// GetKey 返回一个key对象，其SKI是传递的对象。
func (ks *fileBasedKeyStore) GetKey(ski []byte) (k bccsp.Key, err error) {
	// 验证参数
	if len(ski) == 0 {
		return nil, errors.New("无效 SKI. 长度不能为零。")
	}
	// 获取ski后缀
	suffix := ks.getSuffix(hex.EncodeToString(ski))

	switch suffix {
	case "key":
		// 从文件夹下找到文件, 加载密钥
		key, err := ks.loadKey(hex.EncodeToString(ski))
		if err != nil {
			return nil, fmt.Errorf("加载密钥失败 [%x] [%s]", ski, err)
		}

		// 生成sm4对称密钥
		return &gmsm4PrivateKey{key, false}, nil
	case "sk":
		// 从文件夹下找到文件, 加载私钥
		key, err := ks.loadPrivateKey(hex.EncodeToString(ski))
		if err != nil {
			return nil, fmt.Errorf("加载密钥失败 [%x] [%s]", ski, err)
		}

		switch key.(type) {
		case *sm2.PrivateKey:
			// 生成sm2非对称私钥
			return &gmsm2PrivateKey{key.(*sm2.PrivateKey)}, nil
		default:
			return nil, errors.New("未识别密钥类型, 仅支持sm2PrivateKey")
		}
	case "pk":
		// 从文件夹下找到文件, 加载公钥
		key, err := ks.loadPublicKey(hex.EncodeToString(ski))
		if err != nil {
			return nil, fmt.Errorf("加载公钥失败 [%x] [%s]", ski, err)
		}

		switch key.(type) {
		case *sm2.PublicKey:
			// 生成sm2非对称公钥
			return &gmsm2PublicKey{key.(*sm2.PublicKey)}, nil
		default:
			return nil, errors.New("无法识别公钥类型, 仅支持sm2PublicKey")
		}
	default:
		// 注意这里如果没有匹配到指定类型的话，将使用searchKeystoreForSKI方法再搜一遍目录，以找到对应SKI的密钥
		return ks.searchKeystoreForSKI(ski)
	}
}

// StoreKey 将密钥k存储在此密钥库中。
// 如果此密钥库是只读的，则该方法将失败。
func (ks *fileBasedKeyStore) StoreKey(k bccsp.Key) (err error) {
	if ks.readOnly {
		return errors.New("只读密钥库")
	}

	if k == nil {
		return errors.New("无效密钥。它必须不同于nil")
	}

	switch k.(type) {
	case *gmsm2PrivateKey:
		kk := k.(*gmsm2PrivateKey)

		err = ks.storePrivateKey(hex.EncodeToString(k.SKI()), kk.privKey)
		if err != nil {
			return fmt.Errorf("存储GMSM2私钥失败 [%s]", err)
		}

	case *gmsm2PublicKey:
		kk := k.(*gmsm2PublicKey)

		err = ks.storePublicKey(hex.EncodeToString(k.SKI()), kk.pubKey)
		if err != nil {
			return fmt.Errorf("存储GMSM2公钥失败 [%s]", err)
		}
	case *gmsm4PrivateKey:
		kk := k.(*gmsm4PrivateKey)
		err = ks.storeKey(hex.EncodeToString(k.SKI()), kk.privKey)
		if err != nil {
			return fmt.Errorf("存储GMSM4密钥失败 [%s]", err)
		}
	default:
		return fmt.Errorf("存储的密钥类型未确认 [%s]", k)
	}

	return
}

// searchKeystoreForSKI 搜一遍目录，以找到对应SKI的密钥
func (ks *fileBasedKeyStore) searchKeystoreForSKI(ski []byte) (k bccsp.Key, err error) {

	files, _ := ioutil.ReadDir(ks.path)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		raw, err := ioutil.ReadFile(filepath.Join(ks.path, f.Name()))
		if err != nil {
			continue
		}

		key, err := utils.PEMtoPrivateKey(raw, ks.pwd)
		if err != nil {
			continue
		}

		switch key.(type) {
		case *sm2.PrivateKey:
			k = &gmsm2PrivateKey{key.(*sm2.PrivateKey)}
		default:
			continue
		}

		if !bytes.Equal(k.SKI(), ski) {
			continue
		}

		return k, nil
	}
	return nil, errors.New("密钥类型无法识别")
}

// getSuffix 获取后缀
func (ks *fileBasedKeyStore) getSuffix(alias string) string {
	files, _ := ioutil.ReadDir(ks.path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), alias) {
			if strings.HasSuffix(f.Name(), "sk") {
				return "sk"
			}
			if strings.HasSuffix(f.Name(), "pk") {
				return "pk"
			}
			if strings.HasSuffix(f.Name(), "key") {
				return "key"
			}
			break
		}
	}
	return ""
}

// storePrivateKey 保存非对称私钥到文件
func (ks *fileBasedKeyStore) storePrivateKey(alias string, privateKey interface{}) error {
	// 将私钥内容转化为pem格式的内容(密钥肯定不能是txt文本保存啊, 不然太low了吧。pem就是正常保存密钥的文件格式)
	rawKey, err := utils.PrivateKeyToPEM(privateKey, ks.pwd)
	if err != nil {
		logger.Errorf("将私钥转换为PEM失败 [%s]: [%s]", alias, err)
		return err
	}
	// 创建文件并保存
	// 第一个参数是文件路径，第二个参数是要写入的数据，第三个参数是文件的权限
	// 文件名称使用ski_sk, 内容是rawKey, 文件所有者有读写权限，其他用户没有权限
	// (还有其他后缀, 通用(AES)是key, 公钥是pk, 私钥是sk)
	err = ioutil.WriteFile(ks.getPathForAlias(alias, "sk"), rawKey, 0700)
	if err != nil {
		logger.Errorf("存储私钥失败 [%s]: [%s]", alias, err)
		return err
	}

	return nil
}

// storePublicKey 存储非对称公钥
func (ks *fileBasedKeyStore) storePublicKey(alias string, publicKey interface{}) error {
	// 将私钥内容转化为pem格式的内容(密钥肯定不能是txt文本保存啊, 不然太low了吧。pem就是正常保存密钥的文件格式)
	rawKey, err := utils.PublicKeyToPEM(publicKey, ks.pwd)
	if err != nil {
		logger.Errorf("将公钥转换为PEM失败 [%s]: [%s]", alias, err)
		return err
	}
	// 创建文件并保存
	// 第一个参数是文件路径，第二个参数是要写入的数据，第三个参数是文件的权限
	// 文件名称使用ski_sk, 内容是rawKey, 文件所有者有读写权限，其他用户没有权限
	// (还有其他后缀, 通用(AES)是key, 公钥是pk, 私钥是sk)
	err = ioutil.WriteFile(ks.getPathForAlias(alias, "pk"), rawKey, 0700)
	if err != nil {
		logger.Errorf("存储私钥失败 [%s]: [%s]", alias, err)
		return err
	}

	return nil
}

// storeKey 存储对称密钥
func (ks *fileBasedKeyStore) storeKey(alias string, key []byte) error {
	if len(ks.pwd) == 0 {
		ks.pwd = nil
	}

	if len(key) == 0 {
		return errors.New("sm4密钥无效。它必须不同于nil")
	}
	// 将私钥内容转化为pem格式的内容(密钥肯定不能是txt文本保存啊, 不然太low了吧。pem就是正常保存密钥的文件格式)
	pem, err := sm4.WriteKeyToPem(key, ks.pwd)
	if err != nil {
		logger.Errorf("将密钥转换为PEM失败 [%s]: [%s]", alias, err)
		return err
	}
	// 创建文件并保存
	// 第一个参数是文件路径，第二个参数是要写入的数据，第三个参数是文件的权限
	// 文件名称使用ski_sk, 内容是rawKey, 文件所有者有读写权限，其他用户没有权限
	// (还有其他后缀, 通用(AES)是key, 公钥是pk, 私钥是sk)
	err = ioutil.WriteFile(ks.getPathForAlias(alias, "key"), pem, 0700)
	if err != nil {
		logger.Errorf("存储密钥失败 [%s]: [%s]", alias, err)
		return err
	}

	return nil
}

// loadPrivateKey 从文件夹下, 加载非对称私钥
func (ks *fileBasedKeyStore) loadPrivateKey(alias string) (interface{}, error) {
	path := ks.getPathForAlias(alias, "sk")
	logger.Debugf("正在加载私钥 [%s] at [%s]...", alias, path)
	// 读取私钥
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Errorf("加载私钥失败 [%s]: [%s].", alias, err.Error())

		return nil, err
	}
	// pem私钥转私钥格式
	privateKey, err := utils.PEMtoPrivateKey(raw, ks.pwd)
	if err != nil {
		logger.Errorf("解析私钥失败 [%s]: [%s].", alias, err.Error())

		return nil, err
	}

	return privateKey, nil
}

// loadKey 从文件夹下, 加载非对称公钥
func (ks *fileBasedKeyStore) loadPublicKey(alias string) (interface{}, error) {
	path := ks.getPathForAlias(alias, "pk")
	logger.Debugf("正在加载公钥 [%s] at [%s]...", alias, path)
	// 读取公钥
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Errorf("加载公钥失败 [%s]: [%s].", alias, err.Error())

		return nil, err
	}
	// 转公钥
	privateKey, err := x509.ReadPublicKeyFromPem(raw)
	if err != nil {
		logger.Errorf("解析公钥失败 [%s]: [%s].", alias, err.Error())

		return nil, err
	}

	return privateKey, nil
}

// loadKey 从文件夹下, 加载对称密钥
func (ks *fileBasedKeyStore) loadKey(alias string) ([]byte, error) {
	path := ks.getPathForAlias(alias, "key")
	logger.Infof("加载密钥 : %s", path)
	// 读取密钥
	pem, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Errorf("加载密钥失败 [%s]: [%s].", alias, err.Error())

		return nil, err
	}
	// pem转密钥
	key, err := sm4.ReadKeyFromPem(pem, ks.pwd)
	if err != nil {
		logger.Errorf("解析密钥失败 [%s]: [%s]", alias, err)

		return nil, err
	}

	return key, nil
}

// createKeyStoreIfNotExists 如果不存在，则创建密钥目录
func (ks *fileBasedKeyStore) createKeyStoreIfNotExists() error {
	// 检查密钥库目录
	ksPath := ks.path
	missing, err := utils.DirMissingOrEmpty(ksPath)

	if missing {
		logger.Debugf("密钥库路径 [%s] 缺失 [%t]: [%s]", ksPath, missing, utils.ErrToString(err))

		err := ks.createKeyStore()
		if err != nil {
			logger.Errorf("创建密钥库失败 At [%s]: [%s]", ksPath, err.Error())
			return nil
		}
	}

	return nil
}

// createKeyStore 创建密钥目录
func (ks *fileBasedKeyStore) createKeyStore() error {
	// 创建密钥库目录根 (如果它还不存在)
	ksPath := ks.path
	logger.Debugf("创建密钥库 at [%s]...", ksPath)

	// 递归地创建目录，如果目录已经存在则不会报错。ksPath是目录的路径，0o755是目录的权限。
	// 0o755是一个八进制数，表示目录的权限为 rwxr-xr-x，即所有者具有读、写和执行权限，而其他用户具有读和执行权限。
	os.MkdirAll(ksPath, 0755)

	logger.Debugf("已创建密钥库 at [%s].", ksPath)
	return nil
}

func (ks *fileBasedKeyStore) openKeyStore() error {
	if ks.isOpen {
		return nil
	}
	ks.isOpen = true
	logger.Debugf("密钥目录初始化 [%s]...完毕", ks.path)

	return nil
}

// getPathForAlias 拼接名称
func (ks *fileBasedKeyStore) getPathForAlias(alias, suffix string) string {
	return filepath.Join(ks.path, alias+"_"+suffix)
}
