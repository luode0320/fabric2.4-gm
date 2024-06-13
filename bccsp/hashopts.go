package bccsp

import "fmt"

// SHA256Opts 包含与SHA-256相关的选项。
type SHA256Opts struct{}

// Algorithm 返回哈希算法标识符 (要使用)。
func (opts *SHA256Opts) Algorithm() string {
	return SHA256
}

// SHA384Opts 包含与SHA-384相关的选项。
type SHA384Opts struct{}

// Algorithm 返回哈希算法标识符 (要使用)。
func (opts *SHA384Opts) Algorithm() string {
	return SHA384
}

// SHA3_256Opts 包含与SHA3-256相关的选项。
type SHA3_256Opts struct{}

// Algorithm 返回哈希算法标识符 (要使用)。
func (opts *SHA3_256Opts) Algorithm() string {
	return SHA3_256
}

// SHA3_384Opts contains options relating to SHA3-384.
type SHA3_384Opts struct{}

// Algorithm returns the hash algorithm identifier (to be used).
func (opts *SHA3_384Opts) Algorithm() string {
	return SHA3_384
}

// GMSM3Opts 国密 SM3.
type GMSM3Opts struct {
}

// Algorithm 国密 sm3 算法
func (opts *GMSM3Opts) Algorithm() string {
	return GMSM3
}

// GetHashOpt 返回与传递的哈希函数对应的 HashOpts
func GetHashOpt(hashFunction string) (HashOpts, error) {
	switch hashFunction {
	case SHA2:
		return &SHA256Opts{}, nil
	case SHA3:
		return &SHA3_256Opts{}, nil
	case SHA256:
		return &SHA256Opts{}, nil
	case SHA384:
		return &SHA384Opts{}, nil
	case SHA3_256:
		return &SHA3_256Opts{}, nil
	case SHA3_384:
		return &SHA3_384Opts{}, nil
	case GMSM3:
		return &GMSM3Opts{}, nil
	}
	return nil, fmt.Errorf("无法识别的hash函数 [%s],只支持SHA2、SHA3、SHA256、SHA384、SHA3_256、SHA3_384、GMSM3", hashFunction)
}
