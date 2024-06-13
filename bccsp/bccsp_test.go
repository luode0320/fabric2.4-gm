/*
版权所有IBM Corp.2017保留所有权利。

根据Apache许可证版本2.0 (“许可证”) 获得许可；
除非符合许可证，否则您不得使用此文件。
您可以在以下位置获得许可证的副本

http://www.apache.org/licenses/许可证-2.0

除非适用法律要求或书面同意，软件
根据许可证分发是在 “按现状” 的基础上分发的，
没有任何形式的明示或暗示的保证或条件。
请参阅管理权限的特定语言的许可证和
许可证的限制。
*/

package bccsp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAESOpts(t *testing.T) {
	test := func(ephemeral bool) {
		for _, opts := range []KeyGenOpts{
			&AES128KeyGenOpts{ephemeral},
			&AES192KeyGenOpts{ephemeral},
			&AES256KeyGenOpts{ephemeral},
		} {
			expectedAlgorithm := reflect.TypeOf(opts).String()[7:13]
			require.Equal(t, expectedAlgorithm, opts.Algorithm())
			require.Equal(t, ephemeral, opts.Ephemeral())
		}
	}
	test(true)
	test(false)

	opts := &AESKeyGenOpts{true}
	require.Equal(t, "AES", opts.Algorithm())
	require.True(t, opts.Ephemeral())
	opts.Temporary = false
	require.False(t, opts.Ephemeral())
}

func TestECDSAOpts(t *testing.T) {
	test := func(ephemeral bool) {
		for _, opts := range []KeyGenOpts{
			&ECDSAP256KeyGenOpts{ephemeral},
			&ECDSAP384KeyGenOpts{ephemeral},
		} {
			expectedAlgorithm := reflect.TypeOf(opts).String()[7:16]
			require.Equal(t, expectedAlgorithm, opts.Algorithm())
			require.Equal(t, ephemeral, opts.Ephemeral())
		}
	}
	test(true)
	test(false)

	test = func(ephemeral bool) {
		for _, opts := range []KeyGenOpts{
			&ECDSAKeyGenOpts{ephemeral},
			&ECDSAPKIXPublicKeyImportOpts{ephemeral},
			&ECDSAPrivateKeyImportOpts{ephemeral},
			&ECDSAGoPublicKeyImportOpts{ephemeral},
		} {
			require.Equal(t, "ECDSA", opts.Algorithm())
			require.Equal(t, ephemeral, opts.Ephemeral())
		}
	}
	test(true)
	test(false)

	opts := &ECDSAReRandKeyOpts{Temporary: true}
	require.True(t, opts.Ephemeral())
	opts.Temporary = false
	require.False(t, opts.Ephemeral())
	require.Equal(t, "ECDSA_RERAND", opts.Algorithm())
	require.Empty(t, opts.ExpansionValue())
}

func TestHashOpts(t *testing.T) {
	for _, ho := range []HashOpts{&SHA256Opts{}, &SHA384Opts{}, &SHA3_256Opts{}, &SHA3_384Opts{}} {
		s := strings.Replace(reflect.TypeOf(ho).String(), "*bccsp.", "", -1)
		algorithm := strings.Replace(s, "Opts", "", -1)
		require.Equal(t, algorithm, ho.Algorithm())
		ho2, err := GetHashOpt(algorithm)
		require.NoError(t, err)
		require.Equal(t, ho.Algorithm(), ho2.Algorithm())
	}
	_, err := GetHashOpt("foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash function not recognized")

	require.Equal(t, "SHA", (&SHAOpts{}).Algorithm())
}

func TestHMAC(t *testing.T) {
	opts := &HMACTruncated256AESDeriveKeyOpts{Arg: []byte("arg")}
	require.False(t, opts.Ephemeral())
	opts.Temporary = true
	require.True(t, opts.Ephemeral())
	require.Equal(t, "HMAC_TRUNCATED_256", opts.Algorithm())
	require.Equal(t, []byte("arg"), opts.Argument())

	opts2 := &HMACDeriveKeyOpts{Arg: []byte("arg")}
	require.False(t, opts2.Ephemeral())
	opts2.Temporary = true
	require.True(t, opts2.Ephemeral())
	require.Equal(t, "HMAC", opts2.Algorithm())
	require.Equal(t, []byte("arg"), opts2.Argument())
}

func TestKeyGenOpts(t *testing.T) {
	expectedAlgorithms := map[reflect.Type]string{
		reflect.TypeOf(&HMACImportKeyOpts{}):       "HMAC",
		reflect.TypeOf(&X509PublicKeyImportOpts{}): "X509Certificate",
		reflect.TypeOf(&AES256ImportKeyOpts{}):     "AES",
	}
	test := func(ephemeral bool) {
		for _, opts := range []KeyGenOpts{
			&HMACImportKeyOpts{ephemeral},
			&X509PublicKeyImportOpts{ephemeral},
			&AES256ImportKeyOpts{ephemeral},
		} {
			expectedAlgorithm := expectedAlgorithms[reflect.TypeOf(opts)]
			require.Equal(t, expectedAlgorithm, opts.Algorithm())
			require.Equal(t, ephemeral, opts.Ephemeral())
		}
	}
	test(true)
	test(false)
}
