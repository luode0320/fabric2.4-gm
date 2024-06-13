package gm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGmsm4PrivateKey_Bytes(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: 32,
	}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm4k, _ := k.(*gmsm4PrivateKey)

	raw, err := sm4k.Bytes()
	if err != nil {
		t.Errorf("get Bytes error : %s\n", err.Error())
		return
	}
	t.Logf("raw : %s\n", string(raw))
}

func TestGmsm4PrivateKey_Private(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: 32,
	}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm4k, _ := k.(*gmsm4PrivateKey)

	b := sm4k.Private()
	if b {
		t.Logf("%s\n", "私钥")
	} else {
		t.Logf("%s\n", "非私钥")
	}
}

func TestGmsm4PrivateKey_PublicKey(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: 32,
	}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm4k, _ := k.(*gmsm4PrivateKey)
	_, err = sm4k.PublicKey()
	require.Contains(t, err.Error(), "无法对对称密钥调用此方法")
}

func TestGmsm4PrivateKey_SKI(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: 32,
	}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm4k, _ := k.(*gmsm4PrivateKey)
	ski := sm4k.SKI()
	t.Logf("ski : %s\n", string(ski))
}
