package gm

import (
	"fmt"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGmsm2KeyGenerator_KeyGen(t *testing.T) {
	t.Parallel()

	kg := &gmsm2KeyGenerator{}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm2k, ok := k.(*gmsm2PrivateKey)
	require.True(t, ok)
	require.NotNil(t, sm2k.privKey)
	require.Equal(t, sm2k.privKey.Curve, sm2.P256Sm2())
}

func TestGmsm4KeyGenerator_KeyGen(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: 32,
	}
	k, err := kg.KeyGen(nil)
	require.NoError(t, err)

	sm4k, ok := k.(*gmsm4PrivateKey)
	require.True(t, ok)
	require.NotNil(t, sm4k.privKey)
	require.Equal(t, len(sm4k.privKey), 32)

	fmt.Printf("privkey : [%s]\n", string(sm4k.privKey))
}

func TestGmsm4KeyGeneratorInvalidInputs(t *testing.T) {
	t.Parallel()

	kg := &gmsm4KeyGenerator{
		length: -1,
	}
	_, err := kg.KeyGen(nil)
	require.NoError(t, err)
}
