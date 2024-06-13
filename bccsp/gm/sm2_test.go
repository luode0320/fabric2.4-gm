package gm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGmsm2Signer_Sign(t *testing.T) {
	t.Parallel()
	sm2gen := gmsm2KeyGenerator{}
	key, _ := sm2gen.KeyGen(nil)

	signer := gmsm2Signer{}
	digest := "hello world"
	signature, err := signer.Sign(key, []byte(digest), nil)
	require.NoError(t, err)

	t.Logf("signature : %s\n", string(signature))
}

func TestGmsm2PrivateKeyVerifier_Verify(t *testing.T) {
	t.Parallel()
	sm2gen := gmsm2KeyGenerator{}
	key, _ := sm2gen.KeyGen(nil)

	signer := gmsm2Signer{}
	digest := "hello world"
	signature, err := signer.Sign(key, []byte(digest), nil)
	require.NoError(t, err)

	verifier := gmsm2PrivateKeyVerifier{}
	valid, err := verifier.Verify(key, signature, []byte(digest), nil)
	require.NoError(t, err)
	require.True(t, valid)

	valid, err = verifier.Verify(key, signature, []byte("hello"), nil)
	require.False(t, valid)
}

func TestGmsm2PublicKeyKeyVerifier_Verify(t *testing.T) {
	t.Parallel()
	sm2gen := gmsm2KeyGenerator{}
	key, _ := sm2gen.KeyGen(nil)

	signer := gmsm2Signer{}
	digest := "hello world"
	signature, err := signer.Sign(key, []byte(digest), nil)
	require.NoError(t, err)

	verifier := gmsm2PublicKeyKeyVerifier{}
	pk, _ := key.PublicKey()
	valid, err := verifier.Verify(pk, signature, []byte(digest), nil)
	require.NoError(t, err)
	require.True(t, valid)

	valid, err = verifier.Verify(pk, signature, []byte("hello"), nil)
	require.False(t, valid)
}
