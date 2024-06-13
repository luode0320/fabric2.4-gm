package gm

import (
	"github.com/hyperledger/fabric/bccsp"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSM4Encrypt(t *testing.T) {
	t.Parallel()

	raw, err := GetRandomBytes(16)
	require.NoError(t, err)

	k := &gmsm4PrivateKey{
		privKey:    raw,
		exportable: false,
	}

	msg := []byte("Hello World hello world")
	encryptor := &gmsm4Encryptor{}

	ct, err := encryptor.Encrypt(k, msg, bccsp.AESCBCPKCS7ModeOpts{})
	if err != nil {
		t.Errorf("encrypt error : %s\n", err.Error())
		return
	}
	t.Logf("cipher text : %s\n", string(ct))
}

func TestSM4Decrypt(t *testing.T) {
	t.Parallel()

	raw, err := GetRandomBytes(16)
	require.NoError(t, err)

	k := &gmsm4PrivateKey{
		privKey:    raw,
		exportable: false,
	}

	msg := []byte("Hello World hello")
	encryptor := &gmsm4Encryptor{}

	ct, err := encryptor.Encrypt(k, msg, bccsp.AESCBCPKCS7ModeOpts{})
	if err != nil {
		t.Errorf("encrypt error : %s\n", err.Error())
		return
	}

	decryptor := &gmsm4Decryptor{}
	msg2, err := decryptor.Decrypt(k, ct, &bccsp.AESCBCPKCS7ModeOpts{})
	if err != nil {
		t.Errorf("decrypt error : %s\n", err.Error())
		return
	}
	t.Logf("message : %s\n", string(msg2))
}
