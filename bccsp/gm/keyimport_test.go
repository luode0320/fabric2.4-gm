package gm

import (
	"crypto/rand"
	"errors"
	"github.com/Hyperledger-TWGC/tjfoc-gm/sm2"
	mocks2 "github.com/hyperledger/fabric/bccsp/mocks"
	"github.com/hyperledger/fabric/bccsp/sw/mocks"
	"github.com/hyperledger/fabric/bccsp/utils"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestKeyImport(t *testing.T) {
	t.Parallel()

	expectedRaw := []byte{1, 2, 3}
	expectedOpts := &mocks2.KeyDerivOpts{EphemeralValue: true}
	expectetValue := &mocks2.MockKey{BytesValue: []byte{1, 2, 3, 4, 5}}
	expectedErr := errors.New("Expected Error")

	keyImporters := make(map[reflect.Type]KeyImporter)
	keyImporters[reflect.TypeOf(&mocks2.KeyDerivOpts{})] = &mocks.KeyImporter{
		RawArg:  expectedRaw,
		OptsArg: expectedOpts,
		Value:   expectetValue,
		Err:     expectedErr,
	}
	csp := CSP{KeyImporters: keyImporters}
	value, err := csp.KeyImport(expectedRaw, expectedOpts)
	require.Nil(t, value)
	require.Contains(t, err.Error(), expectedErr.Error())

	keyImporters = make(map[reflect.Type]KeyImporter)
	keyImporters[reflect.TypeOf(&mocks2.KeyDerivOpts{})] = &mocks.KeyImporter{
		RawArg:  expectedRaw,
		OptsArg: expectedOpts,
		Value:   expectetValue,
		Err:     nil,
	}
	csp = CSP{KeyImporters: keyImporters}
	value, err = csp.KeyImport(expectedRaw, expectedOpts)
	require.Equal(t, expectetValue, value)
	require.Nil(t, err)
}

func TestGmsm4ImportKeyOptsKeyImporter_KeyImport(t *testing.T) {
	t.Parallel()

	ki := gmsm4ImportKeyOptsKeyImporter{}
	_, err := ki.KeyImport("Hello World", &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw material. Expected byte array.")

	_, err = ki.KeyImport(nil, &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw material. Expected byte array.")

	_, err = ki.KeyImport([]byte(nil), &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw material. It must not be nil.")

	_, err = ki.KeyImport([]byte{0}, &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid Key Length [")
}

func TestGmsm2PrivateKeyImportOptsKeyImporter_KeyImport(t *testing.T) {
	t.Parallel()

	ki := gmsm2PrivateKeyImportOptsKeyImporter{}

	_, err := ki.KeyImport("Hello World", &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw material. Expected byte array.")

	_, err = ki.KeyImport(nil, &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw material. Expected byte array.")

	_, err = ki.KeyImport([]byte(nil), &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid raw. It must not be nil.")

	_, err = ki.KeyImport([]byte{0}, &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Failed converting PKIX to ECDSA public key")

	k, err := sm2.GenerateKey(rand.Reader)
	require.NoError(t, err)
	km, _ := utils.PrivateKeyToDER(k)
	_, err = ki.KeyImport(km, &mocks2.KeyImportOpts{})
	require.NoError(t, err)
}

func TestGmsm2PublicKeyImportOptsKeyImporter_KeyImport(t *testing.T) {
	t.Parallel()

	ki := gmsm2PublicKeyImportOptsKeyImporter{}
	_, err := ki.KeyImport("Hello World", &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "[GMSM2PublicKeyImportOpts] Invalid raw material. Expected byte array.")

	_, err = ki.KeyImport(nil, &mocks2.KeyImportOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "[GMSM2PublicKeyImportOpts] Invalid raw material. Expected byte array.")

	k, _ := sm2.GenerateKey(rand.Reader)
	kp := k.PublicKey
	kpm, _ := utils.PublicKeyToDER(&kp)
	_, err = ki.KeyImport(kpm, &mocks2.KeyImportOpts{})
	require.NoError(t, err)
}
