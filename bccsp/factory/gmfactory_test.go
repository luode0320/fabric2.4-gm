package factory

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestGMFactoryName(t *testing.T) {
	f := &GMFactory{}
	require.Equal(t, f.Name(), GuomiBasedFactoryName)
}

func TestGMFactoryGetInvalidArgs(t *testing.T) {
	f := &GMFactory{}

	_, err := f.Get(nil)
	require.Error(t, err, "Invalid config. It must not be nil.")

	_, err = f.Get(&FactoryOpts{})
	require.Error(t, err, "Invalid config. It must not be nil.")

	// 国密工厂，若不是临时性秘钥，不需要配置信息
	opts := &FactoryOpts{
		SW: &SwOpts{},
	}
	_, err = f.Get(opts)
	require.Error(t, err, "CSP:500 - Failed initializing configuration at [0,]")
}

func TestGMFactoryGet(t *testing.T) {
	f := &GMFactory{}

	opts := &FactoryOpts{
		SW: &SwOpts{
			Security: 256,
			Hash:     "GMSM3",
		},
	}
	csp, err := f.Get(opts)
	require.NoError(t, err)
	require.NotNil(t, csp)

	opts = &FactoryOpts{
		SW: &SwOpts{
			Security:     256,
			Hash:         "GMSM3",
			FileKeystore: &FileKeystoreOpts{KeyStorePath: os.TempDir()},
		},
	}
	csp, err = f.Get(opts)
	require.NoError(t, err)
	require.NotNil(t, csp)

}
