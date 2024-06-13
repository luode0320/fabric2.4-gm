package osnadmin

import (
	"crypto/tls"
	"github.com/Hyperledger-TWGC/tjfoc-gm/gmtls"
	"net/http"
)

type Transport struct {
	// TLSClientConfig specifies the TLS configuration to use with
	// tls.Client.
	// If nil, the default configuration is used.
	// If non-nil, HTTP/2 support may not be enabled by default.
	TLSClientConfig *gmtls.Config
	HttpTransport   *http.Transport
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.HttpTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.TLS = &tls.ConnectionState{
		VerifiedChains: nil,
	}
	return resp, err
}
