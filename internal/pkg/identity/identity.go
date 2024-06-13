/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package identity provides a set of interfaces for identity-related operations.

package identity

// Signer is an interface which wraps the Sign method.
//
// Sign signs message bytes and returns the signature or an error on failure.
type Signer interface {
	Sign(message []byte) ([]byte, error)
}

// Serializer 是包装序列化函数的接口。
//
// Serialize converts an identity to bytes.  It returns an error on failure.
type Serializer interface {
	Serialize() ([]byte, error)
}

//go:generate counterfeiter -o mocks/signer_serializer.go --fake-name SignerSerializer . SignerSerializer

// SignerSerializer 对Sign和Serialize方法进行分组。
type SignerSerializer interface {
	Signer
	Serializer
}
