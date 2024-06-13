/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package implicitcollection

import (
	"strings"
)

const (
	prefix = "_implicit_org_"
)

// NameForOrg 构造指定组织的隐式集合的名称
func NameForOrg(mspid string) string {
	return prefix + mspid
}

// MspIDIfImplicitCollection returns <true, mspid> if the specified name is a valid implicit collection name
// else it returns <false, "">
func MspIDIfImplicitCollection(collectionName string) (isImplicitCollection bool, mspid string) {
	if !strings.HasPrefix(collectionName, prefix) {
		return false, ""
	}
	return true, collectionName[len(prefix):]
}

func IsImplicitCollection(collectionName string) bool {
	return strings.HasPrefix(collectionName, prefix)
}
