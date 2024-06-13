/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ccprovider

// LoadPackage 从文件系统加载链码包
func LoadPackage(ccNameVersion string, path string, getHasher GetHasher) (CCPackage, error) {
	return (&CCInfoFSImpl{GetHasher: getHasher}).GetChaincodeFromPath(ccNameVersion, path)
}
