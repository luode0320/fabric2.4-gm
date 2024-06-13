/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fileutil

// SyncDir 这是windows上的noop
func SyncDir(dirPath string) error {
	return nil
}
