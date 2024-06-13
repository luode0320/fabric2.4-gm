/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package container

import (
	"sync"
)

// BuildRegistry 用于注册链码构建状态。
type BuildRegistry struct {
	mutex  sync.Mutex              // 互斥锁，用于保护 builds 的并发访问
	builds map[string]*BuildStatus // 链码构建状态的映射表
}

// BuildStatus returns a BuildStatus for the ccid, and whether the caller
// is waiting in line (true), or this build status is new and their responsibility.
// If the build status is new, then the caller must call Notify with the error
// (or nil) upon completion.
func (br *BuildRegistry) BuildStatus(ccid string) (*BuildStatus, bool) {
	br.mutex.Lock()
	defer br.mutex.Unlock()
	if br.builds == nil {
		br.builds = map[string]*BuildStatus{}
	}

	bs, ok := br.builds[ccid]
	if !ok {
		bs = NewBuildStatus()
		br.builds[ccid] = bs
	}

	return bs, ok
}

// ResetBuildStatus returns a new BuildStatus for the ccid. This build status
// is new and the caller's responsibility. The caller must use external
// locking to ensure the build status is not reset by another install request
// and must call Notify with the error (or nil) upon completion.
func (br *BuildRegistry) ResetBuildStatus(ccid string) *BuildStatus {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	bs := NewBuildStatus()
	br.builds[ccid] = bs

	return bs
}

type BuildStatus struct {
	mutex sync.Mutex
	doneC chan struct{}
	err   error
}

func NewBuildStatus() *BuildStatus {
	return &BuildStatus{
		doneC: make(chan struct{}),
	}
}

func (bs *BuildStatus) Err() error {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	return bs.err
}

func (bs *BuildStatus) Notify(err error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	bs.err = err
	close(bs.doneC)
}

func (bs *BuildStatus) Done() <-chan struct{} {
	return bs.doneC
}
