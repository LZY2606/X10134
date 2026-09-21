// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio_test

// Lifecycle tests that depend on the *nix write cache (writeList): queued
// buffers being drained or released on close, and fixed-seed races.

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lesismal/nbio/mempool"
)

// trackingAllocator wraps a real allocator and records every live block so
// tests can detect:
//   - write callbacks observing an already freed/reused block
//     ("release buffer before completing write callback"),
//   - double Free,
//   - leaked blocks after a conn closes.
type trackingAllocator struct {
	inner mempool.Allocator

	mu    sync.Mutex
	live  map[uintptr]int // base -> size
	freed map[uintptr]int

	mallocN int64
	freeN   int64
	errMu   sync.Mutex
	errs    []string
}

func newTrackingAllocator() *trackingAllocator {
	return &trackingAllocator{
		inner: mempool.New(1024, 1024*1024*1024),
		live:  map[uintptr]int{},
		freed: map[uintptr]int{},
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	p := a.inner.Malloc(size)
	atomic.AddInt64(&a.mallocN, 1)
	base := basePtr(p)
	a.mu.Lock()
	a.live[base] = cap(*p)
	delete(a.freed, base)
	a.mu.Unlock()
	return p
}

func (a *trackingAllocator) Realloc(p *[]byte, size int) *[]byte {
	return a.inner.Realloc(p, size)
}

func (a *trackingAllocator) Append(p *[]byte, more ...byte) *[]byte {
	oldBase := basePtr(p)
	np := a.inner.Append(p, more...)
	if basePtr(np) != oldBase {
		// Reallocation path: new block is independent, the old one was
		// released by Append internally; nbio only hands np back to Free.
		atomic.AddInt64(&a.mallocN, 1)
		a.mu.Lock()
		a.live[basePtr(np)] = cap(*np)
		a.mu.Unlock()
	}
	return np
}

func (a *trackingAllocator) AppendString(p *[]byte, s string) *[]byte {
	return a.Append(p, []byte(s)...)
}

func (a *trackingAllocator) Free(p *[]byte) {
	base := basePtr(p)
	a.mu.Lock()
	if _, ok := a.live[base]; !ok {
		a.recordLocked(fmt.Sprintf("double or unknown free of block %x", base))
	} else {
		delete(a.live, base)
		a.freed[base] = cap(*p)
	}
	a.mu.Unlock()
	atomic.AddInt64(&a.freeN, 1)
	a.inner.Free(p)
}

// checkCallback verifies a block handed to OnWrittenSize is still a live,
// unmodified allocation belonging to this allocator.
func (a *trackingAllocator) checkCallback(b []byte) {
	if len(b) == 0 {
		return
	}
	base := basePtrOfSlice(b)
	a.mu.Lock()
	defer a.mu.Unlock()
	if size, ok := a.freed[base]; ok {
		_ = size
		a.recordLocked(fmt.Sprintf("write callback observed freed block %x len=%d", base, len(b)))
	}
}

func (a *trackingAllocator) recordLocked(msg string) {
	a.errMu.Lock()
	a.errs = append(a.errs, msg)
	a.errMu.Unlock()
}

func (a *trackingAllocator) errors() []string {
	a.errMu.Lock()
	defer a.errMu.Unlock()
	out := make([]string, len(a.errs))
	copy(out, a.errs)
	return out
}

func basePtr(p *[]byte) uintptr {
	return basePtrOfSlice(*p)
}

func basePtrOfSlice(b []byte) uintptr {
	if len(b) == 0 && cap(b) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&b[:1][0]))
}

// isPeerResetOrBroken reports whether err is one of the normal peer-side
// close causes a writer may observe (EOF from a half-close, or RST/EPIPE).
func isPeerCloseError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return isConnResetOrBroken(err)
}
