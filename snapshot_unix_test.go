// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// snapshotServerConns returns the currently registered unix server conns.
func snapshotServerConns(g *Engine, n int) []*Conn {
	ret := make([]*Conn, 0, n)
	g.mux.Lock()
	for _, c := range g.connsUnix {
		if c != nil && c.typ == ConnTypeTCP {
			ret = append(ret, c)
		}
	}
	g.mux.Unlock()
	sortConnsByID(ret)
	return ret
}
