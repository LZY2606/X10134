// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

// snapshotServerConns returns the currently registered std server conns.
func snapshotServerConns(g *Engine, n int) []*Conn {
	ret := make([]*Conn, 0, n)
	g.mux.Lock()
	for c := range g.connsStd {
		if c != nil && c.typ == ConnTypeTCP {
			ret = append(ret, c)
		}
	}
	g.mux.Unlock()
	sortConnsByID(ret)
	return ret
}
