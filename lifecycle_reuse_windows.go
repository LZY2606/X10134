//go:build windows
// +build windows

package nbio

import "syscall"

func reuseAddrControl(network, address string, c syscall.RawConn) error {
	return nil
}
