// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
)

var (
	ErrReadTimeout = errors.New("read timeout")
	errReadTimeout = ErrReadTimeout

	ErrWriteTimeout = errors.New("write timeout")
	errWriteTimeout = ErrWriteTimeout

	ErrOverflow = errors.New("write overflow")
	errOverflow = ErrOverflow

	ErrDialTimeout = errors.New("dial timeout")

	ErrUnsupported = errors.New("unsupported operation")

	// errEngineStopping is returned when the Engine is stopping and no
	// longer accepts new connections.
	errEngineStopping = errors.New("engine is stopping")
)
