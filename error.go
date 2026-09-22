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

	// engineClosing is returned internally when a conn is added after Stop
	// has taken its conn snapshot; it is never delivered to user callbacks.
	engineClosing = errors.New("engine is closing")
)
