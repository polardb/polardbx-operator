package io

import "io"

func CopyBufferN(dst io.Writer, src io.Reader, n int64, buffer []byte) (written int64, err error) {
	written, err = io.CopyBuffer(dst, io.LimitReader(src, n), buffer)
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		// src stopped early; must have been EOF.
		err = io.EOF
	}
	return
}
