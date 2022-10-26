package io

import (
	"encoding/binary"
	"io"
)

func WriteUint64(writer io.Writer, data uint64) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, data)
	_, err := writer.Write(bytes)
	return err
}
