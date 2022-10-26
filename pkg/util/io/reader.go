package io

import "io"

func ReadBytes(reader io.Reader, len int, bytes []byte) (int, error) {
	readLen := 0
	for readLen = 0; readLen < len; {
		nowReadLen, err := reader.Read(bytes[readLen:])
		if nowReadLen == 0 {
			return readLen, err
		}
		readLen += nowReadLen
	}
	return readLen, nil
}
