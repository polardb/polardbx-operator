package cpusetbind

import (
	"io"
	"os"
	"strconv"
	"strings"
)

const NewLineSeparator = "\n"

func ReadFile(filepath string) (string, error) {
	f, err := os.OpenFile(filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func ReadIntFromFile(filepath string) (int64, error) {
	str, err := ReadFile(filepath)
	if err != nil {
		return -1, err
	}
	intVal, err := strconv.ParseInt(strings.TrimSpace(str), 10, 32)
	if err != nil {
		return -1, err
	}
	return intVal, nil
}
