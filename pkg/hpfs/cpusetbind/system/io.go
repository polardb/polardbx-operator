/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
