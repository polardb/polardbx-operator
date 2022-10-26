/*
Copyright 2022 Alibaba Group Holding Limited.

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

package meta

import (
	"fmt"
)

// BinaryLogFilesConsistentAndContinuousInName determines whether the given binlog files forms a continuous
// binlog flow by checking the prefix and index in the filenames, according to the official doc located at
// https://dev.mysql.com/doc/internals/en/binary-log-structure-and-contents.html.
//
// It also returns nil if the filename slice is nil or empty.
func BinaryLogFilesConsistentAndContinuousInName(filenames []string) ([]BinlogFile, error) {
	binlogFiles, err := ParseBinlogFilesAndSortByIndex(filenames...)
	if err != nil {
		return nil, err
	}

	// Check basename and index.
	basename, lastIndex := binlogFiles[0].Basename, binlogFiles[0].Index
	for i := 1; i < len(binlogFiles); i++ {
		if binlogFiles[i].Basename != basename {
			return nil, fmt.Errorf("binlog filenames not consistent: expect to have basename %s, but found %s", basename, binlogFiles[i].Basename)
		}
		if binlogFiles[i].Index != lastIndex+1 {
			return nil, fmt.Errorf("binlog filenames not continuous at %d", lastIndex+1)
		}
		lastIndex++
	}

	return binlogFiles, nil
}
