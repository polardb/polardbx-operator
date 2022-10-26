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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinaryLogFilesConsistentAndContinuousInName(t *testing.T) {
	testcases := map[string]struct {
		filenames []string
		expect    error
	}{
		"nil-filenames-slice": {
			filenames: nil,
			expect:    nil,
		},
		"empty-filenames-slice": {
			filenames: []string{},
			expect:    nil,
		},
		"one-filenames-slice": {
			filenames: []string{"HOSTNAME-bin.0000101"},
			expect:    nil,
		},
		"one-invalid-filenames-slice": {
			filenames: []string{"HOSTNAME-bin.b000101"},
			expect:    fmt.Errorf("failed to parse filename: %w", ErrInvalidBinaryLogFilenameFormat),
		},
		"valid-filenames": {
			filenames: []string{"HOSTNAME-bin.0000101", "HOSTNAME-bin.0000102", "HOSTNAME-bin.0000103"},
			expect:    nil,
		},
		"non-continuous-filenames": {
			filenames: []string{"HOSTNAME-bin.0000101", "HOSTNAME-bin.0000102", "HOSTNAME-bin.0000104"},
			expect:    fmt.Errorf("binlog filenames not continuous at %d", 103),
		},
		"non-consistent-filenames": {
			filenames: []string{"HOSTNAME-bin.0000101", "HOSTNAME-bin.0000102", "HOSTNAME-in.0000103"},
			expect:    fmt.Errorf("binlog filenames not consistent: expect to have basename %s, but found %s", "HOSTNAME-bin", "HOSTNAME-in"),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			_, err := BinaryLogFilesConsistentAndContinuousInName(tc.filenames)
			assert.Equal(t, tc.expect, err)
		})
	}
}
