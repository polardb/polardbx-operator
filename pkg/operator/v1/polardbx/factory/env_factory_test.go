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

package factory

import "testing"

const (
	KiloByte = int64(1) << 10
	MegaByte = int64(1) << 20
	GigaByte = int64(1) << 30
)

func TestEnvFactory_roundDownMemory(t *testing.T) {
	ef := &envFactory{}

	minMemory, leftForNonJvm := 2*GigaByte, 512*MegaByte

	testCases := map[string]struct {
		Total  int64
		Expect int64
	}{
		"2GB": {
			Total:  2 * GigaByte,
			Expect: 2 * GigaByte,
		},
		"6GB": {
			Total:  6 * GigaByte,
			Expect: 6 * GigaByte,
		},
		"24GB": {
			Total:  24 * GigaByte,
			Expect: 16 * GigaByte,
		},
		"28GB": {
			Total:  28 * GigaByte,
			Expect: 28 * GigaByte,
		},
		"50GB": {
			Total:  50 * GigaByte,
			Expect: 32 * GigaByte,
		},
		"64GB": {
			Total:  64 * GigaByte,
			Expect: 64 * GigaByte,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			containerAwareMemory := ef.roundDownMemory(tc.Total, minMemory, leftForNonJvm)
			if tc.Expect != containerAwareMemory {
				t.Fatalf("total %d, expect %d, but is %d", tc.Total, tc.Expect, containerAwareMemory)
			}
		})
	}
}
