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

package path

import "strings"

func NewPathFromStringSequence(sequence ...string) string {
	return strings.Join(sequence, "/")
}

// GetBaseNameFromPath gets last non-empty token in the path
func GetBaseNameFromPath(path string) string {
	sequence := strings.Split(path, "/")
	for i := len(sequence) - 1; i >= 0; i-- {
		if len(sequence[i]) != 0 {
			return sequence[i]
		}
	}
	return ""
}
