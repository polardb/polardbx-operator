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

package polardbxcluster

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"
)

func CalculateReplicas(total int, replicas intstr.IntOrString) (int, error) {
	if replicas.Type == intstr.Int {
		val := replicas.IntValue()
		return val, nil
	} else {
		s := replicas.StrVal
		if strings.HasSuffix(s, "%") || strings.HasSuffix(s, "%+") {
			var percentageStr string
			roundUp := false
			if s[len(s)-1] == '+' {
				percentageStr = s[:len(s)-2]
				roundUp = true
			} else {
				percentageStr = s[:len(s)-1]
			}
			percentage, err := strconv.Atoi(strings.TrimSpace(percentageStr))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a percentage, %w", err)
			}
			if percentage >= 100 {
				return 0, fmt.Errorf("invalid replicas: not a valid percentage, should be less than 1")
			}

			if roundUp {
				return (total*percentage + 99) / 100, nil
			} else {
				return total * percentage / 100, nil
			}
		} else if strings.Contains(s, "/") {
			split := strings.SplitN(s, "/", 2)
			if len(split) < 2 {
				return 0, fmt.Errorf("invalid replicas: not a fraction")
			}
			a, err := strconv.Atoi(strings.TrimSpace(split[0]))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a fraction, %w", err)
			}
			b, err := strconv.Atoi(strings.TrimSpace(split[1]))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a fraction, %w", err)
			}
			if a < 0 {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, numerator should be non-negative integer")
			}
			if b <= 0 {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, denominator should be positive integer")
			}
			if a >= b {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, should be less than 1")
			}
			return total * a / b, nil
		} else {
			val, err := strconv.Atoi(replicas.StrVal)
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: %w", err)
			}
			return val, nil
		}
	}
}
