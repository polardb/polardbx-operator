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

package math

import (
	"errors"
	"unicode"
)

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MinInt32(a, b int32) int32 {
	if a > b {
		return b
	}
	return a
}

func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

var Funcs = map[string]interface{}{
	"LEAST":    MinSlice,
	"GREATEST": MaxSlice,
	"SUM":      SumSlice,
}

func MinSlice(values []int) (min int, e error) {
	if len(values) == 0 {
		return 0, errors.New("cannot detect a minimum value in an empty slice")
	}

	min = values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}

	return min, nil
}

func MaxSlice(values []int) (max int, e error) {
	if len(values) == 0 {
		return 0, errors.New("cannot detect a maximum value in an empty slice")
	}

	max = values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}

	return max, nil
}

func SumSlice(values []int) (sum int, e error) {
	if len(values) == 0 {
		return 0, errors.New("cannot sum values in an empty slice")
	}

	sum = 0
	for _, v := range values {
		sum += v
	}

	return sum, nil
}

func Calculate(s string) int {
	var stk []int
	curr := 0
	operator := '+'

	for i, ch := range s {
		if unicode.IsDigit(ch) {
			curr = (curr * 10) + int(ch-'0')

			if i != len(s)-1 {
				continue
			}
		}

		if ch == ' ' && i != len(s)-1 {
			continue
		}

		switch operator {
		case '+':
			stk = append(stk, curr)
		case '-':
			stk = append(stk, -curr)
		case '*':
			newNum := stk[len(stk)-1] * curr
			stk[len(stk)-1] = newNum
		case '/':
			newNum := stk[len(stk)-1] / curr
			stk[len(stk)-1] = newNum
		}
		operator = ch
		curr = 0
	}

	res := 0
	for _, el := range stk {
		res += el
	}
	return res
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
