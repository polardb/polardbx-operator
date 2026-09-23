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

package parameter

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestIntOrStringSupportedValues(t *testing.T) {
	//valueStr := "{LEAST(DBInstanceClassMemory/1048576*128, 262144)}"
	//valueStr := "{DBInstanceClassMemory*3/4}"
	valueStr := "{GREATEST(DBInstanceClassMemory/1048576/128, 262144)}"

	memory := 1024 * 1024 * 1024 * 4
	cpu := 2
	storage := 1024 * 1024 * 1024

	r := strings.NewReplacer("{", "", "}", "", "(", "", ")", "", " ", "")
	valueStr = r.Replace(valueStr)

	if strings.Contains(valueStr, "DBInstanceClassMemory") {
		valueStr = strings.Replace(valueStr, "DBInstanceClassMemory", strconv.Itoa(memory), -1)
	} else if strings.Contains(valueStr, "DBInstanceClassCPU") {
		valueStr = strings.Replace(valueStr, "DBInstanceClassCPU", strconv.Itoa(cpu), -1)
	} else if strings.Contains(valueStr, "AllocatedStorage") {
		valueStr = strings.Replace(valueStr, "AllocatedStorage", strconv.Itoa(storage), -1)
	} else {
		panic(fmt.Errorf("error format"))
	}

	numsCalculate := strings.Split(valueStr, ",")

	var result int
	var err error
	if len(numsCalculate) > 1 {
		exists := false
		for k, v := range funcs {
			if strings.Contains(valueStr, k) {
				nums := make([]int, 0)
				for _, numStr := range numsCalculate {
					nums = append(nums, int(calculate(strings.ReplaceAll(numStr, k, ""))))
				}
				result, err = v.(func([]int) (int, error))(nums)
				if err != nil {
					panic(err)
				}
				exists = true
			}
		}
		if !exists {
			panic(fmt.Errorf("error format"))
		}
	} else {
		result = int(calculate(numsCalculate[0]))
	}

	fmt.Println("result: ", result)

}
