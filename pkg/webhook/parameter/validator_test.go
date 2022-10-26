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
					nums = append(nums, calculate(strings.ReplaceAll(numStr, k, "")))
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
		result = calculate(numsCalculate[0])
	}

	fmt.Println("result: ", result)

}
