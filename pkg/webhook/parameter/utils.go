package parameter

import (
	"errors"
	"strconv"
	"strings"
	"unicode"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

var funcs = map[string]interface{}{
	"LEAST":    min,
	"GREATEST": max,
	"SUM":      sum,
}

func min(values []int) (min int, e error) {
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

func max(values []int) (max int, e error) {
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

func sum(values []int) (sum int, e error) {
	if len(values) == 0 {
		return 0, errors.New("cannot sum values in an empty slice")
	}

	sum = 0
	for _, v := range values {
		sum += v
	}

	return sum, nil
}

func calculate(s string) int64 {
	var stk []int64
	curr := int64(0)
	operator := '+'

	for i, ch := range s {
		if unicode.IsDigit(ch) {
			curr = (curr * int64(10)) + int64(ch-'0')

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

	res := int64(0)
	for _, el := range stk {
		res += el
	}
	return res
}

/*
* 支持变量

AllocatedStorage：实例购买的存储空间大小，整数型
DBInstanceClassMemory：实例规格的内存大小，整数型
DBInstanceClassCPU：实例规格的CPU核数，整数型

* 支持公式

数据库参数公式支持两个运算符：除法和乘法。
除法运算符：/
用除数除以被除数，返回整数型商。商中的小数不四舍五入，直接截断。
语法
dividend / divisor
被除数和除数参数必须是整数型表达式。
乘法运算符：*
用除数除以被除数，返回整数型商。商中的小数不四舍五入，直接截断。
语法
expression * expression
两个表达式必须都是整数型。

* 支持函数

GREATEST()
返回整数型或者参数公式列表中最大的值。
语法
GREATEST(argument1, argument2,...argumentn)
返回整数。
LEAST()
返回整数型或者参数公式列表中最小的值。
语法
LEAST(argument1, argument2,...argumentn)
返回整数。
SUM()
添加指定整数型或者参数公式的值。
语法
SUM(argument1, argument2,...argumentn)
返回整数。

例如
innodb_buffer_pool_size = {DBInstanceClassMemory*3/4}
read_buffer_size = {LEAST(DBInstanceClassMemory/1048576*128, 262144)}
*/
func formulaComputing(valueStr string, polardbxcluster *polardbxv1.PolarDBXCluster) (int64, error) {
	if valueStr[len(valueStr)-1] != '}' {
		return 0, errors.New("invalid format")
	}

	memory := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Memory().Value())
	cpu := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Cpu().Value())
	storage := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Storage().Value())

	r := strings.NewReplacer("{", "", "}", "", "(", "", ")", "", " ", "")
	valueStr = r.Replace(valueStr)

	if strings.Contains(valueStr, "DBInstanceClassMemory") {
		valueStr = strings.Replace(valueStr, "DBInstanceClassMemory", strconv.Itoa(memory), -1)
	} else if strings.Contains(valueStr, "DBInstanceClassCPU") {
		valueStr = strings.Replace(valueStr, "DBInstanceClassCPU", strconv.Itoa(cpu), -1)
	} else if strings.Contains(valueStr, "AllocatedStorage") {
		valueStr = strings.Replace(valueStr, "AllocatedStorage", strconv.Itoa(storage), -1)
	} else {
		return 0, errors.New("error format, Invalid VarLabel")
	}

	numsCalculate := strings.Split(valueStr, ",")

	var result int64
	var err error
	if len(numsCalculate) > 1 {
		exists := false
		for k, v := range funcs {
			if strings.Contains(valueStr, k) {
				nums := make([]int64, 0)
				for _, numStr := range numsCalculate {
					nums = append(nums, calculate(strings.ReplaceAll(numStr, k, "")))
				}
				result, err = v.(func([]int64) (int64, error))(nums)
				if err != nil {
					return 0, err
				}
				exists = true
			}
		}
		if !exists {
			return 0, errors.New("error format, Invalid Formula")
		}
	} else {
		result = calculate(numsCalculate[0])
	}

	return result, nil
}
