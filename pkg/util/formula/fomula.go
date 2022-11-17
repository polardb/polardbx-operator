package formula

import (
	"errors"
	"strconv"
	"strings"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/util/math"
)

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
func formulaComputing(valueStr string, memory, cpu, storage int) (int, error) {
	if valueStr[len(valueStr)-1] != '}' {
		return 0, errors.New("invalid format")
	}

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

	var result int
	var err error
	if len(numsCalculate) > 1 {
		exists := false
		for k, v := range math.Funcs {
			if strings.Contains(valueStr, k) {
				nums := make([]int, 0)
				for _, numStr := range numsCalculate {
					nums = append(nums, math.Calculate(strings.ReplaceAll(numStr, k, "")))
				}
				result, err = v.(func([]int) (int, error))(nums)
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
		result = math.Calculate(numsCalculate[0])
	}

	return result, nil
}

func FormulaComputingPolarDBXCluster(valueStr string, polardbxcluster *polardbxv1.PolarDBXCluster) (int, error) {
	memory := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Memory().Value())
	cpu := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Cpu().Value())
	storage := int(polardbxcluster.Spec.Topology.Nodes.DN.Template.Resources.Limits.Storage().Value())

	return formulaComputing(valueStr, memory, cpu, storage)
}

func FormulaComputingXStore(valueStr string, xstore *polardbxv1.XStore) (int, error) {
	memory := int(xstore.Spec.Topology.NodeSets[0].Template.Spec.Resources.Limits.Memory().Value())
	cpu := int(xstore.Spec.Topology.NodeSets[0].Template.Spec.Resources.Limits.Cpu().Value())
	storage := int(xstore.Spec.Topology.NodeSets[0].Template.Spec.Resources.Limits.Storage().Value())

	return formulaComputing(valueStr, memory, cpu, storage)
}
