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

package process

import (
	"fmt"
	"strings"

	gops "github.com/shirou/gopsutil/v3/process"
)

func isJavaProcess(proc *gops.Process, mainClass string) (bool, error) {
	name, err := proc.Name()
	if err != nil {
		return false, fmt.Errorf("unable to get process name: %w", err)
	}
	cmd, err := proc.Cmdline()
	if err != nil {
		return false, fmt.Errorf("unable to get process command line: %w", err)
	}
	return name == "java" && strings.Contains(cmd, mainClass), nil
}

func GetPidForJavaProcess(mainClass string) (int, error) {
	// Scan the process list
	procs, err := gops.Processes()
	if err != nil {
		return -1, fmt.Errorf("unable to list processes: %w", err)
	}

	for _, proc := range procs {
		if ok, err := isJavaProcess(proc, mainClass); err != nil {
		} else if ok {
			return int(proc.Pid), nil
		}
	}

	return -1, nil
}

func CheckJavaProcessOrFind(pid int, mainClass string) (int, error) {
	// Check the given pid
	proc, err := gops.NewProcess(int32(pid))
	if err == nil {
		if ok, _ := isJavaProcess(proc, mainClass); ok {
			return pid, nil
		}
	}

	return GetPidForJavaProcess(mainClass)
}
