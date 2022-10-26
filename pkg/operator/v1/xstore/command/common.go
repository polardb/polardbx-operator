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

package command

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func CheckConnectivityLocally(rc *xstorev1reconcile.Context, pod *corev1.Pod, container string, logger logr.Logger) error {
	cmd := NewCanonicalCommandBuilder().Ping().Build()
	return rc.ExecuteCommandOn(pod, container, cmd, control.ExecOptions{
		Logger: logger,
	})
}

func UpdateMycnfParameters(rc *xstorev1reconcile.Context, pod *corev1.Pod, container string, logger logr.Logger) error {
	cmd := NewCanonicalCommandBuilder().MyConfig().Build()
	return rc.ExecuteCommandOn(pod, container, cmd, control.ExecOptions{
		Logger: logger,
	})
}

func CheckMycnfOverrideUpdates(rc *xstorev1reconcile.Context, pod *corev1.Pod, container string, logger logr.Logger, expectedParams map[string]string) (bool, error) {
	cmd := []string{
		"sh",
		"-c",
		"cat /data/config/my.cnf.override",
	}
	stdout := new(bytes.Buffer)
	err := rc.ExecuteCommandOn(pod, container, cmd, control.ExecOptions{
		Logger: logger,
		Stdout: stdout,
	})
	if err != nil {
		return false, err
	}

	configs := strings.Split(strings.ReplaceAll(stdout.String(), " ", ""), "\n")

	nowConfigs := make(map[string]string)
	for _, config := range configs {
		if config == "" || config[0] == '[' {
			continue
		}
		kv := strings.Split(config, "=")
		if len(kv) == 2 {
			nowConfigs[kv[0]] = kv[1]
		}
	}

	for k, v := range expectedParams {
		if nowConfigs[k] == v {
			return true, nil
		}
	}

	return false, nil
}

func ParseCommandResultGenerally(commandResult string) ([]map[string]interface{}, error) {
	lines := strings.Split(commandResult, "\n")
	//check line count
	if len(lines) == 0 {
		return nil, fmt.Errorf("no lines")
	}
	//parse header
	header := strings.Split(lines[0], "|")
	result := make([]map[string]interface{}, 0)
	for i := 1; i < len(lines); i++ {
		if lines[i] == "" && i == len(lines)-1 {
			break
		}
		row := strings.Split(lines[i], "|")
		if len(row) != len(header) {
			return nil, fmt.Errorf("invalid data, header len %d, row len %d", len(header), len(row))
		}
		rowResult := map[string]interface{}{}
		for j := 0; j < len(row); j++ {
			rowResult[strings.TrimSpace(header[j])] = strings.TrimSpace(row[j])
		}
		result = append(result, rowResult)
	}
	return result, nil
}
