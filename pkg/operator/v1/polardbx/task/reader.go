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

package task

import (
	"encoding/json"
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type ContextAccess struct {
	taskCm *corev1.ConfigMap
	key    string
}

func (ca *ContextAccess) Read(v interface{}) (bool, error) {
	data, ok := ca.taskCm.Data[ca.key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal([]byte(data), v)
}

func (ca *ContextAccess) ReadAndReportErrIfNotFound(v interface{}) error {
	ok, err := ca.Read(v)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("not found")
	}
	return nil
}

func (ca *ContextAccess) Write(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if ca.taskCm.Data == nil {
		ca.taskCm.Data = make(map[string]string)
	}
	ca.taskCm.Data[ca.key] = string(data)
	return nil
}

func (ca *ContextAccess) Clear() bool {
	_, ok := ca.taskCm.Data[ca.key]
	if ok {
		delete(ca.taskCm.Data, ca.key)
	}
	return ok
}

func NewContextAccess(taskCm *corev1.ConfigMap, key string) *ContextAccess {
	return &ContextAccess{
		taskCm: taskCm,
		key:    key,
	}
}
