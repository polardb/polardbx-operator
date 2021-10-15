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

package common

import corev1 "k8s.io/api/core/v1"

// ConfigMapValueSource defines a config map datasource.
type ConfigMapValueSource struct {
	corev1.LocalObjectReference `json:",inline" protobuf:"bytes,1,opt,name=localObjectReference"`

	// Key of the config map
	// +optional
	Key string `json:"key,omitempty"`
}

// ValueFrom defines a value from various external sources.
type ValueFrom struct {
	// ConfigMap value source. Ignored if not found.
	// +optional
	ConfigMap *ConfigMapValueSource `json:"configMap,omitempty"`
}

// Value defines a value.
type Value struct {
	// Value is the raw string value.
	// +optional
	Value *string `json:"value,omitempty"`

	// ValueFrom is the value from external source.
	ValueFrom *ValueFrom `json:"valueFrom,omitempty"`
}

func (v *Value) GetRaw() *string {
	if v == nil {
		return nil
	}
	return v.Value
}

func (v *Value) GetValueFrom() *ValueFrom {
	if v == nil {
		return nil
	}
	return v.ValueFrom
}
