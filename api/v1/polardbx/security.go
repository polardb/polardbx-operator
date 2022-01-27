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

package polardbx

import corev1 "k8s.io/api/core/v1"

// TLS defines the TLS config.
type TLS struct {
	// SecretName of the TLS config's secret.
	SecretName string `json:"secretName,omitempty"`

	// GenerateSelfSigned represents if let the operator generate and use a self-signed cert.
	GenerateSelfSigned bool `json:"generateSelfSigned,omitempty"`
}

// Security represents the security config of the cluster.
type Security struct {
	// TLS defines the TLS config of the access port.
	// +optional
	TLS *TLS `json:"tls,omitempty"`

	// EncodeKey defines the encode key used by the cluster. If not provided,
	// operator will generate a random key.
	// +optional
	EncodeKey *corev1.SecretKeySelector `json:"encodeKey,omitempty"`
}
