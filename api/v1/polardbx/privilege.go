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

type PrivilegeType string

// Valid privilege types.
const (
	ReadWrite PrivilegeType = "RW"
	ReadOnly  PrivilegeType = "RO"
	DDLOnly   PrivilegeType = "DDL"
	DMLOnly   PrivilegeType = "DML"
	Super     PrivilegeType = "SUPER"
)

// PrivilegeItem represents an item for privilege definition.
type PrivilegeItem struct {
	// Username for the account.
	Username string `json:"username,omitempty"`

	// Password for the account. The operator will generate a
	// random password if not provided.
	Password string `json:"password,omitempty"`

	// +kubebuilder:default="RW"
	// +kubebuilder:validation:Enum=RW;RO;DDL;DML;SUPER

	// Type represents the type of privilege, default is ReadWrite.
	// +optional
	Type PrivilegeType `json:"type,omitempty"`
}
