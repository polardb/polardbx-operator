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

package convention

import (
	"errors"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
)

const RootAccount = "polardbx_root"

// Conventions for services.

type ServiceType string

const (
	ServiceTypeReadWrite ServiceType = "readwrite"
	ServiceTypeReadOnly  ServiceType = "readonly"
	ServiceTypeMetrics   ServiceType = "metrics"
)

func GetPolarDBXServiceName(polardbx *polardbxv1.PolarDBXCluster) string {
	if polardbx.Spec.ServiceName == "" {
		return polardbx.Name
	} else {
		return polardbx.Spec.ServiceName
	}
}

func NewServiceName(polardbx *polardbxv1.PolarDBXCluster, serviceType ServiceType) string {
	polardbxServiceName := GetPolarDBXServiceName(polardbx)

	switch serviceType {
	case ServiceTypeReadWrite:
		return polardbxServiceName
	case ServiceTypeReadOnly:
		return polardbxServiceName + "-ro"
	case ServiceTypeMetrics:
		return polardbxServiceName + "-metrics"
	}
	panic("invalid service type: " + serviceType)
}

// Conventions for secrets.

type SecretType string

const (
	SecretTypeAccount  SecretType = "account"
	SecretTypeSecurity SecretType = "security"
)

func NewSecretName(polardbx *polardbxv1.PolarDBXCluster, secretType SecretType) string {
	switch secretType {
	case SecretTypeAccount:
		return polardbx.Name
	case SecretTypeSecurity:
		return fmt.Sprintf("%s-%s-%s", polardbx.Name, polardbx.Status.Rand, SecretTypeSecurity)
	default:
		panic("invalid secret type: " + secretType)
	}
}

const (
	SecretKeyEncodeKey = "security.encode-key"
)

// Conventions for configs.

const (
	ConfigKeyForCN = "cn.dynamic"
	ConfigKeyForDN = "dn.override"
)

type ConfigMapType string

const (
	ConfigMapTypeConfig ConfigMapType = "config"
	ConfigMapTypeTask   ConfigMapType = "task"
)

func NewConfigMapName(polardbx *polardbxv1.PolarDBXCluster, cmType ConfigMapType) string {
	return fmt.Sprintf("%s-%s-%s", polardbx.Name, polardbx.Status.Rand, cmType)
}

// Conventions for containers.

const (
	PortAccess  = "mysql"
	PortMetrics = "metrics"
	PortDebug   = "debug"
)

const (
	ContainerInit     = "init"
	ContainerEngine   = "engine"
	ContainerExporter = "exporter"
	ContainerProber   = "prober"
)

// Conventions for XStore names.

func NewGMSName(polardbx *polardbxv1.PolarDBXCluster) string {
	return fmt.Sprintf("%s-%s-gms", polardbx.Name, polardbx.Status.Rand)
}

func NewDNName(polardbx *polardbxv1.PolarDBXCluster, i int) string {
	return fmt.Sprintf("%s-%s-dn-%d", polardbx.Name, polardbx.Status.Rand, i)
}

// Conventions for labels.

func ConstLabels(polardbx *polardbxv1.PolarDBXCluster, role string) map[string]string {
	return map[string]string{
		polardbxmeta.LabelName: polardbx.Name,
		polardbxmeta.LabelRand: polardbx.Status.Rand,
		polardbxmeta.LabelRole: role,
	}
}

func ConstLabelsForDN(polardbx *polardbxv1.PolarDBXCluster, index int) map[string]string {
	return k8shelper.PatchLabels(
		ConstLabels(polardbx, polardbxmeta.RoleDN),
		map[string]string{
			polardbxmeta.LabelDNIndex: strconv.Itoa(index),
		},
	)
}

func ConstLabelsForCN(polardbx *polardbxv1.PolarDBXCluster, cnType string) map[string]string {
	return k8shelper.PatchLabels(
		ConstLabels(polardbx, polardbxmeta.RoleCN),
		map[string]string{
			polardbxmeta.LabelCNType: cnType,
		},
	)
}

func ConstLabelsForGMS(polardbx *polardbxv1.PolarDBXCluster) map[string]string {
	return ConstLabels(polardbx, polardbxmeta.RoleGMS)
}

func ConstLabelsForCDC(polardbx *polardbxv1.PolarDBXCluster) map[string]string {
	return ConstLabels(polardbx, polardbxmeta.RoleCDC)
}

func ParseIndexFromDN(xstore *polardbxv1.XStore) (int, error) {
	indexVal, ok := xstore.Labels[polardbxmeta.LabelDNIndex]
	if !ok {
		return 0, errors.New("label for dn index not found")
	}
	return strconv.Atoi(indexVal)
}

func MustParseIndexFromDN(xstore *polardbxv1.XStore) int {
	index, err := ParseIndexFromDN(xstore)
	if err != nil {
		panic(err)
	}
	return index
}

func ParseGroupFromDeployment(deploy *appsv1.Deployment) string {
	return deploy.Labels[polardbxmeta.LabelGroup]
}

// Convention for names.

func NewDeploymentName(polardbx *polardbxv1.PolarDBXCluster, role string, group string) string {
	if role != polardbxmeta.RoleCN && role != polardbxmeta.RoleCDC {
		panic("required role to be cn or cdc, but found " + role)
	}

	if group == "" {
		group = "default"
	}
	return fmt.Sprintf("%s-%s-%s-%s", polardbx.Name, polardbx.Status.Rand, role, group)
}

// Generation.

func LabelGeneration(polardbx *polardbxv1.PolarDBXCluster) map[string]string {
	return map[string]string{
		polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
	}
}

func GetGenerationLabelValue(object client.Object) (int64, error) {
	labels := object.GetLabels()
	val, ok := labels[polardbxmeta.LabelGeneration]
	if !ok {
		return 0, errors.New("generation label not found")
	}
	return strconv.ParseInt(val, 10, 64)
}

func CopyMetadataForUpdate(dest, src *metav1.ObjectMeta, generation int64) {
	src.DeepCopyInto(dest)
	dest.Labels = k8shelper.PatchLabels(dest.Labels, map[string]string{
		polardbxmeta.LabelGeneration: strconv.FormatInt(generation, 10),
	})
}
