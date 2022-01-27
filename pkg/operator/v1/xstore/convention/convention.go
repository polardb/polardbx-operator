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
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

// Conventions for services.

type ServiceType string

const (
	ServiceTypeReadWrite ServiceType = "readwrite"
	ServiceTypeReadOnly  ServiceType = "readonly"
	ServiceTypeMetrics   ServiceType = "metrics"
	ServiceTypeHeadless  ServiceType = "headless"
)

func GetXStoreServiceName(xstore *polardbxv1.XStore) string {
	if xstore.Spec.ServiceName == "" {
		return xstore.Name
	} else {
		return xstore.Spec.ServiceName
	}
}

func NewHeadlessServiceName(podName string) string {
	return podName
}

func NewServiceName(xstore *polardbxv1.XStore, serviceType ServiceType) string {
	xstoreServiceName := GetXStoreServiceName(xstore)

	switch serviceType {
	case ServiceTypeReadWrite:
		return xstoreServiceName
	case ServiceTypeReadOnly:
		return xstoreServiceName + "-ro"
	case ServiceTypeMetrics:
		return xstoreServiceName + "-metrics"
	}
	panic("invalid service type: " + serviceType)
}

// Conventions for port names.

const (
	PortAccess  = "mysql"
	PortMetrics = "metrics"
	PortProbe   = "probe"
)

// Conventions for secret.

const SuperAccount = "admin"

func NewSecretName(xstore *polardbxv1.XStore) string {
	return xstore.Name
}

// Convention for labels.

func ConstLabels(xstore *polardbxv1.XStore) map[string]string {
	labels := map[string]string{
		xstoremeta.LabelName: xstore.Name,
	}

	if xstore.Status.Rand != "" {
		labels[xstoremeta.LabelRand] = xstore.Status.Rand
	}

	return labels
}

func LabelGeneration(xstore *polardbxv1.XStore) map[string]string {
	return map[string]string{
		xstoremeta.LabelGeneration: strconv.FormatInt(xstore.Status.ObservedGeneration, 10),
	}
}

func DefaultRoleOf(nodeRole polardbxv1xstore.NodeRole) string {
	switch nodeRole {
	case polardbxv1xstore.RoleCandidate:
		return xstoremeta.RoleFollower
	case polardbxv1xstore.RoleVoter:
		return xstoremeta.RoleLogger
	case polardbxv1xstore.RoleLearner:
		return xstoremeta.RoleLearner
	default:
		panic("invalid node role: " + nodeRole)
	}
}

func ConstPodLabels(xstore *polardbxv1.XStore, nodeSet *polardbxv1xstore.NodeSet) map[string]string {
	return k8shelper.PatchLabels(
		ConstLabels(xstore),
		LabelGeneration(xstore),
		map[string]string{
			xstoremeta.LabelNodeRole: strings.ToLower(string(nodeSet.Role)),
			xstoremeta.LabelNodeSet:  nodeSet.Name,
		},
	)
}

func GetGenerationLabelValue(object client.Object) (int64, error) {
	labels := object.GetLabels()
	val, ok := labels[xstoremeta.LabelGeneration]
	if !ok {
		return 0, errors.New("generation label not found")
	}
	return strconv.ParseInt(val, 10, 64)
}

func IsGenerationOutdated(xstore *polardbxv1.XStore, object client.Object) (bool, error) {
	observedGeneration, err := GetGenerationLabelValue(object)
	if err != nil {
		return false, err
	}

	generation := xstore.Generation
	return observedGeneration < generation, nil
}

// Conventions for pods.

func NewPodName(xstore *polardbxv1.XStore, nodeSet *polardbxv1xstore.NodeSet, index int) string {
	// Dash linked string for empty parts [xstore name, xstore rand, node set name, index]

	podName := xstore.Name
	if xstore.Status.Rand != "" {
		podName += "-" + xstore.Status.Rand
	}
	if nodeSet.Name != "" {
		podName += "-" + nodeSet.Name
	}
	podName += "-" + strconv.Itoa(index)
	return podName
}

// Conventions for config.

const (
	ConfigMyCnfTemplate = "my.cnf.template"
	ConfigMyCnfOverride = "my.cnf.override"
)

type ConfigMapType string

const (
	ConfigMapTypeConfig ConfigMapType = "config"
	ConfigMapTypeShared ConfigMapType = "shared"
	ConfigMapTypeTask   ConfigMapType = "task"
)

func NewConfigMapName(xstore *polardbxv1.XStore, cmType ConfigMapType) string {
	if xstore.Status.Rand != "" {
		return fmt.Sprintf("%s-%s-%s", xstore.Name, xstore.Status.Rand, cmType)
	} else {
		return fmt.Sprintf("%s-%s", xstore.Name, cmType)
	}
}

// Conventions for containers.

const (
	ContainerEngine   = "engine"
	ContainerExporter = "exporter"
	ContainerProber   = "prober"
)

// Conventions for jobs.

func NewJobName(xstore *polardbxv1.XStore, name string) string {
	if xstore.Status.Rand != "" {
		return fmt.Sprintf("%s-%s-%s", xstore.Name, xstore.Status.Rand, name)
	} else {
		return fmt.Sprintf("%s-%s", xstore.Name, name)
	}
}
