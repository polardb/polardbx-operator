package cri

import (
	"bytes"
	"encoding/json"
	"fmt"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"time"
)

type Runtime interface {
	// Image present the image url of the container
	Image() string
	// Id is the identifier
	Id() string
	// CreatedAt is the time when the runtime is created
	CreatedAt() time.Time
	// Namespace presents the namespace int the kubernetes cluster
	Namespace() string
	// PolarDbXCluster presents the polardbx cluster name
	PolarDbXCluster() string
	// Pod presents the pod name
	Pod() string
	// ContainerResources presents the container resource
	ContainerResources() *v1.ContainerResources
	// CpuCores presents the cpu cores
	CpuCores() int
	// UseExec presents if use exec method to get cpu info
	UseExec() bool
	// String presents the string format of the runtime
	String() string
	// PreferredNuma presents the preferred Numa Node
	PreferredNuma() string
	// SetPreferredNuma set the value of preferred numa node
	SetPreferredNuma(preferredNuma string)
	// ForcedCpuSet presents the forced cpu set
	ForcedCpuSet() string
	// CompareStringVal presents the string value for compare
	CompareStringVal() string
}

type engineRuntime struct {
	image              string
	id                 string
	createdAt          time.Time
	namespace          string
	polardbXCluster    string
	pod                string
	containerResources *v1.ContainerResources
	useExec            bool
	preferredNuma      string
	forcedCpuSet       string
	compareStringVal   string
}

func (r *engineRuntime) Image() string {
	return r.image
}

func (r *engineRuntime) Id() string {
	return r.id
}

func (r *engineRuntime) CreatedAt() time.Time {
	return r.createdAt
}

func (r *engineRuntime) Namespace() string {
	return r.namespace
}

func (r *engineRuntime) PolarDbXCluster() string {
	return r.polardbXCluster
}

func (r *engineRuntime) Pod() string {
	return r.pod
}

func (r *engineRuntime) CpuCores() int {
	if r.containerResources != nil {
		linux := r.containerResources.Linux
		if linux.CpuPeriod != 0 && linux.CpuQuota != 0 {
			return int(linux.CpuQuota / linux.CpuPeriod)
		}
	}
	return 0
}

func (r *engineRuntime) ContainerResources() *v1.ContainerResources {
	return r.containerResources
}

func (r *engineRuntime) UseExec() bool {
	return r.useExec
}

func (r *engineRuntime) PreferredNuma() string {
	return r.preferredNuma
}

func (r *engineRuntime) SetPreferredNuma(preferredNuma string) {
	r.preferredNuma = preferredNuma
}

func (r *engineRuntime) ForcedCpuSet() string {
	return r.forcedCpuSet
}

func (r *engineRuntime) CompareStringVal() string {
	if r.compareStringVal == "" {
		r.compareStringVal = fmt.Sprintf("%s%s%s", r.forcedCpuSet, r.createdAt.String(), r.id)
	}
	return r.compareStringVal
}

func (r *engineRuntime) String() string {
	buffer := bytes.Buffer{}
	buffer.WriteString(fmt.Sprintf("image=%s", r.image))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("id=%s", r.id))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("createdAt=%s", r.createdAt.String()))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("namespace=%s", r.namespace))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("polardbxCluster=%s", r.polardbXCluster))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("pod=%s", r.pod))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("cpucores=%d", r.CpuCores()))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("preferNuma=%s", r.preferredNuma))
	buffer.WriteString(",")
	buffer.WriteString(fmt.Sprintf("forceCpuSet=%s", r.forcedCpuSet))
	buffer.WriteString(",")
	buffer.WriteString("containerResources=")
	if r.containerResources != nil {
		jsonBytes, _ := json.Marshal(r.containerResources)
		buffer.Write(jsonBytes)
	}
	return buffer.String()
}
