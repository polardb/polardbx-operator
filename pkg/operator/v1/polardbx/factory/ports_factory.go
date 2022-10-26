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

package factory

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"k8s.io/apimachinery/pkg/util/rand"
)

type CNPorts struct {
	AccessPort  int
	MgrPort     int
	MppPort     int
	HtapPort    int
	LogPort     int
	DebugPort   int
	MetricsPort int
	ProbePort   int
}

type CDCPorts struct {
	DaemonPort  int
	MetricsPort int
}

type PortsFactory interface {
	NewPortsForCNEngine(mustStaticPorts bool) CNPorts
	NewPortsForCDCEngine() CDCPorts
}

type portsFactory struct {
	rc       *polardbxv1reconcile.Context
	polardbx *polardbxv1.PolarDBXCluster
}

func (f *portsFactory) NewPortsForCNEngine(mustStaticPorts bool) CNPorts {
	topology := f.polardbx.Status.SpecSnapshot.Topology
	template := topology.Nodes.CN.Template

	if template.HostNetwork && !mustStaticPorts {
		accessPort := (rand.IntnRange(3000, 13000) / 8) * 8
		return CNPorts{
			AccessPort:  accessPort,
			MgrPort:     accessPort + 1,
			MppPort:     accessPort + 2,
			HtapPort:    accessPort + 3,
			LogPort:     accessPort + 4,
			DebugPort:   accessPort + 5,
			MetricsPort: accessPort + 6,
			ProbePort:   accessPort + 7,
		}
	} else {
		return CNPorts{
			AccessPort:  3306,
			MgrPort:     3406,
			MppPort:     3506,
			HtapPort:    3606,
			LogPort:     8507,
			DebugPort:   5005,
			MetricsPort: 8081,
			ProbePort:   9090,
		}
	}
}

func (f *portsFactory) NewPortsForCDCEngine() CDCPorts {
	// FIXME Host network is ignored.
	return CDCPorts{
		DaemonPort:  3007,
		MetricsPort: 8081,
	}
}

func NewPortsFactory(rc *polardbxv1reconcile.Context, polardbx *polardbxv1.PolarDBXCluster) PortsFactory {
	return &portsFactory{
		rc: rc, polardbx: polardbx,
	}
}
