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

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	. "github.com/onsi/gomega"
	"testing"
)

func TestCommon(t *testing.T) {
	result := map[string]discovery.HostInfo{}
	if err := json.Unmarshal([]byte("{\"cn-beijing.192.168.0.1\":{\"NodeName\":\"cn-beijing.192.168.0.1\",\"HpfsHost\":\"192.168.0.18\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.120\":{\"NodeName\":\"cn-beijing.192.168.0.120\",\"HpfsHost\":\"192.168.0.145\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.121\":{\"NodeName\":\"cn-beijing.192.168.0.121\",\"HpfsHost\":\"192.168.0.12\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.122\":{\"NodeName\":\"cn-beijing.192.168.0.122\",\"HpfsHost\":\"192.168.0.142\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.252\":{\"NodeName\":\"cn-beijing.192.168.0.252\",\"HpfsHost\":\"192.168.0.6\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.3\":{\"NodeName\":\"cn-beijing.192.168.0.3\",\"HpfsHost\":\"192.168.0.26\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.4\":{\"NodeName\":\"cn-beijing.192.168.0.4\",\"HpfsHost\":\"192.168.0.42\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.43\":{\"NodeName\":\"cn-beijing.192.168.0.43\",\"HpfsHost\":\"192.168.0.114\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.44\":{\"NodeName\":\"cn-beijing.192.168.0.44\",\"HpfsHost\":\"192.168.0.113\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.5\":{\"NodeName\":\"cn-beijing.192.168.0.5\",\"HpfsHost\":\"192.168.0.23\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.51\":{\"NodeName\":\"cn-beijing.192.168.0.51\",\"HpfsHost\":\"192.168.0.81\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.74\":{\"NodeName\":\"cn-beijing.192.168.0.74\",\"HpfsHost\":\"192.168.0.82\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.76\":{\"NodeName\":\"cn-beijing.192.168.0.76\",\"HpfsHost\":\"192.168.0.111\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.77\":{\"NodeName\":\"cn-beijing.192.168.0.77\",\"HpfsHost\":\"192.168.0.84\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643},\"cn-beijing.192.168.0.78\":{\"NodeName\":\"cn-beijing.192.168.0.78\",\"HpfsHost\":\"192.168.0.15\",\"HpfsPort\":6543,\"SshPort\":22,\"FsPort\":6643}}}"), &result); err != nil {
		fmt.Println(err)
	}
}

func TestCommon1(t *testing.T) {
	g := NewGomegaWithT(t)
	host, port := ParseNetAddr("127.0.0.1:10 ")
	g.Expect(host).Should(BeEquivalentTo("127.0.0.1"))
	g.Expect(port).Should(BeEquivalentTo(10))
}

func TestCommon2(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeEquivalentTo("invalid addr 127.0.0.110 "))
	}()
	ParseNetAddr("127.0.0.110 ")
}

func TestCommon3(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeEquivalentTo("invalid addr 127.0.0.1:a10  strconv.Atoi: parsing \"a10\": invalid syntax"))
	}()
	ParseNetAddr("127.0.0.1:a10 ")
}
