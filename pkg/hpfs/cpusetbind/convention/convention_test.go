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
	. "github.com/onsi/gomega"
	"testing"
)

func TestParseKVConfig(t *testing.T) {
	g := NewGomegaWithT(t)
	configContent :=
		`cpuset-scheduler="true"
kubernetes.io/config.seen="2024-03-12T20:03:37.659417039+08:00"
kubernetes.io/config.source="api"
kubernetes.io/psp="ack.privileged"
pod-template-hash="5b7598c788"
polardbx/cn-type="rw"

polardbx/enableAuditLog="false"
polardbx/group="host-0"
polardbx/name="busu-pxchostnet"

polardbx/port-lock="9608"
polardbx/rand="tdjv"
polardbx/role="cn"

`
	result := ParseKVConfig([]byte(configContent))
	g.Expect(result["cpuset-scheduler"]).Should(BeEquivalentTo("true"))
	g.Expect(result["kubernetes.io/config.seen"]).Should(BeEquivalentTo("2024-03-12T20:03:37.659417039+08:00"))
	g.Expect(result["kubernetes.io/config.source"]).Should(BeEquivalentTo("api"))
	g.Expect(result["kubernetes.io/psp"]).Should(BeEquivalentTo("ack.privileged"))
	g.Expect(result["pod-template-hash"]).Should(BeEquivalentTo("5b7598c788"))
	g.Expect(result["polardbx/cn-type"]).Should(BeEquivalentTo("rw"))
	g.Expect(result["polardbx/enableAuditLog"]).Should(BeEquivalentTo("false"))
	g.Expect(result["polardbx/group"]).Should(BeEquivalentTo("host-0"))
	g.Expect(result["polardbx/name"]).Should(BeEquivalentTo("busu-pxchostnet"))
	g.Expect(result["polardbx/port-lock"]).Should(BeEquivalentTo("9608"))
	g.Expect(result["polardbx/rand"]).Should(BeEquivalentTo("tdjv"))
	g.Expect(result["polardbx/role"]).Should(BeEquivalentTo("cn"))
}
