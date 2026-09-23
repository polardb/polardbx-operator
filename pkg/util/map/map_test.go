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

package _map

import (
	. "github.com/onsi/gomega"
	"testing"
)

func TestMergeMapOverwriteFalse1(t *testing.T) {
	g := NewGomegaWithT(t)
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m2": "m1",
	}
	result := MergeMap(map1, map2, false).(map[string]string)
	g.Expect(result["m1"]).Should(BeEquivalentTo("m2"))
	g.Expect(result["m2"]).Should(BeEquivalentTo("m1"))
}

func TestMergeMapOverwriteFalse2(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeEquivalentTo("overwriting key is not allowed"))
	}()
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m1": "m1",
	}
	MergeMap(map1, map2, false)
}

func TestMapOverwriteTrue(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeNil())
	}()
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m1": "m1",
	}
	MergeMap(map1, map2, true)
}
