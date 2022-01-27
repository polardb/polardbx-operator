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

package jvm

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alibaba/polardbx-operator/third-party/hsperfdata"
)

func TestJvmStats_Parse(t *testing.T) {
	perf, err := hsperfdata.ReadPerfData("./testdata/181", true)
	if err != nil {
		t.Fatal(err)
	}

	jvmStats := JvmStats{}
	err = jvmStats.Parse(perf)
	if err != nil {
		t.Fatal(err)
	}

	js, _ := json.MarshalIndent(jvmStats, "", "  ")
	fmt.Printf("%s\n", string(js))
}

func TestJvmStats_Parse_Java11(t *testing.T) {
	perf, err := hsperfdata.ReadPerfData("./testdata/11/211", true)
	if err != nil {
		t.Fatal(err)
	}

	jvmStats := JvmStats{}
	err = jvmStats.Parse(perf)
	if err != nil {
		t.Fatal(err)
	}
	js, _ := json.MarshalIndent(jvmStats, "", "  ")
	fmt.Printf("%s\n", string(js))
}

func TestJvmStats_ParseAll(t *testing.T) {
	perfFiles := []string{
		"./testdata/181",
		"../hsperfdata/test-data/2036",
		"../hsperfdata/test-data/2956",
		"../hsperfdata/test-data/13223",
		"../hsperfdata/test-data/13984",
		"../hsperfdata/test-data/15192",
		"../hsperfdata/test-data/21916",
	}

	for _, f := range perfFiles {
		perf, err := hsperfdata.ReadPerfData(f, true)
		if err != nil {
			t.Fatalf("read err: " + f + ", " + err.Error())
		}

		jvmStats := JvmStats{}
		err = jvmStats.Parse(perf)
		if err != nil {
			t.Fatalf("parse err: " + f + ", " + err.Error())
		}
	}
}
