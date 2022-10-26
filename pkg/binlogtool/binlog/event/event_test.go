/*
Copyright 2022 Alibaba Group Holding Limited.

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

package event

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
)

func jsonPrettyFormat(d any) string {
	r, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(r)
}

var fdeData = []byte{
	// binlog version
	4, 0,
	// server version
	53, 46, 55, 46, 49, 52, 45, 65, 108, 105,
	83, 81, 76, 45, 88, 45, 67, 108, 117, 115,
	116, 101, 114, 45, 49, 46, 54, 46, 49, 46,
	48, 45, 50, 48, 50, 49, 49, 50, 49, 53,
	45, 108, 111, 103, 0, 0, 0, 0, 0, 0,
	// timestamp
	0, 0, 0, 0,
	// header length
	19,
	// post header length
	56, 13, 0, 8, 0, 18, 0, 4, 4, 4,
	4, 18, 0, 0, 161, 0, 4, 26, 8, 0,
	0, 0, 8, 8, 8, 2, 0, 0, 0, 10,
	10, 10, 42, 42, 0, 18, 52, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
	0, 0, 0, 0, 1, 8, 190, 225, 210}

func TestDeclAndExtract(t *testing.T) {
	e := FormatDescriptionEvent{}
	l := e.Layout(spec.V4, 0, nil)
	_, err := l.FromBlock(fdeData)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(jsonPrettyFormat(e))
}

func BenchmarkDecl(b *testing.B) {
	e := FormatDescriptionEvent{}
	for i := 0; i < b.N; i++ {
		e.Layout(spec.V4, 0, nil)
	}
}

func BenchmarkExtract(b *testing.B) {
	e := FormatDescriptionEvent{}
	l := e.Layout(spec.V4, 0, nil)
	for i := 0; i < b.N; i++ {
		_, err := l.FromBlock(fdeData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeclAndExtract(b *testing.B) {
	for i := 0; i < b.N; i++ {
		e := FormatDescriptionEvent{}
		l := e.Layout(spec.V4, 0, nil)
		_, err := l.FromBlock(fdeData)
		if err != nil {
			b.Fatal(err)
		}
	}
}
