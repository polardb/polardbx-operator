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

package local

import "testing"

func Expect(t *testing.T, args ...interface{}) {
	if len(args)&1 > 0 {
		t.Fatalf("Args' length must be even")
	}

	middle := len(args) / 2
	for i := 0; i < middle; i++ {
		if args[i] != args[i+middle] {
			t.Fatalf("Expect equal, but is %+v and %+v", args[i], args[i+middle])
		}
	}
}

func TestIsOperationOnFileOrDirectoryPermitted(t *testing.T) {
	lfs, err := newLocalFileService([]string{
		"/cat",
		"/fish",
		"/usr/bin",
		"/usr/lib",
		"/usr/lib/grpc",
	})
	if err != nil {
		t.Fatalf("Unable to new local file service: %s", err.Error())
	}

	Expect(t, false, lfs.isOperationOnFileOrDirectoryPermitted("/usr"))
	Expect(t, true, lfs.isOperationOnFileOrDirectoryPermitted("/usr/bin"))
	Expect(t, true, lfs.isOperationOnFileOrDirectoryPermitted("/usr/lib/grpc/libgrpc.so"))
	Expect(t, false, lfs.isOperationOnFileOrDirectoryPermitted("/usr/lib64/brpc/libbrpc.so"))
	Expect(t, true, lfs.isOperationOnFileOrDirectoryPermitted("/cat/abc"))
	Expect(t, true, lfs.isOperationOnFileOrDirectoryPermitted("/fish/abc/123/a"))
	Expect(t, false, lfs.isOperationOnFileOrDirectoryPermitted("/"))
}
