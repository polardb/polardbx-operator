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

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/rand"
)

var (
	lockedPortsMu sync.Mutex
	lockedPorts   = make(map[int]interface{})
)

func AcquireLocalPort() int {
	lockedPortsMu.Lock()
	defer lockedPortsMu.Unlock()

	const min, max = 5000, 6000
	if len(lockedPorts) >= max-min {
		panic("exhaust")
	}

	for {
		port := rand.IntnRange(min, max)
		if _, ok := lockedPorts[port]; !ok {
			lockedPorts[port] = nil
			return port
		}
	}
}

func ReleaseLocalPort(port int) {
	lockedPortsMu.Lock()
	defer lockedPortsMu.Unlock()

	delete(lockedPorts, port)
}
