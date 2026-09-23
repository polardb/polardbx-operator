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

package importbackupset

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func GetDownloadUrl(addresses ...string) string {
	var val atomic.Value
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	for _, address := range addresses {
		if address == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			parsedUrl, err := url.Parse(address)
			if err != nil {
				panic(err)
			}
			hostPort := parsedUrl.Host
			if !strings.Contains(hostPort, ":") {
				hostPort = hostPort + ":443"
			}
			conn, err := net.DialTimeout("tcp", hostPort, 2*time.Second)
			if err != nil {
				fmt.Println("dial err for " + hostPort)
				return
			}
			defer conn.Close()
			val.CompareAndSwap(nil, address)
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(10 * time.Millisecond):
			if val.Load() != nil {
				return val.Load().(string)
			}
			break
		}
	}
}
