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

package hint

import (
	"context"
	"os"
	"path"
	"sync"
	"time"
)

const rootPath = "/etc/operator/hints"

func load(hint string) (string, error) {
	instrPath := path.Join(rootPath, hint)
	b, err := os.ReadFile(instrPath)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

var once = sync.Once{}

var (
	hints = make(map[string]func(string, error))
	mu    sync.Mutex
)

func register(hint string, f func(string, error)) {
	mu.Lock()
	defer mu.Unlock()

	if f == nil {
		panic("nil process function")
	}
	_, ok := hints[hint]
	if ok {
		panic("duplicate instruction: " + hint)
	}
	hints[hint] = f
}

func loadAllAndProcess() {
	mu.Lock()
	defer mu.Unlock()

	for h, f := range hints {
		f(load(h))
	}
}

func StartLoader(ctx context.Context) {
	once.Do(func() {
		go func() {
			for {
				loadAllAndProcess()

				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}()
	})
}
