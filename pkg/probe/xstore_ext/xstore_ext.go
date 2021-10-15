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

package xstore_ext

import (
	"context"
	"database/sql"
	"sync"
)

type XStoreExt interface {
	Readiness(ctx context.Context, host string, db *sql.DB) error
}

var (
	mu           sync.RWMutex
	xstoreExtMap = make(map[string]XStoreExt)
)

func RegisterXStoreExt(name string, ext XStoreExt) {
	mu.Lock()
	defer mu.Unlock()
	xstoreExtMap[name] = ext
}

func GetXStoreExt(name string) XStoreExt {
	mu.RLock()
	defer mu.RUnlock()
	return xstoreExtMap[name]
}
