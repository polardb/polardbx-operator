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

package loader

import (
	"context"

	bundle2 "github.com/alibaba/polardbx-operator/pkg/util/config/bundle"
)

type fileSystemLoader struct {
	path string
}

func (l *fileSystemLoader) Load(ctx context.Context) (bundle2.Bundle, error) {
	return bundle2.NewFileSystemBundle(l.path), nil
}

func NewFileSystemLoader(path string) Loader {
	return &fileSystemLoader{
		path: path,
	}
}
