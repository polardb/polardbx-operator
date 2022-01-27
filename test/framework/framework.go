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

package framework

import (
	"bytes"
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Framework struct {
	Ctx            context.Context
	Client         client.Client
	KubeConfigFile string
	Namespace      string
	Scheme         *runtime.Scheme
	serializer     *json.Serializer
}

func NewDefaultFramework(tc *testContext) *Framework {
	return &Framework{
		Ctx:            tc.Ctx,
		Client:         tc.Client,
		KubeConfigFile: tc.KubeConfigFile,
		Namespace:      tc.Namespace,
		Scheme:         scheme,
		serializer: json.NewSerializerWithOptions(
			json.DefaultMetaFactory, nil, nil,
			json.SerializerOptions{
				Yaml:   true,
				Pretty: true,
				Strict: true,
			},
		),
	}
}

func (f *Framework) ToYaml(obj client.Object) string {
	w := &bytes.Buffer{}
	_ = f.serializer.Encode(obj, w)
	return w.String()
}
