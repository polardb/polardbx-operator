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

package cache

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type ObjectLoadingCache interface {
	GetObject(ctx context.Context, name types.NamespacedName, obj client.Object) (client.Object, error)
	SetObject(object client.Object)
}

type objectCache struct {
	client            client.Client
	scheme            *runtime.Scheme
	kindNameObjectMap map[string]map[string]client.Object
}

func (c *objectCache) GetObject(ctx context.Context, name types.NamespacedName, obj client.Object) (client.Object, error) {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		panic("should never pass invalid objects")
	}
	objects, ok := c.kindNameObjectMap[gvk.String()]
	if !ok {
		err = c.client.Get(ctx, name, obj)
		if err != nil {
			return nil, err
		}
		c.SetObject(obj)
		return obj, nil
	}
	if obj, ok := objects[name.String()]; ok {
		return obj, nil
	}
	err = c.client.Get(ctx, name, obj)
	if err != nil {
		return nil, err
	}
	c.SetObject(obj)
	return obj, nil
}

func (c *objectCache) SetObject(object client.Object) {
	if object == nil {
		return
	}

	kind := object.GetObjectKind().GroupVersionKind()
	objects, ok := c.kindNameObjectMap[kind.String()]
	if !ok {
		objects = make(map[string]client.Object)
		c.kindNameObjectMap[kind.String()] = objects
	}

	name := types.NamespacedName{
		Namespace: object.GetNamespace(),
		Name:      object.GetName(),
	}

	objects[name.String()] = object
}

func NewObjectCache(c client.Client, s *runtime.Scheme) ObjectLoadingCache {
	return &objectCache{
		client:            c,
		scheme:            s,
		kindNameObjectMap: make(map[string]map[string]client.Object),
	}
}
