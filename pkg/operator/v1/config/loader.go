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

package config

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configutil "github.com/alibaba/polardbx-operator/pkg/util/config"
	"github.com/alibaba/polardbx-operator/pkg/util/config/bundle"
	"github.com/alibaba/polardbx-operator/pkg/util/config/loader"
	"github.com/alibaba/polardbx-operator/pkg/util/config/store"
)

type LoaderFactory func() func() Config

type loaderConfig struct {
	loader loader.Loader
	logger logr.Logger
	store  *store.Store
}

type Option func(opts *loaderConfig)

func LoadFromPath(path string) Option {
	return func(opts *loaderConfig) {
		opts.loader = loader.NewFileSystemLoader(path)
	}
}

func LoadFromK8sConfigMap(client client.Client, key types.NamespacedName) Option {
	return func(opts *loaderConfig) {
		opts.loader = loader.NewK8sConfigMapLoader(
			loader.K8sOptions{
				Client: client,
				Key:    key,
			},
		)
	}
}

func LoadFromK8sSecret(client client.Client, key types.NamespacedName) Option {
	return func(opts *loaderConfig) {
		opts.loader = loader.NewK8sSecretLoader(
			loader.K8sOptions{
				Client: client,
				Key:    key,
			},
		)
	}
}

func WithLogger(logger logr.Logger) Option {
	return func(opts *loaderConfig) {
		opts.logger = logger
	}
}

func newConfigRefreshDriver(opts *loaderConfig) configutil.WatchDriver {
	return configutil.NewConfigWatchDriver(
		opts.loader,
		func(b bundle.Bundle) (interface{}, error) {
			r, err := bundle.GetOneFromBundle(b, "config.yaml", "config.json")
			if err != nil {
				return nil, err
			}

			var c config
			decoder := yaml.NewYAMLOrJSONDecoder(r, 512)
			err = decoder.Decode(&c)
			if err != nil {
				return nil, err
			}

			return &c, nil
		},
		configutil.WatchDriverOptions{
			Logger:   opts.logger,
			Interval: time.Second,
			Watchers: []configutil.Watcher{
				func(i interface{}) {
					opts.store.Update(i)
				},
			},
		},
	)
}

func NewConfigLoaderAndStartBackgroundRefresh(ctx context.Context, opts ...Option) (LoaderFactory, error) {
	loaderConfig := &loaderConfig{
		store: &store.Store{},
	}
	for _, o := range opts {
		o(loaderConfig)
	}

	driver := newConfigRefreshDriver(loaderConfig)
	err := driver.Start(ctx)
	if err != nil {
		return nil, err
	}

	s := loaderConfig.store
	return func() func() Config {
		s := s.Cached(true)
		return func() Config {
			v := s.Get()
			if v == nil {
				return nil
			}
			return v.(Config)
		}
	}, nil
}
