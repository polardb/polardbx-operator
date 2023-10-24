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

package polardbxcluster

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/yaml"

	configutil "github.com/alibaba/polardbx-operator/pkg/util/config"
	"github.com/alibaba/polardbx-operator/pkg/util/config/bundle"
	"github.com/alibaba/polardbx-operator/pkg/util/config/loader"
	"github.com/alibaba/polardbx-operator/pkg/util/config/store"
)

type DefaulterConfig struct {
	ProtocolVersion string `json:"protocol_version,omitempty"`
	StorageEngine   string `json:"storage_engine,omitempty"`
	ServiceType     string `json:"service_type,omitempty"`
	UpgradeStrategy string `json:"upgrade_strategy,omitempty"`
	OperatorVersion string `json:"operator_version,omitempty"`
}

type ValidatorConfig struct {
}

type WebhookAdmissionConfig struct {
	Defaulter DefaulterConfig `json:"default,omitempty"`
	Validator ValidatorConfig `json:"validator,omitempty"`
}

type WebhookAdmissionConfigLoaderFunc func() *WebhookAdmissionConfig

func NewConfigLoaderAndStartBackgroundRefresh(ctx context.Context, path string, logger logr.Logger) (WebhookAdmissionConfigLoaderFunc, error) {
	configStore := &store.Store{}
	driver := configutil.NewConfigWatchDriver(
		loader.NewFileSystemLoader(path),
		func(b bundle.Bundle) (interface{}, error) {
			r, err := bundle.GetOneFromBundle(b, "webhook.yaml", "webhook.json")
			if err != nil {
				return nil, err
			}

			var c WebhookAdmissionConfig
			decoder := yaml.NewYAMLOrJSONDecoder(r, 512)
			err = decoder.Decode(&c)
			if err != nil {
				return nil, err
			}

			return &c, nil
		},
		configutil.WatchDriverOptions{
			Logger:   logger,
			Interval: time.Second,
			Watchers: []configutil.Watcher{
				func(i interface{}) {
					configStore.Update(i)
				},
			},
		},
	)
	err := driver.Start(ctx)
	if err != nil {
		return nil, err
	}

	return func() *WebhookAdmissionConfig {
		v := configStore.Get()
		if v == nil {
			return nil
		}
		return v.(*WebhookAdmissionConfig)
	}, nil
}
