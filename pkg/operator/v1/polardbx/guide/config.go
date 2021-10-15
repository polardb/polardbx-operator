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

package guide

import (
	"errors"
	"strings"

	"k8s.io/apimachinery/pkg/util/yaml"

	v1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

func init() {
	registerHandler(polardbxmeta.AnnotationConfigGuide, &configHandler{})
}

type configHandler struct{}

const DefaultPolarDBXClusterConfigMap = "default-polardbx-cluster-config"

func (h *configHandler) parseConfigGuide(s string) (string, string) {
	params := strings.Split(s, "/")
	if len(params) > 1 {
		return params[0], params[1]
	} else {
		return "", s
	}
}

func (h *configHandler) loadConfig(rc *polardbxv1reconcile.Context, cmName string, key string) (*yaml.YAMLOrJSONDecoder, error) {
	// use default config map, if no cm name given
	if cmName == "" {
		cmName = DefaultPolarDBXClusterConfigMap
	}

	cm, err := rc.GetConfigMap(cmName)
	if err != nil {
		return nil, err
	}

	if _, ok := cm.Data[key]; !ok {
		return nil, errors.New("key " + key + " does not exist")
	}

	reader := strings.NewReader(cm.Data[key])
	return yaml.NewYAMLOrJSONDecoder(reader, len(cm.Data[key])), nil
}

func (h *configHandler) Handle(rc *polardbxv1reconcile.Context, obj *v1.PolarDBXCluster) error {
	configGuide := obj.Annotations[polardbxmeta.AnnotationConfigGuide]

	cmName, key := h.parseConfigGuide(configGuide)

	decoder, err := h.loadConfig(rc, cmName, key)
	if err != nil {
		return err
	}

	obj.Spec.Config = polardbxv1polardbx.Config{}
	err = decoder.Decode(&obj.Spec.Config)
	if err != nil {
		return err
	}

	delete(obj.Annotations, polardbxmeta.AnnotationConfigGuide)

	return nil
}
