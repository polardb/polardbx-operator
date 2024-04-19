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

package polardbxbackup

import (
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
)

func SetupWebhooks(ctx context.Context, mgr ctrl.Manager, apiPath string, configLoader func() config.Config) error {
	gvk := schema.GroupVersionKind{
		Group:   polardbxv1.GroupVersion.Group,
		Version: polardbxv1.GroupVersion.Version,
		Kind:    "PolarDBXBackup",
	}

	mgr.GetWebhookServer().Register(extension.GenerateValidatePath(apiPath, gvk),
		extension.WithCustomValidator(&polardbxv1.PolarDBXBackup{},
			NewPolarDBXBackupValidator(mgr.GetAPIReader(),
				mgr.GetLogger().WithName("webhook.validate.polardbxbackup"),
				configLoader), mgr.GetScheme()))
	return nil
}
