/*
Copyright 2022 Alibaba Group Holding Limited.

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

package webhook

import (
	"context"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/webhook/polardbxbackup"
	backupbinlog "github.com/alibaba/polardbx-operator/pkg/webhook/polardbxbackup/binlog"
	"net/http"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/alibaba/polardbx-operator/pkg/webhook/knobs"
	"github.com/alibaba/polardbx-operator/pkg/webhook/parameter"
	"github.com/alibaba/polardbx-operator/pkg/webhook/polardbxcluster"
)

const ApiPath = "/apis/admission.polardbx.aliyun.com/v1"

func SetupWebhooks(ctx context.Context, mgr ctrl.Manager, configPath string, configLoader func() config.Config) error {
	// Hack: for discovery. Awful hacking.
	mgr.GetWebhookServer().Register(ApiPath, http.HandlerFunc(
		func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
			w.WriteHeader(http.StatusOK)
		}),
	)

	if err := polardbxcluster.SetupWebhooks(ctx, mgr, configPath, ApiPath); err != nil {
		return err
	}

	if err := knobs.SetupWebhooks(ctx, mgr, ApiPath); err != nil {
		return err
	}

	if err := parameter.SetupWebhooks(ctx, mgr, ApiPath); err != nil {
		return err
	}

	if err := polardbxbackup.SetupWebhooks(ctx, mgr, ApiPath, configLoader); err != nil {
		return err
	}

	if err := backupbinlog.SetupWebhooks(ctx, mgr, ApiPath, configLoader); err != nil {
		return err
	}

	return nil
}
