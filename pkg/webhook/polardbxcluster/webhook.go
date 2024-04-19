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
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	admission2 "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
)

const apiPath = "/apis/admission.polardbx.aliyun.com/v1"

func generateMutatePath(gvk schema.GroupVersionKind) string {
	return apiPath + "/mutate-" + strings.ReplaceAll(gvk.Group, ".", "-") + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func generateValidatePath(gvk schema.GroupVersionKind) string {
	return apiPath + "/validate-" + strings.ReplaceAll(gvk.Group, ".", "-") + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func setupWebhooksForPolarDBXClusterV1(mgr ctrl.Manager, webhookConfigLoader WebhookAdmissionConfigLoaderFunc, apiPath string) error {
	gvk := schema.GroupVersionKind{
		Group:   polardbxv1.GroupVersion.Group,
		Version: polardbxv1.GroupVersion.Version,
		Kind:    "PolarDBXCluster",
	}

	// Register defaulter (mutate) webhook.
	mgr.GetWebhookServer().Register(extension.GenerateMutatePath(apiPath, gvk),
		extension.WithCustomDefaulter(&polardbxv1.PolarDBXCluster{},
			NewPolarDBXClusterV1Defaulter(
				func() *DefaulterConfig {
					return &webhookConfigLoader().Defaulter
				},
			), mgr.GetScheme(),
		),
	)

	// Register validator (validate) webhook.
	mgr.GetWebhookServer().Register(extension.GenerateValidatePath(apiPath, gvk),
		extension.WithCustomValidator(&polardbxv1.PolarDBXCluster{},
			NewPolarDBXClusterV1Validator(
				func() *ValidatorConfig {
					return &webhookConfigLoader().Validator
				},
			),
			mgr.GetScheme(),
		),
	)

	return nil
}

func registerWebhook(mux *http.ServeMux, path string, webhook *admission2.Webhook, mgr ctrl.Manager) error {
	opts := admission2.StandaloneOptions{
		Logger:      mgr.GetLogger().WithName("webhooks.admission"),
		MetricsPath: path,
	}
	handler, err := admission2.StandaloneWebhook(webhook, opts)
	if err != nil {
		return err
	}
	mux.Handle(path, handler)
	return nil
}

func webhookHandlerForPolarDBXClusterV1(mux *http.ServeMux, mgr ctrl.Manager, webhookConfigLoader WebhookAdmissionConfigLoaderFunc) error {
	gvk := schema.GroupVersionKind{
		Group:   polardbxv1.GroupVersion.Group,
		Version: polardbxv1.GroupVersion.Version,
		Kind:    "PolarDBXCluster",
	}

	err := registerWebhook(mux, generateMutatePath(gvk),
		extension.WithCustomDefaulter(&polardbxv1.PolarDBXCluster{},
			NewPolarDBXClusterV1Defaulter(
				func() *DefaulterConfig {
					return &webhookConfigLoader().Defaulter
				},
			),
			mgr.GetScheme(),
		),
		mgr,
	)
	if err != nil {
		return err
	}

	err = registerWebhook(mux, generateValidatePath(gvk),
		extension.WithCustomValidator(&polardbxv1.PolarDBXCluster{},
			NewPolarDBXClusterV1Validator(
				func() *ValidatorConfig {
					return &webhookConfigLoader().Validator
				},
			),
			mgr.GetScheme(),
		),
		mgr,
	)
	if err != nil {
		return err
	}

	return nil
}

func isFileExists(f string) bool {
	_, err := os.Lstat(f)
	return err == nil
}

func areCertFilesExists(certsPath string) bool {
	certFile, keyFile := path.Join(certsPath, "tls.crt"), path.Join(certsPath, "tls.key")
	return isFileExists(certFile) && isFileExists(keyFile)
}

func StartStandaloneWebhookServer(ctx context.Context, mgr ctrl.Manager, port int, configPath string, certsPath string) error {
	webhookConfigLoader, err := NewConfigLoaderAndStartBackgroundRefresh(ctx,
		configPath, ctrl.Log.WithName("webhook"))
	if err != nil {
		return err
	}

	serverMux := http.NewServeMux()

	err = webhookHandlerForPolarDBXClusterV1(serverMux, mgr, webhookConfigLoader)
	if err != nil {
		return err
	}

	addr := fmt.Sprintf(":%d", port)
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		var err error
		if areCertFilesExists(certsPath) {
			err = http.ServeTLS(lis, serverMux,
				path.Join(certsPath, "tls.crt"), path.Join(certsPath, "tls.key"))
		} else {
			err = http.Serve(lis, serverMux)
		}
		if err != nil && err != ctx.Err() {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func SetupWebhooks(ctx context.Context, mgr ctrl.Manager, configPath string, apiPath string) error {
	webhookConfigLoader, err := NewConfigLoaderAndStartBackgroundRefresh(ctx,
		configPath, ctrl.Log.WithName("webhook"))
	if err != nil {
		return err
	}

	err = setupWebhooksForPolarDBXClusterV1(mgr, webhookConfigLoader, apiPath)
	if err != nil {
		return err
	}

	return nil
}
