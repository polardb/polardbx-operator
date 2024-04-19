package binlog

import (
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
	"github.com/alibaba/polardbx-operator/pkg/webhook/polardbxbackup"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
)

func SetupWebhooks(ctx context.Context, mgr ctrl.Manager, apiPath string, configLoader func() config.Config) error {
	gvk := schema.GroupVersionKind{
		Group:   polardbxv1.GroupVersion.Group,
		Version: polardbxv1.GroupVersion.Version,
		Kind:    "PolarDBXBackupBinlog",
	}

	mgr.GetWebhookServer().Register(extension.GenerateValidatePath(apiPath, gvk),
		extension.WithCustomValidator(&polardbxv1.PolarDBXBackupBinlog{},
			polardbxbackup.NewPolarDBXBackupValidator(mgr.GetAPIReader(),
				mgr.GetLogger().WithName("webhook.validate.polardbxbackupbinlog"),
				configLoader), mgr.GetScheme()))
	return nil
}
