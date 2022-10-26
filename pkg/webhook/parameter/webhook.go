package parameter

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
)

func SetupWebhooks(ctx context.Context, mgr ctrl.Manager, apiPath string) error {
	gvk := schema.GroupVersionKind{
		Group:   polardbxv1.GroupVersion.Group,
		Version: polardbxv1.GroupVersion.Version,
		Kind:    "PolarDBXParameter",
	}

	// Validate.
	mgr.GetWebhookServer().Register(extension.GenerateValidatePath(apiPath, gvk),
		extension.WithCustomValidator(
			&polardbxv1.PolarDBXParameter{},
			NewParameterValidator(mgr.GetAPIReader(), mgr.GetLogger().WithName("webhook.validate.polardbxparameter")),
		),
	)

	return nil
}
