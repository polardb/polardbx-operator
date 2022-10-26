package controllers

import (
	"bytes"
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/cache"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"time"
)

type PolarDBXLogCollectorReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *PolarDBXLogCollectorReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := r.Logger.WithValues("namespace", request.Namespace, "polardbxlogcollector", request.Name)
	if hint.IsNamespacePaused(request.Namespace) {
		logger.Info("Log Collector reconciling is paused, skip")
		return reconcile.Result{}, nil
	}
	baseReconcileContext := control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request)
	objectCache := cache.NewObjectCache(baseReconcileContext.Client(), baseReconcileContext.Scheme())
	logCollector, err := objectCache.GetObject(baseReconcileContext.Context(), request.NamespacedName, &polardbxv1.PolarDBXLogCollector{})
	if err != nil {
		logger.Error(err, "Failed to get object PolarDBXLogCollector", "namespace", request.Namespace, "name", request.Name)
		return reconcile.Result{}, err
	}
	logCollectorSpec := &logCollector.(*polardbxv1.PolarDBXLogCollector).Spec
	logCollectorStatus := &logCollector.(*polardbxv1.PolarDBXLogCollector).Status

	componentNames := []string{logCollectorSpec.FileBeatName, logCollectorSpec.LogStashName}
	componentTypes := []client.Object{&v1.DaemonSet{}, &v1.Deployment{}}

	if len(componentTypes) != len(componentTypes) {
		panic("componentNames array size should equal componentTypes array size")
	}

	component2ConfigMap := make(map[string]string)
	for index, componentName := range componentNames {
		componentType := componentTypes[index]
		component, err := objectCache.GetObject(baseReconcileContext.Context(), types.NamespacedName{Namespace: request.Namespace, Name: componentName}, componentType)
		if err != nil {
			logger.Error(err, "Failed to get object", "componentName", componentName)
			return reconcile.Result{RequeueAfter: time.Second * 5}, err
		}

		var podSpec *corev1.PodSpec

		switch componentName {
		case logCollectorSpec.FileBeatName:
			podSpec = &component.(*v1.DaemonSet).Spec.Template.Spec
			status := &component.(*v1.DaemonSet).Status
			if logCollectorStatus.ConfigStatus == nil {
				logCollectorStatus.ConfigStatus = &polardbxv1.LogCollectorConfigStatus{}
			}
			logCollectorStatus.ConfigStatus.FileBeatCount = status.DesiredNumberScheduled
			logCollectorStatus.ConfigStatus.FileBeatReadyCount = status.NumberReady
			component2ConfigMap[componentName] = logCollectorStatus.ConfigStatus.FileBeatConfigId
		case logCollectorSpec.LogStashName:
			podSpec = &component.(*v1.Deployment).Spec.Template.Spec
			status := &component.(*v1.Deployment).Status
			if logCollectorStatus.ConfigStatus == nil {
				logCollectorStatus.ConfigStatus = &polardbxv1.LogCollectorConfigStatus{}
			}
			logCollectorStatus.ConfigStatus.LogStashCount = status.Replicas
			logCollectorStatus.ConfigStatus.LogStashReadyCount = status.ReadyReplicas
			component2ConfigMap[componentName] = logCollectorStatus.ConfigStatus.LogStashConfigId
		default:
			logger.Info(componentType.GetName())
			logger.Info("Do not watch ", "Kind", component.GetObjectKind().GroupVersionKind())
			continue
		}
		var stringBuffer bytes.Buffer
		for _, volume := range podSpec.Volumes {
			if volume.ConfigMap != nil {
				configMapNamespacedName := types.NamespacedName{Namespace: request.Namespace, Name: volume.ConfigMap.Name}
				configMap, err := objectCache.GetObject(baseReconcileContext.Context(), configMapNamespacedName, &corev1.ConfigMap{})
				if err != nil {
					logger.Error(err, "Failed to get object", "componentName", configMapNamespacedName.String())
					return reconcile.Result{}, err
				}
				uuidWithVersion := string(configMap.GetUID()) + configMap.GetResourceVersion()
				stringBuffer.WriteString(uuidWithVersion)
				stringBuffer.WriteString("|")
			} else if volume.Secret != nil {
				secretNamespacedName := types.NamespacedName{Namespace: request.Namespace, Name: volume.Secret.SecretName}
				secret, err := objectCache.GetObject(baseReconcileContext.Context(), secretNamespacedName, &corev1.Secret{})
				if err != nil {
					logger.Error(err, "Failed to get object", "componentName", secretNamespacedName.String())
					return reconcile.Result{}, err
				}
				uuidWithVersion := string(secret.GetUID()) + secret.GetResourceVersion()
				stringBuffer.WriteString(uuidWithVersion)
				stringBuffer.WriteString("|")
			}
		}

		configOrSecretHash := defaults.FNV64(stringBuffer.String())
		configId := strconv.FormatInt(int64(configOrSecretHash), 16)
		if configId != component2ConfigMap[componentName] {
			annotations := component.GetAnnotations()
			annotations["date"] = strconv.FormatInt(time.Now().UnixNano(), 10)
			component.SetAnnotations(annotations)
			baseReconcileContext.Client().Update(baseReconcileContext.Context(), component)
			pod := &corev1.Pod{}
			opts := []client.DeleteAllOfOption{
				client.InNamespace(request.NamespacedName.Namespace),
				client.MatchingLabels{"app": component.GetName()},
				client.MatchingFields{"status.phase": "Running"},
			}
			baseReconcileContext.Client().DeleteAllOf(baseReconcileContext.Context(), pod, opts...)
			component2ConfigMap[componentName] = configId
		}
	}
	if val, ok := component2ConfigMap[logCollectorSpec.FileBeatName]; ok {
		logCollectorStatus.ConfigStatus.FileBeatConfigId = val
	}
	if val, ok := component2ConfigMap[logCollectorSpec.LogStashName]; ok {
		logCollectorStatus.ConfigStatus.LogStashConfigId = val
	}
	logCollectorStatus.SpecSnapshot = logCollectorSpec.DeepCopy()
	baseReconcileContext.Client().Update(baseReconcileContext.Context(), logCollector)
	return reconcile.Result{RequeueAfter: time.Second * 5}, err
}

func (r *PolarDBXLogCollectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 5*time.Second),
				// 10 qps, 1 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 1)},
			),
		}).
		For(&polardbxv1.PolarDBXLogCollector{}).
		Complete(r)
}
