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

package finalizer

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	errutil "github.com/alibaba/polardbx-operator/pkg/util/error"
)

func removeFinalizers(rc *polardbxv1reconcile.Context, log logr.Logger, pods []corev1.Pod) error {
	errs := make([]error, 0)
	for _, pod := range pods {
		if controllerutil.ContainsFinalizer(&pod, polardbxmeta.Finalizer) {
			controllerutil.RemoveFinalizer(&pod, polardbxmeta.Finalizer)
			err := rc.Client().Update(rc.Context(), &pod)
			if err != nil {
				log.Error(err, "Unable to remove finalizer", "pod", pod.Name)
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func handleFinalizerForPods(rc *polardbxv1reconcile.Context, log logr.Logger, deletedOrFailedPods []corev1.Pod, role string) error {
	polardbxmeta.AssertRoleIn(role, polardbxmeta.RoleCN, polardbxmeta.RoleCDC, polardbxmeta.RoleColumnar)

	if len(deletedOrFailedPods) == 0 {
		return nil
	}
	polardbx := rc.MustGetPolarDBX()

	pxcNotDeleting := polardbx.DeletionTimestamp.IsZero()
	if polardbx.Spec.Readonly {
		_, err := rc.GetPrimaryPolarDBX()
		if apierrors.IsNotFound(err) {
			pxcNotDeleting = false
		}
	}

	var mgr gms.Manager
	if pxcNotDeleting {
		var err error
		mgr, err = rc.GetPolarDBXGMSManager()
		if err != nil && pxcNotDeleting {
			return err
		}
	}

	canDeleteTime := v1.NewTime(time.Now().Add(-5 * time.Second))
	deletedOrFailedPods = k8shelper.FilterPodsBy(deletedOrFailedPods, func(pod *corev1.Pod) bool {
		if pod.CreationTimestamp.Before(&canDeleteTime) {
			return true
		}
		return false
	})

	// Delete records in GMS.
	if role == polardbxmeta.RoleCN {
		toDeleteInfo := make([]gms.ComputeNodeInfo, 0, len(deletedOrFailedPods))
		for _, pod := range deletedOrFailedPods {
			toDeleteInfo = append(toDeleteInfo, gms.ComputeNodeInfo{
				Host: pod.Status.PodIP,
				Port: k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					convention.PortAccess,
				).ContainerPort,
				Extra: pod.Name,
			})
		}
		if pxcNotDeleting {
			err := mgr.DeleteComputeNodes(toDeleteInfo...)
			if err != nil {
				return err
			}
		}
	} else if role == polardbxmeta.RoleCDC {
		toDeleteInfo := make([]gms.CdcNodeInfo, 0, len(deletedOrFailedPods))
		for _, pod := range deletedOrFailedPods {
			if pod.Status.PodIP == "" {
				continue
			}
			toDeleteInfo = append(toDeleteInfo, gms.CdcNodeInfo{
				Host: pod.Status.PodIP,
				DaemonPort: k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					"daemon",
				).ContainerPort,
			})
		}
		if pxcNotDeleting {
			err := mgr.DeleteCdcNodes(toDeleteInfo...)
			if err != nil {
				return err
			}
		}
	}

	// Try to remove finalizer from pods.
	return removeFinalizers(rc, log, deletedOrFailedPods)
}

var RemoveResidualFinalizersOnPods = polardbxv1reconcile.NewStepBinder("RemoveResidualFinalizersOnPods",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnPods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CN")
		}

		cdcPods, err := rc.GetPods(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CDC")
		}

		columnarPods, err := rc.GetPods(polardbxmeta.RoleColumnar)
		if err != nil {
			return flow.Error(err, "Unable to get pods for Columnar")
		}

		if err := errutil.FirstNonNil(
			removeFinalizers(rc, flow.Logger(), cnPods),
			removeFinalizers(rc, flow.Logger(), cdcPods),
			removeFinalizers(rc, flow.Logger(), columnarPods),
		); err != nil {
			return flow.Error(err, "Failed to remove some finalizer.")
		}

		return flow.Pass()
	},
)

func removeFinalizerOnStore(rc *polardbxv1reconcile.Context, xstore *polardbxv1.XStore) error {
	if controllerutil.ContainsFinalizer(xstore, polardbxmeta.Finalizer) {
		controllerutil.RemoveFinalizer(xstore, polardbxmeta.Finalizer)
		return rc.Client().Update(rc.Context(), xstore)
	}
	return nil
}

var RemoveFinalizersOnStores = polardbxv1reconcile.NewStepBinder("RemoveFinalizersOnStores",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		if !polardbx.Spec.ShareGMS {
			gms, err := rc.GetGMS()
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get xstore of GMS.")
			}

			if gms != nil {
				err = removeFinalizerOnStore(rc, gms)
				if err != nil {
					return flow.Error(err, "Unable to remove finalizer on xstore of GMS.", "xstore", gms.Name)
				}
			}
		}

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		errCnt := int32(0)
		wg := &sync.WaitGroup{}
		for i := range dnStores {
			xstore := dnStores[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := removeFinalizerOnStore(rc, xstore)
				if err != nil {
					flow.Logger().Error(err, "Unable to remove finalizer on xstore of DN.", "xstore", xstore.Name)
					atomic.AddInt32(&errCnt, 1)
				}
			}()
		}
		wg.Wait()

		if errCnt > 0 {
			return flow.RetryAfter(5*time.Second, "Retry finalizer remove after 5 seconds...")
		}

		return flow.Continue("Finalizers on XStores are removed!")
	},
)

var HandleFinalizerForStatelessPods = polardbxv1reconcile.NewStepBinder("HandleFinalizerForStatelessPods",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnPods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CN")
		}

		cdcPods, err := rc.GetPods(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CDC")
		}

		columnarPods, err := rc.GetPods(polardbxmeta.RoleColumnar)
		if err != nil {
			return flow.Error(err, "Unable to get pods for Columnar")
		}

		isPodDeletedOrFailedAndContainsFinalizer := func(pod *corev1.Pod) bool {
			return k8shelper.IsPodDeletedOrFailed(pod) && controllerutil.ContainsFinalizer(pod, polardbxmeta.Finalizer)
		}

		if err := errutil.FirstNonNil(
			// Handle for CN pods.
			handleFinalizerForPods(rc, flow.Logger(),
				k8shelper.FilterPodsBy(cnPods, isPodDeletedOrFailedAndContainsFinalizer),
				polardbxmeta.RoleCN,
			),
			// Handle for CDC pods.
			handleFinalizerForPods(rc, flow.Logger(),
				k8shelper.FilterPodsBy(cdcPods, isPodDeletedOrFailedAndContainsFinalizer),
				polardbxmeta.RoleCDC,
			),
			// Handle for Columnar pods.
			handleFinalizerForPods(rc, flow.Logger(),
				k8shelper.FilterPodsBy(columnarPods, isPodDeletedOrFailedAndContainsFinalizer),
				polardbxmeta.RoleColumnar,
			),
		); err != nil {
			return flow.Error(err, "Failed to handle some finalizer.")
		}

		return flow.Pass()
	},
)
