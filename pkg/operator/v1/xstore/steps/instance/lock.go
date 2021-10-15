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

package instance

import (
	"github.com/go-logr/logr"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func IsDiskQuotaExceeds(xstore *polardbxv1.XStore) bool {
	// Skip when disk quota not specified.
	if xstore.Spec.Config.Dynamic.DiskQuota == nil {
		return false
	}
	diskQuota := xstore.Spec.Config.Dynamic.DiskQuota.Value()

	// Calculate the total volume size.
	totalVolSize := int64(0)
	for _, vol := range xstore.Status.BoundVolumes {
		totalVolSize += vol.Size
	}

	return totalVolSize > diskQuota
}

func WhenDiskQuotaExceeds(binders ...control.BindFunc) control.BindFunc {
	return reconcile.NewStepIfBinder("DiskQuotaExceeds",
		func(rc *reconcile.Context, log logr.Logger) (bool, error) {
			return IsDiskQuotaExceeds(rc.MustGetXStore()), nil
		},
		binders...,
	)
}

func WhenDiskQuotaNotExceeds(binders ...control.BindFunc) control.BindFunc {
	return reconcile.NewStepIfBinder("DiskQuotaNotExceeds",
		func(rc *reconcile.Context, log logr.Logger) (bool, error) {
			return !IsDiskQuotaExceeds(rc.MustGetXStore()), nil
		},
		binders...,
	)
}
