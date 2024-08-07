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

package reconcile

import (
	"github.com/go-logr/logr"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Reconciler interface {
	Reconcile(rc *Context, log logr.Logger, request reconcile.Request) (reconcile.Result, error)
}

type BackupReconciler interface {
	Reconcile(rc *BackupContext, log logr.Logger, request reconcile.Request) (reconcile.Result, error)
}

type BackupBinlogReconciler interface {
	Reconcile(rc *BackupBinlogContext, log logr.Logger, request reconcile.Request) (reconcile.Result, error)
}
