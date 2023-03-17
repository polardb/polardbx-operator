package common

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Reconciler interface {
	Reconcile(rc *Context, log logr.Logger, request reconcile.Request) (reconcile.Result, error)
}
