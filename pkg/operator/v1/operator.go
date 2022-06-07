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

package v1

import (
	"context"
	"os"

	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxv1controllers "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/controllers"
	xstorev1controllers "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/controllers"
	"github.com/alibaba/polardbx-operator/pkg/webhook"
	"github.com/alibaba/polardbx-operator/pkg/webhook/polardbxcluster"
)

var (
	scheme = runtime.NewScheme()
)

func initScheme() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)
	_ = promv1.AddToScheme(scheme)
	_ = apiextensionsv1.AddToScheme(scheme)
}

func init() {
	initScheme()
}

type Options struct {
	Debug bool

	MetricsAddr             string
	ListenPort              int
	WebhookListenPort       int
	LeaderElection          bool
	LeaderElectionNamespace string
	MaxConcurrentReconciles int
	CertDir                 string

	ConfigPath string
}

var setupLog = ctrl.Log.WithName("setup")

type controllerOptions struct {
	*control.BaseReconcileContext
	ctrl.Manager
	config.LoaderFactory
	opts *Options
}

func setupXStoreControllers(opts controllerOptions) error {
	xstoreReconciler := xstorev1controllers.XStoreReconciler{
		BaseRc:         opts.BaseReconcileContext,
		LoaderFactory:  opts.LoaderFactory,
		Logger:         ctrl.Log.WithName("controller").WithName("xstore"),
		MaxConcurrency: opts.opts.MaxConcurrentReconciles,
	}
	err := xstoreReconciler.SetupWithManager(opts.Manager)
	if err != nil {
		return err
	}

	return nil
}

func setupPolarDBXControllers(opts controllerOptions) error {
	polardbxReconciler := polardbxv1controllers.PolarDBXReconciler{
		BaseRc:         opts.BaseReconcileContext,
		LoaderFactory:  opts.LoaderFactory,
		Logger:         ctrl.Log.WithName("controller").WithName("polardbx"),
		MaxConcurrency: opts.opts.MaxConcurrentReconciles,
	}
	if err := polardbxReconciler.SetupWithManager(opts.Manager); err != nil {
		return err
	}

	knobsReconciler := polardbxv1controllers.PolarDBXClusterKnobsReconciler{
		Client:         opts.Manager.GetClient(),
		Logger:         ctrl.Log.WithName("controller").WithName("polardbxknobs"),
		MaxConcurrency: opts.opts.MaxConcurrentReconciles,
	}
	if err := knobsReconciler.SetupWithManager(opts.Manager); err != nil {
		return err
	}

	monitorReconciler := polardbxv1controllers.PolarDBXMonitorReconciler{
		BaseRc:         opts.BaseReconcileContext,
		Client:         opts.Manager.GetClient(),
		LoaderFactory:  opts.LoaderFactory,
		Logger:         ctrl.Log.WithName("controller").WithName("polardbxmonitor"),
		MaxConcurrency: opts.opts.MaxConcurrentReconciles,
	}
	if err := monitorReconciler.SetupWithManager(opts.Manager); err != nil {
		return err
	}

	return nil
}

// Start starts all related controllers of PolarDB-X. The first parameter ctx is used to control the
// stop of the controllers. Recommendation is to use the context returned by `ctrl.SetupSignalHandler`
// to handle signals correctly. The second parameter opts defines the configurable options of controllers.
//
// Currently, these controllers are included:
//   1. Controller for PolarDBXCluster (v1)
//   2. Controller for XStore (v1)
//   3. Controllers for PolarDBXBackup, PolarDBXBinlogBackup (v1)
//   4. Controllers for XStoreBackup, XStoreBinlogBackup (v1)
//   5. Controllers for PolarDBXBackupSchedule, PolarDBXBinlogBackupSchedule (v1)
func Start(ctx context.Context, opts Options) {
	// Start instruction loader.
	hint.StartLoader(ctx)

	// Start operator config loader.
	configLoaderFactory, err := config.NewConfigLoaderAndStartBackgroundRefresh(ctx,
		config.LoadFromPath(opts.ConfigPath),
		config.WithLogger(ctrl.Log.WithName("config")),
	)
	if err != nil {
		setupLog.Error(err, "Unable to start operator config loader.")
		os.Exit(1)
	}

	// Get REST config.
	restConfig := ctrl.GetConfigOrDie()
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "Unable to new rest config.")
		os.Exit(1)
	}

	// New manager.
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                  scheme,
		MetricsBindAddress:      opts.MetricsAddr,
		Port:                    opts.ListenPort,
		LeaderElection:          opts.LeaderElection,
		LeaderElectionNamespace: opts.LeaderElectionNamespace,
		LeaderElectionID:        "polardbx.aliyun.com",
		CertDir:                 opts.CertDir,
	})
	if err != nil {
		setupLog.Error(err, "Unable to new manager.")
		os.Exit(1)
	}

	ctrlOpts := controllerOptions{
		BaseReconcileContext: control.NewBaseReconcileContext(
			mgr.GetClient(),
			restConfig,
			clientset,
			scheme,
			context.Background(),
			reconcile.Request{},
		),
		Manager:       mgr,
		LoaderFactory: configLoaderFactory,
		opts:          &opts,
	}

	// Setup controllers.
	err = setupXStoreControllers(ctrlOpts)
	if err != nil {
		setupLog.Error(err, "Unable to setup controllers for xstore.")
		os.Exit(1)
	}

	err = setupPolarDBXControllers(ctrlOpts)
	if err != nil {
		setupLog.Error(err, "Unable to setup controllers for polardbx.")
		os.Exit(1)
	}

	// Setup webhooks.
	if opts.WebhookListenPort < 0 {
		// Disable if manually specified.
		setupLog.Info("Webhooks disabled, will not start!")
	} else if opts.WebhookListenPort > 0 {
		// Start a standalone webhook server. TLS enabled or not is dynamically determined.
		err = polardbxcluster.StartStandaloneWebhookServer(ctx, mgr, opts.WebhookListenPort, opts.ConfigPath, opts.CertDir)
		if err != nil {
			setupLog.Error(err, "Unable to start webhook server...")
			os.Exit(1)
		}
	} else {
		// Defaults ot setup on manager.
		err = webhook.SetupWebhooks(ctx, mgr, opts.ConfigPath)
		if err != nil {
			setupLog.Error(err, "Unable to setup webhooks...")
			os.Exit(1)
		}
	}

	// Start.
	setupLog.Info("Starting controllers...")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "Unable to start controllers.")
		os.Exit(1)
	}
}
