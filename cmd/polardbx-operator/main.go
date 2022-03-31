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

package main

import (
	"flag"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/alibaba/polardbx-operator/pkg/debug"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	operatorv1 "github.com/alibaba/polardbx-operator/pkg/operator/v1"
)

var (
	operatorOptions operatorv1.Options
	featureGates    string
)

func init() {
	// Bind options to arguments.
	flag.BoolVar(&operatorOptions.Debug, "debug", false, "Enable debug mode.")
	flag.StringVar(&operatorOptions.MetricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.IntVar(&operatorOptions.MaxConcurrentReconciles, "concurrency", 64, "The max concurrency of each controller.")
	flag.IntVar(&operatorOptions.ListenPort, "listen-port", 9443, "The port for operator to listen.")
	flag.IntVar(&operatorOptions.WebhookListenPort, "webhook-listen-port", 0, "The port for webhook to listen. If not specified, "+
		"webhooks will serve on operator's port. Set to -1 to disable the webhooks (for debug purpose).")
	flag.StringVar(&operatorOptions.CertDir, "cert-dir", "/etc/operator/certs", "Directory that stores the cert files.")
	flag.BoolVar(&operatorOptions.LeaderElection, "enable-leader-election", false, "Enable leader election for controller manager.")
	flag.StringVar(&operatorOptions.LeaderElectionNamespace, "leader-election-namespace", "", "The namespace where leader election happens. "+
		"If not specified, the namespace where this operator's running is used.")
	flag.StringVar(&operatorOptions.ConfigPath, "config-path", "/etc/operator/polardbx", "The path that contains configs of polardbx operator.")
	flag.StringVar(&featureGates, "feature-gates", "", "Feature gates to enable.")

	flag.Parse()

	// Setup logger.
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	// Hack to fix the stupid call stack frame problem in zapr 0.4.0.
	logger := logr.WithCallDepth(zapr.NewLogger(zapLogger), -1)
	ctrl.SetLogger(logger)

	// Enable feature gates.
	featuregate.SetupFeatureGates(strings.Split(strings.ReplaceAll(featureGates, " ", ""), ","))
}

func main() {
	// Setup signal handler.
	ctx := ctrl.SetupSignalHandler()

	// Mark local environment.
	if operatorOptions.Debug {
		debug.EnableDebug()
	}

	// Start operator.
	operatorv1.Start(ctx, operatorOptions)
}
