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

package framework

import (
	"context"
	"flag"
	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)
	_ = promv1.AddToScheme(scheme)
}

type TestContextType struct {
	TimeoutInSeconds int
	Namespace        string
}

type testContext struct {
	Ctx            context.Context
	CancelFn       context.CancelFunc
	Namespace      string
	KubeConfig     *rest.Config
	Client         client.Client
	KubeConfigFile string
}

func (t *TestContextType) RegisterAndParse() {
	flag.IntVar(&t.TimeoutInSeconds, "timeout", 0, "Timeout in seconds.")
	flag.StringVar(&t.Namespace, "namespace", "default", "Namespace.")

	flag.Parse()
}

var (
	testContextType TestContextType
	TestContext     *testContext
)

func RegisterFlagsAndParse() {
	testContextType.RegisterAndParse()
	TestContext = newTestContext(&testContextType)
}

func (t *TestContextType) NewContext() (context.Context, context.CancelFunc) {
	if t.TimeoutInSeconds > 0 {
		return context.WithTimeout(context.Background(), time.Duration(t.TimeoutInSeconds)*time.Second)
	} else {
		return context.WithCancel(context.Background())
	}
}

func newClient(config *rest.Config) (client.Client, error) {
	mapper, err := apiutil.NewDynamicRESTMapper(config)
	if err != nil {
		return nil, err
	}
	return client.New(config, client.Options{
		Scheme: scheme,
		Mapper: mapper,
		Opts:   client.WarningHandlerOptions{},
	})
}

func newTestContext(t *TestContextType) *testContext {
	ctx, cancel := t.NewContext()
	tc := &testContext{
		Ctx:            ctx,
		CancelFn:       cancel,
		Namespace:      t.Namespace,
		KubeConfigFile: flag.Lookup("kubeconfig").Value.String(),
		KubeConfig:     ctrl.GetConfigOrDie(),
	}
	c, err := newClient(tc.KubeConfig)
	if err != nil {
		panic("failed to new test client")
	}
	tc.Client = c
	return tc
}
