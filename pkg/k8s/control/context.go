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

package control

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
)

type ExecOptions struct {
	Logger  logr.Logger
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Timeout time.Duration
}

func (opts *ExecOptions) setDefaults() {
	// Set default timeout to 5s if not specified.
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}

	// Branch stdin/stdout/stderr are all null, set one.
	if opts.Stdin == nil && opts.Stdout == nil && opts.Stderr == nil {
		opts.Stdout = &bytes.Buffer{}
	}

	// Logging
	if opts.Logger == nil {
		opts.Logger = log.NullLogger{}
	}
}

type ReconcileRemoteCommandHelper interface {
	ExecuteCommandOn(pod *corev1.Pod, container string, command []string, opts ExecOptions) error
}

type ReconcileRequestHelper interface {
	// Name is a helper method that returns the name of current reconcile object.
	Name() string
	// Namespace is a helper method that returns the namespace of current reconcile object.
	Namespace() string
	// NameInto is a helper method that returns a string like "f({name})" where name is
	// the name of current reconcile object.
	NameInto(f func(name string) string) string
	// NamespacedNameInto is a helper method that returns a namespaced name like "{namespace}/f({name})"
	// where "{namespace}/{name}" is of the current reconcile object.
	NamespacedNameInto(f func(name string) string) types.NamespacedName
}

type ReconcileCustomResourceDefinitionHelper interface {
	// GetCrd queries the api server and get the target custom resource definition if found.
	GetCrd(apiVersion, kind string) (*apiextensionsv1.CustomResourceDefinition, error)
}

type ReconcileOptions interface {
	Debug() bool
}

type ReconcileControl interface {
	ForceRequeueAfter() time.Duration
	ResetForceRequeueAfter(d time.Duration)
}

// ReconcileContext declares the context for reconciliation.
type ReconcileContext interface {
	ReconcileOptions

	ReconcileControl

	ReconcileRemoteCommandHelper
	ReconcileRequestHelper
	ReconcileCustomResourceDefinitionHelper

	// Client returns a API client of k8s.
	Client() client.Client
	// RestConfig returns the rest config used by client.
	RestConfig() *rest.Config
	// ClientSet returns the client set.
	ClientSet() *kubernetes.Clientset
	// Scheme returns the currently using scheme.
	Scheme() *runtime.Scheme

	// Context returns the current reconcile context.
	Context() context.Context
	// Request returns the current reconcile request.
	Request() reconcile.Request

	// Close closes the context to avoid resource leaks.
	Close() error
}

type BaseReconcileContext struct {
	client     client.Client
	restConfig *rest.Config
	clientSet  *kubernetes.Clientset
	scheme     *runtime.Scheme

	context context.Context
	request reconcile.Request

	forceRequeueAfter time.Duration

	customResourceDefinitions *apiextensionsv1.CustomResourceDefinitionList
}

func (rc *BaseReconcileContext) Debug() bool {
	return false
}

func (rc *BaseReconcileContext) ExecuteCommandOn(pod *corev1.Pod, container string, command []string, opts ExecOptions) error {
	if k8shelper.GetContainerFromPod(pod, container) == nil {
		return errors.New("container " + container + " not found in pod " + pod.Name)
	}

	// Set defaults.
	opts.setDefaults()

	logger := opts.Logger
	logger.Info("Executing command", "pod", pod.Namespace, "container", container, "command", command, "timeout", opts.Timeout)

	// Start execute
	req := rc.clientSet.
		CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		Timeout(opts.Timeout)
	req.VersionedParams(&corev1.PodExecOptions{
		Container: container,
		Command:   command,
		Stdin:     opts.Stdin != nil,
		Stdout:    opts.Stdout != nil,
		Stderr:    opts.Stderr != nil,
		TTY:       false,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(rc.restConfig, "POST", req.URL())
	if err != nil {
		return err
	}

	ctx, cancelFn := rc.context, func() {}
	if opts.Timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, opts.Timeout)
	}
	defer cancelFn()

	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  opts.Stdin,
		Stdout: opts.Stdout,
		Stderr: opts.Stderr,
	})
}

func (rc *BaseReconcileContext) Name() string {
	return rc.request.Name
}

func (rc *BaseReconcileContext) Namespace() string {
	return rc.request.Namespace
}

func (rc *BaseReconcileContext) NameInto(f func(name string) string) string {
	return f(rc.request.Name)
}

func (rc *BaseReconcileContext) NamespacedNameInto(f func(name string) string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: rc.request.Namespace,
		Name:      f(rc.request.Name),
	}
}

func (rc *BaseReconcileContext) GetCrd(apiVersion, kind string) (*apiextensionsv1.CustomResourceDefinition, error) {
	groupVersion := strings.Split(apiVersion, "/")
	if len(groupVersion) != 2 {
		return nil, errors.New("invalid api version")
	}
	group, version := groupVersion[0], groupVersion[1]

	// Branch not cached, query from api server.
	if rc.customResourceDefinitions == nil {
		var customResourceDefinitions apiextensionsv1.CustomResourceDefinitionList
		if err := rc.client.List(rc.context, &customResourceDefinitions); err != nil {
			return nil, err
		}
		rc.customResourceDefinitions = &customResourceDefinitions
	}

	// Scan the CRD list.
	for _, crd := range rc.customResourceDefinitions.Items {
		if crd.Spec.Group == group && crd.Spec.Names.Kind == kind {
			for _, ver := range crd.Spec.Versions {
				if ver.Name == version {
					return crd.DeepCopy(), nil
				}
			}
		}
	}

	return nil, nil
}

func (rc *BaseReconcileContext) Client() client.Client {
	return rc.client
}

func (rc *BaseReconcileContext) RestConfig() *rest.Config {
	return rc.restConfig
}

func (rc *BaseReconcileContext) ClientSet() *kubernetes.Clientset {
	return rc.clientSet
}

func (rc *BaseReconcileContext) Scheme() *runtime.Scheme {
	return rc.scheme
}

func (rc *BaseReconcileContext) Context() context.Context {
	return rc.context
}

func (rc *BaseReconcileContext) Request() reconcile.Request {
	return rc.request
}

func (rc *BaseReconcileContext) Close() error {
	return nil
}

func (rc *BaseReconcileContext) ForceRequeueAfter() time.Duration {
	return rc.forceRequeueAfter
}

func (rc *BaseReconcileContext) ResetForceRequeueAfter(d time.Duration) {
	rc.forceRequeueAfter = d
}

func (rc *BaseReconcileContext) shallowCopy() *BaseReconcileContext {
	shallowCopy := *rc
	return &shallowCopy
}

func NewBaseReconcileContextFrom(base *BaseReconcileContext, context context.Context, request reconcile.Request) *BaseReconcileContext {
	rc := *base.shallowCopy()
	rc.forceRequeueAfter = 0

	rc.request = request
	rc.context = context
	rc.customResourceDefinitions = nil
	return &rc
}

func NewBaseReconcileContext(client client.Client, restConfig *rest.Config, clientSet *kubernetes.Clientset,
	scheme *runtime.Scheme, context context.Context, request reconcile.Request) *BaseReconcileContext {
	return &BaseReconcileContext{
		client:                    client,
		restConfig:                restConfig,
		clientSet:                 clientSet,
		scheme:                    scheme,
		context:                   context,
		request:                   request,
		customResourceDefinitions: nil,
	}
}
