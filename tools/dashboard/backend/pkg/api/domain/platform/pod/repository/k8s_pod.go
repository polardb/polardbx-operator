package repository

import (
	"context"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/k8s"
)

// K8sPodRepository implements PodRepository using Kubernetes client
type K8sPodRepository struct {
	client    client.Client
	clientset kubernetes.Interface
	restCfg   *rest.Config
}

// NewK8sPodRepository creates new K8s implementation
func NewK8sPodRepository(cli client.Client, cs kubernetes.Interface, restCfg *rest.Config) *K8sPodRepository {
	return &K8sPodRepository{
		client:    cli,
		clientset: cs,
		restCfg:   restCfg,
	}
}

// NewK8sPodRepositorySimple creates simplified version that only needs client.Client
func NewK8sPodRepositorySimple(cli client.Client, cs kubernetes.Interface) *K8sPodRepository {
	return &K8sPodRepository{
		client:    cli,
		clientset: cs,
	}
}

// List lists all Pods in the specified namespace
func (r *K8sPodRepository) List(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.client.List(ctx, &podList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// ListForCluster lists all Pods for a PolarDBX cluster
func (r *K8sPodRepository) ListForCluster(ctx context.Context, namespace, clusterName string) ([]corev1.Pod, error) {
	return k8s.ListPodsForPolarDBXCluster(r.client, namespace, clusterName)
}

// Get retrieves the specified Pod
func (r *K8sPodRepository) Get(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	var pod corev1.Pod
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// Delete removes the specified Pod
func (r *K8sPodRepository) Delete(ctx context.Context, namespace, name string) error {
	pod := &corev1.Pod{}
	pod.Namespace = namespace
	pod.Name = name
	return r.client.Delete(ctx, pod)
}

// GetLogs retrieves Pod logs
func (r *K8sPodRepository) GetLogs(ctx context.Context, namespace, podName, container string, tailLines int64) (string, error) {
	return k8s.GetPodLogsWithContext(ctx, r.clientset, namespace, podName, container, tailLines)
}

// Exec executes command in Pod
func (r *K8sPodRepository) Exec(ctx context.Context, cfg ExecConfig) error {
	if r.restCfg == nil {
		return nil
	}
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(cfg.PodName).
		Namespace(cfg.Namespace).
		SubResource("exec")

	execOpts := &corev1.PodExecOptions{
		Container: cfg.Container,
		Command:   cfg.Command,
		Stdin:     cfg.Stdin != nil,
		Stdout:    cfg.Stdout != nil,
		Stderr:    cfg.Stderr != nil,
		TTY:       cfg.TTY,
	}
	req.VersionedParams(execOpts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(r.restCfg, http.MethodPost, req.URL())
	if err != nil {
		return err
	}

	return executor.Stream(remotecommand.StreamOptions{
		Stdin:             cfg.Stdin,
		Stdout:            cfg.Stdout,
		Stderr:            cfg.Stderr,
		Tty:               cfg.TTY,
		TerminalSizeQueue: cfg.SizeQueue,
	})
}
