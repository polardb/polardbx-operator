package k8s

import (
	"context"
	"fmt"
	"io"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	kubescheme = runtime.NewScheme()
)

func init() {
	// Register standard Kubernetes APIs.
	_ = scheme.AddToScheme(kubescheme)
	// Register CRD definitions.
	_ = apiextensionsv1.AddToScheme(kubescheme)
	// Register PolarDB-X custom APIs.
	_ = polardbxv1.AddToScheme(kubescheme)
}

// ClientProvider defines the interface for creating a Kubernetes client.
type ClientProvider interface {
	NewClientFromKubeconfig(kubeconfigData []byte) (client.Client, error)
}

// DefaultClientProvider is the default implementation of ClientProvider.
type DefaultClientProvider struct{}

// NewClientFromKubeconfig creates a new typed Kubernetes client from a kubeconfig byte slice.
func NewClientFromKubeconfig(kubeconfig []byte) (client.Client, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create new client: %w", err)
	}
	return c, nil
}

// NewClientsFromKubeconfig creates a controller-runtime client and a standard clientset
// from the given kubeconfig bytes.
func NewClientsFromKubeconfig(kubeconfig []byte) (client.Client, kubernetes.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new client: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new clientset: %w", err)
	}

	return c, clientset, nil
}

// NewAllClientsFromKubeconfig creates a controller-runtime client, standard clientset, and dynamic client
// from the given kubeconfig bytes.
func NewAllClientsFromKubeconfig(kubeconfig []byte) (client.Client, kubernetes.Interface, dynamic.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new client: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new clientset: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new dynamic client: %w", err)
	}

	return c, clientset, dynClient, nil
}

// GetPodLogsWithContext retrieves logs from a specified pod with context.
func GetPodLogsWithContext(ctx context.Context, clientset kubernetes.Interface, namespace, podName, containerName string, tailLines int64) (string, error) {
	opts := &corev1.PodLogOptions{Container: containerName}
	if tailLines > 0 {
		opts.TailLines = &tailLines
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("error in opening log stream: %w", err)
	}
	defer podLogs.Close()

	logBytes, err := io.ReadAll(podLogs)
	if err != nil {
		return "", fmt.Errorf("error in reading log stream: %w", err)
	}

	return string(logBytes), nil
}

// Deprecated: use GetPodLogsWithContext
func GetPodLogs(clientset kubernetes.Interface, namespace, podName, containerName string, tailLines int64) (string, error) {
	return GetPodLogsWithContext(context.Background(), clientset, namespace, podName, containerName, tailLines)
}
