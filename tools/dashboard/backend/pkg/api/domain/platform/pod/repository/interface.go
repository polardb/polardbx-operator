package repository

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/remotecommand"
)

// PodRepository defines the repository interface for Pod operations
type PodRepository interface {
	// List lists all Pods in the specified namespace
	List(ctx context.Context, namespace string) ([]corev1.Pod, error)

	// ListForCluster lists all Pods for a PolarDBX cluster
	ListForCluster(ctx context.Context, namespace, clusterName string) ([]corev1.Pod, error)

	// Get retrieves the specified Pod
	Get(ctx context.Context, namespace, name string) (*corev1.Pod, error)

	// Delete removes the specified Pod
	Delete(ctx context.Context, namespace, name string) error

	// GetLogs retrieves Pod logs
	GetLogs(ctx context.Context, namespace, podName, container string, tailLines int64) (string, error)
}

// ExecConfig configuration for command execution
type ExecConfig struct {
	Namespace string
	PodName   string
	Container string
	Command   []string
	Stdin     io.Reader
	Stdout    io.Writer
	Stderr    io.Writer
	TTY       bool
	SizeQueue remotecommand.TerminalSizeQueue
}

// ExecRepository defines the repository interface for Pod Exec operations
type ExecRepository interface {
	// Exec executes command in Pod
	Exec(ctx context.Context, cfg ExecConfig) error
}
