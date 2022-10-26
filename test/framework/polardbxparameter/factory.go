package polardbxparameter

import (
	"context"
	"errors"
	"io"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type ParameterFactoryOption func(parameter *polardbxv1.PolarDBXParameter)
type ParameterTemplateFactoryOption func(parameterTemplate *polardbxv1.PolarDBXParameterTemplate)

func NewPolarDBXParameter(name, namespace string, opts ...ParameterFactoryOption) *polardbxv1.PolarDBXParameter {
	obj := &polardbxv1.PolarDBXParameter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	for _, opt := range opts {
		opt(obj)
	}

	return obj
}

func SetLabel(key, value string) ParameterFactoryOption {
	return func(polardbxparameter *polardbxv1.PolarDBXParameter) {
		if polardbxparameter.Labels == nil {
			polardbxparameter.Labels = make(map[string]string)
		}
		polardbxparameter.Labels[key] = value
	}
}

func SetTemplateNameAndClusterName(templateName, clusterName string) ParameterFactoryOption {
	return func(polardbxparameter *polardbxv1.PolarDBXParameter) {
		polardbxparameter.Spec.TemplateName = templateName
		polardbxparameter.Spec.ClusterName = clusterName
	}
}

const (
	RollingRestart = "rollingRestart"
	Restart        = "restart"
)

func SetNodeType(role, name, restartType string, paramList []polardbxv1.Params) ParameterFactoryOption {
	switch role {
	case polardbxmeta.RoleCN:
		return func(polardbxparameter *polardbxv1.PolarDBXParameter) {
			polardbxparameter.Spec.NodeType.CN = polardbxv1.ParamNode{
				Name:        name,
				RestartType: restartType,
				ParamList:   paramList,
			}
		}
	case polardbxmeta.RoleDN:
		return func(polardbxparameter *polardbxv1.PolarDBXParameter) {
			polardbxparameter.Spec.NodeType.DN = polardbxv1.ParamNode{
				Name:        name,
				RestartType: restartType,
				ParamList:   paramList,
			}
		}
	case polardbxmeta.RoleGMS:
		return func(polardbxparameter *polardbxv1.PolarDBXParameter) {
			polardbxparameter.Spec.NodeType.GMS = &polardbxv1.ParamNode{
				Name:        name,
				RestartType: restartType,
				ParamList:   paramList,
			}
		}
	default:
		panic("unrecognized role: " + role)
	}
}

func NewPolarDBXParameterTemplate(name, namespace string, opts ...ParameterTemplateFactoryOption) *polardbxv1.PolarDBXParameterTemplate {
	obj := &polardbxv1.PolarDBXParameterTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	for _, opt := range opts {
		opt(obj)
	}

	return obj
}

func SetTemplateNodeType(role string, paramList []polardbxv1.TemplateParams) ParameterTemplateFactoryOption {
	switch role {
	case polardbxmeta.RoleCN:
		return func(polardbxparameter *polardbxv1.PolarDBXParameterTemplate) {
			polardbxparameter.Spec.NodeType.CN = polardbxv1.TemplateNode{
				Name:      "e2e-test",
				ParamList: paramList,
			}
		}
	case polardbxmeta.RoleDN:
		return func(polardbxparameter *polardbxv1.PolarDBXParameterTemplate) {
			polardbxparameter.Spec.NodeType.DN = polardbxv1.TemplateNode{
				Name:      "e2e-test",
				ParamList: paramList,
			}
		}
	default:
		panic("unrecognized role: " + role)
	}
}

// ExecCmd exec command on specific pod and wait the command's output.
func ExecCmd(client kubernetes.Interface, config *restclient.Config, pod *corev1.Pod, namespace string,
	command string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	if k8shelper.GetContainerFromPod(pod, convention.ContainerEngine) == nil {
		return errors.New("container " + "engine" + " not found in pod " + pod.Name)
	}
	cmd := []string{
		"sh",
		"-c",
		command,
	}
	req := client.
		CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(namespace).
		SubResource("exec").
		Timeout(5 * time.Second)
	option := &corev1.PodExecOptions{
		Container: convention.ContainerEngine,
		Command:   cmd,
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}
	if stdin == nil {
		option.Stdin = false
	}
	req.VersionedParams(
		option,
		scheme.ParameterCodec,
	)
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return err
	}
	ctx := context.Background()
	cancelFn := func() {}
	ctx, cancelFn = context.WithTimeout(ctx, 5*time.Second)
	defer cancelFn()

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	})
	if err != nil {
		return err
	}

	return nil
}
