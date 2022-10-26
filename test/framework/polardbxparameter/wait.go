package polardbxparameter

import (
	"bytes"
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/test/framework/common"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	poll = 2 * time.Second
)

func WaitPolarDBXParameterInPhase(c client.Client, name, namespace string,
	statuslist []polardbx.ParameterPhase, timeout time.Duration) (*polardbxv1.PolarDBXParameter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var polardbxparameter polardbxv1.PolarDBXParameter
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, &polardbxparameter)
		if err != nil {
			return true, err // stop wait with error
		}
		for _, s := range statuslist {
			if polardbxparameter.Status.Phase == s {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return &polardbxparameter, nil
}

func WaitForMyConfOverrideUpdates(clientset kubernetes.Interface, config *restclient.Config, pod corev1.Pod,
	ns string, timeout time.Duration, expectedParams map[string]string) error {
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		stdout, stderr := new(bytes.Buffer), new(bytes.Buffer)
		err := ExecCmd(clientset, config, &pod, ns, "cat /data/config/my.cnf.override", nil, stdout, stderr)
		common.ExpectNoError(err, "err in executing command")

		s := strings.ReplaceAll(stdout.String(), " ", "")
		configs := strings.Split(s, "\n")

		nowConfigs := make(map[string]string)
		for _, config := range configs {
			if config == "" || config[0] == '[' {
				continue
			}
			kv := strings.Split(config, "=")
			if len(kv) == 2 {
				nowConfigs[kv[0]] = kv[1]
			}
		}

		for k, v := range expectedParams {
			if nowConfigs[k] == v {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	return nil
}

func WaitForPolarDBXParameterToDisappear(c client.Client, name, namespace string, timeout time.Duration) error {
	return common.WaitForObjectToDisappear(c, name, namespace, poll, timeout, &polardbxv1.PolarDBXParameter{})
}

func WaitForPolarDBXParameterWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &polardbxv1.PolarDBXParameterList{})
}

func WaitForPolarDBXParameterTemplateToDisappear(c client.Client, name, namespace string, timeout time.Duration) error {
	return common.WaitForObjectToDisappear(c, name, namespace, poll, timeout, &polardbxv1.PolarDBXParameterTemplate{})
}
