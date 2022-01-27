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

package common

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/onsi/ginkgo"

	"github.com/alibaba/polardbx-operator/test/framework/log"
)

type StopFunc func()

func StartPortForward(kubeconfig string, svc, ns, port string, localPort int) (StopFunc, error) {
	args := []string{
		"-n", ns, "port-forward", "svc/" + svc, fmt.Sprintf("%d:%s", localPort, port),
	}
	if len(kubeconfig) > 0 {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	cmd := exec.Command("kubectl", args...)
	cmd.Stdout = ginkgo.GinkgoWriter
	cmd.Stderr = ginkgo.GinkgoWriter

	log.Logf("Start kubectl %s", strings.Join(args, " "))
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	proc := cmd.Process
	return func() {
		log.Logf("Stop kubectl %s", strings.Join(args, " "))
		_ = proc.Kill()
		_, _ = proc.Wait()
	}, nil
}
