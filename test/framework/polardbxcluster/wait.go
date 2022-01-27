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

package polardbxcluster

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/test/framework/common"
)

var (
	poll = 2 * time.Second
)

func WaitForPolarDBXClusterToDisappear(c client.Client, name, namespace string, timeout time.Duration) error {
	return common.WaitForObjectToDisappear(c, name, namespace, poll, timeout, &polardbxv1.PolarDBXCluster{})
}

func WaitForPolarDBXClusterToInPhases(c client.Client, name, namespace string, phases []polardbxv1polardbx.Phase, timeout time.Duration) (*polardbxv1.PolarDBXCluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var polardbxcluster polardbxv1.PolarDBXCluster
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, &polardbxcluster)
		if err != nil {
			return true, err // stop wait with error
		}
		for _, p := range phases {
			if polardbxcluster.Status.Phase == p {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return &polardbxcluster, nil
}

func WaitUntilPolarDBXClusterToNotInPhases(c client.Client, name, namespace string, phases []polardbxv1polardbx.Phase, timeout time.Duration) (*polardbxv1.PolarDBXCluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var polardbxcluster polardbxv1.PolarDBXCluster
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, &polardbxcluster)
		if err != nil {
			return true, err // stop wait with error
		}
		for _, p := range phases {
			if polardbxcluster.Status.Phase == p {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return &polardbxcluster, nil
}

func WaitUntilPolarDBXClusterUpgradeCompleteOrFail(c client.Client, name, namespace string, timeout time.Duration) (*polardbxv1.PolarDBXCluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var polardbxcluster polardbxv1.PolarDBXCluster
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, &polardbxcluster)
		if err != nil {
			return true, err // stop wait with error
		}
		if polardbxcluster.Status.Phase != polardbxv1polardbx.PhaseRunning &&
			polardbxcluster.Status.Phase != polardbxv1polardbx.PhaseUpgrading {
			return true, nil
		}

		return polardbxcluster.Status.ObservedGeneration == polardbxcluster.Generation &&
			polardbxcluster.Status.Phase == polardbxv1polardbx.PhaseRunning, nil
	})
	if err != nil {
		return nil, err
	}
	return &polardbxcluster, nil
}
