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

package instance

import (
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/featuregate"
	xstoreexec "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

var CreateSecret = xstorev1reconcile.NewStepBinder("CreateSecret",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		secret, err := rc.GetXStoreSecret()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get secret.")
		}

		if secret == nil {
			secret = factory.NewSecret(xstore)
			err := rc.SetControllerRefAndCreate(secret)
			if err != nil {
				return flow.Error(err, "Unable to create secret.")
			}
		}

		return flow.Continue("Secret ready.")
	},
)

func CreatePodsAndHeadlessServicesWithExtraFactory(extraPodFactory factory.ExtraPodFactory) control.BindFunc {
	return xstorev1reconcile.NewStepBinder("CreatePodsAndHeadlessServices",
		func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			xstore := rc.MustGetXStore()

			topology := xstore.Status.ObservedTopology
			generation := xstore.Status.ObservedGeneration

			// Get current pods.
			pods, err := rc.GetXStorePods()
			if err != nil {
				return flow.Error(err, "Unable to get pods.")
			}
			podMap := k8shelper.BuildPodMap(pods, func(pod *corev1.Pod) string {
				return pod.Name
			})

			// Set stable single node set if there's no node set.
			nodeSets := topology.NodeSets
			if len(nodeSets) == 0 {
				nodeSets = []polardbxv1xstore.NodeSet{
					{
						Role:     polardbxv1xstore.RoleCandidate,
						Replicas: 1,
					},
				}
			}

			// For each node set and index, create or update if not found or not latest.
			newCnt := 0
			for _, nodeSet := range nodeSets {
				for i := 0; i < int(nodeSet.Replicas); i++ {
					podName := convention.NewPodName(xstore, &nodeSet, i)
					pod, exists := podMap[podName]
					if !exists {
						// Create a new one.
						pod, err = factory.NewPod(rc, xstore, &nodeSet, i, factory.PodFactoryOptions{
							ExtraPodFactory:     extraPodFactory,
							TemplateMergePolicy: factory.TemplateMergePolicyOverwrite,
						})
						if err != nil {
							return flow.Error(err, "Unable to construct new pod.", "pod", podName)
						}

						if err := rc.SetControllerRefAndCreate(pod); err != nil {
							return flow.Error(err, "Unable to create new pod", "pod", podName)
						}

						newCnt++
					} else {
						// update if generation is too old.
						observedGeneration, _ := convention.GetGenerationLabelValue(pod)

						if observedGeneration < generation {
							pod, err = factory.NewPod(rc, xstore, &nodeSet, i, factory.PodFactoryOptions{
								ExtraPodFactory:     extraPodFactory,
								TemplateMergePolicy: factory.TemplateMergePolicyOverwrite,
							})
							if err != nil {
								return flow.Error(err, "Unable to construct new pod.", "pod", podName)
							}

							if err := rc.SetControllerRef(pod); err != nil {
								return flow.Error(err, "Unable to set controller reference.", "pod", podName)
							}

							if err := rc.Client().Update(rc.Context(), pod); err != nil {
								return flow.Error(err, "Unable to update pod.", "pod", podName)
							}

							newCnt++
						}
					}
				}
			}

			if featuregate.EnableXStoreWithHeadlessService.Enabled() {
				// Get current headless services.
				headlessServices, err := rc.GetXStoreHeadlessServices()
				if err != nil {
					return flow.Error(err, "Unable to get headless services.")
				}

				// For each pod, create a headless service.
				for _, nodeSet := range nodeSets {
					for i := 0; i < int(nodeSet.Replicas); i++ {
						podName := convention.NewPodName(xstore, &nodeSet, i)
						_, exists := headlessServices[podName]
						if !exists {
							svc := factory.NewHeadlessService(xstore, podName)
							err := rc.SetControllerRefAndCreate(svc)
							if err != nil {
								return flow.Error(err, "Unable to create headless service for pod.", "pod", podName)
							}
						}
					}
				}
			}

			if newCnt > 0 {
				return flow.Wait("Some pod's updated or created!")
			}

			return flow.Pass()
		},
	)
}

var WaitUntilPodsScheduled = xstorev1reconcile.NewStepBinder("WaitUntilPodsScheduled",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		// Should ensure that pod has been assigned an IP.
		unscheduled := k8shelper.FilterPodsBy(pods, func(pod *corev1.Pod) bool {
			return !k8shelper.IsPodScheduled(pod) || pod.Status.PodIP == ""
		})

		if len(unscheduled) > 0 {
			return flow.Wait("Found unscheduled pods, keep waiting...", "unscheduled-pods",
				strings.Join(k8shelper.ToObjectNames(unscheduled), ","))
		}

		return flow.Pass()
	},
)

var WaitUntilPodsReady = xstorev1reconcile.NewStepBinder("WaitUntilPodsReady",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		unready := k8shelper.FilterPodsBy(pods, func(pod *corev1.Pod) bool {
			return !k8shelper.IsPodReady(pod)
		})

		if len(unready) > 0 {
			return flow.Wait("Found unready pods, keep waiting...", "unready-pods",
				strings.Join(k8shelper.ToObjectNames(unready), ","))
		}

		return flow.Pass()
	},
)

var WaitUntilCandidatesAndVotersReady = xstorev1reconcile.NewStepBinder("WaitUntilCandidatesAndVotersReady",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		for _, pod := range pods {
			// Ignore learner pods.
			if xstoremeta.IsPodRoleLearner(&pod) {
				continue
			}

			if !k8shelper.IsPodReady(&pod) {
				return flow.Wait("Found candidate or voter pod not ready. Just wait.",
					"pod", pod.Name, "pod.phase", pod.Status.Phase)
			}

			// Do connectivity check locally.
			err := xstoreexec.CheckConnectivityLocally(rc, &pod, "engine", flow.Logger())
			if err != nil {
				return flow.Error(err, "Failed to check connectivity locally.", "pod", pod.Name)
			}
		}
		return flow.Continue("All candidates and voters are ready for connections.")
	},
)

var WaitUntilLearnersReady = xstorev1reconcile.NewStepBinder("WaitUntilLearnersReady",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		for _, pod := range pods {
			// Ignore non-learner pods.
			if !xstoremeta.IsPodRoleLearner(&pod) {
				continue
			}

			if !k8shelper.IsPodReady(&pod) {
				return flow.Wait("Found learner pod not ready. Just wait.",
					"pod", pod.Name, "pod.phase", pod.Status.Phase)
			}

			// Do connectivity check locally.
			err := xstoreexec.CheckConnectivityLocally(rc, &pod, "engine", flow.Logger())
			if err != nil {
				return flow.Error(err, "Failed to check connectivity locally.", "pod", pod.Name)
			}
		}
		return flow.Continue("All learners are ready for connections.")
	},
)

func WhenPodsDeletedFound(binders ...control.BindFunc) control.BindFunc {
	return xstorev1reconcile.NewStepIfBinder("PodsDeletedFound",
		func(rc *xstorev1reconcile.Context, log logr.Logger) (bool, error) {
			xstore := rc.MustGetXStore()
			topology := xstore.Status.ObservedTopology

			expectedPods := 0
			for _, nodeSet := range topology.NodeSets {
				expectedPods += int(nodeSet.Replicas)
			}

			pods, err := rc.GetXStorePods()
			if err != nil {
				return false, err
			}

			notFailedPods := 0
			for _, pod := range pods {
				if !k8shelper.IsPodFailed(&pod) {
					notFailedPods++
				}
			}

			return notFailedPods < expectedPods, nil
		}, binders...)
}
