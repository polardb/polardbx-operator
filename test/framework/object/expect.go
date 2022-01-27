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

package object

import (
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
)

func ExpectToHaveContainer(pod *corev1.Pod, container string) {
	c := k8shelper.GetContainerFromPod(pod, container)
	gomega.Expect(c).NotTo(gomega.BeNil(), "container "+container+" not found in pod "+pod.Name)
}

func ExpectToHaveResources(pod *corev1.Pod, container string, resources corev1.ResourceRequirements) {
	ExpectToHaveContainer(pod, container)
	c := k8shelper.GetContainerFromPod(pod, container)
	gomega.Expect(c.Resources).To(gomega.BeEquivalentTo(resources), "resources of container "+container+" not match")
}

func ExpectToBeHostNetworkEqualTo(pod *corev1.Pod, hostNetwork bool) {
	gomega.Expect(pod.Spec.HostNetwork).To(gomega.BeEquivalentTo(hostNetwork), "host network not match")
}

func ExpectToBeScheduled(pod *corev1.Pod) {
	gomega.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty(), "not scheduled yet")
}

func ExpectHaveLabel(obj client.Object, key string) {
	gomega.Expect(obj.GetLabels()).To(gomega.HaveKey(key), "label "+key+" not found")
}

func ExpectAllHaveLabel(objList interface{}, key string) {

}

func ExpectNotHaveLabel(obj client.Object, key string) {
	gomega.Expect(obj.GetLabels()).NotTo(gomega.HaveKey(key), "label "+key+" found")
}

func ExpectHaveLabelWithValue(obj client.Object, key string, value string) {
	gomega.Expect(obj.GetLabels()).To(gomega.HaveKeyWithValue(key, value), "label "+key+" not found or with value "+value)
}

func ExpectHaveAnnotation(obj client.Object, key string) {
	gomega.Expect(obj.GetAnnotations()).To(gomega.HaveKey(key), "annotation "+key+" not found")
}

func ExpectNotHaveAnnotation(obj client.Object, key string) {
	gomega.Expect(obj.GetAnnotations()).NotTo(gomega.HaveKey(key), "annotation "+key+" found")
}

func ExpectHaveAnnotationWithValue(obj client.Object, key string, value string) {
	gomega.Expect(obj.GetAnnotations()).To(gomega.HaveKeyWithValue(key, value), "annotation "+key+" not found or with value "+value)
}

func ExpectHaveFinalizer(obj client.Object, finalizer string) {
	gomega.Expect(obj.GetFinalizers()).To(gomega.ContainElement(finalizer), "finalizer "+finalizer+" not found")
}

func ExpectNoFinalizers(obj client.Object) {
	gomega.Expect(obj.GetFinalizers()).To(gomega.BeEmpty(), "finalizers not empty")
}
