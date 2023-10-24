package helper

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
)

func GetPaxosAddr(pod *corev1.Pod) string {
	paxosPort := MustGetPortFromContainer(MustGetContainerFromPod(pod, "engine"), "paxos").ContainerPort
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, paxosPort)
	return addr
}

func GetPaxosAddrs(pods []corev1.Pod) []string {
	addrs := make([]string, 0, len(pods))
	for _, pod := range pods {
		addrs = append(addrs, GetPaxosAddr(&pod))
	}
	return addrs
}
