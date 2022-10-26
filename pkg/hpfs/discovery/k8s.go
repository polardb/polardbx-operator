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

package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
)

const (
	HpfsNodeJsonFilePath = "/tools/xstore/hdfs-nodes.json"
)

type k8sHostDiscovery struct {
	kubernetes.Interface

	node      string
	namespace string
	selector  map[string]string

	mu         sync.RWMutex
	aliveHosts map[string]HostInfo
	closeChan  chan struct{}
}

func (h *k8sHostDiscovery) IsLocal(name string) bool {
	return h.node == name
}

func (h *k8sHostDiscovery) GetHost(name string) (HostInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	host, ok := h.aliveHosts[name]
	if !ok {
		return host, ErrHostNotFound
	}
	return host, nil
}

func (h *k8sHostDiscovery) GetHosts() (map[string]HostInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Return the cached hosts, no needs to copy
	return h.aliveHosts, nil
}

func (h *k8sHostDiscovery) labelSelector() string {
	l := make([]string, 0, len(h.selector))
	for k, v := range h.selector {
		l = append(l, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(l, ",")
}

func (h *k8sHostDiscovery) sync() error {
	hosts := make(map[string]HostInfo)

	// Retrieve from k8s
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hpfsPods, err := h.CoreV1().Pods(h.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: h.labelSelector(),
	})

	if err != nil {
		return err
	}

	for _, p := range hpfsPods.Items {
		// Skip those not ready
		if !k8shelper.IsPodReady(&p) {
			continue
		}

		c := &p.Spec.Containers[0]
		hpfsPort := k8shelper.GetPortFromContainer(c, "hpfs")
		if hpfsPort == nil {
			continue
		}
		sshPort := k8shelper.GetPortFromContainer(c, "ssh")
		if sshPort == nil {
			continue
		}
		filestreamPort := k8shelper.GetPortFromContainer(c, "filestream")
		if filestreamPort == nil {
			continue
		}

		hosts[p.Spec.NodeName] = HostInfo{
			NodeName: p.Spec.NodeName,
			HpfsHost: p.Status.PodIP,
			HpfsPort: uint32(hpfsPort.ContainerPort),
			SshPort:  uint32(sshPort.ContainerPort),
			FsPort:   uint32(filestreamPort.ContainerPort),
		}
	}

	// update the cached hosts
	h.mu.Lock()
	defer h.mu.Unlock()

	toPrint := !reflect.DeepEqual(h.aliveHosts, hosts)
	h.aliveHosts = hosts

	// Output alive hosts
	if toPrint {
		s, _ := json.Marshal(h.aliveHosts)
		fmt.Println("alive hosts: " + string(s))
		os.Remove(HpfsNodeJsonFilePath)
		f, err := os.OpenFile(HpfsNodeJsonFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		if err != nil {
			fmt.Println(fmt.Errorf("failed to open file %w", err))
		} else {
			f.Write(s)
			f.Close()
		}
	}

	return nil
}

func (h *k8sHostDiscovery) syncLoop() {
	for {
		select {
		case <-time.After(5 * time.Second):
			if err := h.sync(); err != nil {
				fmt.Printf("failed to sync nodes: %s\n", err.Error())
			}
		case <-h.closeChan:
			return
		}
	}
}

func (h *k8sHostDiscovery) Close() {
	close(h.closeChan)
}

func NewK8sHostDiscovery(node string, namespace string, selector map[string]string) (HostDiscovery, error) {
	if len(node) == 0 {
		return nil, errors.New("invalid node")
	}

	if selector == nil || len(selector) == 0 {
		return nil, errors.New("invalid selector")
	}

	if len(namespace) == 0 {
		namespace = "default"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	hs := &k8sHostDiscovery{
		Interface:  clientset,
		node:       node,
		namespace:  namespace,
		selector:   selector,
		aliveHosts: make(map[string]HostInfo),
		closeChan:  make(chan struct{}),
	}

	// Sync the first time, and then starts the sync loop
	if err = hs.sync(); err != nil {
		return nil, err
	}

	go hs.syncLoop()

	return hs, nil
}
