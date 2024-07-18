package cpusetbind

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/convention"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/cri"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/pool"
	cpusetbind "github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/system"
	polarxJson "github.com/alibaba/polardbx-operator/pkg/util/json"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Manager struct {
	kubernetes.Interface
	nodeName              string
	namespace             string
	criClient             *cri.Client
	outputFilepath        string
	originLogger          logr.Logger
	logger                logr.Logger
	reservedCpus          string
	kubeletParams         map[string]string
	node                  *v1.Node
	currentCpuSetStrategy config.CpuSetStrategy
	resetValue            string
	forceReallocate       bool
}

func NewManager(logger logr.Logger, nodeName string, namespace string, outputFilepath string) *Manager {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	return &Manager{
		Interface:      clientset,
		nodeName:       nodeName,
		namespace:      namespace,
		outputFilepath: outputFilepath,
		logger:         logger.WithName("isolate-cpu-manager"),
		originLogger:   logger.WithName("isolate-cpu-manager"),
	}
}

func (m *Manager) IsNodeCpuIsolated() bool {
	return m.isNodeLabelTrue(convention.NodeLabelIsolateCpu)
}

func (m *Manager) IsNodeCpuIsolateDisabled() bool {
	return m.isNodeLabelTrue(convention.NodeLabelIsolateCpuDisable)
}

func (m *Manager) LoadNodeInfo() error {
	node, err := m.CoreV1().Nodes().Get(context.TODO(), m.nodeName, metav1.GetOptions{})
	if err != nil {
		m.logger.Error(err, "failed to get node", "nodeName", m.nodeName)
		return err
	}
	m.node = node
	return nil
}

func (m *Manager) isNodeLabelTrue(nodeLabel string) bool {
	if m.node != nil && len(m.node.Labels) > 0 {
		labelIsolateCpuVal, ok := m.node.Labels[nodeLabel]
		if ok {
			if strings.TrimSpace(labelIsolateCpuVal) == "true" {
				return true
			}
		}
	}
	return false
}

func (m *Manager) needReset() bool {
	result := false
	if m.node != nil && len(m.node.Labels) > 0 {
		labelIsolateCpuVal, ok := m.node.Labels[convention.NodeLabelIsolateCpuReset]
		if ok {
			if m.resetValue != "" && m.resetValue != labelIsolateCpuVal {
				result = true
			}
			m.resetValue = labelIsolateCpuVal
		}
	}
	return result
}

func (m *Manager) IsForceReallocate() bool {
	return m.forceReallocate
}

func (m *Manager) SetForceReallocate(forceReallocate bool) {
	m.forceReallocate = forceReallocate
}

func (m *Manager) parseKubeletCmdLine() error {
	cmdParams, err := cpusetbind.GetCmdLineByComm("kubelet")
	if err != nil {
		m.logger.Error(err, "failed to get cmdline")
		return err
	}
	kubeletParams := map[string]string{}
	for _, param := range cmdParams {
		paramKv := strings.Split(strings.TrimSpace(param), "=")
		if len(paramKv) == 2 {
			kubeletParams[strings.TrimLeft(strings.TrimSpace(paramKv[0]), "-")] = strings.TrimSpace(paramKv[1])
		}
	}
	m.kubeletParams = kubeletParams
	return nil
}

func (m *Manager) createCriClient() error {
	if m.criClient != nil {
		m.criClient.Close()
		m.criClient = nil
	}
	containerRuntimeEndpoints := make([]string, 0)
	containerRuntimeEndpoints = append(containerRuntimeEndpoints, m.getCriEndpointInConfig())
	containerRuntimeEndpoints = append(containerRuntimeEndpoints, m.kubeletParams[convention.KubeletContainerRuntimeEndpoint])
	containerRuntimeEndpoints = append(containerRuntimeEndpoints, "/var/run/dockershim.sock", "/run/containerd/containerd.sock", "/run/crio/crio.sock")
	for _, containerRuntimeEndpoint := range containerRuntimeEndpoints {
		if containerRuntimeEndpoint == "" {
			continue
		}
		if !strings.HasPrefix(containerRuntimeEndpoint, "unix://") {
			containerRuntimeEndpoint = fmt.Sprintf("unix://%s", containerRuntimeEndpoint)
		}
		criClient, err := cri.NewClient(m.logger, containerRuntimeEndpoint)
		if err != nil {
			m.logger.Error(err, "failed to find cri client", "containerRuntimeEndpoint", containerRuntimeEndpoint)
			continue
		}
		m.logger.Info("succeeding to get cri client", "cirEndpoint", containerRuntimeEndpoint)
		m.criClient = criClient
		break
	}
	if m.criClient == nil {
		return errors.New("failed to get cri client")
	}
	return nil
}

func (m *Manager) markCpuReserved(reservedCpus map[int]bool, cpusStr string) {
	cpus, err := cpusetbind.ParseCpuSetFormat(cpusStr)
	if err != nil {
		m.logger.Error(err, "failed to parse cpu set", "cpus", cpusStr)
	}
	if len(cpus) > 0 {
		for _, cpu := range cpus {
			reservedCpus[cpu] = true
		}
	}
}

func (m *Manager) getCriEndpointInConfig() string {
	hpfsConfig := config.GetConfig()
	if hpfsConfig.CGroupControlConfig != nil {
		return hpfsConfig.CGroupControlConfig.CriEndpoint
	}
	return ""
}

func (m *Manager) collectReservedCpus() []int {
	reservedCpus := map[int]bool{}
	// 1. from hpfs config
	hpfsConfig := config.GetConfig()
	if hpfsConfig.CGroupControlConfig != nil {
		configCpus := hpfsConfig.CGroupControlConfig.ReservedCpus
		if configCpus != "" {
			m.markCpuReserved(reservedCpus, configCpus)
		}
	}
	// 2. from kubelet param of reserved-cpus
	kubeletReservedCpus := m.kubeletParams[convention.KubeletReservedCpus]
	if kubeletReservedCpus != "" {
		m.markCpuReserved(reservedCpus, kubeletReservedCpus)
	}
	result := make([]int, 0, len(reservedCpus))
	for k, _ := range reservedCpus {
		result = append(result, k)
	}
	return result
}

func (m *Manager) updateLoggerTrace() {
	m.logger = m.originLogger.WithValues("trace", uuid.New().String())
}

func (m *Manager) createCpuSetPool() pool.CpuSetPool {
	cpuSetStrategy := config.Auto
	defer func() {
		// record the cpu set strategy
		m.currentCpuSetStrategy = cpuSetStrategy
	}()
	hpfsConfig := config.GetConfig()
	if hpfsConfig.CGroupControlConfig != nil && hpfsConfig.CGroupControlConfig.CpuSetStrategy != nil {
		cpuSetStrategy = *hpfsConfig.CGroupControlConfig.CpuSetStrategy
	}
	if cpuSetStrategy == config.Auto {
		cpuSetStrategy = config.Normal
		// check if numa is open
		numaOpen, err := cpusetbind.IsNumaOpen()
		if err != nil {
			m.logger.Error(err, "failed to check numa open")
		}
		if numaOpen {
			cpuSetStrategy = config.NumaNodePrefer
		}
	}
	if string(m.currentCpuSetStrategy) != "" {
		if m.currentCpuSetStrategy != cpuSetStrategy {
			m.SetForceReallocate(true)
		}
	}
	if cpuSetStrategy == config.NumaNodePrefer {
		return pool.NewNumaCpuSetPool(cpusetbind.GetCpuInfo())
	}
	return pool.NewNormalCpuSetPool(cpusetbind.GetCpuInfo())
}

func (m *Manager) syncCpuSet() error {
	m.logger.Info("sync cpu set, timeout is 2 minutes")
	ctx, _ := context.WithTimeout(context.Background(), time.Minute*5)
	runtimes, err := m.criClient.ListEngineRuntimes(ctx)
	if err != nil {
		m.logger.Error(err, "failed to list engine runtimes")
		return err
	}
	outputBuf := bytes.Buffer{}
	cpuSetPool := m.createCpuSetPool()
	reservedCpus := m.collectReservedCpus()
	cpuSetPool.MarkAllocated(reservedCpus)

	if cpuSetPool.ConsiderNuma() {
		podPreferNumaMap, err := cpusetbind.GetPodPreferNuma()
		if err != nil {
			m.logger.Error(err, "failed to get pod prefer numa map")
		}
		if len(podPreferNumaMap) > 0 {
			for i := range runtimes {
				podName := runtimes[i].Pod()
				if numaNode, ok := podPreferNumaMap[podName]; ok {
					runtimes[i].SetPreferredNuma(strconv.FormatInt(int64(numaNode), 10))
				}
			}
		}
	}
	m.logger.Info(fmt.Sprintf("cpu set pool name = %s, forceReallocate = %v", cpuSetPool.Name(), m.IsForceReallocate()))
	for _, runtime := range runtimes {
		outputBuf.WriteString(runtime.String())
		outputBuf.WriteString("\n")
		if runtime.ContainerResources() != nil && runtime.ContainerResources().Linux != nil {
			linuxContainerResources := runtime.ContainerResources().Linux

			if runtime.ForcedCpuSet() != "" {
				cpuSet, err := cpusetbind.ParseCpuSetFormat(runtime.ForcedCpuSet())
				if err != nil {
					m.logger.Error(err, "failed to parse forced cpuset cpus", "cpuset cpus", runtime.ForcedCpuSet(), "namespace", runtime.Namespace(), "pod", runtime.Pod(), "container id", runtime.Id())
					return err
				}
				cpuSetPool.MarkAllocated(cpuSet)
				if runtime.ForcedCpuSet() != linuxContainerResources.CpusetCpus {
					m.updateCpuSet(ctx, runtime, cpuSet)
				}
				continue
			}

			if linuxContainerResources.CpusetCpus != "" && !m.IsForceReallocate() {
				cpuSet, err := cpusetbind.ParseCpuSetFormat(linuxContainerResources.CpusetCpus)
				if err != nil {
					m.logger.Error(err, "failed to parse cpuset cpus", "cpuset cpus", linuxContainerResources.CpusetCpus, "namespace", runtime.Namespace(), "pod", runtime.Pod(), "container id", runtime.Id())
					return err
				}
				if len(cpuSet) == runtime.CpuCores() {
					if cpuSetPool.TestAvailable(cpuSet) {
						cpuSetPool.MarkAllocated(cpuSet)
						continue
					}
				}
			}
			// cpu set binding has not been set.
			// allocate cpu set here
			if linuxContainerResources.CpuQuota != 0 && linuxContainerResources.CpuPeriod != 0 && linuxContainerResources.CpuQuota >= linuxContainerResources.CpuPeriod {
				cpuCores := linuxContainerResources.CpuQuota / linuxContainerResources.CpuPeriod

				if runtime.PreferredNuma() != "" {
					numaNodeInt, err := strconv.Atoi(runtime.PreferredNuma())
					if err == nil {
						cpuSetPool.SetPreferredNumaNode(numaNodeInt)
					}
				}

				cpuset, err := cpuSetPool.AllocateProcessors(int(cpuCores))
				if err != nil {
					m.logger.Error(err, "failed to allocate processors", "namespace", runtime.Namespace(), "pod", runtime.Pod(), "container id", runtime.Id())
					return err
				}
				m.updateCpuSet(ctx, runtime, cpuset)
			}
		}
	}
	writeFileErr := os.WriteFile(m.outputFilepath, outputBuf.Bytes(), 0644)
	if err != nil {
		m.logger.Error(writeFileErr, "failed to write file", "filepath", m.outputFilepath)
	}
	return nil
}

func (m *Manager) updateCpuSet(ctx context.Context, runtime cri.Runtime, cpuSet []int) {
	convertedCpuSet := cpusetbind.Convert2CpuSetFormat(cpuSet)
	m.logger.Info("allocate", "cpuset", convertedCpuSet, "namespace", runtime.Namespace(), "pod", runtime.Pod(), "container id", runtime.Id())
	runtime.ContainerResources().Linux.CpusetCpus = convertedCpuSet
	m.logger.Info("begin persist")
	runtime.ContainerResources().Linux.CpuPeriod = 0
	runtime.ContainerResources().Linux.CpuQuota = 0
	persistErr := m.criClient.PersistContainerResources(ctx, runtime)
	if persistErr != nil {
		m.logger.Error(persistErr, "failed to persist container resource", "cpuset", convertedCpuSet, "namespace", runtime.Namespace(), "pod", runtime.Pod(), "container id", runtime.Id())
	} else {
		m.logger.Info("finish persist")
	}
}

func (m *Manager) Start() error {
	if runtime.GOOS != "linux" {
		return errors.New("os must be linux")
	}
	cpusetbind.InitCpuInfo()
	err := m.parseKubeletCmdLine()
	if err != nil {
		return err
	}
	err = m.createCriClient()
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				m.logger.Error(errors.New(polarxJson.Convert2JsonString(err)), "failed to init cpu bind")
			}
		}()
		for {
			select {
			case <-time.After(30 * time.Second):
			}
			err := m.LoadNodeInfo()
			if err != nil {
				m.logger.Error(err, "failed to load node info")
				continue
			}
			if m.needReset() && !m.IsForceReallocate() {
				m.SetForceReallocate(true)
			}
			if m.IsNodeCpuIsolated() && !m.IsNodeCpuIsolateDisabled() {
				err = m.createCriClient()
				if err != nil {
					m.logger.Error(err, "failed to get cri client")
					continue
				}
				m.updateLoggerTrace()
				m.syncCpuSet()
				m.SetForceReallocate(false)
			}
		}
	}()
	return nil
}
