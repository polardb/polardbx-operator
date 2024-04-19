package cri

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/convention"
	polarxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	runtimeapiV1alpha2 "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const maxMsgSize = 1024 * 1024 * 16

const CpuSetContainerId = "polarx_cpuset_container_id"
const CpuSet = "polarx_cpuset"
const EnvCpuCores = "cpu_cores"
const EnvLimitsCpu = "LIMITS_CPU"
const CpuSetGetCmd = `
env | grep -i cpu
echo ''
echo showcpubind
if [[ -f /tmp/cpubind ]]; then
cat /tmp/cpubind
fi
`
const ShowLabelAndAnnotationCmd = `
cat /etc/podinfo/annotations
echo ''
cat /etc/podinfo/labels
`

const CpuSetWriteCmd = "echo '%s' >> /tmp/cpubind"

type Client struct {
	address               string
	runtimeClient         runtimeapi.RuntimeServiceClient
	runtimeClientV1alpha2 runtimeapiV1alpha2.RuntimeServiceClient
	conn                  *grpc.ClientConn
	logger                logr.Logger
}

func NewClient(logger logr.Logger, address string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialOpts := make([]grpc.DialOption, 0)
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		logger.Error(err, "failed to dial unix socket", "address", address)
		return nil, err
	}
	runtimeClientV1, runtimeClientV1alpha2, err := initRuntimeClient(conn)
	if err != nil {
		logger.Error(err, "failed to find runtime client")
		return nil, err
	}
	if runtimeClientV1 != nil {
		logger.Info("cri api use v1")
	}
	if runtimeClientV1alpha2 != nil {
		logger.Info("cri api use v1alpha2")
	}
	return &Client{
		conn:                  conn,
		address:               address,
		runtimeClient:         runtimeClientV1,
		runtimeClientV1alpha2: runtimeClientV1alpha2,
		logger:                logger.WithValues("component", "cri client"),
	}, nil
}

func initRuntimeClient(conn *grpc.ClientConn) (runtimeapi.RuntimeServiceClient, runtimeapiV1alpha2.RuntimeServiceClient, error) {
	runtimeApiCtx, runtimeApiCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer runtimeApiCancel()
	runtimeClientV1 := runtimeapi.NewRuntimeServiceClient(conn)
	_, err := runtimeClientV1.Version(runtimeApiCtx, &runtimeapi.VersionRequest{})
	if err == nil {
		return runtimeClientV1, nil, nil
	}
	fmt.Println(fmt.Sprintf("v1 %+v", err))

	runtimeApiV1alpha2Ctx, runtimeApiV1alpha2Cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer runtimeApiV1alpha2Cancel()
	runtimeClientV1alpha2 := runtimeapiV1alpha2.NewRuntimeServiceClient(conn)
	_, err = runtimeClientV1alpha2.Version(runtimeApiV1alpha2Ctx, &runtimeapiV1alpha2.VersionRequest{})
	if err == nil {
		return nil, runtimeClientV1alpha2, nil
	}
	fmt.Println(fmt.Sprintf("v2 %+v", err))
	return nil, nil, errors.New("not found")
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) PersistContainerResources(ctx context.Context, runtime Runtime) error {
	if c.runtimeClient != nil {
		c.logger.Info("api version: v1")
		_, err := c.runtimeClient.UpdateContainerResources(ctx, &runtimeapi.UpdateContainerResourcesRequest{
			ContainerId: runtime.Id(),
			Linux:       runtime.ContainerResources().Linux,
		})
		if err != nil {
			return err
		}
		c.writeCpuInfoByExec(ctx, runtime.Id(), runtime.ContainerResources().Linux.CpusetCpus)
		return nil
	}

	if c.runtimeClientV1alpha2 != nil {
		c.logger.Info("api version: v1alpha2")
		_, err := c.runtimeClientV1alpha2.UpdateContainerResources(ctx, &runtimeapiV1alpha2.UpdateContainerResourcesRequest{
			ContainerId: runtime.Id(),
			Linux:       (*runtimeapiV1alpha2.LinuxContainerResources)(unsafe.Pointer(runtime.ContainerResources().Linux)),
		})
		if err != nil {
			return err
		}
		c.writeCpuInfoByExec(ctx, runtime.Id(), runtime.ContainerResources().Linux.CpusetCpus)
		return nil
	}

	return errors.New("no client")
}

func (c *Client) ListPodSandboxes(ctx context.Context) ([]*runtimeapi.PodSandbox, error) {
	if c.runtimeClient != nil {
		c.logger.Info("api version: v1")
		resp, err := c.runtimeClient.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
			Filter: &runtimeapi.PodSandboxFilter{
				State: &runtimeapi.PodSandboxStateValue{
					State: runtimeapi.PodSandboxState_SANDBOX_READY,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return resp.GetItems(), nil
	}

	if c.runtimeClientV1alpha2 != nil {
		c.logger.Info("api version: v1alpha2")
		resp, err := c.runtimeClientV1alpha2.ListPodSandbox(ctx, &runtimeapiV1alpha2.ListPodSandboxRequest{
			Filter: &runtimeapiV1alpha2.PodSandboxFilter{
				State: &runtimeapiV1alpha2.PodSandboxStateValue{
					State: runtimeapiV1alpha2.PodSandboxState_SANDBOX_READY,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		items := make([]*runtimeapi.PodSandbox, 0)
		if resp.GetItems() != nil {
			for _, item := range resp.GetItems() {
				items = append(items, (*runtimeapi.PodSandbox)(unsafe.Pointer(item)))
			}
		}
		return items, nil
	}

	return nil, nil
}

func (c *Client) ListContainers(ctx context.Context) ([]*runtimeapi.Container, error) {
	if c.runtimeClient != nil {
		c.logger.Info("api version: v1")
		resp, err := c.runtimeClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
			Filter: &runtimeapi.ContainerFilter{
				State: &runtimeapi.ContainerStateValue{
					State: runtimeapi.ContainerState_CONTAINER_RUNNING,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return resp.Containers, err
	}

	if c.runtimeClientV1alpha2 != nil {
		c.logger.Info("api version: v1alpha2")
		resp, err := c.runtimeClientV1alpha2.ListContainers(ctx, &runtimeapiV1alpha2.ListContainersRequest{
			Filter: &runtimeapiV1alpha2.ContainerFilter{
				State: &runtimeapiV1alpha2.ContainerStateValue{
					State: runtimeapiV1alpha2.ContainerState_CONTAINER_RUNNING,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		containers := make([]*runtimeapi.Container, 0)
		if resp.Containers != nil {
			for _, container := range resp.Containers {
				containers = append(containers, (*runtimeapi.Container)(unsafe.Pointer(container)))
			}
		}
		return containers, nil
	}

	return nil, errors.New("no client")
}

func (c *Client) ContainerStatus(ctx context.Context, containerId string) (*runtimeapi.ContainerStatusResponse, error) {
	if c.runtimeClient != nil {
		c.logger.Info("api version: v1")
		resp, err := c.runtimeClient.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	if c.runtimeClientV1alpha2 != nil {
		c.logger.Info("api version: v1alpha2")
		resp, err := c.runtimeClientV1alpha2.ContainerStatus(ctx, &runtimeapiV1alpha2.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return nil, err
		}
		return (*runtimeapi.ContainerStatusResponse)(unsafe.Pointer(resp)), nil
	}
	return nil, errors.New("no client")
}

func (c *Client) ExecCommand(ctx context.Context, containerId string, cmd string) ([]byte, []byte, error) {
	c.logger.Info(fmt.Sprintf("ExecCommand containerId = %s , cmd = %s", containerId, cmd))
	if c.runtimeClient != nil {
		c.logger.Info("api version: v1")
		resp, err := c.runtimeClient.ExecSync(ctx, &runtimeapi.ExecSyncRequest{
			ContainerId: containerId,
			Cmd:         []string{"/bin/bash", "-c", cmd},
		})
		if err != nil {
			return nil, nil, err
		}
		return resp.Stdout, resp.Stderr, nil
	}

	if c.runtimeClientV1alpha2 != nil {
		c.logger.Info("api version: v1alpha2")
		resp, err := c.runtimeClientV1alpha2.ExecSync(ctx, &runtimeapiV1alpha2.ExecSyncRequest{
			ContainerId: containerId,
			Cmd:         []string{"/bin/bash", "-c", cmd},
		})
		if err != nil {
			return nil, nil, err
		}
		return resp.Stdout, resp.Stderr, nil
	}
	return nil, nil, errors.New("no client")
}

func (c *Client) ListEngineRuntimes(ctx context.Context) ([]Runtime, error) {
	c.logger.Info("begin list engine runtimes")
	podSandboxes, err := c.ListPodSandboxes(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list pod sand box")
	}
	containers, err := c.ListContainers(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list containers")
	}
	podSandboxIdContainerMap := map[string]*runtimeapi.Container{}
	for _, container := range containers {
		if container.Metadata.Name == "engine" {
			podSandboxIdContainerMap[container.PodSandboxId] = container
		}
	}

	runtimes := make([]Runtime, 0)
	// filter podsandbox by label of polardbx/name
	for _, podSandbox := range podSandboxes {
		//podSandbox.Labels
		if len(podSandbox.Labels) > 0 {
			_, polardbxNameExist := podSandbox.Labels[polarxmeta.LabelName]
			_, xstoreNameExist := podSandbox.Labels[xstoremeta.LabelName]
			_, containerExist := podSandboxIdContainerMap[podSandbox.Id]
			if (polardbxNameExist || xstoreNameExist) && containerExist {
				var instanceName string
				if polardbxNameExist {
					instanceName = podSandbox.Labels[polarxmeta.LabelName]
				} else {
					instanceName = podSandbox.Labels[xstoremeta.LabelName]
				}
				container := podSandboxIdContainerMap[podSandbox.Id]
				containerStatusResponse, err := c.ContainerStatus(ctx, container.Id)
				if err != nil {
					c.logger.Error(err, "failed to get container status", "container id", container.Id)
					return nil, err
				}
				jsonBytes, _ := json.Marshal(containerStatusResponse)
				c.logger.Info(fmt.Sprintf("containerStatusResponse %s", string(jsonBytes)))
				var linuxContainerResources *runtimeapi.LinuxContainerResources
				if containerStatusResponse.Status != nil && containerStatusResponse.Status.Resources != nil && containerStatusResponse.Status.Resources.Linux != nil {
					linuxContainerResources = containerStatusResponse.Status.Resources.Linux
				}
				if linuxContainerResources == nil {
					// get resource from info if resource in the status is nil
					if containerStatusResponse.Info != nil {
						if containerInfoJson, ok := containerStatusResponse.Info["info"]; ok {
							containerInfo := ContainerInfo{}
							err := json.Unmarshal([]byte(containerInfoJson), &containerInfo)
							if err != nil {
								return nil, err
							}
							if containerInfo.RuntimeSpec != nil && containerInfo.RuntimeSpec.Linux != nil && containerInfo.RuntimeSpec.Linux.Resources != nil && containerInfo.RuntimeSpec.Linux.Resources.Cpu != nil && containerInfo.RuntimeSpec.Linux.Resources.Cpu.Quota != 0 && containerInfo.RuntimeSpec.Linux.Resources.Cpu.Period != 0 {
								linuxContainerResources = &runtimeapi.LinuxContainerResources{
									CpuPeriod:  containerInfo.RuntimeSpec.Linux.Resources.Cpu.Period,
									CpuQuota:   containerInfo.RuntimeSpec.Linux.Resources.Cpu.Quota,
									CpusetCpus: containerInfo.RuntimeSpec.Linux.Resources.Cpu.Cpus,
								}
							}
						}
					}
				}

				var useExec bool
				if linuxContainerResources == nil {
					// get by exec
					cpuCores, cpuSetContainerId, cpuSet, err := c.getCpuInfoByExec(ctx, container.Id)
					if err != nil {
						c.logger.Error(err, "failed to get cpu info by exec", "containerId", container.Id)
					} else {
						c.logger.Info("succeed to get cpu info by exec", "cpuCores", cpuCores, "cpuSetContainerId", cpuSetContainerId, "cpuSet", cpuSet)
						if cpuSetContainerId != container.Id {
							// the cpu set info does not belong to container of  container.Id
							cpuSet = ""
						}
						linuxContainerResources = &runtimeapi.LinuxContainerResources{
							CpuPeriod:  1,
							CpuQuota:   int64(cpuCores),
							CpusetCpus: cpuSet,
						}
						useExec = true
					}
				}

				if linuxContainerResources != nil {
					var forcedCpuSet string
					labelAnnotations, _ := c.getLabelAndAnnotation(ctx, container.Id)
					if labelAnnotations != nil {
						forcedCpuSet = labelAnnotations[convention.PodAnnotationIsolateCpuCpuSet]
					}
					runtime := engineRuntime{
						image:              container.Image.Image,
						id:                 container.Id,
						createdAt:          time.UnixMicro(container.CreatedAt / 1000),
						namespace:          podSandbox.GetMetadata().Namespace,
						pod:                podSandbox.GetMetadata().Name,
						polardbXCluster:    instanceName,
						containerResources: &runtimeapi.ContainerResources{Linux: linuxContainerResources},
						useExec:            useExec,
						forcedCpuSet:       forcedCpuSet,
					}
					runtimes = append(runtimes, &runtime)
					c.logger.Info("new runtime", "image", runtime.image, "id", runtime.id, "namespace", runtime.namespace, "pod", runtime.pod, "polardbXCluster", runtime.polardbXCluster)
				}
			}
		}
	}
	sort.Slice(runtimes, func(i int, j int) bool {
		return strings.Compare(runtimes[j].CompareStringVal(), runtimes[i].CompareStringVal()) < 0
	})
	return runtimes, nil
}

func (c *Client) getCpuInfoByExec(ctx context.Context, containerId string) (cpuCores int, cpuSetContainerId string, cpuSet string, err error) {
	stdoutBytes, stderrBytes, err := c.ExecCommand(ctx, containerId, CpuSetGetCmd)
	if len(stdoutBytes) == 0 && err != nil {
		c.logger.Error(err, " ")
		return 0, "", "", err
	}
	stdout := string(stdoutBytes)
	stderr := string(stderrBytes)
	c.logger.Info(fmt.Sprintf("stdout %s stderr %s", stdout, stderr))
	lines := strings.Split(stdout, "\n")
	for _, line := range lines {
		envItem := strings.Split(strings.TrimSpace(line), "=")
		if len(envItem) == 2 {
			key := strings.TrimSpace(envItem[0])
			value := strings.TrimSpace(envItem[1])
			switch key {
			case CpuSetContainerId:
				cpuSetContainerId = value
				break
			case CpuSet:
				cpuSet = value
				break
			case EnvCpuCores:
				cpuCores, err = strconv.Atoi(value)
				break
			case EnvLimitsCpu:
				var limitsCpu int
				limitsCpu, err = strconv.Atoi(value)
				cpuCores = limitsCpu / 1000
				break
			}
		}
	}
	if cpuCores == 0 {
		return 0, "", "", errors.New("not found")
	}
	return
}

func (c *Client) writeCpuInfoByExec(ctx context.Context, containerId string, cpuSet string) error {
	content := fmt.Sprintf("%s=%s\n%s=%s", CpuSetContainerId, containerId, CpuSet, cpuSet)
	cmd := fmt.Sprintf(CpuSetWriteCmd, content)
	_, _, err := c.ExecCommand(ctx, containerId, cmd)
	if err != nil {
		c.logger.Error(err, "failed to exec cmd", "containerId", containerId, "cmd", cmd)
		return err
	}
	return nil
}

func (c *Client) getLabelAndAnnotation(ctx context.Context, containerId string) (map[string]string, error) {
	stdout, stderr, err := c.ExecCommand(ctx, containerId, ShowLabelAndAnnotationCmd)
	if err != nil {
		c.logger.Error(err, "failed to exec cmmdn get label and annotation", "containerId", containerId)
		return nil, err
	}
	c.logger.Info(fmt.Sprintf("get label and annotation, stdout = %s, stderr = %s", string(stdout), string(stderr)))
	kvConfig := convention.ParseKVConfig(stdout)
	return kvConfig, nil
}
