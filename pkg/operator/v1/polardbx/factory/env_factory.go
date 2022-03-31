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

package factory

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	dictutil "github.com/alibaba/polardbx-operator/pkg/util/dict"
)

type StorageConnection struct {
	Host     string
	Port     int
	User     string
	Passwd   string
	Database string
}

type EnvFactory interface {
	NewSystemEnvVars() []corev1.EnvVar
	NewEnvVarsForCNEngine(gmsConn StorageConnection, ports CNPorts) []corev1.EnvVar
	NewEnvVarsForCDCEngine(gmsConn StorageConnection) []corev1.EnvVar
}

type envFactory struct {
	rc       *polardbxv1reconcile.Context
	polardbx *polardbxv1.PolarDBXCluster
	cipher   security.PasswordCipher
}

func (e *envFactory) newValueFromObjectFiled(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{
			FieldPath: fieldPath,
		},
	}
}

func (e *envFactory) newValueFromSecretKey(secret, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: secret,
			},
			Key: key,
		},
	}
}

func (e *envFactory) NewSystemEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "POD_ID", ValueFrom: e.newValueFromObjectFiled("metadata.name")},
		{Name: "POD_IP", ValueFrom: e.newValueFromObjectFiled("status.podIP")},
		{Name: "HOST_IP", ValueFrom: e.newValueFromObjectFiled("status.hostIP")},
		{Name: "NODE_NAME", ValueFrom: e.newValueFromObjectFiled("spec.nodeName")},
	}
}

type cnJvmMemory struct {
	total int64
	jvm   int64
}

var (
	cnJvmMemorySteps = []cnJvmMemory{
		{total: 2 << 30, jvm: 1 << 30},
		{total: 4 << 30, jvm: 2 << 30},
		{total: 8 << 30, jvm: 4 << 30},
		{total: 16 << 30, jvm: 10 << 30},
		{total: 32 << 30, jvm: 24 << 30},
		{total: 64 << 30, jvm: 50 << 30},
		{total: 128 << 30, jvm: 120 << 30},
	}
)

func (e *envFactory) roundDownMemory(memory int64, min int64, left int64) int64 {
	if memory < min {
		memory = min
	}

	// Available giga bytes
	availBytes := memory - left

	// Round down by steps
	matchStep := cnJvmMemory{}
	for _, step := range cnJvmMemorySteps {
		if availBytes < step.jvm {
			break
		}
		matchStep = step
	}

	if matchStep.total >= memory {
		return memory
	} else {
		return matchStep.total
	}
}

func (e *envFactory) newBasicEnvVarsForCNEngine(gmsConn *StorageConnection, ports *CNPorts, cpuLimit, memoryLimit int64, storageEngine string) []corev1.EnvVar {

	// At least 2Gi and left 1Gi for non JVM usage.
	const minMemoryBytes, leftMemoryBytes = int64(2) << 30, int64(1) << 30
	containerAwareMemoryLimit := e.roundDownMemory(memoryLimit, minMemoryBytes, leftMemoryBytes)

	envs := []corev1.EnvVar{
		{Name: "switchCloud", Value: "aliyun"},
		{Name: "metaDbAddr", Value: fmt.Sprintf("%s:%d", gmsConn.Host, gmsConn.Port)},
		{Name: "metaDbName", Value: fmt.Sprintf(gms.MetaDBName)},
		{Name: "metaDbUser", Value: gmsConn.User},
		{Name: "metaDbPasswd", Value: e.cipher.Encrypt(gmsConn.Passwd)},
		{Name: "dnPasswordKey", Value: e.cipher.Key()},
		{Name: "metaDbXprotoPort", Value: strconv.Itoa(0)},
		{Name: "storageDbXprotoPort", Value: strconv.Itoa(0)},
		{Name: "metaDbConn", Value: fmt.Sprintf("mysql -h%s -P%d -u%s -p%s -D%s", gmsConn.Host, gmsConn.Port, gmsConn.User, gmsConn.Passwd, gms.MetaDBName)},
		{Name: "instanceId", Value: e.polardbx.Name},
		{Name: "instanceType", Value: gms.MasterCluster.String()},
		{Name: "serverPort", Value: strconv.Itoa(ports.AccessPort)},
		{Name: "mgrPort", Value: strconv.Itoa(ports.MgrPort)},
		{Name: "mppPort", Value: strconv.Itoa(ports.MppPort)},
		{Name: "htapPort", Value: strconv.Itoa(ports.HtapPort)},
		{Name: "logPort", Value: strconv.Itoa(ports.LogPort)},
		// -- begin log port: force the worker reading the log port value by setting the `polarx_${ins_id}_log_port`
		{Name: "ins_id", Value: "dummy"},
		{Name: "polarx_dummy_log_port", Value: "$(logPort)"},
		// -- end log port
		// -- begin ssh port
		{Name: "polarx_dummy_ssh_port", Value: "-1"},
		// -- end ssh port
		{Name: "cpuCore", Value: strconv.FormatInt(cpuLimit, 10)},
		{Name: "memSize", Value: strconv.FormatInt(containerAwareMemoryLimit, 10)},
		// Env $memory for start up script
		{Name: "cpu_cores", Value: strconv.FormatInt(cpuLimit, 10)},
		// {Name: "cpu_cores", Value: strconv.FormatInt(16, 10)},
		{Name: "memory", Value: strconv.FormatInt(containerAwareMemoryLimit, 10)},
	}

	// Switch on the protocol for galaxy engine.
	if storageEngine == "galaxy" {
		envs = append(envs, corev1.EnvVar{
			Name:  "galaxyXProtocol",
			Value: "1",
		})
	}

	return envs
}

func (e *envFactory) newServerPropertyEnvVarsForCNEngine(cpuLimit int64, ignore map[string]struct{}) []corev1.EnvVar {
	serverExecutorLimit := 1024
	serverProps := make(map[string]string)
	serverProps["processors"] = strconv.FormatInt(cpuLimit, 10)
	serverProps["processorHandler"] = strconv.FormatInt(cpuLimit, 10)
	serverProps["serverExecutor"] = strconv.Itoa(serverExecutorLimit)

	// If reset trust ips enabled, set it to empty
	if featuregate.ResetTrustIpsBeforeStart.Enabled() {
		serverProps["trustedIps"] = ""
	}

	// Build from config
	staticConfig := e.polardbx.Spec.Config.CN.Static
	if staticConfig != nil {
		if staticConfig.EnableReplicaRead {
			serverProps["supportSlaveRead"] = strconv.FormatBool(true)
		}

		if staticConfig.ServerProperties != nil {
			for k, v := range staticConfig.ServerProperties {
				if _, ok := ignore[k]; !ok {
					serverProps[k] = v.String()
				}
			}
		}
	}

	// Ignore TDDL_OPTS
	delete(serverProps, "TDDL_OPTS")

	// Build the env vars. Sort the keys to make sure it's stable.
	envVars := make([]corev1.EnvVar, 0, len(serverProps))
	for _, k := range dictutil.SortedStringKeys(serverProps) {
		v := serverProps[k]
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	return envVars
}

func (e *envFactory) newJvmInjectionEnvVarForCNEngine(debugPort int) corev1.EnvVar {
	// Injecting JVM startup parameters by setting the TDDL_OPTS env
	tddlOpts := []string{"-Dpod.id=$(POD_ID)"}

	// Deprecated.
	// tddlOpts = append(tddlOpts, "-XX:+ParallelRefProcEnabled")

	// Build from configs.
	staticConfig := e.polardbx.Spec.Config.CN.Static
	if staticConfig != nil {
		if staticConfig.EnableCoroutine {
			tddlOpts = append(tddlOpts, "-XX:+UseWisp2")
		}
		if staticConfig.EnableJvmRemoteDebug {
			tddlOpts = append(tddlOpts, fmt.Sprintf("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%d", debugPort))
		}
	}

	// Property for 8.0
	if strings.HasPrefix(e.polardbx.Spec.ProtocolVersion.String(), "8") {
		tddlOpts = append(tddlOpts, "-DinstanceVersion=8.0.3")
	}

	return corev1.EnvVar{Name: "TDDL_OPTS", Value: strings.Join(tddlOpts, " ")}
}

func (e *envFactory) newSslEnvVarForCNEngine() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "sslEnable",
			Value: "true",
		},
		{
			Name:  "sslRootCertPath",
			Value: "/home/admin/drds-server/security/root.crt",
		},
		{
			Name:  "sslKeyPath",
			Value: "/home/admin/drds-server/security/server.key",
		},
		{
			Name:  "sslCertPath",
			Value: "/home/admin/drds-server/security/server.crt",
		},
	}
}

func (e *envFactory) NewEnvVarsForCNEngine(gmsConn StorageConnection, ports CNPorts) []corev1.EnvVar {
	topology := e.polardbx.Status.SpecSnapshot.Topology
	template := topology.Nodes.CN.Template

	// Resource limits.
	cpuLimit := template.Resources.Limits.Cpu().Value()
	memoryLimit := template.Resources.Limits.Memory().Value()

	// Basic environment variables.
	systemEnvs := e.NewSystemEnvVars()
	basicEnvs := e.newBasicEnvVarsForCNEngine(&gmsConn, &ports,
		cpuLimit, memoryLimit, topology.Nodes.DN.Template.Engine)

	systemAndBasicEnvKeys := make(map[string]struct{})
	for _, e := range systemEnvs {
		systemAndBasicEnvKeys[e.Name] = struct{}{}
	}
	for _, e := range basicEnvs {
		systemAndBasicEnvKeys[e.Name] = struct{}{}
	}

	sslEnvs := make([]corev1.EnvVar, 0)
	if helper.IsTLSEnabled(e.polardbx) {
		sslEnvs = e.newSslEnvVarForCNEngine()
		for _, e := range sslEnvs {
			systemAndBasicEnvKeys[e.Name] = struct{}{}
		}
	}

	serverPropertyEnvs := e.newServerPropertyEnvVarsForCNEngine(cpuLimit, systemAndBasicEnvKeys)
	tddlOptsEnv := e.newJvmInjectionEnvVarForCNEngine(ports.DebugPort)

	envVars := systemEnvs
	envVars = append(envVars, basicEnvs...)
	envVars = append(envVars, sslEnvs...)
	envVars = append(envVars, serverPropertyEnvs...)
	envVars = append(envVars, tddlOptsEnv)
	return envVars
}

func (e *envFactory) newBasicEnvVarsForCDCEngine(gmsConn *StorageConnection) []corev1.EnvVar {
	topology := e.polardbx.Status.SpecSnapshot.Topology
	if topology.Nodes.CDC == nil {
		return nil
	}

	template := topology.Nodes.CDC.Template
	pxcServiceName := e.polardbx.Spec.ServiceName
	if len(pxcServiceName) == 0 {
		pxcServiceName = e.polardbx.Name
	}

	toServiceEnvName := func(serviceName string) string {
		return strings.ToUpper(strings.ReplaceAll(serviceName, "-", "_"))
	}

	// FIXME CDC currently doesn't support host network, so ports are hard coded.
	return []corev1.EnvVar{
		{Name: "switchCloud", Value: "aliyun"},
		{Name: "cluster_id", Value: e.polardbx.Name},
		{Name: "ins_id", ValueFrom: e.newValueFromObjectFiled("metadata.uid")},
		{Name: "ins_ip", ValueFrom: e.newValueFromObjectFiled("status.podIP")},
		{Name: "daemon_port", Value: "3007"},
		{Name: "common_ports", Value: `{"cdc1_port":"3009","cdc3_port":"3011","cdc2_port":"3010","cdc6_port":"3014","cdc5_port":"3013","cdc4_port":"3012"}`},
		{Name: "cpu_cores", Value: strconv.FormatInt(template.Resources.Limits.Cpu().Value(), 10)},
		{Name: "mem_size", Value: strconv.FormatInt(template.Resources.Limits.Memory().Value()>>20, 10)},
		{Name: "disk_size", Value: "10240"},
		{Name: "disk_quota", Value: "10240"},
		{Name: "metaDb_url", Value: fmt.Sprintf("jdbc:mysql://%s:%d/polardbx_meta_db?useSSL=false", gmsConn.Host, gmsConn.Port)},
		{Name: "metaDb_username", Value: gmsConn.User},
		{Name: "metaDb_password", Value: gmsConn.Passwd},
		{Name: "polarx_url", Value: fmt.Sprintf("jdbc:mysql://$(%s_SERVICE_HOST):$(%s_SERVICE_PORT)/__cdc__?useSSL=false", toServiceEnvName(pxcServiceName), toServiceEnvName(pxcServiceName))},
		{Name: "polarx_username", Value: "polardbx_root"},
		{Name: "polarx_password", ValueFrom: e.newValueFromSecretKey(e.polardbx.Name, "polardbx_root")},
		{Name: "dnPasswordKey", Value: e.cipher.Key()},
	}
}

func (e *envFactory) NewEnvVarsForCDCEngine(gmsConn StorageConnection) []corev1.EnvVar {
	systemEnvs := e.NewSystemEnvVars()
	basicEnvs := e.newBasicEnvVarsForCDCEngine(&gmsConn)
	return append(systemEnvs, basicEnvs...)
}

func NewEnvFactory(rc *polardbxv1reconcile.Context, polardbx *polardbxv1.PolarDBXCluster) (EnvFactory, error) {
	cipher, err := rc.GetPolarDBXPasswordCipher()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx password cipher: %w", err)
	}
	return &envFactory{rc: rc, polardbx: polardbx, cipher: cipher}, nil
}
