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
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/alibaba/polardbx-operator/pkg/util/gms"

	"github.com/alibaba/polardbx-operator/pkg/util/formula"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/common"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"

	"sigs.k8s.io/controller-runtime/pkg/client"

	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"
	"gopkg.in/ini.v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	_ "github.com/go-sql-driver/mysql"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func checkTopologySpec(xstore *polardbxv1.XStore) error {
	// Check the topology
	topology := &xstore.Spec.Topology

	// Empty means single node.
	if len(topology.NodeSets) == 0 {
		return nil
	}

	nodeSetNames := make(map[string]interface{})
	candidates := make([]*polardbxv1xstore.NodeSet, 0, 1)
	voter := make([]*polardbxv1xstore.NodeSet, 0, 1)
	learner := make([]*polardbxv1xstore.NodeSet, 0, 1)
	candidateCnt, voterCnt, learnerCnt := 0, 0, 0
	for _, ns := range topology.NodeSets {
		if _, ok := nodeSetNames[ns.Name]; ok {
			return errors.New("invalid topology: duplicate node set names: " + ns.Name)
		}
		nodeSetNames[ns.Name] = nil

		switch ns.Role {
		case polardbxv1xstore.RoleCandidate:
			candidates = append(candidates, &ns)
			candidateCnt += int(ns.Replicas)
		case polardbxv1xstore.RoleVoter:
			voter = append(voter, &ns)
			voterCnt += int(ns.Replicas)
		case polardbxv1xstore.RoleLearner:
			learner = append(learner, &ns)
			learnerCnt += int(ns.Replicas)
		default:
			return errors.New(string("invalid topology: unrecognized node set role: " + ns.Role))
		}
	}

	if !xstore.Spec.Readonly {
		// For primary cluster, it must meet the following requirements:
		//   1. The total number of candidates and voters must be odd.
		//   2. There must be at least one candidate.
		if candidateCnt == 0 {
			return errors.New("invalid topology: no candidates found")
		}
		if (candidateCnt+voterCnt)&1 == 0 {
			return errors.New("invalid topology: sum(candidate_size, voter_size) is even")
		}
	} else {
		// For readonly cluster, it has and only has learners
		if learnerCnt == 0 {
			return errors.New("invalid topology: no learners found")
		}
		if candidateCnt != 0 || voterCnt != 0 {
			return errors.New("invalid topology: containing candidates or voters")
		}
	}

	return nil
}

var CheckTopologySpec = xstorev1reconcile.NewStepBinder("CheckTopologySpec",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		err := checkTopologySpec(xstore)
		if err != nil {
			xstore.Status.Phase = polardbxv1xstore.PhaseFailed
			return flow.Error(err, "Check topology failed. Transfer phase into Failed.")
		}
		return flow.Pass()
	},
)

var UpdateObservedTopologyAndConfig = xstorev1reconcile.NewStepBinder("UpdateObservedTopologyAndConfig",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		xstore.Status.ObservedTopology = xstore.Spec.Topology.DeepCopy()
		xstore.Status.ObservedConfig = xstore.Spec.Config.DeepCopy()
		return flow.Continue("Update observed topology and config.", "current-generation", xstore.Generation)
	},
)

var UpdateObservedGeneration = xstorev1reconcile.NewStepBinder("UpdateObservedGeneration",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		prevGen := xstore.Status.ObservedGeneration
		xstore.Status.ObservedGeneration = xstore.Generation
		return flow.Continue("Update observed generation.", "previous-generation", prevGen,
			"current-generation", xstore.Generation)
	},
)

var QueryAndUpdateEngineVersion = xstorev1reconcile.NewStepBinder("QueryAndUpdateEngineVersion",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leader pod.")
		}
		if leaderPod == nil {
			return flow.Wait("Leader pod not found, wait.")
		}

		cmd := command.NewCanonicalCommandBuilder().Engine().Version().Build()
		buf := &bytes.Buffer{}
		err = rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdout:  buf,
			Timeout: 2 * time.Second,
		})
		if err != nil {
			return flow.Error(err, "Failed to query version on leader pod.", "pod", leaderPod.Name)
		}

		engineVersion := strings.TrimSpace(buf.String())
		if engineVersion == "" {
			return flow.Error(errors.New("empty engine version"), "Engine version is empty.")
		}

		// Update the engine version in status.
		xstore := rc.MustGetXStore()
		xstore.Status.EngineVersion = engineVersion

		return flow.Pass()
	},
)

var CheckConnectivityAndSetEngineVersion = xstorev1reconcile.NewStepBinder("CheckConnectivityFromController",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		passwd, err := rc.GetXStoreAccountPassword(convention.SuperAccount)
		if err != nil {
			return flow.Error(err, "Unable to get password for super account.")
		}

		readonly := rc.MustGetXStore().Spec.Readonly
		serviceType := convention.ServiceTypeReadWrite
		if readonly {
			serviceType = convention.ServiceTypeReadOnly
		}

		clusterAddr, err := rc.GetXStoreClusterAddr(serviceType, convention.PortAccess)
		if err != nil {
			return flow.Error(err, "Unable to get cluster address.")
		}

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/information_schema?timeout=1s",
			convention.SuperAccount, passwd, clusterAddr))
		if err != nil {
			return flow.Error(err, "Unable to open connection to cluster address.")
		}
		defer db.Close()

		if err := db.PingContext(rc.Context()); err != nil {
			// Wait 10 seconds for any error.
			flow.Logger().Error(err, "Ping failed.")
			return flow.RetryAfter(10*time.Second, "Failed to ping, wait for 10 seconds and retry...")
		}

		// Get version via SQL and update status.

		//goland:noinspection SqlNoDataSourceInspection,SqlDialectInspection
		row := db.QueryRowContext(rc.Context(), "SELECT VERSION()")
		var version string
		err = row.Scan(&version)
		if err != nil {
			return flow.Error(err, "Unable to read version")
		}
		xstore := rc.MustGetXStore()
		xstore.Status.EngineVersion = version

		return flow.Continue("Succeed.")
	},
)

var InjectFinalizerOnXStore = xstorev1reconcile.NewStepBinder("InjectFinalizerOnXStore",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if controllerutil.ContainsFinalizer(xstore, xstoremeta.Finalizer) {
			return flow.Pass()
		}

		controllerutil.AddFinalizer(xstore, xstoremeta.Finalizer)
		rc.MarkXStoreChanged()

		return flow.Continue("Inject finalizer.")
	},
)

var RemoveFinalizerFromXStore = xstorev1reconcile.NewStepBinder("RemoveFinalizerFromXStore",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if !controllerutil.ContainsFinalizer(xstore, xstoremeta.Finalizer) {
			return flow.Pass()
		}

		controllerutil.RemoveFinalizer(xstore, xstoremeta.Finalizer)
		rc.MarkXStoreChanged()

		return flow.Continue("Remove finalizer.")
	},
)

var GenerateRandInStatus = xstorev1reconcile.NewStepBinder("GenerateRandInStatus",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if len(xstore.Status.Rand) == 0 {
			if val, ok := xstore.Annotations[xstoremeta.AnnotationGuideRand]; ok {
				xstore.Status.Rand = val
			} else {
				xstore.Status.Rand = rand.String(4)
			}
		}

		return flow.Pass()
	},
)

var DeleteAllPods = xstorev1reconcile.NewStepBinder("DeleteAllPods",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		for _, pod := range pods {
			if pod.DeletionTimestamp.IsZero() {
				if err := rc.Client().Delete(rc.Context(), &pod); err != nil {
					if apierrors.IsNotFound(err) {
						continue
					}
					return flow.Error(err, "Unable to delete pod.", "pod", pod.Name)
				}
			}
		}

		return flow.Continue("All pods deleted.")
	},
)

var AbortReconcileIfHintFound = xstorev1reconcile.NewStepBinder("AbortReconcileIfHintFound",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.ContainsControllerHint(xstoremeta.HintForbidden) {
			return flow.Wait("Found hint, abort reconcile.")
		}
		return flow.Pass()
	},
)

var FillServiceNameIfNotProvided = xstorev1reconcile.NewStepBinder("FillServiceNameIfNotProvided",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if len(xstore.Spec.ServiceName) == 0 {
			xstore.Spec.ServiceName = xstore.Name
			rc.MarkXStoreChanged()
		}
		return flow.Pass()
	},
)

var BindPodPorts = xstorev1reconcile.NewStepBinder("BindPodPorts",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		podPorts := make(map[string]polardbxv1xstore.PodPorts)
		for _, pod := range pods {
			ports := polardbxv1xstore.PodPorts{}
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					ports[port.Name] = port.ContainerPort
				}
			}
			podPorts[pod.Name] = ports
		}

		xstore.Status.PodPorts = podPorts
		return flow.Continue("Pod ports updated!")
	},
)

var InitializeParameterTemplate = xstorev1reconcile.NewStepBinder("InitializeParameterTemplate",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		paramMap := rc.GetPolarDBXParams()
		parameterTemplateName := rc.GetPolarDBXTemplateName()
		templateParams := rc.GetPolarDBXTemplateParams()

		if parameterTemplateName == "" {
			return flow.Continue("No parameter template specified, use default.")
		}

		pt, err := rc.GetPolarDBXParameterTemplate(parameterTemplateName)
		if pt == nil {
			return flow.Error(err, "Unable to get parameter template.", "node", parameterTemplateName)
		}

		// DN
		for _, param := range pt.Spec.NodeType.DN.ParamList {
			// read only / read write
			mode := common.GetMode(polardbxmeta.RoleDN, param.Mode)
			paramMap[mode][param.Name] = polardbxv1.Params{
				Name:  param.Name,
				Value: param.DefaultValue,
			}
			// restart
			if param.Restart {
				paramMap[polardbxv1.DNRestart][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.DefaultValue,
				}
			}
			templateParams[polardbxmeta.RoleDN][param.Name] = param
		}

		// GMS
		if len(pt.Spec.NodeType.GMS.ParamList) != 0 {
			for _, param := range pt.Spec.NodeType.GMS.ParamList {
				// read only / read write
				mode := common.GetMode(polardbxmeta.RoleGMS, param.Mode)
				paramMap[mode][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.DefaultValue,
				}
				// restart
				if param.Restart {
					paramMap[polardbxv1.GMSRestart][param.Name] = polardbxv1.Params{
						Name:  param.Name,
						Value: param.DefaultValue,
					}
				}
				templateParams[polardbxmeta.RoleGMS][param.Name] = param
			}
		} else {
			paramMap[polardbxv1.GMSReadOnly] = paramMap[polardbxv1.DNReadOnly]
			paramMap[polardbxv1.GMSReadWrite] = paramMap[polardbxv1.DNReadWrite]
			paramMap[polardbxv1.GMSRestart] = paramMap[polardbxv1.DNRestart]
			templateParams[polardbxmeta.RoleGMS] = templateParams[polardbxmeta.RoleDN]
		}

		rc.SetPolarDBXParams(paramMap)

		return flow.Pass()
	},
)

var GetParametersRoleMap = xstorev1reconcile.NewStepBinder("GetParametersRoleMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		paramsRoleMap := rc.GetPolarDBXParams()
		templateParams := rc.GetPolarDBXTemplateParams()
		xstore := rc.MustGetXStore()

		pt, err := rc.GetPolarDBXParameterTemplate(xstore.Spec.ParameterTemplate.Name)
		if pt == nil {
			return flow.Error(err, "Unable to get parameter template.")
		}

		for _, param := range pt.Spec.NodeType.DN.ParamList {
			templateParams[polardbxmeta.RoleDN][param.Name] = param
		}

		if len(pt.Spec.NodeType.GMS.ParamList) != 0 {
			for _, param := range pt.Spec.NodeType.GMS.ParamList {
				templateParams[polardbxmeta.RoleGMS][param.Name] = param
			}
		} else {
			for _, param := range pt.Spec.NodeType.DN.ParamList {
				templateParams[polardbxmeta.RoleGMS][param.Name] = param
			}
		}

		polarDBXParameterList := polardbxv1.PolarDBXParameterList{}

		err = rc.Client().List(rc.Context(), &polarDBXParameterList,
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels(convention.GetParameterLabel()),
		)
		if err != nil {
			return flow.Error(err, "Unable to get parameter template.")
		}

		var parameter *polardbxv1.PolarDBXParameter
		for _, p := range polarDBXParameterList.Items {
			if xstore.Labels[polardbxmeta.LabelName] == p.Spec.ClusterName && xstore.Spec.ParameterTemplate.Name == p.Spec.TemplateName {
				parameter = &p
				break
			}
		}
		if parameter == nil {
			return flow.Error(err, "Unable to get parameter", "cluster", xstore.Labels[polardbxmeta.LabelName])
		}

		for _, param := range parameter.Spec.NodeType.DN.ParamList {
			// read only / read write
			mode := common.GetMode(polardbxmeta.RoleDN, templateParams[polardbxmeta.RoleDN][param.Name].Mode)
			paramsRoleMap[mode][param.Name] = polardbxv1.Params{
				Name:  param.Name,
				Value: param.Value,
			}
			// restart
			if templateParams[polardbxmeta.RoleDN][param.Name].Restart {
				paramsRoleMap[polardbxv1.DNRestart][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.Value,
				}
			}
		}

		gmsParamList := gms.GetGmsParamList(parameter)

		for _, param := range gmsParamList {
			// read only / read write
			mode := common.GetMode(polardbxmeta.RoleGMS, templateParams[polardbxmeta.RoleGMS][param.Name].Mode)
			paramsRoleMap[mode][param.Name] = polardbxv1.Params{
				Name:  param.Name,
				Value: param.Value,
			}
			// restart
			if templateParams[polardbxmeta.RoleGMS][param.Name].Restart {
				paramsRoleMap[polardbxv1.GMSRestart][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.Value,
				}
			}
		}

		rc.SetPolarDBXParams(paramsRoleMap)
		rc.SetPolarDBXTemplateParams(templateParams)

		return flow.Pass()
	},
)

func setMycnfOverrideParams(paramsRoleMap map[string]map[string]polardbxv1.Params, paramList []polardbxv1.Params, mycnfOverrride *ini.File, xstore *polardbxv1.XStore) {
	for _, param := range paramList {
		if _, ok := paramsRoleMap[polardbxv1.DNReadOnly][param.Name]; !ok {
			if len(param.Value) > 0 && param.Value[0] == '{' {
				calculateValue, _ := formula.FormulaComputingXStore(param.Value, xstore)
				mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(strconv.Itoa(calculateValue))
			} else {
				mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(param.Value)
			}
		}
	}
}

var UpdateXStoreConfigMap = xstorev1reconcile.NewStepBinder("UpdateXStoreConfigMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		polarDBXParameterList := polardbxv1.PolarDBXParameterList{}

		err := rc.Client().List(rc.Context(), &polarDBXParameterList,
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels(convention.GetParameterLabel()),
		)

		var polarDBXParameter *polardbxv1.PolarDBXParameter
		for _, p := range polarDBXParameterList.Items {
			if xstore.Labels[polardbxmeta.LabelName] == p.Spec.ClusterName && xstore.Spec.ParameterTemplate.Name == p.Spec.TemplateName {
				polarDBXParameter = &p
				break
			}
		}
		if polarDBXParameter == nil {
			return flow.Error(err, "Unable to get parameter", "cluster", xstore.Labels[polardbxmeta.LabelName])
		}

		templateCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeConfig)
		if err != nil {
			return flow.Error(err, "Unable to get config map for config.")
		}
		if templateCm.Data == nil {
			templateCm.Data = make(map[string]string)
		}

		cmRole := xstore.Labels[polardbxmeta.LabelRole]

		mycnfOverrride, err := ini.LoadSources(ini.LoadOptions{
			AllowBooleanKeys:           true,
			AllowPythonMultilineValues: true,
			SpaceBeforeInlineComment:   true,
			PreserveSurroundedQuote:    true,
			IgnoreInlineComment:        true,
		}, []byte(templateCm.Data[convention.ConfigMyCnfOverride]))
		if err != nil {
			return reconcile.Result{}, err
		}

		paramsRoleMap := rc.GetPolarDBXParams()

		gmsParamList := gms.GetGmsParamList(polarDBXParameter)

		if cmRole == polardbxmeta.RoleDN {
			setMycnfOverrideParams(paramsRoleMap, polarDBXParameter.Spec.NodeType.DN.ParamList, mycnfOverrride, xstore)
		} else {
			setMycnfOverrideParams(paramsRoleMap, gmsParamList, mycnfOverrride, xstore)
		}

		templateCm.Data[convention.ConfigMyCnfOverride] = iniutil.ToString(mycnfOverrride)

		// Update config map.
		err = rc.Client().Update(rc.Context(), templateCm)
		if err != nil {
			return flow.Error(err, "Unable to update task config map.")
		}

		return flow.Pass()
	},
)

func setValue(param polardbxv1.Params, setGlobalParams map[string]string, xstore *polardbxv1.XStore) {
	if len(param.Value) > 0 && param.Value[0] == '{' {
		calculateValue, _ := formula.FormulaComputingXStore(param.Value, xstore)
		setGlobalParams[param.Name] = strconv.Itoa(calculateValue)
	} else {
		setGlobalParams[param.Name] = param.Value
	}
}

var SetGlobalVariables = xstorev1reconcile.NewStepBinder("SetGlobalVariables",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// set global variables
		xstore := rc.MustGetXStore()
		params := rc.GetPolarDBXParams()
		paramsTemplate := rc.GetPolarDBXTemplateParams()
		setGlobalParams := make(map[string]string)

		role := xstore.Labels[polardbxmeta.LabelRole]
		if role == polardbxmeta.RoleDN {
			for _, param := range params[polardbxv1.DNReadWrite] {
				if !paramsTemplate[polardbxmeta.RoleDN][param.Name].Restart {
					setValue(param, setGlobalParams, xstore)
				}
			}
		} else {
			for _, param := range params[polardbxv1.GMSReadWrite] {
				if !paramsTemplate[polardbxmeta.RoleGMS][param.Name].Restart {
					setValue(param, setGlobalParams, xstore)
				}
			}
		}

		if len(setGlobalParams) == 0 {
			return flow.Continue("No global variables need to be set")
		}

		pods, err := rc.GetXStorePods()
		for _, pod := range pods {
			err = rc.ExecuteCommandOn(&pod, convention.ContainerEngine,
				command.NewCanonicalCommandBuilder().Engine().SetGlobal(setGlobalParams).Build(),
				control.ExecOptions{
					Logger:  flow.Logger(),
					Timeout: 10 * time.Second,
				},
			)
			if err != nil {
				return flow.Error(err, "error executing set global command")
			}
		}
		if err != nil {
			return flow.Error(err, "failed to get xstore pods", "xstore", rc.MustGetXStore().Name)
		}

		return flow.Pass()
	},
)

var CloseXStoreUpdatePhase = xstorev1reconcile.NewStepBinder("CloseXStoreUpdatePhase",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// change dn stage
		xstore := rc.MustGetXStore()
		xstore.Status.UpdateConfigMap = false
		err := rc.Client().Status().Update(rc.Context(), xstore)
		if err != nil {
			if apierrors.IsConflict(err) {
				return flow.Retry("Update conflict, Retry")
			} else {
				return flow.Error(err, "Unable to Update dn status")
			}
		}
		return flow.Pass()
	},
)

var GetRestartingPods = xstorev1reconcile.NewStepBinder("GetRestartingPods",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if len(xstore.Status.RestartingPods.ToDeletePod) == 0 && xstore.Status.RestartingPods.LastDelectedPod == "" {
			pods, err := rc.GetXStorePods()
			if err != nil {
				return flow.Error(err, "Unable to get pods fo XStore.", "XStore", xstore.Name)
			}

			// Restart follower pods first
			sortPods := make([]corev1.Pod, 0)
			leaderPods := make([]corev1.Pod, 0)
			currentLeader, _ := TryDetectLeaderAndTryReconcileLabels(rc, pods, flow.Logger())
			for _, pod := range pods {
				if pod.Name != currentLeader {
					sortPods = append(sortPods, pod)
				} else {
					leaderPods = append(leaderPods, pod)
				}
			}
			sortPods = append(sortPods, leaderPods...)

			for _, pod := range sortPods {
				xstore.Status.RestartingPods.ToDeletePod = append(xstore.Status.RestartingPods.ToDeletePod, pod.Name)
			}
		}

		return flow.Pass()
	},
)

var RollingRestartPods = plugin.NewStepBinder(galaxy.Engine, "RollingRestartPods",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		lastDeletedPod := xstore.Status.RestartingPods.LastDelectedPod
		if lastDeletedPod != "" {
			podDel := corev1.Pod{}
			var podList corev1.PodList
			podLabels := convention.ConstLabels(xstore)
			err := rc.Client().List(
				rc.Context(),
				&podList,
				client.InNamespace(rc.Namespace()),
				client.MatchingLabels(podLabels))
			if err != nil {
				return flow.Error(err, "Error getting pods")
			}
			for _, podTemp := range podList.Items {
				if podTemp.Name == lastDeletedPod {
					podDel = podTemp
					break
				}
			}
			if !helper.IsPodReady(&podDel) || podDel.Name == "" || !podDel.DeletionTimestamp.IsZero() {
				return flow.Retry("Pod hasn't been deleted", "pod", podDel.Name)
			}
		}

		xstore.Status.RestartingPods.LastDelectedPod = ""

		if len(xstore.Status.RestartingPods.ToDeletePod) == 0 {
			return flow.Pass()
		}

		var deletePodName string
		deletePodName, xstore.Status.RestartingPods.ToDeletePod =
			xstore.Status.RestartingPods.ToDeletePod[0], xstore.Status.RestartingPods.ToDeletePod[1:]

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: rc.Namespace(),
				Name:      deletePodName,
			},
		}

		err := rc.Client().Delete(rc.Context(), pod)
		if err != nil {
			return flow.Error(err, "Unable to delete pod", "pod name", pod.Name)
		}

		xstore.Status.RestartingPods.LastDelectedPod = deletePodName

		return flow.Retry("Rolling Restart...")
	},
)

var RestartingPods = xstorev1reconcile.NewStepBinder("RestartingPods",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods fo DN.")
		}

		// Restart follower pods first
		sortPods := make([]corev1.Pod, 0)
		leaderPods := make([]corev1.Pod, 0)
		currentLeader, _ := TryDetectLeaderAndTryReconcileLabels(rc, pods, flow.Logger())
		for _, pod := range pods {
			if pod.Name != currentLeader {
				sortPods = append(sortPods, pod)
			} else {
				leaderPods = append(leaderPods, pod)
			}
		}
		sortPods = append(sortPods, leaderPods...)

		for _, pod := range sortPods {
			err := rc.Client().Delete(rc.Context(), &pod)
			if err != nil {
				return flow.Error(err, "Unable to delete pod", "pod name", pod.Name)
			}
		}

		return flow.Pass()
	},
)

func IsRollingRestart(xcluster *polardbxv1.XStore) bool {
	return xcluster.Status.RestartingType == polardbxv1polardbx.RolingRestart
}

var CloseXStoreRestartPhase = plugin.NewStepBinder(galaxy.Engine, "CloseXStoreRestartPhase",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// change dn stage
		xstore := rc.MustGetXStore()
		xstore.Status.Restarting = false
		err := rc.Client().Status().Update(rc.Context(), xstore)
		if err != nil {
			if apierrors.IsConflict(err) {
				return flow.Retry("Update conflict, Retry")
			} else {
				return flow.Error(err, "Unable to Update dn status")
			}
		}
		return flow.Pass()
	},
)
