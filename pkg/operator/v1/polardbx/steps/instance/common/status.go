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
	"time"

	dictutil "github.com/alibaba/polardbx-operator/pkg/util/dict"

	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"
	"gopkg.in/ini.v1"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/util/unit"
)

func TransferPhaseTo(phase polardbxv1polardbx.Phase, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			polardbx := rc.MustGetPolarDBX()
			polardbx.Status.Stage = polardbxv1polardbx.StageEmpty
			polardbx.Status.Phase = phase
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

func TransferStageTo(stage polardbxv1polardbx.Stage, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferStageTo"+string(stage),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			polardbx := rc.MustGetPolarDBX()
			polardbx.Status.Stage = stage
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

func WhenDeletedAndNotDeleting(binders ...control.BindFunc) control.BindFunc {
	return polardbxv1reconcile.NewStepIfBinder("Deleted",
		func(rc *polardbxv1reconcile.Context, log logr.Logger) (bool, error) {
			polardbx := rc.MustGetPolarDBX()
			if helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseDeleting) {
				return false, nil
			}
			return !polardbx.DeletionTimestamp.IsZero(), nil
		},
		binders...,
	)
}

var UpdateSnapshotAndObservedGeneration = polardbxv1reconcile.NewStepBinder("UpdateSnapshotAndObservedGeneration",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		polardbx.Status.SpecSnapshot = &polardbxv1polardbx.SpecSnapshot{
			Topology: *polardbx.Spec.Topology.DeepCopy(),
			Config:   *polardbx.Spec.Config.DeepCopy(),
		}
		polardbx.Status.ObservedGeneration = polardbx.Generation
		return flow.Pass()
	},
)

func countAvailableXStores(xstores ...*polardbxv1.XStore) int {
	cnt := 0
	for _, xs := range xstores {
		if xs != nil && xs.Status.Phase == polardbxv1xstore.PhaseRunning {
			cnt += 1
		}
	}
	return cnt
}

func countAvailableReplicasFromDeployments(deployments map[string]*appsv1.Deployment) int {
	cnt := 0
	for _, d := range deployments {
		cnt += int(d.Status.AvailableReplicas)
	}
	return cnt
}

func getTotalDataSizeOfXStore(xstore *polardbxv1.XStore) int64 {
	totalSize := int64(0)
	for _, v := range xstore.Status.BoundVolumes {
		totalSize += v.Size
	}
	return totalSize
}

var UpdateDisplayStorageSize = polardbxv1reconcile.NewStepBinder("UpdateDisplayStorageSize",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get xstore of GMS.")
		}
		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		now := time.Now()
		statusForPrintRef := &polardbx.Status.StatusForPrint

		// update storage size per minute.
		if statusForPrintRef.StorageSizeUpdateTime == nil ||
			statusForPrintRef.StorageSizeUpdateTime.Time.Add(1*time.Minute).Before(now) {
			totalSize := int64(0)
			if !polardbx.Spec.ShareGMS && gmsStore != nil && !polardbx.Spec.Readonly {
				totalSize += getTotalDataSizeOfXStore(gmsStore)
			}
			for _, dnStore := range dnStores {
				totalSize += getTotalDataSizeOfXStore(dnStore)
			}
			statusForPrintRef.StorageSizeUpdateTime = &metav1.Time{
				Time: now,
			}
			statusForPrintRef.StorageSize = unit.ByteCountIEC(totalSize)

			return flow.Continue("Storage size updated!")
		}

		return flow.Pass()
	})

var UpdateDisplayReplicas = polardbxv1reconcile.NewStepBinder("UpdateDisplayReplicas",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		statusRef := &polardbx.Status

		// Ignore display update on initializing.
		if statusRef.ObservedGeneration <= 0 {
			return flow.Pass()
		}

		statusForPrintRef, snapshot := &polardbx.Status.StatusForPrint, polardbx.Status.SpecSnapshot

		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get xstore of GMS.")
		}
		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		// update replicas status.
		statusRef.ReplicaStatus = polardbxv1polardbx.ClusterReplicasStatus{
			GMS:      polardbxv1polardbx.ReplicasStatus{Total: 1, Available: int32(countAvailableXStores(gmsStore))},
			DN:       polardbxv1polardbx.ReplicasStatus{Total: snapshot.Topology.Nodes.DN.Replicas, Available: int32(countAvailableXStores(dnStores...))},
			CN:       nil,
			CDC:      nil,
			Columnar: nil,
		}

		cnDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of CN.")
		}
		statusRef.ReplicaStatus.CN = &polardbxv1polardbx.ReplicasStatus{
			Total:     *snapshot.Topology.Nodes.CN.Replicas,
			Available: int32(countAvailableReplicasFromDeployments(cnDeployments)),
		}

		if snapshot.Topology.Nodes.CDC != nil {
			cdcDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCDC)
			if err != nil {
				return flow.Error(err, "Unable to get deployments of CDC.")
			}
			totalCdcReplicas := snapshot.Topology.Nodes.CDC.Replicas.IntValue() + snapshot.Topology.Nodes.CDC.XReplicas
			if snapshot.Topology.Nodes.CDC.Groups != nil {
				for _, group := range snapshot.Topology.Nodes.CDC.Groups {
					totalCdcReplicas += int(group.Replicas)
				}
			}
			statusRef.ReplicaStatus.CDC = &polardbxv1polardbx.ReplicasStatus{
				Available: int32(countAvailableReplicasFromDeployments(cdcDeployments)),
				Total:     int32(totalCdcReplicas),
			}
		}

		if snapshot.Topology.Nodes.Columnar != nil {
			columnarDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleColumnar)
			if err != nil {
				return flow.Error(err, "Unable to get deployments of Columnar.")
			}
			statusRef.ReplicaStatus.Columnar = &polardbxv1polardbx.ReplicasStatus{
				Available: int32(countAvailableReplicasFromDeployments(columnarDeployments)),
				Total:     snapshot.Topology.Nodes.Columnar.Replicas,
			}
		}

		gmsDisplay := statusRef.ReplicaStatus.GMS.Display()

		if polardbx.Spec.Readonly {
			gmsDisplay = " - "
		}

		statusForPrintRef.ReplicaStatus = polardbxv1polardbx.ReplicaStatusForPrint{
			GMS:      gmsDisplay,
			CN:       statusRef.ReplicaStatus.CN.Display(),
			DN:       statusRef.ReplicaStatus.DN.Display(),
			CDC:      statusRef.ReplicaStatus.CDC.Display(),
			Columnar: statusRef.ReplicaStatus.Columnar.Display(),
		}

		return flow.Pass()
	},
)

var UpdateDisplayDetailedVersion = polardbxv1reconcile.NewStepBinder("UpdateDisplayDetailedVersion",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		mgr, err := rc.GetPolarDBXGroupManager()
		if err != nil {
			return flow.Error(err, "Unable to get group manager.")
		}
		clusterVersion := ""
		if polardbx.Spec.Readonly && !rc.HasCNs() {
			clusterVersion, err = rc.GetDnVersion()
			clusterVersion = "0.0.0-PXC-0.0.0-00000000/" + clusterVersion
		} else {
			clusterVersion, err = mgr.GetClusterVersion()
		}
		if err != nil {
			return flow.Error(err, "Unable to get cluster version.")
		}
		polardbx.Status.StatusForPrint.DetailedVersion = clusterVersion
		return flow.Pass()
	},
)

var GenerateRandInStatus = polardbxv1reconcile.NewStepBinder("GenerateRandInStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if len(polardbx.Status.Rand) == 0 {
			polardbx.Status.Rand = rand.String(4)
		}
		return flow.Pass()
	},
)

func GetMode(node, mode string) string {
	if node == polardbxmeta.RoleCN {
		if mode == polardbxv1.ReadOnly {
			return polardbxv1.CNReadOnly
		} else {
			return polardbxv1.CNReadWrite
		}
	} else if node == polardbxmeta.RoleDN {
		if mode == polardbxv1.ReadOnly {
			return polardbxv1.DNReadOnly
		} else {
			return polardbxv1.DNReadWrite
		}
	} else {
		if mode == polardbxv1.ReadOnly {
			return polardbxv1.GMSReadOnly
		} else {
			return polardbxv1.GMSReadWrite
		}
	}
}

var InitializeParameterTemplate = polardbxv1reconcile.NewStepBinder("InitializeParameterTemplate",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {

		templateCm, err := rc.GetPolarDBXConfigMap(convention.ConfigMapTypeConfig)
		if err != nil {
			return flow.Error(err, "Unable to get config map for task.")
		}

		if templateCm.Data == nil {
			templateCm.Data = make(map[string]string)
		}

		cnParams, err := ini.LoadSources(ini.LoadOptions{
			AllowBooleanKeys:           true,
			AllowPythonMultilineValues: true,
			SpaceBeforeInlineComment:   true,
			PreserveSurroundedQuote:    true,
			IgnoreInlineComment:        true,
		}, []byte{})
		if err != nil {
			return flow.Error(err, "Unable to load config map to ini")
		}

		paramMap := rc.GetPolarDBXParams()
		parameterTemplateName := rc.GetPolarDBXTemplateName()
		parameterTemplateNameSpace := rc.GetPolarDBXTemplateNameSpace()
		templateParams := rc.GetPolarDBXTemplateParams()

		if parameterTemplateName == "" {
			return flow.Continue("No parameter template specified, use default.")
		}

		pt, err := rc.GetPolarDBXParameterTemplate(parameterTemplateNameSpace, parameterTemplateName)
		if pt == nil {
			return flow.Error(err, "Unable to get parameter template.", "node", parameterTemplateName)
		}

		// CN
		for _, param := range pt.Spec.NodeType.CN.ParamList {
			cnParams.Section("").Key(param.Name).SetValue(param.DefaultValue)
			// read only / read write
			mode := GetMode(polardbxmeta.RoleCN, param.Mode)
			paramMap[mode][param.Name] = polardbxv1.Params{
				Name:  param.Name,
				Value: param.DefaultValue,
			}
			// restart
			if param.Restart {
				paramMap[polardbxv1.CNRestart][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.DefaultValue,
				}
			}
			templateParams[polardbxmeta.RoleCN][param.Name] = param
		}
		templateCm.Data[polardbxmeta.RoleCN] = iniutil.ToString(cnParams)

		//DN
		for _, param := range pt.Spec.NodeType.DN.ParamList {
			// read only / read write
			mode := GetMode(polardbxmeta.RoleDN, param.Mode)
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
		if pt.Spec.NodeType.GMS != nil && len(pt.Spec.NodeType.GMS.ParamList) != 0 {
			for _, param := range pt.Spec.NodeType.GMS.ParamList {
				// read only / read write
				mode := GetMode(polardbxmeta.RoleGMS, param.Mode)
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
		rc.SetPolarDBXTemplateParams(templateParams)

		// Update config map.
		err = rc.Client().Update(rc.Context(), templateCm)
		if err != nil {
			return flow.Error(err, "Unable to update task config map.")
		}

		return flow.Pass()
	},
)

var SyncCnParameters = polardbxv1reconcile.NewStepBinder("SyncCnParameters",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		parameter := rc.MustGetPolarDBXParameter()

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		observedParams, err := mgr.ListDynamicParams()
		if err != nil {
			return flow.Error(err, "Unable to list current configs.")
		}

		// Dynamic part always uses the spec.
		params := parameter.Spec.NodeType.CN.ParamList
		targetParams := make(map[string]string)
		for _, param := range params {
			targetParams[param.Name] = param.Value
		}

		toUpdateDynamicConfigs := dictutil.DiffStringMap(targetParams, observedParams)
		if len(toUpdateDynamicConfigs) > 0 {
			flow.Logger().Info("Syncing dynamic configs...")
			err := mgr.SyncDynamicParams(toUpdateDynamicConfigs)
			if err != nil {
				return flow.Error(err, "Unable to sync dynamic configs.")
			}
			return flow.Continue("Dynamic configs synced.")
		}
		return flow.Pass()
	},
)

var UpdateTdeStatus = polardbxv1reconcile.NewStepBinder("UpdateTdeStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		polardbx.Status.TdeStatus = polardbx.Spec.TDE.Enable
		err := rc.UpdatePolarDBXStatus()
		if err != nil {
			return flow.Error(err, "Unable to Update PolarDBX Status")
		}
		return flow.Pass()
	},
)
