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

package parameter

import (
	"time"

	"github.com/alibaba/polardbx-operator/pkg/util/gms"

	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"gopkg.in/ini.v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/common"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"

	dictutil "github.com/alibaba/polardbx-operator/pkg/util/dict"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var SyncPolarDBXParameterStatus = polardbxv1reconcile.NewStepBinder("SyncPolarDBXParameterStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxParameter := rc.MustGetPolarDBXParameter()

		// sync previous snapshot
		polardbxParameter.Status.PrevParameterSpecSnapshot = polardbxParameter.Status.ParameterSpecSnapshot.DeepCopy()

		// sync spec snapshot
		polardbxParameter.Status.ParameterSpecSnapshot = polardbxParameter.Spec.DeepCopy()

		return flow.Pass()
	},
)

var PersistPolarDBXParameter = polardbxv1reconcile.NewStepBinder("PersistPolarDBXParameter",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXParameterChanged() {
			err := rc.UpdatePolarDBXParameter()
			if err != nil {
				return flow.Error(err, "Unable to persistent parameter.")
			}
		}

		return flow.Pass()
	},
)

var PersistPolarDBXParameterStatus = polardbxv1reconcile.NewStepBinder("PersistPolarDBXParameterStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXParameterStatusChanged() {
			err := rc.UpdatePolarDBXParameterStatus()
			if err != nil {
				return flow.Error(err, "Unable to persistent parameter status.")
			}
		}
		return flow.Pass()
	},
)

func TransferParameterPhaseTo(phase polardbxv1polardbx.ParameterPhase, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferParameterPhaseTo"+string(phase),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			parameter := rc.MustGetPolarDBXParameter()
			parameter.Status.Phase = phase
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

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

var UpdateCnConfigMap = polardbxv1reconcile.NewStepBinder("UpdateCnConfigMap",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		parameter := rc.MustGetPolarDBXParameter()
		paramsRoleMap := rc.GetPolarDBXParams()

		cm, err := rc.GetPolarDBXConfigMap(convention.ConfigMapTypeConfig)
		if err != nil {
			return flow.Error(err, "Unable to get config map for task.")
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}

		cnParams, err := ini.LoadSources(ini.LoadOptions{
			AllowBooleanKeys:           true,
			AllowPythonMultilineValues: true,
			SpaceBeforeInlineComment:   true,
			PreserveSurroundedQuote:    true,
			IgnoreInlineComment:        true,
		}, []byte(cm.Data[polardbxmeta.RoleCN]))
		if err != nil {
			return flow.Error(err, "Unable to load config map to ini")
		}

		for _, param := range parameter.Spec.NodeType.CN.ParamList {
			if _, ok := paramsRoleMap[polardbxv1.CNReadOnly][param.Name]; !ok {
				cnParams.Section("").Key(param.Name).SetValue(param.Value)
			}
		}

		cm.Data[polardbxmeta.RoleCN] = iniutil.ToString(cnParams)

		// Update config map.
		err = rc.Client().Update(rc.Context(), cm)
		if err != nil {
			return flow.Error(err, "Unable to update task config map.")
		}

		return flow.Pass()
	},
)

var SyncDnParameters = polardbxv1reconcile.NewStepBinder("SyncDnParameters",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// change dn stage
		dns, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Can not get DN map")
		}
		for _, dn := range dns {
			dn.Status.UpdateConfigMap = true
			err := rc.Client().Status().Update(rc.Context(), dn)
			if err != nil {
				if apierrors.IsConflict(err) {
					return flow.Retry("Update conflict, Retry")
				} else {
					return flow.Error(err, "Unable to Update dn status")
				}
			}
		}

		return flow.Pass()
	},
)

var SyncGMSParameters = polardbxv1reconcile.NewStepBinder("SyncGMSParameters",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// change gms stage
		gms, err := rc.GetGMS()
		if err != nil {
			return flow.Error(err, "Unable to get DN map")
		}

		gms.Status.UpdateConfigMap = true
		err = rc.Client().Status().Update(rc.Context(), gms)
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

func TransferCNRestartPhase(rc *polardbxv1reconcile.Context) error {
	polardbx := rc.MustGetPolarDBX()
	polardbx.Status.Restarting = true

	err := rc.UpdatePolarDBXStatus()
	if err != nil {
		return err
	}

	return nil
}

var CNRestart = polardbxv1reconcile.NewStepBinder("CNRestart",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.GetRoleToRestartByRole(polardbxmeta.RoleCN) {
			err := TransferCNRestartPhase(rc)
			if err != nil {
				if apierrors.IsConflict(err) {
					return flow.Retry("Update conflict, Retry")
				} else {
					return flow.Error(err, "Unable to Transfer CN to Restart Phase")
				}

			}
		}
		return flow.Pass()
	},
)

func TransferDNRestartPhase(rc *polardbxv1reconcile.Context) error {
	// change dn stage
	dns, err := rc.GetDNMap()
	if err != nil {
		return err
	}
	for _, dn := range dns {
		dn.Status.Restarting = true
		err := rc.Client().Status().Update(rc.Context(), dn)
		if err != nil {
			return err
		}
	}

	return nil
}

var DNRestart = polardbxv1reconcile.NewStepBinder("DNRestart",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.GetRoleToRestartByRole(polardbxmeta.RoleDN) {
			err := TransferDNRestartPhase(rc)
			if err != nil {
				if apierrors.IsConflict(err) {
					return flow.Retry("Update conflict, Retry")
				} else {
					return flow.Error(err, "Unable to Transfer CN to Restart Phase")
				}
			}
		}
		return flow.Pass()
	},
)

func TransferGMSRestartPhase(rc *polardbxv1reconcile.Context) error {
	// change gms stage
	gms, err := rc.GetGMS()
	if err != nil {
		return err
	}
	gms.Status.Restarting = true
	err = rc.Client().Status().Update(rc.Context(), gms)
	if err != nil {
		return err
	}

	return nil
}

var GMSRestart = polardbxv1reconcile.NewStepBinder("GMSRestart",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.GetRoleToRestartByRole(polardbxmeta.RoleGMS) {
			err := TransferGMSRestartPhase(rc)
			if err != nil {
				if apierrors.IsConflict(err) {
					return flow.Retry("Update conflict, Retry")
				} else {
					return flow.Error(err, "Unable to Transfer CN to Restart Phase")
				}
			}
		}
		return flow.Pass()
	},
)

var GetParametersRoleMap = polardbxv1reconcile.NewStepBinder("GetParametersRoleMap",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		paramsRoleMap := rc.GetPolarDBXParams()
		parameterTemplateName := rc.MustGetPolarDBXParameter().Spec.TemplateName
		templateParams := rc.GetPolarDBXTemplateParams()

		//if parameterTemplateName == "" {
		//	return flow.Continue("No parameter template specified, use default.")
		//}

		pt, err := rc.GetPolarDBXParameterTemplate(parameterTemplateName)
		if pt == nil {
			return flow.Error(err, "Unable to get parameter template.", "node", parameterTemplateName)
		}

		for _, param := range pt.Spec.NodeType.CN.ParamList {
			templateParams[polardbxmeta.RoleCN][param.Name] = param
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

		parameter := rc.MustGetPolarDBXParameter()
		for _, param := range parameter.Spec.NodeType.CN.ParamList {
			// read only / read write
			mode := common.GetMode(polardbxmeta.RoleCN, templateParams[polardbxmeta.RoleCN][param.Name].Mode)
			paramsRoleMap[mode][param.Name] = polardbxv1.Params{
				Name:  param.Name,
				Value: param.Value,
			}
			// restart
			if templateParams[polardbxmeta.RoleCN][param.Name].Restart {
				paramsRoleMap[polardbxv1.CNRestart][param.Name] = polardbxv1.Params{
					Name:  param.Name,
					Value: param.Value,
				}
			}
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

var SyncCNRestartType = polardbxv1reconcile.NewStepBinder("SyncPolarDBXRestartType",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxParameter := rc.MustGetPolarDBXParameter()
		polardbx := rc.MustGetPolarDBX()

		polardbx.Status.RestartingType = polardbxParameter.Spec.NodeType.CN.RestartType

		err := rc.UpdatePolarDBXStatus()
		if err != nil {
			if apierrors.IsConflict(err) {
				return flow.RetryAfter(time.Second*2, "Update conflict, Retry")
			} else {
				return flow.Error(err, "Unable to Update dn status")
			}
		}

		return flow.Pass()
	},
)

var SyncDNRestartType = polardbxv1reconcile.NewStepBinder("SyncDNRestartType",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxParameter := rc.MustGetPolarDBXParameter()
		dns, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get DN map")
		}
		for _, dn := range dns {
			dn.Status.RestartingType = polardbxParameter.Spec.NodeType.DN.RestartType
			err := rc.Client().Status().Update(rc.Context(), dn)
			if err != nil {
				if apierrors.IsConflict(err) {
					return flow.Retry("Update conflict, Retry")
				} else {
					return flow.Error(err, "Unable to Update dn status")
				}
			}
		}

		return flow.Pass()
	},
)

var SyncGMSRestartType = polardbxv1reconcile.NewStepBinder("SyncGMSRestartType",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxParameter := rc.MustGetPolarDBXParameter()
		gms, err := rc.GetGMS()
		if err != nil {
			return flow.Error(err, "Unable to get DN map")
		}
		if polardbxParameter.Spec.NodeType.GMS == nil {
			gms.Status.RestartingType = polardbxParameter.Spec.NodeType.DN.RestartType
		} else {
			gms.Status.RestartingType = polardbxParameter.Spec.NodeType.GMS.RestartType
		}
		err = rc.Client().Status().Update(rc.Context(), gms)
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

func contains(s []polardbxv1.Params, e polardbxv1.Params) bool {
	for _, a := range s {
		if a.Name == e.Name && a.Value == e.Value {
			return true
		}
	}
	return false
}

var GetRolesToRestart = polardbxv1reconcile.NewStepBinder("GetRolesToRestart",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxParameter := rc.MustGetPolarDBXParameter()
		paramsRoleMap := rc.GetPolarDBXParams()
		snapshot := polardbxParameter.Status.PrevParameterSpecSnapshot
		roleToRestart := rc.GetRoleToRestart()

		// CN
		if snapshot != nil {
			for _, p := range polardbxParameter.Spec.NodeType.CN.ParamList {
				if !contains(snapshot.NodeType.CN.ParamList, p) {
					if _, ok := paramsRoleMap[polardbxv1.CNRestart][p.Name]; ok {
						roleToRestart[polardbxmeta.RoleCN] = true
						break
					}
				}
			}
		} else {
			for _, param := range polardbxParameter.Spec.NodeType.CN.ParamList {
				if _, ok := paramsRoleMap[polardbxv1.CNRestart][param.Name]; ok {
					roleToRestart[polardbxmeta.RoleCN] = true
					break
				}
			}

		}

		// DN
		if snapshot != nil {
			for _, p := range polardbxParameter.Spec.NodeType.DN.ParamList {
				if !contains(snapshot.NodeType.DN.ParamList, p) {
					if _, ok := paramsRoleMap[polardbxv1.DNRestart][p.Name]; ok {
						roleToRestart[polardbxmeta.RoleDN] = true
						break
					}
				}
			}
		} else {
			for _, param := range polardbxParameter.Spec.NodeType.DN.ParamList {
				if _, ok := paramsRoleMap[polardbxv1.DNRestart][param.Name]; ok {
					roleToRestart[polardbxmeta.RoleDN] = true
					break
				}
			}

		}

		// GMS
		if polardbxParameter.Spec.NodeType.GMS == nil {
			roleToRestart[polardbxmeta.RoleGMS] = roleToRestart[polardbxmeta.RoleDN]
		} else {
			if snapshot != nil {
				for _, p := range polardbxParameter.Spec.NodeType.GMS.ParamList {
					if !contains(snapshot.NodeType.GMS.ParamList, p) {
						if _, ok := paramsRoleMap[polardbxv1.GMSRestart][p.Name]; ok {
							roleToRestart[polardbxmeta.RoleGMS] = true
							break
						}
					}
				}
			} else {
				for _, param := range polardbxParameter.Spec.NodeType.GMS.ParamList {
					if _, ok := paramsRoleMap[polardbxv1.GMSRestart][param.Name]; ok {
						roleToRestart[polardbxmeta.RoleGMS] = true
						break
					}
				}

			}
		}

		rc.SetRoleToRestart(roleToRestart)

		return flow.Pass()
	},
)
