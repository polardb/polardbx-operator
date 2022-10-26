package gms

import polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

func GetGmsParamList(parameter *polardbxv1.PolarDBXParameter) []polardbxv1.Params {
	gmsParamList := make([]polardbxv1.Params, 0)
	if parameter.Spec.NodeType.GMS == nil {
		gmsParamList = append(gmsParamList, parameter.Spec.NodeType.DN.ParamList...)
	} else {
		gmsParamList = append(gmsParamList, parameter.Spec.NodeType.GMS.ParamList...)
	}
	return gmsParamList
}
