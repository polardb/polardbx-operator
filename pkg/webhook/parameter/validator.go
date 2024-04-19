package parameter

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/util/gms"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
	"github.com/go-logr/logr"
	"github.com/itchyny/timefmt-go"

	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type Validator struct {
	client.Reader
	logr.Logger
}

func (v *Validator) validateObject(ctx context.Context, obj *polardbxv1.PolarDBXParameter, polardbxcluster *polardbxv1.PolarDBXCluster) field.ErrorList {
	var errList field.ErrorList

	polarDBXParameterList := polardbxv1.PolarDBXParameterList{}
	err := v.List(context.Background(), &polarDBXParameterList,
		client.InNamespace(obj.Namespace),
		client.MatchingLabels(convention.GetParameterLabel()),
	)
	if err != nil {
		errList = append(errList, field.Required(field.NewPath("spec", "templateName"), "parameter template is different from polardbxcluster's template."))
		return errList
	}

	for _, p := range polarDBXParameterList.Items {
		if p.Name != obj.Name && p.Labels[polardbxmeta.LabelName] == obj.Spec.ClusterName && p.Spec.TemplateName == obj.Spec.TemplateName {
			errList = append(errList, field.Required(field.NewPath("spec", "templateName"), "same cluster and parameter template only have one dynamic parameter."))
		}
	}

	return errList
}

func checkTime(time string, paramName string) field.ErrorList {
	var errList field.ErrorList

	t, err := timefmt.Parse(time, "%H:%M")
	if err != nil {
		errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Out of Hour Range, paramName %s", paramName)))
	} else {
		str := timefmt.Format(t, "%H:%M")
		if str != time {
			errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Out of Hour Range, paramName %s", paramName)))
		}
	}

	return errList
}

func checkHelper(param polardbxv1.Params, nodeRole string,
	templateParams map[string]polardbxv1.TemplateParams, polardbxcluster *polardbxv1.PolarDBXCluster) field.ErrorList {
	var errList field.ErrorList

	allMatch := ".*"

	// check parameters unit
	switch templateParams[param.Name].Unit {
	case polardbxv1.UnitInt:
		optional := templateParams[param.Name].Optional
		value := new(big.Int)
		var ok bool
		if param.Value[0] == '{' {
			// check if value need formula compute
			valueInt64, err := formulaComputing(param.Value, polardbxcluster)
			if err != nil {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), err.Error()))
			}
			value = new(big.Int).SetInt64(valueInt64)
		} else {
			if strings.ContainsAny(optional, "|") && optional != allMatch {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid unit type, Need Unit INT, paramName %s", param.Name)))
			}

			value, ok = value.SetString(param.Value, 10)
			if !ok {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid unit type, Need Unit INT, paramName %s", param.Name)))
			}

		}

		// check value whether divisible by divisibility factor
		modulus := new(big.Int)
		new(big.Int).DivMod(value, new(big.Int).SetInt64(templateParams[param.Name].DivisibilityFactor), modulus)
		if modulus.Cmp(new(big.Int).SetInt64(0)) != 0 {
			errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Can Not Divisible By Divisibility Factor, paramName %s", param.Name)))
		}

		if optional != allMatch {
			// check value is in the range of optional by regex
			re := regexp.MustCompile(`(-?\d+) ?- ?(-?\d+)`)
			match := re.FindStringSubmatch(optional[1 : len(optional)-1])
			num1, num2 := new(big.Int), new(big.Int)
			num1, _ = num1.SetString(match[1], 10)
			num2, _ = num2.SetString(match[2], 10)
			if value.Cmp(num1) == -1 || value.Cmp(num2) == 1 {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Out of Range of Optional, paramName %s", param.Name)))
			}
		}

	case polardbxv1.UnitDouble:
		optional := templateParams[param.Name].Optional
		if strings.ContainsAny(optional, "|") && optional != allMatch {
			errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid unit type, Need Unit DOUBLE, paramName %s", param.Name)))
		}

		value, _ := strconv.ParseFloat(param.Value, 64)

		if optional != allMatch {
			// check value is in the range of optional by regex
			re := regexp.MustCompile(`(-?\d+.?\d+) ?- ?(-?\d+.?\d+)`)
			match := re.FindStringSubmatch(optional[1 : len(optional)-1])
			var num1, num2 float64
			num1, _ = strconv.ParseFloat(match[1], 64)
			num2, _ = strconv.ParseFloat(match[2], 64)
			if value < num1 || value > num2 {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Out of Range of Optional, paramName %s", param.Name)))
			}
		}

	case polardbxv1.UnitString:
		// special parameter sql_mode
		if param.Name == "sql_mode" {
			break
		}

		optional := templateParams[param.Name].Optional

		if !strings.ContainsAny(templateParams[param.Name].Optional, "|") {
			errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid unit type, Need Unit STRING, paramName %s", param.Name)))
		}

		if optional != allMatch {
			// check value is in the range of optional
			splits := strings.Split(optional[1:len(optional)-1], "|")
			exists := false
			for _, split := range splits {
				if split == param.Value {
					exists = true
					break
				}
			}
			if !exists {
				errList = append(errList, field.Required(field.NewPath("spec", "parameterTemplate"), fmt.Sprintf("Invalid value, Out of Range of Optional, paramName %s", param.Name)))
			}
		}

	case polardbxv1.UnitTZ:
		if param.Value != "SYSTEM" {
			errList = append(errList, checkTime(param.Value, param.Name)...)
		}

	case polardbxv1.UnitHOUR_RANGE:
		times := strings.Split(param.Value, "-")
		errList = append(errList, checkTime(times[0], param.Name)...)
		errList = append(errList, checkTime(times[1], param.Name)...)
	}

	return errList
}

func (v *Validator) validateParameters(ctx context.Context, obj *polardbxv1.PolarDBXParameter,
	template *polardbxv1.PolarDBXParameterTemplate, polardbxcluster *polardbxv1.PolarDBXCluster) field.ErrorList {
	var errList field.ErrorList

	templateParams := make(map[string]map[string]polardbxv1.TemplateParams)
	templateParams[polardbxmeta.RoleCN] = make(map[string]polardbxv1.TemplateParams)
	templateParams[polardbxmeta.RoleDN] = make(map[string]polardbxv1.TemplateParams)
	templateParams[polardbxmeta.RoleGMS] = make(map[string]polardbxv1.TemplateParams)

	for _, param := range template.Spec.NodeType.CN.ParamList {
		templateParams[polardbxmeta.RoleCN][param.Name] = param
	}

	for _, param := range template.Spec.NodeType.DN.ParamList {
		templateParams[polardbxmeta.RoleDN][param.Name] = param
	}

	if len(template.Spec.NodeType.GMS.ParamList) != 0 {
		for _, param := range template.Spec.NodeType.GMS.ParamList {
			templateParams[polardbxmeta.RoleGMS][param.Name] = param
		}
	} else {
		for _, param := range template.Spec.NodeType.DN.ParamList {
			templateParams[polardbxmeta.RoleGMS][param.Name] = param
		}
	}

	for _, param := range obj.Spec.NodeType.CN.ParamList {
		errList = append(errList, checkHelper(param, polardbxmeta.RoleCN, templateParams[polardbxmeta.RoleCN], polardbxcluster)...)
	}
	for _, param := range obj.Spec.NodeType.DN.ParamList {
		errList = append(errList, checkHelper(param, polardbxmeta.RoleDN, templateParams[polardbxmeta.RoleDN], polardbxcluster)...)

	}

	gmsParamList := gms.GetGmsParamList(obj)

	for _, param := range gmsParamList {
		errList = append(errList, checkHelper(param, polardbxmeta.RoleGMS, templateParams[polardbxmeta.RoleGMS], polardbxcluster)...)
	}

	return errList
}

func (v *Validator) validate(ctx context.Context, obj *polardbxv1.PolarDBXParameter) error {
	var errList field.ErrorList

	if len(obj.Spec.TemplateName) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "templateName"), "template name must be specified"))
		return apierrors.NewInvalid(obj.GroupVersionKind().GroupKind(), obj.Name, errList)
	}

	// get parameter template
	parameterTemplate := &polardbxv1.PolarDBXParameterTemplate{}
	err := v.Get(context.Background(), client.ObjectKey{
		Namespace: obj.Namespace,
		Name:      obj.Spec.TemplateName,
	}, parameterTemplate)
	if err != nil {
		if apierrors.IsNotFound(err) {
			v.Logger.Info("Specified parameter template not exists.", "parameter template", obj.Spec.TemplateName)
		} else {
			v.Logger.Error(err, "Failed to get parameter template.")
		}
		return err
	}

	// sync spec snapshot
	obj.Status.ParameterSpecSnapshot = obj.Spec.DeepCopy()

	if len(obj.Spec.ClusterName) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "clusterName"), "cluster name must be specified"))
		return apierrors.NewInvalid(obj.GroupVersionKind().GroupKind(), obj.Name, errList)
	}

	// get polardbx cluster
	polardbxcluster := &polardbxv1.PolarDBXCluster{}
	err = v.Get(context.Background(), client.ObjectKey{
		Namespace: obj.Namespace,
		Name:      obj.Spec.ClusterName,
	}, polardbxcluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			v.Logger.Info("Specified polardbx cluster not exists.", "polardbxcluster", obj.Spec.TemplateName)
		} else {
			v.Logger.Error(err, "Failed to get polardbx cluster.")
		}
		return err
	}

	errList = append(errList, v.validateObject(ctx, obj, polardbxcluster)...)
	errList = append(errList, v.validateParameters(ctx, obj, parameterTemplate, polardbxcluster)...)

	if len(errList) > 0 {
		return apierrors.NewInvalid(obj.GroupVersionKind().GroupKind(), obj.Name, errList)
	}
	return nil
}

func (v *Validator) ValidateCreate(ctx context.Context, obj runtime.Object) error {
	v.Logger.Info("Validate parameter template creation.")
	return v.validate(ctx, obj.(*polardbxv1.PolarDBXParameter))
}

func (v *Validator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) error {
	newParameter := newObj.(*polardbxv1.PolarDBXParameter)

	if err := v.validate(ctx, newParameter); err != nil {
		return err
	}

	return nil
}

func (v *Validator) ValidateDelete(ctx context.Context, obj runtime.Object) error {
	return nil
}

func NewParameterValidator(r client.Reader, logger logr.Logger) extension.CustomValidator {
	return &Validator{Reader: r, Logger: logger}
}
