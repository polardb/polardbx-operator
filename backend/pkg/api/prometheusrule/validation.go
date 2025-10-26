package prometheusrule

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

type ruleValidationOutcome struct {
	Success  bool
	Message  string
	Details  []map[string]string
	Errors   []string
	Warnings []string
}

func runRuleValidation(yamlContent string) (*unstructured.Unstructured, ruleValidationOutcome) {
	trimmed := strings.TrimSpace(yamlContent)
	if trimmed == "" {
		return nil, ruleValidationOutcome{
			Success: false,
			Message: "YAML content is empty",
			Details: []map[string]string{{
				"level":   "error",
				"message": "YAML content is required",
			}},
			Errors: []string{"YAML content is required"},
		}
	}

	var obj unstructured.Unstructured
	if err := yaml.Unmarshal([]byte(trimmed), &obj.Object); err != nil {
		errMsg := fmt.Sprintf("Invalid YAML format: %v", err)
		return nil, ruleValidationOutcome{
			Success: false,
			Message: errMsg,
			Details: []map[string]string{{
				"level":   "error",
				"message": errMsg,
			}},
			Errors: []string{errMsg},
		}
	}

	errors := []string{}
	warnings := []string{}
	details := []map[string]string{}

	if obj.GetAPIVersion() != "monitoring.coreos.com/v1" {
		errors = append(errors, "apiVersion should be 'monitoring.coreos.com/v1'")
	}
	if obj.GetKind() != "PrometheusRule" {
		errors = append(errors, "kind should be 'PrometheusRule'")
	}
	if obj.GetName() == "" {
		errors = append(errors, "metadata.name is required")
	}

	groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
	if !found {
		errors = append(errors, "spec.groups is required")
	} else if len(groups) == 0 {
		warnings = append(warnings, "No rule groups defined")
	}

	for i, groupInterface := range groups {
		groupMap, ok := groupInterface.(map[string]interface{})
		if !ok {
			errors = append(errors, fmt.Sprintf("Group %d: invalid structure", i))
			continue
		}

		name, hasName := groupMap["name"].(string)
		if !hasName || strings.TrimSpace(name) == "" {
			errors = append(errors, fmt.Sprintf("Group %d: name is required", i))
			continue
		}

		rulesInterface, ok := groupMap["rules"].([]interface{})
		if !ok {
			errors = append(errors, fmt.Sprintf("Group '%s': rules field must be an array", name))
			continue
		}
		if len(rulesInterface) == 0 {
			warnings = append(warnings, fmt.Sprintf("Group '%s': No rules defined", name))
		}

		for j, ruleInterface := range rulesInterface {
			ruleMap, ok := ruleInterface.(map[string]interface{})
			if !ok {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: invalid structure", name, j))
				continue
			}

			expr, hasExpr := ruleMap["expr"].(string)
			if !hasExpr || strings.TrimSpace(expr) == "" {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: expr is required", name, j))
			}

			_, hasAlert := ruleMap["alert"].(string)
			_, hasRecord := ruleMap["record"].(string)
			if !hasAlert && !hasRecord {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: either 'alert' or 'record' must be specified", name, j))
			}
			if hasAlert && hasRecord {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: cannot have both 'alert' and 'record'", name, j))
			}

			if hasExpr && strings.TrimSpace(expr) != "" && !isValidPromQLBasic(expr) {
				warnings = append(warnings, fmt.Sprintf("Group '%s', Rule %d: potentially invalid PromQL expression", name, j))
			}
		}
	}

	for _, err := range errors {
		details = append(details, map[string]string{
			"level":   "error",
			"message": err,
		})
	}
	for _, warn := range warnings {
		details = append(details, map[string]string{
			"level":   "warning",
			"message": warn,
		})
	}

	success := len(errors) == 0
	message := "Validation passed"
	if !success {
		message = "Validation failed"
	} else if len(warnings) > 0 {
		message = "Validation passed with warnings"
	}

	return &obj, ruleValidationOutcome{
		Success:  success,
		Message:  message,
		Details:  details,
		Errors:   errors,
		Warnings: warnings,
	}
}
