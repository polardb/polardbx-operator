package prometheusrule

import (
	"context"
	"net/http"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

var prometheusRuleGVR = schema.GroupVersionResource{
	Group:    "monitoring.coreos.com",
	Version:  "v1",
	Resource: "prometheusrules",
}

// PrometheusRule represents a simplified view of PrometheusRule resource
type PrometheusRule struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		CreationTimestamp string            `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Spec struct {
		Groups []struct {
			Name     string `json:"name"`
			Interval string `json:"interval,omitempty"`
			Rules    []struct {
				Alert       string            `json:"alert,omitempty"`
				Expr        string            `json:"expr"`
				For         string            `json:"for,omitempty"`
				Labels      map[string]string `json:"labels,omitempty"`
				Annotations map[string]string `json:"annotations,omitempty"`
				Record      string            `json:"record,omitempty"`
			} `json:"rules"`
		} `json:"groups"`
	} `json:"spec"`
}

// List returns PrometheusRule resources in the specified namespace
func List(c *gin.Context) {
	var cli dynamic.Interface
	var ok bool
	if cli, ok = util.DynamicClientFromContext(c); !ok {
		return
	}

	namespace := c.Query("namespace")
	if namespace == "" {
		namespace = "polardbx-monitor"
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// List PrometheusRule resources
	list, err := cli.Resource(prometheusRuleGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		// 当命名空间不存在或资源未安装时，返回空列表 200
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusOK, []PrometheusRule{})
			return
		}
		util.HandleK8sError(c, "failed to list PrometheusRules", err)
		return
	}

	var rules []PrometheusRule
	for _, item := range list.Items {
		rule := PrometheusRule{
			APIVersion: item.GetAPIVersion(),
			Kind:       item.GetKind(),
		}
		rule.Metadata.Name = item.GetName()
		rule.Metadata.Namespace = item.GetNamespace()
		rule.Metadata.CreationTimestamp = item.GetCreationTimestamp().Format(time.RFC3339)
		rule.Metadata.Labels = item.GetLabels()

		// Extract spec.groups
		if spec, found, _ := unstructured.NestedSlice(item.Object, "spec", "groups"); found {
			for _, groupInterface := range spec {
				if groupMap, ok := groupInterface.(map[string]interface{}); ok {
					var group struct {
						Name     string `json:"name"`
						Interval string `json:"interval,omitempty"`
						Rules    []struct {
							Alert       string            `json:"alert,omitempty"`
							Expr        string            `json:"expr"`
							For         string            `json:"for,omitempty"`
							Labels      map[string]string `json:"labels,omitempty"`
							Annotations map[string]string `json:"annotations,omitempty"`
							Record      string            `json:"record,omitempty"`
						} `json:"rules"`
					}

					if name, ok := groupMap["name"].(string); ok {
						group.Name = name
					}
					if interval, ok := groupMap["interval"].(string); ok {
						group.Interval = interval
					}

					if rulesInterface, ok := groupMap["rules"].([]interface{}); ok {
						for _, ruleInterface := range rulesInterface {
							if ruleMap, ok := ruleInterface.(map[string]interface{}); ok {
								var rule struct {
									Alert       string            `json:"alert,omitempty"`
									Expr        string            `json:"expr"`
									For         string            `json:"for,omitempty"`
									Labels      map[string]string `json:"labels,omitempty"`
									Annotations map[string]string `json:"annotations,omitempty"`
									Record      string            `json:"record,omitempty"`
								}

								if alert, ok := ruleMap["alert"].(string); ok {
									rule.Alert = alert
								}
								if expr, ok := ruleMap["expr"].(string); ok {
									rule.Expr = expr
								}
								if forDuration, ok := ruleMap["for"].(string); ok {
									rule.For = forDuration
								}
								if record, ok := ruleMap["record"].(string); ok {
									rule.Record = record
								}

								// Extract labels
								if labelsInterface, ok := ruleMap["labels"].(map[string]interface{}); ok {
									rule.Labels = make(map[string]string)
									for k, v := range labelsInterface {
										if str, ok := v.(string); ok {
											rule.Labels[k] = str
										}
									}
								}

								// Extract annotations
								if annotationsInterface, ok := ruleMap["annotations"].(map[string]interface{}); ok {
									rule.Annotations = make(map[string]string)
									for k, v := range annotationsInterface {
										if str, ok := v.(string); ok {
											rule.Annotations[k] = str
										}
									}
								}

								group.Rules = append(group.Rules, rule)
							}
						}
					}

					rule.Spec.Groups = append(rule.Spec.Groups, group)
				}
			}
		}

		rules = append(rules, rule)
	}

	c.JSON(http.StatusOK, rules)
}

// GetYAML returns the YAML representation of a specific PrometheusRule
func GetYAML(c *gin.Context) {
	namespace := c.Param("namespace")
	name := c.Param("name")

	// 先做参数校验，避免因缺少客户端而返回 500
	if namespace == "" || name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace and name are required"})
		return
	}

	var cli dynamic.Interface
	var ok bool
	if cli, ok = util.DynamicClientFromContext(c); !ok {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get the specific PrometheusRule
	obj, err := cli.Resource(prometheusRuleGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "PrometheusRule not found", "namespace": namespace, "name": name})
			return
		}
		util.HandleK8sError(c, "failed to get PrometheusRule", err)
		return
	}

	// Convert to YAML
	yamlBytes, err := yaml.Marshal(obj.Object)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to convert to YAML", "details": err.Error()})
		return
	}

	c.Header("Content-Type", "text/plain")
	c.String(http.StatusOK, string(yamlBytes))
}

// ValidateRule validates PrometheusRule YAML using promtool-like validation
func ValidateRule(c *gin.Context) {
	var payload struct {
		YAML string `json:"yaml"`
	}

	if err := c.ShouldBindJSON(&payload); err != nil || strings.TrimSpace(payload.YAML) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "YAML content is required"})
		return
	}

	_, outcome := runRuleValidation(payload.YAML)
	response := gin.H{
		"success": outcome.Success,
		"message": outcome.Message,
	}
	if len(outcome.Details) > 0 {
		response["details"] = outcome.Details
	}

	c.JSON(http.StatusOK, response)
}

// Basic PromQL validation (simplified)
func isValidPromQLBasic(expr string) bool {
	// Very basic checks - real validation would require promql parser
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}

	// Check for some basic PromQL patterns
	invalidPatterns := []string{
		"((", "))", "[[", "]]", "{{", "}}",
	}

	for _, pattern := range invalidPatterns {
		if strings.Contains(expr, pattern) {
			return false
		}
	}

	return true
}
