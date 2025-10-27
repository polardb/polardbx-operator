package platform

import (
	api_prometheusrule "polardbx-ui-backend/pkg/api/prometheusrule"

	"github.com/gin-gonic/gin"
)

func PrometheusRuleList(c *gin.Context)          { api_prometheusrule.List(c) }
func PrometheusRuleGetYAML(c *gin.Context)       { api_prometheusrule.GetYAML(c) }
func PrometheusRuleValidate(c *gin.Context)      { api_prometheusrule.ValidateRule(c) }
func PrometheusRuleListTemplates(c *gin.Context) { api_prometheusrule.ListTemplates(c) }
func PrometheusRuleGetTemplate(c *gin.Context)   { api_prometheusrule.GetTemplate(c) }
func PrometheusRuleApplyTemplate(c *gin.Context) { api_prometheusrule.ApplyTemplate(c) }
