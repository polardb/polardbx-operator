package polardbxclusters

import (
	domain_parameters "polardbx-dashboard-backend/pkg/api/domain/platform/parameters/handler"

	"github.com/gin-gonic/gin"
)

// --- Thin handlers for parameters/templates ---

func ListParameters(c *gin.Context)  { domain_parameters.List(c) }
func CreateParameter(c *gin.Context) { domain_parameters.Create(c) }
func GetParameter(c *gin.Context)    { domain_parameters.Get(c) }
func UpdateParameter(c *gin.Context) { domain_parameters.Update(c) }
func DeleteParameter(c *gin.Context) { domain_parameters.Delete(c) }

func ListTemplates(c *gin.Context)  { domain_parameters.ListTemplates(c) }
func CreateTemplate(c *gin.Context) { domain_parameters.CreateTemplate(c) }
func GetTemplate(c *gin.Context)    { domain_parameters.GetTemplate(c) }
func UpdateTemplate(c *gin.Context) { domain_parameters.UpdateTemplate(c) }
func DeleteTemplate(c *gin.Context) { domain_parameters.DeleteTemplate(c) }
