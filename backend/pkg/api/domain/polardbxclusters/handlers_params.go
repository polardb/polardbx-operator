package polardbxclusters

import (
	api_parameters "polardbx-ui-backend/pkg/api/parameters"

	"github.com/gin-gonic/gin"
)

// --- Thin handlers for parameters/templates ---

func ListParameters(c *gin.Context)  { api_parameters.List(c) }
func CreateParameter(c *gin.Context) { api_parameters.Create(c) }
func GetParameter(c *gin.Context)    { api_parameters.Get(c) }
func UpdateParameter(c *gin.Context) { api_parameters.Update(c) }
func DeleteParameter(c *gin.Context) { api_parameters.Delete(c) }

func ListTemplates(c *gin.Context)  { api_parameters.ListTemplates(c) }
func CreateTemplate(c *gin.Context) { api_parameters.CreateTemplate(c) }
func GetTemplate(c *gin.Context)    { api_parameters.GetTemplate(c) }
func UpdateTemplate(c *gin.Context) { api_parameters.UpdateTemplate(c) }
func DeleteTemplate(c *gin.Context) { api_parameters.DeleteTemplate(c) }
