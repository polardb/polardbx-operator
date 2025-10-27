package platform

import (
	api_system "polardbx-ui-backend/pkg/api/system"

	"github.com/gin-gonic/gin"
)

func SystemContext(c *gin.Context)  { api_system.ContextInfo(c) }
func ListNamespaces(c *gin.Context) { api_system.ListNamespaces(c) }
