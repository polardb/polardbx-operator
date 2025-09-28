package platform

import (
	api_pod "polardbx-ui-backend/pkg/api/pod"

	"github.com/gin-gonic/gin"
)

func PodList(c *gin.Context)    { api_pod.List(c) }
func PodGet(c *gin.Context)     { api_pod.Get(c) }
func PodExecWS(c *gin.Context)  { api_pod.ExecWS(c) }
func PodDelete(c *gin.Context)  { api_pod.Delete(c) }
func PodGetLogs(c *gin.Context) { api_pod.GetLogs(c) }
