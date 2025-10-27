package diagnostics

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// StartDiagnosis triggers a diagnosis job for a cluster (placeholder implementation)
func Start(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	c.JSON(http.StatusAccepted, gin.H{
		"id":        time.Now().UnixNano(),
		"namespace": namespace,
		"cluster":   cluster,
		"status":    "running",
		"startedAt": time.Now().Format(time.RFC3339),
		"message":   "pending_implementation",
	})
}

// GetStatus returns progress/status of a diagnosis task (placeholder)
func GetStatus(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")
	c.JSON(http.StatusOK, gin.H{
		"id":        id,
		"namespace": namespace,
		"status":    "pending_implementation",
		"progress":  0,
	})
}

// ListReports lists diagnosis reports for a namespace (placeholder)
func ListReports(c *gin.Context) {
	namespace := c.DefaultQuery("namespace", "")
	c.JSON(http.StatusOK, gin.H{
		"namespace": namespace,
		"reports":   []any{},
	})
}

// Download returns a download placeholder for a report (placeholder)
func Download(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")
	c.JSON(http.StatusOK, gin.H{
		"id":        id,
		"namespace": namespace,
		"download":  "pending_implementation",
	})
}
