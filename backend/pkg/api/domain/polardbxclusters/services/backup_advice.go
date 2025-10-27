package services

import (
	"net/http"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
)

// GetBackupAdvice returns suggested backup role based on cluster topology (whether followers exist for all XStores)
func (s *BackupService) GetBackupAdvice(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	clusterName := c.Param("name")
	var xstoreList polardbxv1.XStoreList
	if err := cli.List(c.Request.Context(), &xstoreList, client.InNamespace(namespace), client.MatchingLabels{
		"polardbx/name": clusterName,
	}); err != nil {
		util.HandleK8sError(c, "failed to list xstores for cluster", err)
		return
	}
	if len(xstoreList.Items) == 0 {
		c.JSON(http.StatusOK, gin.H{"hasFollower": false, "role": "leader", "reason": "no xstores found for cluster"})
		return
	}
	namesWithoutFollower := make([]string, 0)
	for _, xs := range xstoreList.Items {
		if xs.Status.TotalPods <= 1 {
			namesWithoutFollower = append(namesWithoutFollower, xs.Name)
		}
	}
	if len(namesWithoutFollower) == 0 {
		c.JSON(http.StatusOK, gin.H{"hasFollower": true, "role": "follower"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"hasFollower": false, "role": "leader", "reason": namesWithoutFollower})
}
