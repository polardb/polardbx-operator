package services

import (
	"errors"
	"net/http"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services/runner"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// RunBackupFlow orchestrates a two-step backup flow: validate the request and then create the backup.
// Behavior remains unchanged compared to calling the underlying handlers directly.
func RunBackupFlow(c *gin.Context) {
	bs := NewBackupService()
	rr := runner.Run(c,
		runner.Step{Name: "validate", Fn: func(c *gin.Context) error {
			cli, ok := util.K8sClientFromContext(c)
			if !ok {
				return apierr.UnauthorizedError("kubernetes client not initialized")
			}
			var backup polardbxv1.PolarDBXBackup
			if err := c.ShouldBindJSON(&backup); err != nil {
				apierr.AbortWithError(c, err)
				return errors.New("validate aborted")
			}
			ns := c.Query("namespace")
			if ns == "" {
				ns = backup.Namespace
			}
			if ns == "" {
				ns = "default"
			}
			if err := bs.ValidateBackup(c.Request.Context(), cli, ns, &backup); err != nil {
				apierr.AbortWithError(c, err)
				return errors.New("validate aborted")
			}
			return nil
		}},
		runner.Step{Name: "create", Fn: func(c *gin.Context) error {
			cli, ok := util.K8sClientFromContext(c)
			if !ok {
				return apierr.UnauthorizedError("kubernetes client not initialized")
			}
			var backup polardbxv1.PolarDBXBackup
			if err := c.ShouldBindJSON(&backup); err != nil {
				apierr.AbortWithError(c, err)
				return errors.New("create aborted")
			}
			clusterName := c.Param("name")
			namespace := c.Param("namespace")
			created, err := bs.CreateBackup(c.Request.Context(), cli, namespace, clusterName, &backup)
			if err != nil {
				apierr.AbortWithError(c, err)
				return errors.New("create aborted")
			}
			c.JSON(http.StatusCreated, created)
			return nil
		}},
	)
	runner.RespondOK(c, rr)
}
