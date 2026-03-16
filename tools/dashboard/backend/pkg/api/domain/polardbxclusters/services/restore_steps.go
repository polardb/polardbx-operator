package services

import (
	"errors"
	"fmt"

	domain_restore "polardbx-dashboard-backend/pkg/api/domain/platform/restore/handler"
	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services/runner"
	"polardbx-dashboard-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

// Flow: Precheck → Prepare → Apply → Verify
func RunRestoreFlow(c *gin.Context) {
	rr := runner.Run(c,
		runner.Step{Name: "precheck", Fn: func(c *gin.Context) error {
			// Simple validation: parameter existence and kube client
			if _, ok := util.K8sClientFromContext(c); !ok {
				return errors.New("k8s client missing")
			}
			ns := c.Param("namespace")
			name := c.Param("name")
			if ns == "" || name == "" {
				return fmt.Errorf("invalid path params: namespace=%s name=%s", ns, name)
			}
			return nil
		}},
		runner.Step{Name: "prepare", Fn: func(c *gin.Context) error {
			// Reserve more validation; not blocking here
			return nil
		}},
		runner.Step{Name: "apply", Fn: func(c *gin.Context) error {
			domain_restore.RestoreCluster(c)
			if c.IsAborted() {
				return errors.New("restore apply aborted")
			}
			return nil
		}},
		runner.Step{Name: "verify", Fn: func(c *gin.Context) error {
			// Reserve: poll based on status or check target cluster creation state once
			return nil
		}},
	)
	runner.RespondOK(c, rr)
}
