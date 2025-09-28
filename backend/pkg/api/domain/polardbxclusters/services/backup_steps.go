package services

import (
	"errors"

	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services/runner"

	"github.com/gin-gonic/gin"
)

// Flow: Validate → Create (behavior remains unchanged; internally forwards to original handlers)
func RunBackupFlow(c *gin.Context) {
	bs := NewBackupService()
	rr := runner.Run(c,
		runner.Step{Name: "validate", Fn: func(c *gin.Context) error {
			bs.Validate(c)
			if c.IsAborted() {
				return errors.New("validate aborted")
			}
			return nil
		}},
		runner.Step{Name: "create", Fn: func(c *gin.Context) error {
			bs.Create(c)
			if c.IsAborted() {
				return errors.New("create aborted")
			}
			return nil
		}},
	)
	runner.RespondOK(c, rr)
}
