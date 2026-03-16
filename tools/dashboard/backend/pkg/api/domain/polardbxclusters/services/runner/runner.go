package runner

import (
	"github.com/gin-gonic/gin"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/logger"
)

type Step struct {
	Name string
	Fn   func(c *gin.Context) error
}

type StepResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type RunResult struct {
	Steps  []StepResult `json:"steps"`
	Status string       `json:"status"`
}

// Run executes steps sequentially; returns immediately on error and outputs a brief result with 200 status (upper layer sets the specific HTTP code).
func Run(c *gin.Context, steps ...Step) *RunResult {
	results := make([]StepResult, 0, len(steps))
	for _, s := range steps {
		logger.Info("flow step start", "step", s.Name)
		r := StepResult{Name: s.Name, Status: "pending"}
		if err := s.Fn(c); err != nil {
			r.Status = "failed"
			r.Error = err.Error()
			results = append(results, r)
			logger.Error("flow step failed",
				"step", s.Name,
				"error", err)
			return &RunResult{Steps: results, Status: "failed"}
		}
		r.Status = "succeeded"
		results = append(results, r)
		logger.Info("flow step succeeded", "step", s.Name)
	}
	return &RunResult{Steps: results, Status: "succeeded"}
}

// JSON response helper function.
func RespondOK(c *gin.Context, rr *RunResult) {
	if !c.Writer.Written() {
		apierr.OK(c, rr)
	}
}
