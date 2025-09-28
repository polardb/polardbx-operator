package runner

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
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

// Run executes steps sequentially; returns immediately on the first error,
// and writes a 200 JSON result (caller decides actual HTTP status code).
func Run(c *gin.Context, steps ...Step) *RunResult {
	results := make([]StepResult, 0, len(steps))
	for _, s := range steps {
		log.Printf("flow step start: %s", s.Name)
		r := StepResult{Name: s.Name, Status: "pending"}
		if err := s.Fn(c); err != nil {
			r.Status = "failed"
			r.Error = err.Error()
			results = append(results, r)
			log.Printf("flow step failed: %s, err=%s", s.Name, err.Error())
			return &RunResult{Steps: results, Status: "failed"}
		}
		r.Status = "succeeded"
		results = append(results, r)
		log.Printf("flow step succeeded: %s", s.Name)
	}
	return &RunResult{Steps: results, Status: "succeeded"}
}

// JSON response helper.
func RespondOK(c *gin.Context, rr *RunResult) {
	if !c.Writer.Written() {
		c.JSON(http.StatusOK, rr)
	}
}
