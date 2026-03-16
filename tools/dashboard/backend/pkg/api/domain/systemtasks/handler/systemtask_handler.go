package handler

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/systemtasks/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// SystemTaskHandler handles HTTP requests related to SystemTask
// Only responsible for HTTP request/response handling, business logic delegated to Service
type SystemTaskHandler struct {
	service *service.SystemTaskService
}

// NewSystemTaskHandler creates a Handler instance
func NewSystemTaskHandler(svc *service.SystemTaskService) *SystemTaskHandler {
	return &SystemTaskHandler{service: svc}
}

// List lists SystemTasks
func (h *SystemTaskHandler) List(c *gin.Context) {
	// 1. Get K8s client
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	// 2. Parse request parameters
	namespace := util.DefaultNamespace(c, "default")

	// 3. Call Service (pass context.Context, not gin.Context)
	tasks, err := h.service.List(c.Request.Context(), cli, namespace)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	// 4. Return response
	apierr.OK(c, tasks)
}

// Get retrieves a single SystemTask
func (h *SystemTaskHandler) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	namespace := c.Param("namespace")
	name := c.Param("name")

	task, err := h.service.Get(c.Request.Context(), cli, namespace, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, task)
}

// Create creates a SystemTask
func (h *SystemTaskHandler) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	namespace := util.DefaultNamespace(c, "default")

	var body polardbxv1.SystemTask
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	created, err := h.service.Create(c.Request.Context(), cli, namespace, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.Created(c, created)
}

// Update updates a SystemTask
func (h *SystemTaskHandler) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	namespace := c.Param("namespace")

	var body polardbxv1.SystemTask
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	updated, err := h.service.Update(c.Request.Context(), cli, namespace, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, updated)
}

// Delete deletes a SystemTask
func (h *SystemTaskHandler) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	namespace := c.Param("namespace")
	name := c.Param("name")

	if err := h.service.Delete(c.Request.Context(), cli, namespace, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, gin.H{"message": "system task deleted"})
}

// RegisterRoutes registers routes
func (h *SystemTaskHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/system-tasks", h.List)
	rg.POST("/system-tasks", h.Create)
	rg.GET("/system-tasks/:namespace/:name", h.Get)
	rg.PUT("/system-tasks/:namespace/:name", h.Update)
	rg.DELETE("/system-tasks/:namespace/:name", h.Delete)
}
