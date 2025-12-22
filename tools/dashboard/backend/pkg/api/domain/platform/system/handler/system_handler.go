package handler

import (
	"polardbx-dashboard-backend/pkg/api/domain/platform/system/repository"
	"polardbx-dashboard-backend/pkg/api/domain/platform/system/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

// SystemHandler handles HTTP requests related to system-level information.
type SystemHandler struct {
	service *service.SystemService
}

// NewSystemHandler creates a new SystemHandler
func NewSystemHandler(svc *service.SystemService) *SystemHandler {
	return &SystemHandler{service: svc}
}

// NewSystemHandlerFromClient creates complete handler chain from K8s client
func NewSystemHandlerFromClient(c *gin.Context) (*SystemHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sSystemRepository(cli)
	svc := service.NewSystemService(repo)
	return NewSystemHandler(svc), true
}

// ContextInfo returns current Kubernetes user, context, and default namespace.
// @Summary Get Kubernetes context info
// @Description Get current Kubernetes user, context, and default namespace inferred from the request.
// @Tags platform, system
// @Produce json
// @Param namespace query string false "Override default namespace"
// @Success 200 {object} service.ContextInfo "Context info (user, context, defaultNamespace)"
// @Failure 500 {object} apierr.ErrorResponse "Internal error"
func ContextInfo(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	user, _ := c.Get("k8sUser")
	ctxName, _ := c.Get("k8sContext")
	defNS := c.DefaultQuery("namespace", "")
	if defNS == "" {
		if v, exists := c.Get("k8sDefaultNamespace"); exists {
			if s, ok := v.(string); ok {
				defNS = s
			}
		}
	}
	apierr.OK(c, service.ContextInfo{
		User:             asString(user),
		Context:          asString(ctxName),
		DefaultNamespace: defNS,
	})
}

// ListNamespaces returns all visible namespaces.
// @Summary List namespaces
// @Description List namespaces visible via the configured Kubernetes client.
// @Tags platform, system
// @Produce json
// @Success 200 {object} ListNamespacesResponse "Namespace list (items, count)"
// @Failure 500 {object} apierr.ErrorResponse "Failed to list namespaces"
func ListNamespaces(c *gin.Context) {
	h, ok := NewSystemHandlerFromClient(c)
	if !ok {
		apierr.AbortWithError(c, apierr.InternalServiceError("kubernetes client not initialized", nil))
		return
	}
	h.listNamespaces(c)
}

// listNamespaces instance method handles namespace list
func (h *SystemHandler) listNamespaces(c *gin.Context) {
	items, err := h.service.ListNamespaces(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, ListNamespacesResponse{Items: items, Count: len(items)})
}

// ListStorageClasses returns all available storage classes.
// @Summary List storage classes
// @Description List Kubernetes StorageClasses visible via the configured Kubernetes client.
// @Tags platform, system
// @Produce json
// @Success 200 {object} ListStorageClassesResponse "StorageClass list (items, count)"
// @Failure 500 {object} apierr.ErrorResponse "Failed to list storage classes"
func ListStorageClasses(c *gin.Context) {
	h, ok := NewSystemHandlerFromClient(c)
	if !ok {
		apierr.AbortWithError(c, apierr.InternalServiceError("kubernetes client not initialized", nil))
		return
	}
	h.listStorageClasses(c)
}

// listStorageClasses instance method handles storage class list
func (h *SystemHandler) listStorageClasses(c *gin.Context) {
	items, err := h.service.ListStorageClasses(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, ListStorageClassesResponse{Items: items, Count: len(items)})
}

// ListPolarDBXVersions returns supported PolarDB-X version list.
// @Summary List PolarDB-X versions
// @Description List supported PolarDB-X versions configured for deployment.
// @Tags platform, system
// @Produce json
// @Success 200 {object} ListPolarDBXVersionsResponse "PolarDB-X version list (items, count)"
func ListPolarDBXVersions(c *gin.Context) {
	h, ok := NewSystemHandlerFromClient(c)
	if !ok {
		// Version list can be returned even without k8s client
		svc := service.NewSystemService(nil)
		versions := svc.GetPolarDBXVersions()
		apierr.OK(c, ListPolarDBXVersionsResponse{Items: versions, Count: len(versions)})
		return
	}
	versions := h.service.GetPolarDBXVersions()
	apierr.OK(c, ListPolarDBXVersionsResponse{Items: versions, Count: len(versions)})
}

type ListNamespacesResponse struct {
	Items []service.NamespaceInfo `json:"items"`
	Count int                    `json:"count"`
}

type ListStorageClassesResponse struct {
	Items []service.StorageClassInfo `json:"items"`
	Count int                       `json:"count"`
}

type ListPolarDBXVersionsResponse struct {
	Items []service.PolarDBXVersionInfo `json:"items"`
	Count int                          `json:"count"`
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
