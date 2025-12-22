package xstores

import (
	"polardbx-dashboard-backend/pkg/api/domain/xstores/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// svc gets XStoreService from Provider
func svc(c *gin.Context) *services.XStoreService {
	return provider.Must(c).XStoreService(c)
}

// List lists XStores in the given namespace (or default namespace when not specified).
// @Summary List XStores
// @Description List XStore instances in the specified namespace.
// @Tags xstores
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Success 200 {array} XStoreDTO "List of XStores"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores [get]
func List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")

	items, err := svc(c).List(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// Create creates a new XStore.
// @Summary Create XStore
// @Description Create a new XStore resource in the specified or default namespace.
// @Tags xstores
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Param body body XStoreSpecDTO true "XStore specification"
// @Success 201 {object} XStoreDTO "Created XStore"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores [post]
func Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")

	var body polardbxv1.XStore
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	created, err := svc(c).Create(c.Request.Context(), cli, ns, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// Get returns a single XStore by namespace and name.
// @Summary Get XStore
// @Description Get an XStore resource by namespace and name.
// @Tags xstores
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} XStoreDTO "XStore"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name} [get]
func Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	item, err := svc(c).Get(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// Update updates an existing XStore.
// @Summary Update XStore
// @Description Update an existing XStore resource.
// @Tags xstores
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Param body body XStoreSpecDTO true "Updated XStore specification"
// @Success 200 {object} XStoreDTO "Updated XStore"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name} [put]
func Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")

	var body polardbxv1.XStore
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	body.Namespace = ns

	updated, err := svc(c).Update(c.Request.Context(), cli, ns, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// Delete deletes an XStore.
// @Summary Delete XStore
// @Description Delete an XStore resource by namespace and name.
// @Tags xstores
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} MessageResponseDTO "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name} [delete]
func Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	if err := svc(c).Delete(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "xstore deleted"})
}

// ListPods lists Pods that belong to the given XStore.
// @Summary List XStore pods
// @Description List Pods under the specified XStore.
// @Tags xstores
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {array} XStorePodDTO "List of Pods"
// @Failure 404 {object} apierr.ErrorResponse "XStore or Pods not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/pods [get]
func ListPods(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	items, err := svc(c).ListPods(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}
