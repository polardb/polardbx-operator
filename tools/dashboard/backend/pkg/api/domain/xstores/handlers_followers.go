package xstores

import (
	"strconv"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/xstores/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"
)

// followersSvc gets FollowersService from Provider
func followersSvc(c *gin.Context) *services.FollowersService {
	return provider.Must(c).FollowersService(c)
}

// rebuildSvc gets RebuildService from Provider
func rebuildSvc(c *gin.Context) *services.RebuildService {
	return provider.Must(c).RebuildService(c)
}

// ListFollowers lists XStore followers.
// @Summary List XStore followers
// @Description List follower XStores in the specified or default namespace.
// @Tags xstores, followers
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Success 200 {array} XStoreFollowerDTO "List of follower XStores"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/followers [get]
func ListFollowers(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", util.DefaultNamespace(c, "default"))
	items, err := followersSvc(c).List(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// CreateFollower creates a follower XStore.
// @Summary Create XStore follower
// @Description Create a follower XStore that replicates from a primary XStore.
// @Tags xstores, followers
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Param body body XStoreFollowerSpecDTO true "Follower XStore specification"
// @Success 201 {object} XStoreFollowerDTO "Created follower XStore"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/followers [post]
func CreateFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", util.DefaultNamespace(c, "default"))
	var payload struct {
		Metadata struct {
			Name      string `json:"name,omitempty"`
			Namespace string `json:"namespace,omitempty"`
		} `json:"metadata,omitempty"`
		Spec struct {
			XStoreName string `json:"xStoreName,omitempty"`
			Role       string `json:"role,omitempty"`
		} `json:"spec,omitempty"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	obj := &polardbxv1.XStoreFollower{}
	obj.Name = payload.Metadata.Name
	obj.Namespace = payload.Metadata.Namespace
	if obj.Namespace == "" {
		obj.Namespace = ns
	}
	obj.Spec.XStoreName = payload.Spec.XStoreName
	if obj.Spec.XStoreName == "" {
		obj.Spec.XStoreName = c.Param("name")
	}
	if payload.Spec.Role != "" {
		obj.Spec.Role = polardbxv1xstore.FollowerRole(payload.Spec.Role)
	}
	created, err := followersSvc(c).Create(c.Request.Context(), cli, obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetFollower gets a follower XStore by namespace and name.
// @Summary Get XStore follower
// @Description Get a follower XStore by namespace and name.
// @Tags xstores, followers
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the follower"
// @Param name path string true "Name of the follower XStore"
// @Success 200 {object} XStoreFollowerDTO "Follower XStore"
// @Failure 404 {object} apierr.ErrorResponse "Follower not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/followers/{namespace}/{name} [get]
func GetFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := followersSvc(c).Get(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// UpdateFollower updates a follower XStore.
// @Summary Update XStore follower
// @Description Update an existing follower XStore.
// @Tags xstores, followers
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the follower"
// @Param name path string true "Name of the follower XStore"
// @Param body body XStoreFollowerSpecDTO true "Updated follower specification"
// @Success 200 {object} XStoreFollowerDTO "Updated follower XStore"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 404 {object} apierr.ErrorResponse "Follower not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/followers/{namespace}/{name} [put]
func UpdateFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body polardbxv1.XStoreFollower
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	obj := &body
	if obj.Namespace == "" {
		obj.Namespace = ns
	}
	updated, err := followersSvc(c).Update(c.Request.Context(), cli, obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteFollower deletes a follower XStore.
// @Summary Delete XStore follower
// @Description Delete a follower XStore by namespace and name.
// @Tags xstores, followers
// @Param namespace path string true "Kubernetes namespace of the follower"
// @Param name path string true "Name of the follower XStore"
// @Success 200 {object} MessageResponseDTO "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Follower not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/followers/{namespace}/{name} [delete]
func DeleteFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := followersSvc(c).Delete(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "xstore follower deleted"})
}

// RebuildLogger triggers a rebuild of the logger role for an XStore.
// @Summary Rebuild logger
// @Description Trigger rebuild of the logger role for the specified XStore.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 202 {object} MessageResponseDTO "Rebuild requested"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/logger [post]
func RebuildLogger(c *gin.Context) { createRebuildFollower(c, polardbxv1xstore.FollowerRole("logger")) }

// RebuildLearner triggers a rebuild of the learner role for an XStore.
// @Summary Rebuild learner
// @Description Trigger rebuild of the learner role for the specified XStore.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 202 {object} MessageResponseDTO "Rebuild requested"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/learner [post]
func RebuildLearner(c *gin.Context) {
	createRebuildFollower(c, polardbxv1xstore.FollowerRole("learner"))
}

// AutoRebuild triggers an automatic rebuild based on internal heuristics.
// @Summary Auto rebuild XStore
// @Description Trigger automatic rebuild of XStore roles using internal heuristics.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 202 {object} MessageResponseDTO "Rebuild requested"
// @Failure 404 {object} apierr.ErrorResponse "XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/auto [post]
func AutoRebuild(c *gin.Context) { createRebuildFollower(c, polardbxv1xstore.FollowerRole("follower")) }

// RebuildStatus returns the status of a rebuild operation.
// @Summary Get rebuild status
// @Description Get status of an ongoing rebuild operation for the specified XStore.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} RebuildStatusDTO "Rebuild status"
// @Failure 404 {object} apierr.ErrorResponse "Rebuild or XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/status [get]
func RebuildStatus(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	items, err := rebuildSvc(c).Status(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"namespace": ns, "xstore": name, "active": items})
}

// RebuildWait waits until a rebuild operation finishes.
// @Summary Wait rebuild
// @Description Block until a rebuild operation finishes or times out.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} RebuildStatusDTO "Final rebuild status"
// @Failure 404 {object} apierr.ErrorResponse "Rebuild or XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/wait [get]
func RebuildWait(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Query("follower")
	if name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("follower is required", nil))
		return
	}
	timeoutSec, _ := strconv.Atoi(c.DefaultQuery("timeoutSec", "300"))
	intervalSec, _ := strconv.Atoi(c.DefaultQuery("intervalSec", "3"))
	f, err := rebuildSvc(c).Wait(c.Request.Context(), cli, ns, name, time.Duration(timeoutSec)*time.Second, time.Duration(intervalSec)*time.Second)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"name": f.Name, "phase": string(f.Status.Phase), "message": f.Status.Message})
}

// RebuildProgress returns detailed progress information for a rebuild operation.
// @Summary Get rebuild progress
// @Description Get detailed progress information for an ongoing rebuild.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} RebuildProgressDTO "Rebuild progress details"
// @Failure 404 {object} apierr.ErrorResponse "Rebuild or XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/progress [get]
func RebuildProgress(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Query("follower")
	if name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("follower is required", nil))
		return
	}
	f, err := rebuildSvc(c).Progress(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"name": f.Name, "phase": string(f.Status.Phase), "message": f.Status.Message, "targetPod": f.Status.TargetPodName})
}

// RebuildCancel cancels a running rebuild operation.
// @Summary Cancel rebuild
// @Description Cancel an ongoing rebuild operation for the specified XStore.
// @Tags xstores, rebuild
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the XStore"
// @Param name path string true "Name of the XStore"
// @Success 200 {object} MessageResponseDTO "Cancellation requested"
// @Failure 404 {object} apierr.ErrorResponse "Rebuild or XStore not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/{namespace}/{name}/rebuild/cancel [delete]
func RebuildCancel(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	f, err := rebuildSvc(c).Cancel(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "rebuild task cancelled", "task": f.Name, "xstore": name})
}

// RetryFollower retries a failed follower provisioning or sync.
// @Summary Retry follower
// @Description Retry a failed follower XStore operation.
// @Tags xstores, followers
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the follower"
// @Param name path string true "Name of the follower XStore"
// @Success 202 {object} map[string]any "Retry requested"
// @Failure 404 {object} apierr.ErrorResponse "Follower not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
func RetryFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	created, err := followersSvc(c).Retry(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "task retried successfully", "original_task": name, "new_task": created.Name})
}

// CancelFollower cancels a follower provisioning or sync operation.
// @Summary Cancel follower
// @Description Cancel a follower provisioning or sync operation.
// @Tags xstores, followers
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the follower"
// @Param name path string true "Name of the follower XStore"
// @Success 200 {object} map[string]any "Cancellation requested"
// @Failure 404 {object} apierr.ErrorResponse "Follower not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
func CancelFollower(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	follower, err := followersSvc(c).Cancel(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "task cancelled successfully", "task": name, "xstore": follower.Spec.XStoreName})
}

func createRebuildFollower(c *gin.Context, role polardbxv1xstore.FollowerRole) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	if ns == "" {
		ns = c.DefaultQuery("namespace", util.DefaultNamespace(c, "default"))
	}
	xstoreName := c.Param("name")
	var body struct {
		Name       string `json:"name"`
		XStoreName string `json:"xStoreName"`
	}
	_ = c.ShouldBindJSON(&body)
	name := body.Name
	if name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("name is required", nil))
		return
	}
	if xstoreName == "" {
		xstoreName = body.XStoreName
	}
	if xstoreName == "" {
		apierr.AbortWithError(c, apierr.ValidationError("xStoreName is required", nil))
		return
	}
	obj, err := rebuildSvc(c).CreateFollower(c.Request.Context(), cli, ns, xstoreName, name, role)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, obj)
}
