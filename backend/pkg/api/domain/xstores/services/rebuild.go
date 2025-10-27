package services

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"polardbx-ui-backend/pkg/api/domain/xstores/k8srepo"
	"polardbx-ui-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RebuildService：先迁移 Status，其他入口后续编排
type RebuildService struct {
	repo k8srepo.XStoreRepository
}

func NewRebuildService() *RebuildService { return &RebuildService{repo: k8srepo.NewXStoreRepository()} }

// 兼容测试需求：根据路径参数与简化体创建 XStoreFollower CR
// 请求体示例：{"name":"rebuild-logger-x1","xStoreName":"xstore1"}
func (s *RebuildService) Logger(c *gin.Context) {
	createFollowerWithRole(c, polardbxv1xstore.FollowerRole("logger"))
}
func (s *RebuildService) Learner(c *gin.Context) {
	createFollowerWithRole(c, polardbxv1xstore.FollowerRole("learner"))
}
func (s *RebuildService) Auto(c *gin.Context) {
	createFollowerWithRole(c, polardbxv1xstore.FollowerRole("follower"))
}

func (s *RebuildService) Status(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	xname := c.Param("name")
	var list polardbxv1.XStoreFollowerList
	if err := cli.List(c.Request.Context(), &list, client.InNamespace(ns)); err != nil {
		util.HandleK8sError(c, "failed to list followers", err)
		return
	}
	items := make([]map[string]any, 0)
	for _, f := range list.Items {
		if f.Spec.XStoreName == xname && !polardbxv1xstore.IsEndPhase(f.Status.Phase) {
			items = append(items, map[string]any{"name": f.Name, "phase": string(f.Status.Phase), "message": f.Status.Message, "targetPod": f.Status.TargetPodName})
		}
	}
	c.JSON(http.StatusOK, gin.H{"namespace": ns, "xstore": xname, "active": items})
}

// Wait 轮询等待指定 follower 进入结束态（成功/失败/删除中），支持超时与间隔。
func (s *RebuildService) Wait(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Query("follower")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "follower is required"})
		return
	}
	timeoutSec, _ := strconv.Atoi(c.DefaultQuery("timeoutSec", "300"))
	intervalSec, _ := strconv.Atoi(c.DefaultQuery("intervalSec", "3"))
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for {
		var f polardbxv1.XStoreFollower
		if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &f); err != nil {
			util.HandleK8sError(c, "failed to get follower", err)
			return
		}
		if polardbxv1xstore.IsEndPhase(f.Status.Phase) {
			c.JSON(http.StatusOK, gin.H{"name": f.Name, "phase": string(f.Status.Phase), "message": f.Status.Message})
			return
		}
		if time.Now().After(deadline) {
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": "timeout", "name": f.Name, "phase": string(f.Status.Phase)})
			return
		}
		log.Printf("rebuild wait: ns=%s follower=%s phase=%s", ns, name, f.Status.Phase)
		time.Sleep(time.Duration(intervalSec) * time.Second)
	}
}

// Progress: 简单返回当前 follower 的状态与消息，便于外部轮询。
func (s *RebuildService) Progress(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Query("follower")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "follower is required"})
		return
	}
	var f polardbxv1.XStoreFollower
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &f); err != nil {
		util.HandleK8sError(c, "failed to get follower", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"name": f.Name, "phase": string(f.Status.Phase), "message": f.Status.Message, "targetPod": f.Status.TargetPodName})
}

// Cancel: 取消（删除）XStoreFollower 任务
func (s *RebuildService) Cancel(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	xstoreName := c.Param("name")

	// 查找该 XStore 相关的所有 Follower 任务
	followers, err := s.repo.ListFollowers(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list followers", err)
		return
	}

	var targetFollower *polardbxv1.XStoreFollower
	for _, f := range followers {
		if f.Spec.XStoreName == xstoreName {
			// 找到非终态的任务
			if f.Status.Phase != polardbxv1xstore.FollowerPhaseSuccess &&
				f.Status.Phase != polardbxv1xstore.FollowerPhaseFailed &&
				f.Status.Phase != polardbxv1xstore.FollowerPhaseDeleting {
				targetFollower = &f
				break
			}
		}
	}

	if targetFollower == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no active rebuild task found for xstore", "xstore": xstoreName})
		return
	}

	// 删除 XStoreFollower 任务
	if err := s.repo.DeleteFollower(c.Request.Context(), cli, ns, targetFollower.Name); err != nil {
		util.HandleK8sError(c, "failed to cancel rebuild task", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "rebuild task cancelled",
		"task":    targetFollower.Name,
		"xstore":  xstoreName,
	})
}

// 内部帮助方法：根据角色创建 XStoreFollower
func createFollowerWithRole(c *gin.Context, role polardbxv1xstore.FollowerRole) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	if ns == "" {
		ns = c.DefaultQuery("namespace", util.DefaultNamespace(c, "default"))
	}
	xstoreName := c.Param("name")
	type payload struct {
		Name       string `json:"name"`
		XStoreName string `json:"xStoreName"`
	}
	var body payload
	_ = c.ShouldBindJSON(&body)
	name := body.Name
	if name == "" {
		log.Printf("rebuild create missing name: ns=%s, xstore=%s", ns, xstoreName)
		util.NotFound(c)
		return
	}
	if xstoreName == "" {
		xstoreName = body.XStoreName
	}
	if xstoreName == "" {
		log.Printf("rebuild create missing xStoreName: ns=%s, name=%s", ns, name)
		c.JSON(http.StatusBadRequest, gin.H{"error": "xStoreName is required"})
		return
	}
	obj := &polardbxv1.XStoreFollower{}
	obj.Namespace = ns
	obj.Name = name
	obj.Spec.XStoreName = xstoreName
	obj.Spec.Role = role
	obj.Spec.Local = false
	// 自动选择目标 Pod（优先 follower 角色且 Running 的 Pod）
	if obj.Spec.TargetPodName == "" || obj.Spec.FromPodName == "" {
		var pods corev1.PodList
		if err := cli.List(c.Request.Context(), &pods, client.InNamespace(ns)); err == nil {
			var candidate string
			for _, p := range pods.Items {
				if p.Labels["xstore/name"] != xstoreName {
					continue
				}
				if p.Status.Phase != corev1.PodRunning {
					continue
				}
				if p.Labels["xstore/role"] == "follower" {
					candidate = p.Name
					break
				}
				if candidate == "" {
					candidate = p.Name
				}
			}
			if candidate != "" {
				if obj.Spec.TargetPodName == "" {
					obj.Spec.TargetPodName = candidate
				}
				if obj.Spec.FromPodName == "" {
					obj.Spec.FromPodName = candidate
				}
			}
		}
	}
	if obj.Labels == nil {
		obj.Labels = map[string]string{}
	}
	obj.Labels["xstore/rebuild-type"] = string(role)
	if err := cli.Create(c.Request.Context(), obj); err != nil {
		log.Printf("rebuild create failed: ns=%s name=%s xstore=%s role=%s err=%v", ns, name, xstoreName, role, err)
		util.HandleK8sError(c, "failed to create xstore follower", err)
		return
	}
	log.Printf("rebuild create ok: ns=%s name=%s xstore=%s role=%s target=%s", ns, name, xstoreName, role, obj.Spec.TargetPodName)
	c.JSON(http.StatusCreated, obj)
}
