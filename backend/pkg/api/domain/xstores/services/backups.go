package services

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/domain/xstores/k8srepo"
	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

// BackupsService 封装 XStore 备份相关编排
type BackupsService struct {
	repo k8srepo.XStoreRepository
}

func NewBackupsService() *BackupsService { return &BackupsService{repo: k8srepo.NewXStoreRepository()} }

func (s *BackupsService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := s.repo.ListBackups(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list xstore backups", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *BackupsService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body k8srepo.XStoreBackupAlias
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore backup", "details": err.Error()})
		return
	}
	obj := body.As()
	created, err := s.repo.CreateBackup(c.Request.Context(), cli, ns, obj)
	if err != nil {
		util.HandleK8sError(c, "failed to create xstore backup", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (s *BackupsService) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := s.repo.GetBackup(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get xstore backup", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *BackupsService) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body k8srepo.XStoreBackupAlias
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore backup", "details": err.Error()})
		return
	}
	obj := body.As()
	obj.Namespace = ns
	updated, err := s.repo.UpdateBackup(c.Request.Context(), cli, ns, obj)
	if err != nil {
		util.HandleK8sError(c, "failed to update xstore backup", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func (s *BackupsService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := s.repo.DeleteBackup(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete xstore backup", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "xstore backup deleted"})
}

func (s *BackupsService) ForceDelete(c *gin.Context) {
	// 保持原行为：直接去掉 finalizers
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	bk, err := s.repo.GetBackup(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get xstore backup", err)
		return
	}
	bk.SetFinalizers([]string{})
	if _, err := s.repo.UpdateBackup(c.Request.Context(), cli, ns, bk); err != nil {
		util.HandleK8sError(c, "failed to remove finalizers", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "xstore backup finalizers removed"})
}

func (s *BackupsService) RemoteInfo(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	bk, err := s.repo.GetBackup(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get xstore backup", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"namespace": ns, "name": name, "phase": bk.Status.Phase})
}
