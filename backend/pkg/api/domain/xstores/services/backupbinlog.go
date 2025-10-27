package services

import (
	"net/http"

	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
)

// BackupBinlogService 封装 XStore 标准版增量日志备份（XStoreBackupBinlog）的最小 CRUD
type BackupBinlogService struct{}

func NewBackupBinlogService() *BackupBinlogService { return &BackupBinlogService{} }

func (s *BackupBinlogService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "")
	var list v1.XStoreBackupBinlogList
	opts := []client.ListOption{}
	if ns != "" {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := cli.List(c.Request.Context(), &list, opts...); err != nil {
		util.HandleK8sError(c, "failed to list xstore backup binlogs", err)
		return
	}
	c.JSON(http.StatusOK, list.Items)
}

func (s *BackupBinlogService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "default")
	var obj v1.XStoreBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore backup binlog", "details": err.Error()})
		return
	}
	if obj.Namespace == "" {
		obj.Namespace = ns
	}
	if err := cli.Create(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to create xstore backup binlog", err)
		return
	}
	c.JSON(http.StatusCreated, &obj)
}

func (s *BackupBinlogService) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var obj v1.XStoreBackupBinlog
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &obj); err != nil {
		util.HandleK8sError(c, "failed to get xstore backup binlog", err)
		return
	}
	c.JSON(http.StatusOK, &obj)
}

func (s *BackupBinlogService) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var obj v1.XStoreBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore backup binlog", "details": err.Error()})
		return
	}
	obj.Namespace = ns
	if err := cli.Update(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to update xstore backup binlog", err)
		return
	}
	c.JSON(http.StatusOK, &obj)
}

func (s *BackupBinlogService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var obj v1.XStoreBackupBinlog
	obj.Namespace = ns
	obj.Name = name
	if err := cli.Delete(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to delete xstore backup binlog", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "xstore backup binlog deleted"})
}
