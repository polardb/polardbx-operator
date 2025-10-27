package services

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"net/http"

	"polardbx-ui-backend/pkg/api/util"
)

// 由于 backupbinlog 的具体 CRUD 在现有模块实现中，这里先以直连 K8sClient 的最小实现承接，
// 后续可以按需抽到 pkg/k8s（若存在便捷函数）。

type BackupBinlogService struct{}

func NewBackupBinlogService() *BackupBinlogService { return &BackupBinlogService{} }

func (s *BackupBinlogService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "")
	var list polardbxv1.PolarDBXBackupBinlogList
	opts := []client.ListOption{}
	if ns != "" {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := cli.List(c.Request.Context(), &list, opts...); err != nil {
		util.HandleK8sError(c, "failed to list backup binlogs", err)
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
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup binlog", "details": err.Error()})
		return
	}
	if obj.Namespace == "" {
		obj.Namespace = ns
	}
	if err := cli.Create(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to create backup binlog", err)
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
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &obj); err != nil {
		util.HandleK8sError(c, "failed to get backup binlog", err)
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
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup binlog", "details": err.Error()})
		return
	}
	obj.Namespace = ns
	if err := cli.Update(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to update backup binlog", err)
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
	var obj polardbxv1.PolarDBXBackupBinlog
	obj.Namespace = ns
	obj.Name = name
	if err := cli.Delete(c.Request.Context(), &obj); err != nil {
		util.HandleK8sError(c, "failed to delete backup binlog", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "backup binlog deleted"})
}
