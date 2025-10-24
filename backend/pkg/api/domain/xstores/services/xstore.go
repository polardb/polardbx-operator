package services

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/domain/xstores/k8srepo"
	"polardbx-ui-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// XStoreService 封装 XStore 基础 CRUD 与 Pod 列表
type XStoreService struct {
	repo k8srepo.XStoreRepository
}

func NewXStoreService() *XStoreService { return &XStoreService{repo: k8srepo.NewXStoreRepository()} }

func (s *XStoreService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := s.repo.List(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list xstores", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *XStoreService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.XStore
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore", "details": err.Error()})
		return
	}
	created, err := s.repo.Create(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to create xstore", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (s *XStoreService) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := s.repo.Get(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get xstore", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *XStoreService) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body polardbxv1.XStore
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid xstore", "details": err.Error()})
		return
	}
	body.Namespace = ns
	updated, err := s.repo.Update(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update xstore", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func (s *XStoreService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := s.repo.Delete(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete xstore", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "xstore deleted"})
}

func (s *XStoreService) ListPods(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	items, err := s.repo.ListPods(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to list pods for xstore", err)
		return
	}
	c.JSON(http.StatusOK, items)
}
