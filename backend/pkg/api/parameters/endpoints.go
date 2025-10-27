package parameters

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// ----- Parameters -----
func List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListPolarDBXParametersWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list parameters", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	item, err := k8s.GetPolarDBXParameterWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get parameter", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameter
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parameter", "details": err.Error()})
		return
	}
	created, err := k8s.CreatePolarDBXParameterWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to create parameter", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameter
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parameter", "details": err.Error()})
		return
	}
	updated, err := k8s.UpdatePolarDBXParameterWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update parameter", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXParameterWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete parameter", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "parameter deleted"})
}

// ----- Parameter Templates -----
func ListTemplates(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListPolarDBXParameterTemplatesWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list parameter templates", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func GetTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	item, err := k8s.GetPolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get parameter template", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func CreateTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameterTemplate
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parameter template", "details": err.Error()})
		return
	}
	created, err := k8s.CreatePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to create parameter template", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func UpdateTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameterTemplate
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parameter template", "details": err.Error()})
		return
	}
	updated, err := k8s.UpdatePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update parameter template", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func DeleteTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete parameter template", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "parameter template deleted"})
}
