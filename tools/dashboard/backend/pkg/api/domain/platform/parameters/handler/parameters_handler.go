// Package handler provides HTTP handlers for parameters and parameter templates

// Follows Clean Architecture design pattern
package handler

import (
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// ======================== Parameters ========================

// List gets parameter list
// @Summary List parameters
// @Description Lists all PolarDB-X parameters in the specified namespace
// @Tags parameters
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Success 200 {array} map[string]any "List of parameters"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameters [get]
func List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListPolarDBXParametersWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// Get gets specified parameter
// @Summary Get parameter
// @Description Retrieves details of a specific PolarDB-X parameter
// @Tags parameters
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param name path string true "Name of the parameter"
// @Success 200 {object} map[string]any "Parameter details"
// @Failure 404 {object} apierr.ErrorResponse "Parameter not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameters/{name} [get]
func Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	item, err := k8s.GetPolarDBXParameterWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// Create creates parameter
// @Summary Create parameter
// @Description Creates a new PolarDB-X parameter
// @Tags parameters
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param body body map[string]any true "Parameter specification"
// @Success 201 {object} map[string]any "Created parameter"
// @Failure 400 {object} apierr.ErrorResponse "Invalid parameter specification"
// @Failure 409 {object} apierr.ErrorResponse "Parameter already exists"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameters [post]
func Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameter
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	created, err := k8s.CreatePolarDBXParameterWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// Update updates parameter
// @Summary Update parameter
// @Description Updates an existing PolarDB-X parameter
// @Tags parameters
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param name path string true "Name of the parameter"
// @Param body body map[string]any true "Updated parameter specification"
// @Success 200 {object} map[string]any "Updated parameter"
// @Failure 400 {object} apierr.ErrorResponse "Invalid parameter specification"
// @Failure 404 {object} apierr.ErrorResponse "Parameter not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameters/{name} [put]
func Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameter
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	updated, err := k8s.UpdatePolarDBXParameterWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// Delete deletes parameter
// @Summary Delete parameter
// @Description Deletes a PolarDB-X parameter
// @Tags parameters
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param name path string true "Name of the parameter"
// @Success 200 {object} map[string]any "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Parameter not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameters/{name} [delete]
func Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXParameterWithContext(c.Request.Context(), cli, ns, name); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "parameter deleted"})
}

// ======================== Parameter Templates ========================

// ListTemplates gets parameter template list
// @Summary List parameter templates
// @Description Lists all PolarDB-X parameter templates in the specified namespace
// @Tags parameter-templates
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Success 200 {array} map[string]any "List of parameter templates"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameter-templates [get]
func ListTemplates(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListPolarDBXParameterTemplatesWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// GetTemplate gets specified parameter template
// @Summary Get parameter template
// @Description Retrieves details of a specific PolarDB-X parameter template
// @Tags parameter-templates
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the parameter template"
// @Success 200 {object} map[string]any "Parameter template details"
// @Failure 404 {object} apierr.ErrorResponse "Parameter template not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameter-templates/{namespace}/{name} [get]
func GetTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	item, err := k8s.GetPolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// CreateTemplate creates parameter template
// @Summary Create parameter template
// @Description Creates a new PolarDB-X parameter template
// @Tags parameter-templates
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param body body map[string]any true "Parameter template specification"
// @Success 201 {object} map[string]any "Created parameter template"
// @Failure 400 {object} apierr.ErrorResponse "Invalid parameter template specification"
// @Failure 409 {object} apierr.ErrorResponse "Parameter template already exists"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameter-templates [post]
func CreateTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameterTemplate
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	created, err := k8s.CreatePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// UpdateTemplate updates parameter template
// @Summary Update parameter template
// @Description Updates an existing PolarDB-X parameter template
// @Tags parameter-templates
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the parameter template"
// @Param body body map[string]any true "Updated parameter template specification"
// @Success 200 {object} map[string]any "Updated parameter template"
// @Failure 400 {object} apierr.ErrorResponse "Invalid parameter template specification"
// @Failure 404 {object} apierr.ErrorResponse "Parameter template not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameter-templates/{namespace}/{name} [put]
func UpdateTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXParameterTemplate
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	updated, err := k8s.UpdatePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteTemplate deletes parameter template
// @Summary Delete parameter template
// @Description Deletes a PolarDB-X parameter template
// @Tags parameter-templates
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the parameter template"
// @Success 200 {object} map[string]any "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Parameter template not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/parameter-templates/{namespace}/{name} [delete]
func DeleteTemplate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXParameterTemplateWithContext(c.Request.Context(), cli, ns, name); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "parameter template deleted"})
}
