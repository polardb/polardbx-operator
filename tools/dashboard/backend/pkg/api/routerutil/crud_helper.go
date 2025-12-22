package routerutil

import "github.com/gin-gonic/gin"

// CRUDHandlers defines handlers for basic CRUD routes.
type CRUDHandlers struct {
	List   gin.HandlerFunc
	Create gin.HandlerFunc
	Get    gin.HandlerFunc
	Update gin.HandlerFunc
	Delete gin.HandlerFunc
}

// LGDHandlers defines handlers for list/get/delete style resources.
// Create/Update are intentionally absent for resources that are read-only
// or managed by other workflows.
type LGDHandlers struct {
	List   gin.HandlerFunc
	Get    gin.HandlerFunc
	Delete gin.HandlerFunc
}

// RegisterCRUD registers common CRUD routes on the given group using the default
// item pattern (base + "/:name").
func RegisterCRUD(group *gin.RouterGroup, base string, h CRUDHandlers) {
	RegisterCRUDWithItemPattern(group, base, base+"/:name", h)
}

// RegisterCRUDWithItemPattern registers common CRUD routes with a custom item path
// pattern (e.g. base+"/:namespace/:name") to support namespaced items.
func RegisterCRUDWithItemPattern(group *gin.RouterGroup, base, itemPattern string, h CRUDHandlers) {
	group.GET(base, h.List)
	group.POST(base, h.Create)
	group.GET(itemPattern, h.Get)
	group.PUT(itemPattern, h.Update)
	group.DELETE(itemPattern, h.Delete)
}

// RegisterLGD registers List/Get/Delete routes with a custom item path.
func RegisterLGD(group *gin.RouterGroup, base, itemPattern string, h LGDHandlers) {
	group.GET(base, h.List)
	group.GET(itemPattern, h.Get)
	group.DELETE(itemPattern, h.Delete)
}
