package router

import "github.com/gin-gonic/gin"

// RouteGroup is a logical group of routes that share a prefix and middleware.
// Prefix is relative to the gin group passed to Apply (e.g. "/api/v1").
type RouteGroup struct {
	Prefix      string
	Description string
	Routes      []Route
	Middleware  []gin.HandlerFunc
}

// Route describes an HTTP route and its handler.
type Route struct {
	Method          string
	Path            string
	Handler         gin.HandlerFunc
	Description     string
	Deprecated      bool
	DeprecatedSince string
	Alternative     string
}

// RouteRegistry stores route metadata and can apply it to a Gin router group.
// This provides a single registry for route wiring and for route-level metadata.
type RouteRegistry struct {
	groups []RouteGroup
}

func NewRouteRegistry() *RouteRegistry {
	return &RouteRegistry{groups: make([]RouteGroup, 0)}
}

func (r *RouteRegistry) RegisterGroup(group RouteGroup) {
	r.groups = append(r.groups, group)
}

func (r *RouteRegistry) Groups() []RouteGroup {
	out := make([]RouteGroup, 0, len(r.groups))
	out = append(out, r.groups...)
	return out
}

func (r *RouteRegistry) Apply(group *gin.RouterGroup) {
	for _, routeGroup := range r.groups {
		g := group.Group(routeGroup.Prefix)
		for _, mw := range routeGroup.Middleware {
			g.Use(mw)
		}
		for _, route := range routeGroup.Routes {
			h := route.Handler
			if route.Deprecated {
				h = wrapDeprecated(route, h)
			}
			switch route.Method {
			case "GET":
				g.GET(route.Path, h)
			case "POST":
				g.POST(route.Path, h)
			case "PUT":
				g.PUT(route.Path, h)
			case "PATCH":
				g.PATCH(route.Path, h)
			case "DELETE":
				g.DELETE(route.Path, h)
			}
		}
	}
}

func wrapDeprecated(route Route, next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("API-Deprecated", "true")
		if route.DeprecatedSince != "" {
			c.Header("API-Deprecated-Since", route.DeprecatedSince)
		}
		if route.Alternative != "" {
			c.Header("API-Alternative", route.Alternative)
		}
		next(c)
	}
}

