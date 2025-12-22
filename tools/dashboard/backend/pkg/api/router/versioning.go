package router

import "github.com/gin-gonic/gin"

// APIVersionInfo describes the API version and optional deprecation metadata.
type APIVersionInfo struct {
	Version     string
	Deprecated  bool
	EOLDate     string // End of Life date (optional)
	Alternative string
}

var apiVersions = map[string]APIVersionInfo{
	"v1": {Version: "v1"},
}

// VersionMiddleware adds version and deprecation headers to responses.
func VersionMiddleware(version string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("API-Version", version)
		if info, ok := apiVersions[version]; ok && info.Deprecated {
			c.Header("API-Deprecated", "true")
			if info.EOLDate != "" {
				c.Header("API-EOL-Date", info.EOLDate)
			}
			if info.Alternative != "" {
				c.Header("API-Alternative", info.Alternative)
			}
		}
		c.Next()
	}
}
