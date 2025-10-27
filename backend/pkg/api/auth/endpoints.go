package auth

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

func getJWTSecret() string {
	return strings.TrimSpace(os.Getenv("JWT_SECRET"))
}

// Login issues a JWT when JWT_SECRET is configured. Otherwise returns 503 (disabled).
func Login(c *gin.Context) {
	secret := getJWTSecret()
	if secret == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "jwt not enabled"})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	adminUser := strings.TrimSpace(os.Getenv("ADMIN_USER"))
	if adminUser == "" {
		adminUser = "admin"
	}
	adminPass := strings.TrimSpace(os.Getenv("ADMIN_PASSWORD"))
	if adminPass == "" {
		adminPass = "admin"
	}

	role := "viewer"
	if body.Username == adminUser && body.Password == adminPass {
		role = "admin"
	} else {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	now := time.Now()
	exp := now.Add(12 * time.Hour)
	claims := Claims{
		Username: body.Username,
		Role:     role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sign token"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"token": signed, "expiresAt": exp.UTC().Format(time.RFC3339), "role": role})
}

// Me returns JWT claims if present/valid. If JWT disabled, returns anonymous.
func Me(c *gin.Context) {
	secret := getJWTSecret()
	if secret == "" {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "role": "anonymous"})
		return
	}
	if v, ok := c.Get("jwtClaims"); ok {
		if cl, ok2 := v.(*Claims); ok2 {
			c.JSON(http.StatusOK, gin.H{"enabled": true, "username": cl.Username, "role": cl.Role, "exp": cl.ExpiresAt.Time})
			return
		}
	}
	c.JSON(http.StatusUnauthorized, gin.H{"enabled": true, "error": "unauthorized"})
}

// JWTAuthMiddleware enforces JWT when JWT_SECRET is set. Minimal RBAC: non-GET requires role=admin.
func JWTAuthMiddleware() gin.HandlerFunc {
	secret := getJWTSecret()
	if secret == "" {
		// disabled; pass-through
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		// allow auth endpoints and connect without token
		if strings.HasPrefix(c.Request.URL.Path, "/api/v1/auth/") || c.Request.URL.Path == "/api/v1/connect" {
			c.Next()
			return
		}

		authz := c.GetHeader("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			c.Abort()
			return
		}
		raw := strings.TrimSpace(strings.TrimPrefix(authz, "Bearer "))
		parsed, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (interface{}, error) { return []byte(secret), nil })
		if err != nil || !parsed.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			c.Abort()
			return
		}
		claims, ok := parsed.Claims.(*Claims)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token claims"})
			c.Abort()
			return
		}
		// minimal RBAC: write operations require admin
		if c.Request.Method != http.MethodGet && strings.ToUpper(c.Request.Method) != http.MethodHead && claims.Role != "admin" {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden: admin required"})
			c.Abort()
			return
		}
		c.Set("jwtClaims", claims)
		c.Next()
	}
}
