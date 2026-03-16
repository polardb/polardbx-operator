package handler

import (
	"net/http"
	"os"
	"strings"
	"time"

	apierr "polardbx-dashboard-backend/pkg/api/errors"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
)

// Claims represents JWT claims used by the auth endpoints.
// It includes username, role, and standard registered claims.
type Claims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

func getJWTSecret() string {
	return strings.TrimSpace(os.Getenv("JWT_SECRET"))
}

// Login authenticates the user and issues a JWT when JWT_SECRET is configured.
// @Summary Login and issue JWT
// @Description Authenticate using configured admin credentials and issue a signed JWT token.
// @Tags auth
// @Accept json
// @Produce json
// @Param body body map[string]any true "Login request (expects fields: username, password)"
// @Success 200 {object} map[string]any "JWT token and metadata"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request payload"
// @Failure 401 {object} apierr.ErrorResponse "Invalid credentials"
// @Failure 503 {object} apierr.ErrorResponse "JWT not enabled (JWT_SECRET not configured)"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func Login(c *gin.Context) {
	secret := getJWTSecret()
	if secret == "" {
		apierr.Abort(c, apierr.ServiceUnavailable("jwt not enabled", 0))
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Username == "" {
		if err != nil {
			apierr.AbortWithError(c, err)
			return
		}
		apierr.AbortWithError(c, apierr.ValidationError("invalid payload", nil))
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
		apierr.AbortUnauthorized(c, "invalid credentials")
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
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to sign token", err))
		return
	}
	apierr.OK(c, gin.H{"token": signed, "expiresAt": exp.UTC().Format(time.RFC3339), "role": role})
}

// Me returns current JWT claims and auth status for the caller.
// @Summary Get current auth info
// @Description Return current authentication status, username, role, and token expiration (if JWT is enabled).
// @Tags auth
// @Produce json
// @Success 200 {object} map[string]any "Auth status and claims"
// @Failure 401 {object} apierr.ErrorResponse "Unauthorized or invalid token"
func Me(c *gin.Context) {
	secret := getJWTSecret()
	if secret == "" {
		apierr.OK(c, gin.H{"enabled": false, "role": "anonymous"})
		return
	}
	if v, ok := c.Get("jwtClaims"); ok {
		if cl, ok2 := v.(*Claims); ok2 {
			apierr.OK(c, gin.H{"enabled": true, "username": cl.Username, "role": cl.Role, "exp": cl.ExpiresAt.Time})
			return
		}
	}
	apierr.AbortUnauthorized(c, "unauthorized")
}

// JWTAuthMiddleware JWT authentication middleware
func JWTAuthMiddleware() gin.HandlerFunc {
	secret := getJWTSecret()
	if secret == "" {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/v1/auth/") || c.Request.URL.Path == "/api/v1/connect" {
			c.Next()
			return
		}

		authz := c.GetHeader("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			apierr.AbortUnauthorized(c, "missing bearer token")
			return
		}
		raw := strings.TrimSpace(strings.TrimPrefix(authz, "Bearer "))
		parsed, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (interface{}, error) { return []byte(secret), nil })
		if err != nil || !parsed.Valid {
			apierr.AbortUnauthorized(c, "invalid token")
			return
		}
		claims, ok := parsed.Claims.(*Claims)
		if !ok {
			apierr.AbortUnauthorized(c, "invalid token claims")
			return
		}
		if c.Request.Method != http.MethodGet && strings.ToUpper(c.Request.Method) != http.MethodHead && claims.Role != "admin" {
			apierr.AbortForbidden(c, "forbidden: admin required")
			return
		}
		c.Set("jwtClaims", claims)
		c.Next()
	}
}
