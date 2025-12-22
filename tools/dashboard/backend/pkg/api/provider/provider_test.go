package provider

import (
	"net/http/httptest"
	"testing"

	"os"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

type mockProvider struct {
	tag string
	Provider
}

func TestInjectAndFromContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	p := &mockProvider{tag: "injected"}
	mw := Inject(p)
	mw(c)

	got, ok := FromContext(c)
	assert.True(t, ok)
	assert.Equal(t, p, got)
}

func TestMustFallbackAndCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	p := Must(c)
	// should provide defaultProvider when none was injected
	_, isDefault := p.(*defaultProvider)
	assert.True(t, isDefault)

	// should be cached back to context
	cached, ok := FromContext(c)
	assert.True(t, ok)
	assert.Equal(t, p, cached)
}

func TestPodServiceWithoutClients(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	dp := &defaultProvider{}
	svc, ok := dp.PodService(c)
	assert.False(t, ok)
	assert.Nil(t, svc)
}

func TestMustPanicWhenFallbackDisabled(t *testing.T) {
	origMode := gin.Mode()
	gin.SetMode(gin.ReleaseMode)
	defer gin.SetMode(origMode)
	_ = os.Unsetenv("PROVIDER_FALLBACK_ENABLED")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	assert.Panics(t, func() { Must(c) })
}

func TestMustFallbackWhenEnvEnabled(t *testing.T) {
	origMode := gin.Mode()
	gin.SetMode(gin.ReleaseMode)
	defer gin.SetMode(origMode)
	t.Setenv("PROVIDER_FALLBACK_ENABLED", "true")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	p := Must(c)
	assert.NotNil(t, p)
}
