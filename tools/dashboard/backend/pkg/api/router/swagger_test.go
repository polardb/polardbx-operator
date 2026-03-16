package router

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSwaggerSpec_IncludesGeneratedAndHealthPaths(t *testing.T) {
	spec, err := getSwaggerSpec()
	assert.NoError(t, err)
	assert.NotEmpty(t, spec)

	var doc map[string]any
	err = json.Unmarshal(spec, &doc)
	assert.NoError(t, err)

	openapi, _ := doc["openapi"].(string)
	assert.NotEmpty(t, openapi)

	paths, ok := doc["paths"].(map[string]any)
	assert.True(t, ok)

	_, ok = paths["/api/v1/diagnostics/reports"]
	assert.True(t, ok, "expected swaggo-generated paths to be present")

	_, ok = paths["/health"]
	assert.True(t, ok, "expected health endpoints to be present")

	components, ok := doc["components"].(map[string]any)
	assert.True(t, ok)
	securitySchemes, ok := components["securitySchemes"].(map[string]any)
	assert.True(t, ok)

	kubeconfigAuth, ok := securitySchemes["KubeconfigAuth"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "apiKey", kubeconfigAuth["type"])
	assert.Equal(t, "header", kubeconfigAuth["in"])
	assert.Equal(t, "X-Kubeconfig-B64", kubeconfigAuth["name"])
}
