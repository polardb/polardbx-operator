package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

	domain_parameters "polardbx-dashboard-backend/pkg/api/domain/platform/parameters/handler"
)

func TestParameterTemplateEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Sample ParameterTemplate for testing
	sampleParameterTemplate := &polardbxv1.PolarDBXParameterTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-parameter-template",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXParameterTemplateSpec{
			Name: "test-template",
			NodeType: polardbxv1.TemplateNodeType{
				CN: polardbxv1.TemplateNode{
					Name: "cnTemplate",
					ParamList: []polardbxv1.TemplateParams{
						{
							Name:         "BACKGROUND_STATISTIC_COLLECTION_END_TIME",
							DefaultValue: "05:00",
							Mode:         "readwrite",
							Restart:      false,
							Unit:         "STRING",
							Optional:     "[00:00|01:00|02:00|...|23:00]",
						},
						{
							Name:         "CN_CPU_CORE",
							DefaultValue: "8",
							Mode:         "readonly",
							Restart:      true,
							Unit:         "INT",
							Optional:     "[1-128]",
						},
					},
				},
				DN: polardbxv1.TemplateNode{
					Name: "dnTemplate",
					ParamList: []polardbxv1.TemplateParams{
						{
							Name:         "auto_increment_increment",
							DefaultValue: "1",
							Mode:         "readwrite",
							Restart:      false,
							Unit:         "INT",
							Optional:     "[1-65535]",
						},
						{
							Name:         "innodb_buffer_pool_size",
							DefaultValue: "128M",
							Mode:         "readwrite",
							Restart:      true,
							Unit:         "STRING",
							Optional:     "",
						},
					},
				},
				GMS: &polardbxv1.TemplateNode{
					Name: "gmsTemplate",
					ParamList: []polardbxv1.TemplateParams{
						{
							Name:         "gms_memory_limit",
							DefaultValue: "4G",
							Mode:         "readwrite",
							Restart:      true,
							Unit:         "STRING",
							Optional:     "",
						},
					},
				},
			},
		},
	}

	// A client pre-populated with ParameterTemplate
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleParameterTemplate).Build()

	router := gin.New()
	getErr := func(resp map[string]interface{}) string {
		switch v := resp["error"].(type) {
		case string:
			return v
		case map[string]interface{}:
			if msg, ok := v["message"].(string); ok {
				return msg
			}
		}
		return ""
	}
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register ParameterTemplate routes
	router.GET("/parameter-templates", domain_parameters.ListTemplates)
	router.POST("/parameter-templates", domain_parameters.CreateTemplate)
	router.GET("/parameter-templates/:namespace/:name", domain_parameters.GetTemplate)
	router.PUT("/parameter-templates/:namespace/:name", domain_parameters.UpdateTemplate)
	router.DELETE("/parameter-templates/:namespace/:name", domain_parameters.DeleteTemplate)

	// --- Test ListParameterTemplates ---
	t.Run("ListParameterTemplates", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/parameter-templates?namespace=default", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var templates []polardbxv1.PolarDBXParameterTemplate
		err := json.Unmarshal(w.Body.Bytes(), &templates)
		assert.NoError(t, err)
		assert.Len(t, templates, 1)
		assert.Equal(t, "test-parameter-template", templates[0].Name)
		assert.Equal(t, "test-template", templates[0].Spec.Name)
		assert.Equal(t, "cnTemplate", templates[0].Spec.NodeType.CN.Name)
		assert.Len(t, templates[0].Spec.NodeType.CN.ParamList, 2)
		assert.Len(t, templates[0].Spec.NodeType.DN.ParamList, 2)
		assert.NotNil(t, templates[0].Spec.NodeType.GMS)
		assert.Len(t, templates[0].Spec.NodeType.GMS.ParamList, 1)
	})

	// --- Test CreateParameterTemplate ---
	t.Run("CreateParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		newParameterTemplate := &polardbxv1.PolarDBXParameterTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-parameter-template",
			},
			Spec: polardbxv1.PolarDBXParameterTemplateSpec{
				Name: "new-template",
				NodeType: polardbxv1.TemplateNodeType{
					CN: polardbxv1.TemplateNode{
						Name: "cnTemplate",
						ParamList: []polardbxv1.TemplateParams{
							{
								Name:         "ENABLE_HTAP",
								DefaultValue: "true",
								Mode:         "readwrite",
								Restart:      false,
								Unit:         "STRING",
								Optional:     "true|false",
							},
						},
					},
					DN: polardbxv1.TemplateNode{
						Name: "dnTemplate",
						ParamList: []polardbxv1.TemplateParams{
							{
								Name:         "max_connections",
								DefaultValue: "1000",
								Mode:         "readwrite",
								Restart:      false,
								Unit:         "INT",
								Optional:     "[1-100000]",
							},
						},
					},
				},
			},
		}
		body, _ := json.Marshal(newParameterTemplate)
		req, _ := http.NewRequest(http.MethodPost, "/parameter-templates?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify it was created
		var createdTemplate polardbxv1.PolarDBXParameterTemplate
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "new-parameter-template", Namespace: "default"}, &createdTemplate)
		assert.NoError(t, err)
		assert.Equal(t, "new-parameter-template", createdTemplate.Name)
		assert.Equal(t, "new-template", createdTemplate.Spec.Name)
		assert.Equal(t, "ENABLE_HTAP", createdTemplate.Spec.NodeType.CN.ParamList[0].Name)
		assert.Equal(t, "true", createdTemplate.Spec.NodeType.CN.ParamList[0].DefaultValue)
		assert.Equal(t, "max_connections", createdTemplate.Spec.NodeType.DN.ParamList[0].Name)
		assert.Equal(t, "1000", createdTemplate.Spec.NodeType.DN.ParamList[0].DefaultValue)
	})

	// --- Test GetParameterTemplate ---
	t.Run("GetParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/parameter-templates/default/test-parameter-template", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var template polardbxv1.PolarDBXParameterTemplate
		err := json.Unmarshal(w.Body.Bytes(), &template)
		assert.NoError(t, err)
		assert.Equal(t, "test-parameter-template", template.Name)
		assert.Equal(t, "test-template", template.Spec.Name)

		// Verify CN parameters
		cnParams := template.Spec.NodeType.CN.ParamList
		assert.Len(t, cnParams, 2)
		assert.Equal(t, "BACKGROUND_STATISTIC_COLLECTION_END_TIME", cnParams[0].Name)
		assert.Equal(t, "05:00", cnParams[0].DefaultValue)
		assert.Equal(t, "readwrite", cnParams[0].Mode)
		assert.False(t, cnParams[0].Restart)
		assert.Equal(t, "STRING", cnParams[0].Unit)

		// Verify DN parameters
		dnParams := template.Spec.NodeType.DN.ParamList
		assert.Len(t, dnParams, 2)
		assert.Equal(t, "auto_increment_increment", dnParams[0].Name)
		assert.Equal(t, "1", dnParams[0].DefaultValue)
		assert.Equal(t, "readwrite", dnParams[0].Mode)
		assert.False(t, dnParams[0].Restart)
		assert.Equal(t, "INT", dnParams[0].Unit)

		// Verify GMS parameters
		assert.NotNil(t, template.Spec.NodeType.GMS)
		gmsParams := template.Spec.NodeType.GMS.ParamList
		assert.Len(t, gmsParams, 1)
		assert.Equal(t, "gms_memory_limit", gmsParams[0].Name)
		assert.Equal(t, "4G", gmsParams[0].DefaultValue)
	})

	// --- Test UpdateParameterTemplate ---
	t.Run("UpdateParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		updatedTemplate := sampleParameterTemplate.DeepCopy()
		updatedTemplate.Spec.Name = "updated-template"
		// Update CN parameter
		updatedTemplate.Spec.NodeType.CN.ParamList[0].DefaultValue = "06:00"
		// Update DN parameter
		updatedTemplate.Spec.NodeType.DN.ParamList[0].DefaultValue = "2"
		body, _ := json.Marshal(updatedTemplate)
		req, _ := http.NewRequest(http.MethodPut, "/parameter-templates/default/test-parameter-template", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify it was updated
		var templateInClient polardbxv1.PolarDBXParameterTemplate
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "test-parameter-template", Namespace: "default"}, &templateInClient)
		assert.NoError(t, err)
		assert.Equal(t, "updated-template", templateInClient.Spec.Name)
		assert.Equal(t, "06:00", templateInClient.Spec.NodeType.CN.ParamList[0].DefaultValue)
		assert.Equal(t, "2", templateInClient.Spec.NodeType.DN.ParamList[0].DefaultValue)
	})

	// --- Test DeleteParameterTemplate ---
	t.Run("DeleteParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/parameter-templates/default/test-parameter-template", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify deletion response
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "parameter template deleted", response["message"])
	})

	// --- Test CreateParameterTemplate with invalid JSON ---
	t.Run("CreateParameterTemplateInvalidJSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/parameter-templates?namespace=default", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, strings.ToLower(getErr(response)), "invalid request format")
	})

	// --- Test GetParameterTemplate for non-existent template ---
	t.Run("GetNonExistentParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/parameter-templates/default/non-existent-template", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, getErr(response), "not found")
	})

	// --- Test DeleteParameterTemplate for non-existent template ---
	t.Run("DeleteNonExistentParameterTemplate", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/parameter-templates/default/non-existent-template", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, getErr(response), "not found")
	})
}

func TestParameterTemplateBusinessLogic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Test different parameter units
	t.Run("TestDifferentParameterUnits", func(t *testing.T) {
		testCases := []struct {
			name     string
			unit     string
			expected bool
		}{
			{"String Parameter", "STRING", true},
			{"Integer Parameter", "INT", true},
			{"Double Parameter", "DOUBLE", true},
			{"Timezone Parameter", "TZ", true},
			{"Hour Range Parameter", "HOUR_RANGE", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/parameter-templates", domain_parameters.CreateTemplate)

				template := &polardbxv1.PolarDBXParameterTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-" + tc.unit,
					},
					Spec: polardbxv1.PolarDBXParameterTemplateSpec{
						Name: "test-template",
						NodeType: polardbxv1.TemplateNodeType{
							CN: polardbxv1.TemplateNode{
								Name: "cnTemplate",
								ParamList: []polardbxv1.TemplateParams{
									{
										Name:         "test_param",
										DefaultValue: "test_value",
										Mode:         "readwrite",
										Unit:         tc.unit,
									},
								},
							},
						},
					},
				}

				body, _ := json.Marshal(template)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/parameter-templates?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				if tc.expected {
					assert.Equal(t, http.StatusCreated, w.Code)
				}
			})
		}
	})

	// Test different parameter modes
	t.Run("TestParameterModes", func(t *testing.T) {
		testCases := []struct {
			name     string
			mode     string
			restart  bool
			expected bool
		}{
			{"ReadOnly Parameter", "readonly", false, true},
			{"ReadWrite Parameter", "readwrite", false, true},
			{"ReadWrite with Restart", "readwrite", true, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/parameter-templates", domain_parameters.CreateTemplate)

				template := &polardbxv1.PolarDBXParameterTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-mode-" + tc.mode,
					},
					Spec: polardbxv1.PolarDBXParameterTemplateSpec{
						Name: "test-template",
						NodeType: polardbxv1.TemplateNodeType{
							DN: polardbxv1.TemplateNode{
								Name: "dnTemplate",
								ParamList: []polardbxv1.TemplateParams{
									{
										Name:         "test_param",
										DefaultValue: "test_value",
										Mode:         tc.mode,
										Restart:      tc.restart,
										Unit:         "STRING",
									},
								},
							},
						},
					},
				}

				body, _ := json.Marshal(template)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/parameter-templates?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				assert.Equal(t, http.StatusCreated, w.Code)
			})
		}
	})

	// Test template with all node types
	t.Run("TestCompleteNodeTypes", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		router := gin.New()
		router.Use(func(c *gin.Context) {
			c.Set("k8sClient", fakeClient)
			c.Next()
		})
		router.POST("/parameter-templates", domain_parameters.CreateTemplate)

		template := &polardbxv1.PolarDBXParameterTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name: "complete-template",
			},
			Spec: polardbxv1.PolarDBXParameterTemplateSpec{
				Name: "complete-template",
				NodeType: polardbxv1.TemplateNodeType{
					CN: polardbxv1.TemplateNode{
						Name: "cnTemplate",
						ParamList: []polardbxv1.TemplateParams{
							{
								Name:         "CN_PARAM",
								DefaultValue: "cn_value",
								Mode:         "readwrite",
								Unit:         "STRING",
							},
						},
					},
					DN: polardbxv1.TemplateNode{
						Name: "dnTemplate",
						ParamList: []polardbxv1.TemplateParams{
							{
								Name:         "DN_PARAM",
								DefaultValue: "dn_value",
								Mode:         "readwrite",
								Unit:         "INT",
							},
						},
					},
					GMS: &polardbxv1.TemplateNode{
						Name: "gmsTemplate",
						ParamList: []polardbxv1.TemplateParams{
							{
								Name:         "GMS_PARAM",
								DefaultValue: "gms_value",
								Mode:         "readonly",
								Unit:         "STRING",
							},
						},
					},
				},
			},
		}

		body, _ := json.Marshal(template)
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/parameter-templates?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify the created template
		var createdTemplate polardbxv1.PolarDBXParameterTemplate
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "complete-template", Namespace: "default"}, &createdTemplate)
		assert.NoError(t, err)
		assert.Equal(t, "CN_PARAM", createdTemplate.Spec.NodeType.CN.ParamList[0].Name)
		assert.Equal(t, "DN_PARAM", createdTemplate.Spec.NodeType.DN.ParamList[0].Name)
		assert.NotNil(t, createdTemplate.Spec.NodeType.GMS)
		assert.Equal(t, "GMS_PARAM", createdTemplate.Spec.NodeType.GMS.ParamList[0].Name)
	})
}
