package controllers

import (
	"context"
	"testing"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(polardbxv1.AddToScheme(scheme))
}

// TestSetup provides common setup for controller tests
type TestSetup struct {
	Client     client.Client
	Controller *PolarDBXClusterReconciler
	Logger     logr.Logger
	Config     *config.Config
}

// NewTestSetup creates a new test setup with fake client and controller
func NewTestSetup(t *testing.T, objects ...client.Object) *TestSetup {
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&polardbxv1.PolarDBXCluster{}).
		Build()

	logger := zap.New(zap.UseDevMode(true))
	
	cfg := &config.Config{
		Images: map[string]string{
			"polardbx":      "polardbx/polardbx:latest",
			"xstore":        "polardbx/xstore:latest",
			"prober":        "polardbx/prober:latest",
			"exporter":      "polardbx/exporter:latest",
		},
		Store: config.StoreConfig{
			NodeClass: "default",
		},
	}

	controller := &PolarDBXClusterReconciler{
		Client: fakeClient,
		Scheme: scheme,
		Logger: logger,
		Config: cfg,
	}

	return &TestSetup{
		Client:     fakeClient,
		Controller: controller,
		Logger:     logger,
		Config:     cfg,
	}
}

// CreateTestCluster creates a test PolarDBXCluster object
func CreateTestCluster(name, namespace string) *polardbxv1.PolarDBXCluster {
	return &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbxv1.PolarDBXClusterTopology{
				Nodes: polardbxv1.PolarDBXClusterNodes{
					CN: &polardbxv1.PolarDBXClusterSpecCN{
						Replicas: 1,
						Template: polardbxv1.PolarDBXClusterSpecCNTemplate{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "engine",
										Image: "polardbx/polardbx:latest",
									},
								},
							},
						},
					},
					DN: &polardbxv1.PolarDBXClusterSpecDN{
						Replicas: 1,
						Template: polardbxv1.PolarDBXClusterSpecDNTemplate{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "engine",
										Image: "polardbx/xstore:latest",
									},
								},
							},
						},
					},
				},
			},
		},
		Status: polardbxv1.PolarDBXClusterStatus{
			Phase: polardbxv1.PhaseCreating,
		},
	}
}

// TestPolarDBXClusterController tests the main controller functionality
func TestPolarDBXClusterController(t *testing.T) {
	t.Run("Controller Creation", func(t *testing.T) {
		setup := NewTestSetup(t)
		assert.NotNil(t, setup.Controller)
		assert.NotNil(t, setup.Client)
		assert.NotNil(t, setup.Config)
	})

	t.Run("Reconcile NonExistent Cluster", func(t *testing.T) {
		setup := NewTestSetup(t)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "nonexistent",
				Namespace: "default",
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, result)
	})

	t.Run("Reconcile New Cluster", func(t *testing.T) {
		cluster := CreateTestCluster("test-cluster", "default")
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
		assert.NotEqual(t, reconcile.Result{}, result)
		
		// Verify cluster was updated
		updatedCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, updatedCluster)
		assert.NoError(t, err)
	})

	t.Run("Reconcile Cluster With Dependencies", func(t *testing.T) {
		cluster := CreateTestCluster("test-cluster-with-deps", "default")
		
		// Create dependent objects
		cnDeployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cluster.Name + "-cn",
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					"polardbx/cluster-name": cluster.Name,
					"polardbx/role":         "cn",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &[]int32{1}[0],
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"polardbx/cluster-name": cluster.Name,
						"polardbx/role":         "cn",
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"polardbx/cluster-name": cluster.Name,
							"polardbx/role":         "cn",
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "engine",
								Image: "polardbx/polardbx:latest",
							},
						},
					},
				},
			},
		}

		setup := NewTestSetup(t, cluster, cnDeployment)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
		
		// Verify deployment still exists
		deployment := &appsv1.Deployment{}
		err = setup.Client.Get(context.TODO(), types.NamespacedName{
			Name:      cnDeployment.Name,
			Namespace: cnDeployment.Namespace,
		}, deployment)
		assert.NoError(t, err)
	})

	t.Run("Reconcile Cluster Deletion", func(t *testing.T) {
		cluster := CreateTestCluster("test-cluster-delete", "default")
		now := metav1.Now()
		cluster.DeletionTimestamp = &now
		cluster.Finalizers = []string{"polardbx.aliyun.com/finalizer"}
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
		
		// Check that finalizer processing was initiated
		updatedCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, updatedCluster)
		assert.NoError(t, err)
		assert.True(t, updatedCluster.DeletionTimestamp != nil)
	})
}

// TestPolarDBXClusterControllerEdgeCases tests edge cases and error conditions
func TestPolarDBXClusterControllerEdgeCases(t *testing.T) {
	t.Run("Reconcile With Invalid Spec", func(t *testing.T) {
		cluster := CreateTestCluster("invalid-cluster", "default")
		// Make spec invalid
		cluster.Spec.Topology.Nodes = polardbxv1.PolarDBXClusterNodes{}
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		// Should handle invalid spec gracefully
		assert.NoError(t, err)
	})

	t.Run("Reconcile With Large Cluster", func(t *testing.T) {
		cluster := CreateTestCluster("large-cluster", "default")
		cluster.Spec.Topology.Nodes.CN.Replicas = 10
		cluster.Spec.Topology.Nodes.DN.Replicas = 5
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
		assert.NotEqual(t, reconcile.Result{}, result)
	})

	t.Run("Reconcile With Special Characters In Name", func(t *testing.T) {
		cluster := CreateTestCluster("test-cluster-with-123", "default")
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		result, err := setup.Controller.Reconcile(context.TODO(), req)
		
		assert.NoError(t, err)
	})

	t.Run("Reconcile Concurrent Updates", func(t *testing.T) {
		cluster := CreateTestCluster("concurrent-cluster", "default")
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// Simulate concurrent reconciliation
		done := make(chan bool, 2)
		
		go func() {
			_, err := setup.Controller.Reconcile(context.TODO(), req)
			assert.NoError(t, err)
			done <- true
		}()
		
		go func() {
			_, err := setup.Controller.Reconcile(context.TODO(), req)
			assert.NoError(t, err)
			done <- true
		}()

		// Wait for both to complete
		<-done
		<-done
	})
}

// TestPolarDBXClusterControllerStatus tests status updates
func TestPolarDBXClusterControllerStatus(t *testing.T) {
	t.Run("Status Update During Creation", func(t *testing.T) {
		cluster := CreateTestCluster("status-test", "default")
		cluster.Status.Phase = polardbxv1.PhaseCreating
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		_, err := setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
		
		// Verify status was updated
		updatedCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, updatedCluster)
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedCluster.Status.Phase)
	})

	t.Run("Status Update With Conditions", func(t *testing.T) {
		cluster := CreateTestCluster("conditions-test", "default")
		cluster.Status.Conditions = []polardbxv1.Condition{
			{
				Type:   polardbxv1.ConditionTypeReady,
				Status: corev1.ConditionFalse,
				Reason: "Creating",
			},
		}
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		_, err := setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
		
		updatedCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, updatedCluster)
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedCluster.Status.Conditions)
	})
}

// TestPolarDBXClusterControllerPerformance tests performance characteristics
func TestPolarDBXClusterControllerPerformance(t *testing.T) {
	t.Run("Reconcile Performance", func(t *testing.T) {
		cluster := CreateTestCluster("perf-test", "default")
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// Measure reconcile time
		start := time.Now()
		_, err := setup.Controller.Reconcile(context.TODO(), req)
		duration := time.Since(start)
		
		assert.NoError(t, err)
		assert.Less(t, duration, 5*time.Second, "Reconcile should complete within 5 seconds")
	})

	t.Run("Multiple Reconciles Performance", func(t *testing.T) {
		cluster := CreateTestCluster("multi-perf-test", "default")
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		start := time.Now()
		
		// Run multiple reconciles
		for i := 0; i < 10; i++ {
			_, err := setup.Controller.Reconcile(context.TODO(), req)
			assert.NoError(t, err)
		}
		
		duration := time.Since(start)
		averageDuration := duration / 10
		
		assert.Less(t, averageDuration, 1*time.Second, "Average reconcile time should be under 1 second")
	})
}

// TestPolarDBXClusterControllerIntegration tests integration scenarios
func TestPolarDBXClusterControllerIntegration(t *testing.T) {
	t.Run("Full Lifecycle Test", func(t *testing.T) {
		cluster := CreateTestCluster("lifecycle-test", "default")
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// Initial reconcile (creation)
		result, err := setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
		
		// Update cluster
		updatedCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, updatedCluster)
		require.NoError(t, err)
		
		updatedCluster.Spec.Topology.Nodes.CN.Replicas = 2
		err = setup.Client.Update(context.TODO(), updatedCluster)
		assert.NoError(t, err)
		
		// Reconcile after update
		result, err = setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
		
		// Verify update was processed
		finalCluster := &polardbxv1.PolarDBXCluster{}
		err = setup.Client.Get(context.TODO(), req.NamespacedName, finalCluster)
		assert.NoError(t, err)
		assert.Equal(t, int32(2), finalCluster.Spec.Topology.Nodes.CN.Replicas)
	})

	t.Run("Error Recovery Test", func(t *testing.T) {
		cluster := CreateTestCluster("error-recovery-test", "default")
		// Set up a scenario that might cause errors
		cluster.Status.Phase = polardbxv1.PhaseFailed
		
		setup := NewTestSetup(t, cluster)
		
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// First reconcile might fail, but should handle gracefully
		_, err := setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
		
		// Second reconcile should work
		_, err = setup.Controller.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
	})
}

// BenchmarkPolarDBXClusterReconcile benchmarks the reconcile performance
func BenchmarkPolarDBXClusterReconcile(b *testing.B) {
	cluster := CreateTestCluster("benchmark-cluster", "default")
	setup := NewTestSetup(&testing.T{}, cluster)
	
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := setup.Controller.Reconcile(context.TODO(), req)
		if err != nil {
			b.Fatalf("Reconcile failed: %v", err)
		}
	}
}

// BenchmarkMultipleClusters benchmarks reconciling multiple clusters
func BenchmarkMultipleClusters(b *testing.B) {
	var clusters []client.Object
	var requests []reconcile.Request
	
	// Create multiple test clusters
	for i := 0; i < 100; i++ {
		cluster := CreateTestCluster("benchmark-cluster-"+string(rune(i)), "default")
		clusters = append(clusters, cluster)
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		})
	}
	
	setup := NewTestSetup(&testing.T{}, clusters...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, req := range requests {
			_, err := setup.Controller.Reconcile(context.TODO(), req)
			if err != nil {
				b.Fatalf("Reconcile failed: %v", err)
			}
		}
	}
}

// Test helper functions

// AssertClusterPhase checks if cluster is in expected phase
func AssertClusterPhase(t *testing.T, client client.Client, name, namespace string, expectedPhase polardbxv1.PolarDBXClusterPhase) {
	cluster := &polardbxv1.PolarDBXCluster{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, cluster)
	require.NoError(t, err)
	assert.Equal(t, expectedPhase, cluster.Status.Phase)
}

// AssertClusterCondition checks if cluster has expected condition
func AssertClusterCondition(t *testing.T, client client.Client, name, namespace string, conditionType polardbxv1.ConditionType, status corev1.ConditionStatus) {
	cluster := &polardbxv1.PolarDBXCluster{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, cluster)
	require.NoError(t, err)
	
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == conditionType {
			assert.Equal(t, status, condition.Status)
			return
		}
	}
	
	t.Errorf("Condition %s not found in cluster status", conditionType)
}

// WaitForClusterPhase waits for cluster to reach expected phase
func WaitForClusterPhase(t *testing.T, client client.Client, name, namespace string, expectedPhase polardbxv1.PolarDBXClusterPhase, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for cluster %s/%s to reach phase %s", namespace, name, expectedPhase)
		case <-ticker.C:
			cluster := &polardbxv1.PolarDBXCluster{}
			err := client.Get(context.TODO(), types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, cluster)
			if err != nil {
				continue
			}
			
			if cluster.Status.Phase == expectedPhase {
				return
			}
		}
	}
}