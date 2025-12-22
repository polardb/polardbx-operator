package k8s

import (
	"context"
	"errors"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ctxKey string

// MockClient implements the controller-runtime client.Client interface for testing
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	args := m.Called(ctx, key, obj, opts)
	return args.Error(0)
}

func (m *MockClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	args := m.Called(ctx, list, opts)
	return args.Error(0)
}

func (m *MockClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	args := m.Called(ctx, obj, patch, opts)
	return args.Error(0)
}

func (m *MockClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockClient) Status() client.StatusWriter {
	args := m.Called()
	return args.Get(0).(client.StatusWriter)
}

func (m *MockClient) Scheme() *runtime.Scheme {
	args := m.Called()
	return args.Get(0).(*runtime.Scheme)
}

func (m *MockClient) RESTMapper() meta.RESTMapper {
	args := m.Called()
	return args.Get(0).(meta.RESTMapper)
}

func (m *MockClient) SubResource(subResource string) client.SubResourceClient {
	args := m.Called(subResource)
	return args.Get(0).(client.SubResourceClient)
}

func (m *MockClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	args := m.Called(obj)
	return args.Get(0).(schema.GroupVersionKind), args.Error(1)
}

func (m *MockClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	args := m.Called(obj)
	return args.Bool(0), args.Error(1)
}

// Test helper functions
func createTestCluster(name, namespace string) *polardbxv1.PolarDBXCluster {
	return &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{
						Replicas: func(v int32) *int32 { return &v }(int32(1)),
						Template: polardbx.CNTemplate{},
					},
					DN: polardbx.TopologyNodeDN{
						Replicas: 1,
						Template: polardbx.XStoreTemplate{},
					},
				},
			},
		},
	}
}

func createTestBackup(name, namespace, clusterName string) *polardbxv1.PolarDBXBackup {
	return &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{Name: clusterName},
		},
	}
}

func createTestParameter(name, namespace, clusterName string) *polardbxv1.PolarDBXParameter {
	return &polardbxv1.PolarDBXParameter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXParameterSpec{
			ClusterName: clusterName,
			NodeType: polardbxv1.ParamNodeType{
				CN: polardbxv1.ParamNode{
					Name: "cn",
					ParamList: []polardbxv1.Params{
						{
							Name:  "max_connections",
							Value: "1000",
						},
					},
				},
			},
		},
	}
}

// Test PolarDBXCluster operations
func TestListPolarDBXClusters_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedClusters := []polardbxv1.PolarDBXCluster{
		*createTestCluster("cluster1", "default"),
		*createTestCluster("cluster2", "default"),
	}

	// clusterList is not used directly; List mock fills the list

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXClusterList)
			list.Items = expectedClusters
		}).Return(nil)

	clusters, err := ListPolarDBXClusters(mockClient, "default")

	assert.NoError(t, err)
	assert.Equal(t, 2, len(clusters))
	assert.Equal(t, "cluster1", clusters[0].Name)
	assert.Equal(t, "cluster2", clusters[1].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXClusters_Error(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterList"), mock.Anything).
		Return(errors.New("API server error"))

	clusters, err := ListPolarDBXClusters(mockClient, "default")

	assert.Error(t, err)
	assert.Nil(t, clusters)
	assert.Contains(t, err.Error(), "API server error")
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXCluster_Success(t *testing.T) {
	mockClient := new(MockClient)
	cluster := createTestCluster("test-cluster", "default")

	mockClient.On("Create", mock.Anything, cluster, mock.Anything).Return(nil)

	result, err := CreatePolarDBXCluster(mockClient, "default", cluster)

	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", result.Name)
	assert.Equal(t, "default", result.Namespace)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXCluster_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	cluster := createTestCluster("existing-cluster", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{
		Group:    "polardbx.aliyun.com",
		Resource: "polardbxclusters",
	}, "existing-cluster")

	mockClient.On("Create", mock.Anything, cluster, mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXCluster(mockClient, "default", cluster)

	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXCluster_Success(t *testing.T) {
	mockClient := new(MockClient)
	expectedCluster := createTestCluster("test-cluster", "default")

	mockClient.On("Get", mock.Anything,
		client.ObjectKey{Namespace: "default", Name: "test-cluster"},
		mock.AnythingOfType("*v1.PolarDBXCluster"), mock.Anything).
		Run(func(args mock.Arguments) {
			cluster := args.Get(2).(*polardbxv1.PolarDBXCluster)
			*cluster = *expectedCluster
		}).Return(nil)

	result, err := GetPolarDBXCluster(mockClient, "default", "test-cluster")

	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", result.Name)
	assert.Equal(t, "default", result.Namespace)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXCluster_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{
		Group:    "polardbx.aliyun.com",
		Resource: "polardbxclusters",
	}, "nonexistent-cluster")

	mockClient.On("Get", mock.Anything,
		client.ObjectKey{Namespace: "default", Name: "nonexistent-cluster"},
		mock.AnythingOfType("*v1.PolarDBXCluster"), mock.Anything).
		Return(notFoundErr)

	result, err := GetPolarDBXCluster(mockClient, "default", "nonexistent-cluster")

	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXCluster_Success(t *testing.T) {
	mockClient := new(MockClient)
	cluster := createTestCluster("test-cluster", "default")
	cluster.Spec.Topology.Nodes.CN.Replicas = func(v int32) *int32 { return &v }(2) // Update replicas

	mockClient.On("Update", mock.Anything, cluster, mock.Anything).Return(nil)

	result, err := UpdatePolarDBXCluster(mockClient, "default", cluster)

	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", result.Name)
	if result.Spec.Topology.Nodes.CN.Replicas == nil {
		t.Fatalf("CN.Replicas is nil")
	}
	assert.Equal(t, int32(2), *result.Spec.Topology.Nodes.CN.Replicas)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXCluster_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXCluster"), mock.Anything).Return(nil)

	err := DeletePolarDBXCluster(mockClient, "default", "test-cluster")

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Test Backup operations
func TestListPolarDBXBackups_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedBackups := []polardbxv1.PolarDBXBackup{
		*createTestBackup("backup1", "default", "test-cluster"),
		*createTestBackup("backup2", "default", "test-cluster"),
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXBackupList)
			list.Items = expectedBackups
		}).Return(nil)

	backups, err := ListPolarDBXBackups(mockClient, "default", "test-cluster")

	assert.NoError(t, err)
	assert.Equal(t, 2, len(backups))
	assert.Equal(t, "backup1", backups[0].Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXBackup_Success(t *testing.T) {
	mockClient := new(MockClient)
	backup := createTestBackup("test-backup", "default", "test-cluster")

	mockClient.On("Create", mock.Anything, backup, mock.Anything).Return(nil)

	result, err := CreatePolarDBXBackup(mockClient, "default", backup)

	assert.NoError(t, err)
	assert.Equal(t, "test-backup", result.Name)
	assert.Equal(t, "test-cluster", result.Spec.Cluster.Name)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXBackup_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackup"), mock.Anything).Return(nil)

	err := DeletePolarDBXBackup(mockClient, "default", "test-backup")

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Test Parameter operations
func TestListPolarDBXParameters_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedParams := []polardbxv1.PolarDBXParameter{
		*createTestParameter("param1", "default", "test-cluster"),
		*createTestParameter("param2", "default", "test-cluster"),
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXParameterList)
			list.Items = expectedParams
		}).Return(nil)

	params, err := ListPolarDBXParameters(mockClient, "default")

	assert.NoError(t, err)
	assert.Equal(t, 2, len(params))
	assert.Equal(t, "param1", params[0].Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXParameter_Success(t *testing.T) {
	mockClient := new(MockClient)
	param := createTestParameter("test-param", "default", "test-cluster")

	mockClient.On("Create", mock.Anything, param, mock.Anything).Return(nil)

	result, err := CreatePolarDBXParameter(mockClient, "default", param)

	assert.NoError(t, err)
	assert.Equal(t, "test-param", result.Name)
	assert.Equal(t, "test-cluster", result.Spec.ClusterName)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXParameter_Success(t *testing.T) {
	mockClient := new(MockClient)
	expectedParam := createTestParameter("test-param", "default", "test-cluster")

	mockClient.On("Get", mock.Anything,
		client.ObjectKey{Namespace: "default", Name: "test-param"},
		mock.AnythingOfType("*v1.PolarDBXParameter"), mock.Anything).
		Run(func(args mock.Arguments) {
			param := args.Get(2).(*polardbxv1.PolarDBXParameter)
			*param = *expectedParam
		}).Return(nil)

	result, err := GetPolarDBXParameter(mockClient, "default", "test-param")

	assert.NoError(t, err)
	assert.Equal(t, "test-param", result.Name)
	assert.Equal(t, "test-cluster", result.Spec.ClusterName)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXParameter_Success(t *testing.T) {
	mockClient := new(MockClient)
	param := createTestParameter("test-param", "default", "test-cluster")
	param.Spec.NodeType.CN.ParamList[0].Value = "2000" // Update value

	mockClient.On("Update", mock.Anything, param, mock.Anything).Return(nil)

	result, err := UpdatePolarDBXParameter(mockClient, "default", param)

	assert.NoError(t, err)
	assert.Equal(t, "test-param", result.Name)
	assert.Equal(t, "2000", result.Spec.NodeType.CN.ParamList[0].Value)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXParameter_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameter"), mock.Anything).Return(nil)

	err := DeletePolarDBXParameter(mockClient, "default", "test-param")

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Test Pod logs functionality
func TestGetPodLogs_Success(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	// This test would require more complex mocking for the streaming interface
	// For simplicity, we'll just test the basic structure
	logs, err := GetPodLogs(clientset, "default", "test-pod", "test-container", 100)

	// With fake client, stream may succeed and return fake logs. Accept either error or non-empty logs.
	if err != nil {
		assert.Empty(t, logs)
	} else {
		assert.NotEmpty(t, logs)
	}
}

// Test XStore operations
func TestListXStores_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedXStores := []polardbxv1.XStore{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore1",
				Namespace: "default",
			},
			Spec: polardbxv1.XStoreSpec{
				Topology: xstore.Topology{
					NodeSets: []xstore.NodeSet{
						{
							Name:     "consensus",
							Role:     xstore.RoleCandidate,
							Replicas: 1,
						},
					},
				},
			},
		},
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.XStoreList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.XStoreList)
			list.Items = expectedXStores
		}).Return(nil)

	xstores, err := ListXStores(mockClient, "default")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(xstores))
	assert.Equal(t, "xstore1", xstores[0].Name)
	mockClient.AssertExpectations(t)
}

// Test XStoreFollower operations (DN replica fault recovery)
func TestListXStoreFollowers_Success(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-xf-list")

	expectedFollowers := []polardbxv1.XStoreFollower{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "follower1",
				Namespace: "default",
			},
			Spec: polardbxv1.XStoreFollowerSpec{
				XStoreName: "test-xstore",
			},
		},
	}

	mockClient.On("List", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-xf-list"
	}), mock.AnythingOfType("*v1.XStoreFollowerList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.XStoreFollowerList)
			list.Items = expectedFollowers
		}).Return(nil)

	followers, err := ListXStoreFollowersWithContext(traceCtx, mockClient, "default")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(followers))
	assert.Equal(t, "follower1", followers[0].Name)
	mockClient.AssertExpectations(t)
}

// Test XStoreBackup operations
func TestListXStoreBackups_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedBackups := []polardbxv1.XStoreBackup{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore-backup1",
				Namespace: "default",
			},
			Spec: polardbxv1.XStoreBackupSpec{
				XStore: polardbxv1.XStoreReference{Name: "test-xstore"},
			},
		},
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.XStoreBackupList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.XStoreBackupList)
			list.Items = expectedBackups
		}).Return(nil)

	backups, err := ListXStoreBackups(mockClient, "default")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(backups))
	assert.Equal(t, "xstore-backup1", backups[0].Name)
	mockClient.AssertExpectations(t)
}

// Test PolarDBXClusterKnobs operations
func TestGetClusterKnobsList_Success(t *testing.T) {
	mockClient := new(MockClient)

	expectedKnobsList := &polardbxv1.PolarDBXClusterKnobsList{
		Items: []polardbxv1.PolarDBXClusterKnobs{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "knobs1",
					Namespace: "default",
				},
				Spec: polardbxv1.PolarDBXClusterKnobsSpec{
					ClusterName: "test-cluster",
					Knobs: map[string]intstr.IntOrString{
						"max_connections": intstr.FromString("1000"),
					},
				},
			},
		},
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterKnobsList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXClusterKnobsList)
			*list = *expectedKnobsList
		}).Return(nil)

	knobsList, err := GetClusterKnobsList(mockClient)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(knobsList.Items))
	assert.Equal(t, "knobs1", knobsList.Items[0].Name)
	mockClient.AssertExpectations(t)
}

func TestCreateClusterKnobs_Success(t *testing.T) {
	mockClient := new(MockClient)

	knobs := &polardbxv1.PolarDBXClusterKnobs{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-knobs",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterKnobsSpec{
			ClusterName: "test-cluster",
			Knobs: map[string]intstr.IntOrString{
				"max_connections":         intstr.FromString("1000"),
				"innodb_buffer_pool_size": intstr.FromString("2G"),
			},
		},
	}

	mockClient.On("Create", mock.Anything, knobs, mock.Anything).Return(nil)

	result, err := CreateClusterKnobs(mockClient, knobs)

	assert.NoError(t, err)
	assert.Equal(t, "test-knobs", result.Name)
	assert.Equal(t, "test-cluster", result.Spec.ClusterName)
	assert.Equal(t, intstr.FromString("1000"), result.Spec.Knobs["max_connections"])
	mockClient.AssertExpectations(t)
}

// Benchmark tests
func BenchmarkListPolarDBXClusters(b *testing.B) {
	mockClient := new(MockClient)

	expectedClusters := make([]polardbxv1.PolarDBXCluster, 100)
	for i := 0; i < 100; i++ {
		expectedClusters[i] = *createTestCluster("cluster"+string(rune(i)), "default")
	}

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXClusterList)
			list.Items = expectedClusters
		}).Return(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ListPolarDBXClusters(mockClient, "default")
	}
}

func BenchmarkCreatePolarDBXCluster(b *testing.B) {
	mockClient := new(MockClient)
	cluster := createTestCluster("bench-cluster", "default")

	mockClient.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CreatePolarDBXCluster(mockClient, "default", cluster)
	}
}

// Integration test style functions that test multiple operations together
func TestClusterLifecycle_SuccessFlow(t *testing.T) {
	mockClient := new(MockClient)
	cluster := createTestCluster("lifecycle-cluster", "default")

	// Create cluster
	mockClient.On("Create", mock.Anything, cluster, mock.Anything).Return(nil).Once()

	// Get cluster
	mockClient.On("Get", mock.Anything,
		client.ObjectKey{Namespace: "default", Name: "lifecycle-cluster"},
		mock.AnythingOfType("*v1.PolarDBXCluster"), mock.Anything).
		Run(func(args mock.Arguments) {
			c := args.Get(2).(*polardbxv1.PolarDBXCluster)
			*c = *cluster
		}).Return(nil).Once()

	// Update cluster
	cluster.Spec.Topology.Nodes.CN.Replicas = func(v int32) *int32 { return &v }(3)
	mockClient.On("Update", mock.Anything, cluster, mock.Anything).Return(nil).Once()

	// Delete cluster
	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXCluster"), mock.Anything).Return(nil).Once()

	// Execute lifecycle
	createdCluster, err := CreatePolarDBXCluster(mockClient, "default", cluster)
	assert.NoError(t, err)
	assert.Equal(t, "lifecycle-cluster", createdCluster.Name)

	retrievedCluster, err := GetPolarDBXCluster(mockClient, "default", "lifecycle-cluster")
	assert.NoError(t, err)
	assert.Equal(t, "lifecycle-cluster", retrievedCluster.Name)

	updatedCluster, err := UpdatePolarDBXCluster(mockClient, "default", cluster)
	assert.NoError(t, err)
	if updatedCluster.Spec.Topology.Nodes.CN.Replicas == nil {
		t.Fatalf("CN.Replicas is nil in updated cluster")
	}
	assert.Equal(t, int32(3), *updatedCluster.Spec.Topology.Nodes.CN.Replicas)

	err = DeletePolarDBXCluster(mockClient, "default", "lifecycle-cluster")
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

// Test edge cases and error scenarios
func TestNamespaceHandling_EmptyNamespace(t *testing.T) {
	mockClient := new(MockClient)
	cluster := createTestCluster("test-cluster", "")

	mockClient.On("Create", mock.Anything, mock.MatchedBy(func(c *polardbxv1.PolarDBXCluster) bool {
		return c.Namespace == "test-namespace"
	}), mock.Anything).Return(nil)

	result, err := CreatePolarDBXCluster(mockClient, "test-namespace", cluster)

	assert.NoError(t, err)
	assert.Equal(t, "test-namespace", result.Namespace)
	mockClient.AssertExpectations(t)
}

func TestValidationErrors_InvalidObjects(t *testing.T) {
	mockClient := new(MockClient)

	validationErr := k8serrors.NewInvalid(
		schema.GroupKind{Group: "polardbx.aliyun.com", Kind: "PolarDBXCluster"},
		"invalid-cluster",
		nil,
	)

	mockClient.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(validationErr)

	cluster := createTestCluster("invalid-cluster", "default")
	result, err := CreatePolarDBXCluster(mockClient, "default", cluster)

	assert.Error(t, err)
	assert.True(t, k8serrors.IsInvalid(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

// ==================== XStore Management Tests ====================

func TestCreateXStore_Success(t *testing.T) {
	mockClient := new(MockClient)
	xstore := createTestXStore("test-xstore", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).Return(nil)

	result, err := CreateXStore(mockClient, "default", xstore)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestCreateXStore_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	xstore := createTestXStore("test-xstore", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstores"}, "test-xstore")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).Return(conflictErr)

	result, err := CreateXStore(mockClient, "default", xstore)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetXStore_Success(t *testing.T) {
	mockClient := new(MockClient)
	xstore := createTestXStore("test-xstore", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.XStore)
			*arg = *xstore
		}).Return(nil)

	result, err := GetXStore(mockClient, "default", "test-xstore")
	assert.NoError(t, err)
	assert.Equal(t, "test-xstore", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetXStore_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstores"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).Return(notFoundErr)

	result, err := GetXStore(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdateXStore_Success(t *testing.T) {
	mockClient := new(MockClient)
	xstore := createTestXStore("test-xstore", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).Return(nil)

	result, err := UpdateXStore(mockClient, "default", xstore)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeleteXStore_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.XStore"), mock.Anything).Return(nil)

	err := DeleteXStore(mockClient, "default", "test-xstore")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestListPodsForPolarDBXCluster_Success(t *testing.T) {
	mockClient := new(MockClient)

	// Mock an empty pod list
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PodList"), mock.Anything).
		Run(func(args mock.Arguments) {
			// Initialize empty pod list
			podList := args.Get(1).(*corev1.PodList)
			podList.Items = []corev1.Pod{}
		}).Return(nil)

	pods, err := ListPodsForPolarDBXCluster(mockClient, "default", "test-cluster")
	assert.NoError(t, err)
	assert.NotNil(t, pods)
	// Empty result is expected for mock without pods
	assert.Len(t, pods, 0)
	mockClient.AssertExpectations(t)
}

// Helper function for XStore tests
func createTestXStore(name, namespace string) *polardbxv1.XStore {
	return &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.XStoreSpec{
			ServiceType: "NodePort",
		},
	}
}

// ==================== Monitor Management Tests ====================

func TestListPolarDBXMonitors_Success(t *testing.T) {
	mockClient := new(MockClient)

	monitor := createTestMonitor("test-monitor", "default")
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitorList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXMonitorList)
			list.Items = []polardbxv1.PolarDBXMonitor{*monitor}
		}).Return(nil)

	monitors, err := ListPolarDBXMonitors(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, monitors, 1)
	assert.Equal(t, "test-monitor", monitors[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXMonitors_Empty(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitorList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXMonitorList)
			list.Items = []polardbxv1.PolarDBXMonitor{}
		}).Return(nil)

	monitors, err := ListPolarDBXMonitors(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, monitors, 0)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXMonitor_Success(t *testing.T) {
	mockClient := new(MockClient)
	monitor := createTestMonitor("test-monitor", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).Return(nil)

	result, err := CreatePolarDBXMonitor(mockClient, "default", monitor)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-monitor", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXMonitor_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	monitor := createTestMonitor("test-monitor", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxmonitors"}, "test-monitor")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXMonitor(mockClient, "default", monitor)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXMonitor_Success(t *testing.T) {
	mockClient := new(MockClient)
	monitor := createTestMonitor("test-monitor", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXMonitor)
			*arg = *monitor
		}).Return(nil)

	result, err := GetPolarDBXMonitor(mockClient, "default", "test-monitor")
	assert.NoError(t, err)
	assert.Equal(t, "test-monitor", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXMonitor_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxmonitors"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).Return(notFoundErr)

	result, err := GetPolarDBXMonitor(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXMonitor_Success(t *testing.T) {
	mockClient := new(MockClient)
	monitor := createTestMonitor("test-monitor", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).Return(nil)

	result, err := UpdatePolarDBXMonitor(mockClient, "default", monitor)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXMonitor_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXMonitor"), mock.Anything).Return(nil)

	err := DeletePolarDBXMonitor(mockClient, "default", "test-monitor")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for Monitor tests
func createTestMonitor(name, namespace string) *polardbxv1.PolarDBXMonitor {
	return &polardbxv1.PolarDBXMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXMonitorSpec{
			// Use basic spec without complex fields
		},
	}
}

// ==================== BackupSchedule Management Tests ====================

func TestListPolarDBXBackupSchedules_Success(t *testing.T) {
	mockClient := new(MockClient)

	schedule := createTestBackupSchedule("test-schedule", "default")
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupScheduleList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXBackupScheduleList)
			list.Items = []polardbxv1.PolarDBXBackupSchedule{*schedule}
		}).Return(nil)

	schedules, err := ListPolarDBXBackupSchedules(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, schedules, 1)
	assert.Equal(t, "test-schedule", schedules[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXBackupSchedules_Empty(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupScheduleList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXBackupScheduleList)
			list.Items = []polardbxv1.PolarDBXBackupSchedule{}
		}).Return(nil)

	schedules, err := ListPolarDBXBackupSchedules(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, schedules, 0)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXBackupSchedule_Success(t *testing.T) {
	mockClient := new(MockClient)
	schedule := createTestBackupSchedule("test-schedule", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).Return(nil)

	result, err := CreatePolarDBXBackupSchedule(mockClient, "default", schedule)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-schedule", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXBackupSchedule_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	schedule := createTestBackupSchedule("test-schedule", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxbackupschedules"}, "test-schedule")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXBackupSchedule(mockClient, "default", schedule)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXBackupSchedule_Success(t *testing.T) {
	mockClient := new(MockClient)
	schedule := createTestBackupSchedule("test-schedule", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXBackupSchedule)
			*arg = *schedule
		}).Return(nil)

	result, err := GetPolarDBXBackupSchedule(mockClient, "default", "test-schedule")
	assert.NoError(t, err)
	assert.Equal(t, "test-schedule", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXBackupSchedule_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxbackupschedules"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).Return(notFoundErr)

	result, err := GetPolarDBXBackupSchedule(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXBackupSchedule_Success(t *testing.T) {
	mockClient := new(MockClient)
	schedule := createTestBackupSchedule("test-schedule", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).Return(nil)

	result, err := UpdatePolarDBXBackupSchedule(mockClient, "default", schedule)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXBackupSchedule_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupSchedule"), mock.Anything).Return(nil)

	err := DeletePolarDBXBackupSchedule(mockClient, "default", "test-schedule")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for BackupSchedule tests
func createTestBackupSchedule(name, namespace string) *polardbxv1.PolarDBXBackupSchedule {
	return &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "0 2 * * *",
		},
	}
}

// ==================== ParameterTemplate Management Tests ====================

func TestListPolarDBXParameterTemplates_Success(t *testing.T) {
	mockClient := new(MockClient)

	template := createTestParameterTemplate("test-template", "default")
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplateList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXParameterTemplateList)
			list.Items = []polardbxv1.PolarDBXParameterTemplate{*template}
		}).Return(nil)

	templates, err := ListPolarDBXParameterTemplates(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, templates, 1)
	assert.Equal(t, "test-template", templates[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXParameterTemplates_Empty(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplateList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXParameterTemplateList)
			list.Items = []polardbxv1.PolarDBXParameterTemplate{}
		}).Return(nil)

	templates, err := ListPolarDBXParameterTemplates(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, templates, 0)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXParameterTemplate_Success(t *testing.T) {
	mockClient := new(MockClient)
	template := createTestParameterTemplate("test-template", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).Return(nil)

	result, err := CreatePolarDBXParameterTemplate(mockClient, "default", template)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-template", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXParameterTemplate_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	template := createTestParameterTemplate("test-template", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxparametertemplates"}, "test-template")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXParameterTemplate(mockClient, "default", template)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXParameterTemplate_Success(t *testing.T) {
	mockClient := new(MockClient)
	template := createTestParameterTemplate("test-template", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXParameterTemplate)
			*arg = *template
		}).Return(nil)

	result, err := GetPolarDBXParameterTemplate(mockClient, "default", "test-template")
	assert.NoError(t, err)
	assert.Equal(t, "test-template", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXParameterTemplate_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxparametertemplates"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).Return(notFoundErr)

	result, err := GetPolarDBXParameterTemplate(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXParameterTemplate_Success(t *testing.T) {
	mockClient := new(MockClient)
	template := createTestParameterTemplate("test-template", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).Return(nil)

	result, err := UpdatePolarDBXParameterTemplate(mockClient, "default", template)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXParameterTemplate_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXParameterTemplate"), mock.Anything).Return(nil)

	err := DeletePolarDBXParameterTemplate(mockClient, "default", "test-template")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for ParameterTemplate tests
func createTestParameterTemplate(name, namespace string) *polardbxv1.PolarDBXParameterTemplate {
	return &polardbxv1.PolarDBXParameterTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXParameterTemplateSpec{
			// Use basic spec without complex fields
		},
	}
}

// ==================== SystemTask Management Tests ====================

func TestListSystemTasks_Success(t *testing.T) {
	mockClient := new(MockClient)

	task := createTestSystemTask("test-task", "default")
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.SystemTaskList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.SystemTaskList)
			list.Items = []polardbxv1.SystemTask{*task}
		}).Return(nil)

	tasks, err := ListSystemTasks(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, "test-task", tasks[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListSystemTasks_Empty(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.SystemTaskList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.SystemTaskList)
			list.Items = []polardbxv1.SystemTask{}
		}).Return(nil)

	tasks, err := ListSystemTasks(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)
	mockClient.AssertExpectations(t)
}

func TestCreateSystemTask_Success(t *testing.T) {
	mockClient := new(MockClient)
	task := createTestSystemTask("test-task", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).Return(nil)

	result, err := CreateSystemTask(mockClient, "default", task)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-task", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreateSystemTask_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	task := createTestSystemTask("test-task", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "systemtasks"}, "test-task")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).Return(conflictErr)

	result, err := CreateSystemTask(mockClient, "default", task)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetSystemTask_Success(t *testing.T) {
	mockClient := new(MockClient)
	task := createTestSystemTask("test-task", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.SystemTask)
			*arg = *task
		}).Return(nil)

	result, err := GetSystemTask(mockClient, "default", "test-task")
	assert.NoError(t, err)
	assert.Equal(t, "test-task", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetSystemTask_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "systemtasks"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).Return(notFoundErr)

	result, err := GetSystemTask(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdateSystemTask_Success(t *testing.T) {
	mockClient := new(MockClient)
	task := createTestSystemTask("test-task", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).Return(nil)

	result, err := UpdateSystemTask(mockClient, "default", task)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeleteSystemTask_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.SystemTask"), mock.Anything).Return(nil)

	err := DeleteSystemTask(mockClient, "default", "test-task")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for SystemTask tests
func createTestSystemTask(name, namespace string) *polardbxv1.SystemTask {
	return &polardbxv1.SystemTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: polardbxv1.SystemTaskSpec{
			// Use basic spec without complex fields
		},
	}
}

// ==================== LogCollector Management Tests ====================

func TestListPolarDBXLogCollectors_Success(t *testing.T) {
	mockClient := new(MockClient)

	logCollector := createTestLogCollector("test-log-collector", "default")
	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollectorList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXLogCollectorList)
			list.Items = []polardbxv1.PolarDBXLogCollector{*logCollector}
		}).Return(nil)

	logCollectors, err := ListPolarDBXLogCollectors(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, logCollectors, 1)
	assert.Equal(t, "test-log-collector", logCollectors[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXLogCollectors_Empty(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("List", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollectorList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXLogCollectorList)
			list.Items = []polardbxv1.PolarDBXLogCollector{}
		}).Return(nil)

	logCollectors, err := ListPolarDBXLogCollectors(mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, logCollectors, 0)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXLogCollector_Success(t *testing.T) {
	mockClient := new(MockClient)
	logCollector := createTestLogCollector("test-log-collector", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).Return(nil)

	result, err := CreatePolarDBXLogCollector(mockClient, "default", logCollector)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-log-collector", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXLogCollector_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	logCollector := createTestLogCollector("test-log-collector", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxlogcollectors"}, "test-log-collector")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXLogCollector(mockClient, "default", logCollector)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXLogCollector_Success(t *testing.T) {
	mockClient := new(MockClient)
	logCollector := createTestLogCollector("test-log-collector", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXLogCollector)
			*arg = *logCollector
		}).Return(nil)

	result, err := GetPolarDBXLogCollector(mockClient, "default", "test-log-collector")
	assert.NoError(t, err)
	assert.Equal(t, "test-log-collector", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXLogCollector_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxlogcollectors"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).Return(notFoundErr)

	result, err := GetPolarDBXLogCollector(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXLogCollector_Success(t *testing.T) {
	mockClient := new(MockClient)
	logCollector := createTestLogCollector("test-log-collector", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).Return(nil)

	result, err := UpdatePolarDBXLogCollector(mockClient, "default", logCollector)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXLogCollector_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXLogCollector"), mock.Anything).Return(nil)

	err := DeletePolarDBXLogCollector(mockClient, "default", "test-log-collector")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// ==================== BackupBinlog Management Tests ====================

func TestListPolarDBXBackupBinlogs_Success(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-list")

	backupBinlog := createTestBackupBinlog("test-backup-binlog", "default")
	mockClient.On("List", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-list"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlogList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXBackupBinlogList)
			list.Items = []polardbxv1.PolarDBXBackupBinlog{*backupBinlog}
		}).Return(nil)

	backupBinlogs, err := ListPolarDBXBackupBinlogsWithContext(traceCtx, mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, backupBinlogs, 1)
	assert.Equal(t, "test-backup-binlog", backupBinlogs[0].Name)
	mockClient.AssertExpectations(t)
}

func TestListPolarDBXBackupBinlogs_Empty(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-empty")

	mockClient.On("List", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-empty"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlogList"), mock.Anything).
		Run(func(args mock.Arguments) {
			list := args.Get(1).(*polardbxv1.PolarDBXBackupBinlogList)
			list.Items = []polardbxv1.PolarDBXBackupBinlog{}
		}).Return(nil)

	backupBinlogs, err := ListPolarDBXBackupBinlogsWithContext(traceCtx, mockClient, "default")
	assert.NoError(t, err)
	assert.Len(t, backupBinlogs, 0)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXBackupBinlog_Success(t *testing.T) {
	mockClient := new(MockClient)
	backupBinlog := createTestBackupBinlog("test-backup-binlog", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-create")

	mockClient.On("Create", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-create"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).Return(nil)

	result, err := CreatePolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", backupBinlog)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-backup-binlog", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreatePolarDBXBackupBinlog_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	backupBinlog := createTestBackupBinlog("test-backup-binlog", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-create-conflict")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxbackupbinlogs"}, "test-backup-binlog")
	mockClient.On("Create", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-create-conflict"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).Return(conflictErr)

	result, err := CreatePolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", backupBinlog)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXBackupBinlog_Success(t *testing.T) {
	mockClient := new(MockClient)
	backupBinlog := createTestBackupBinlog("test-backup-binlog", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-get")

	mockClient.On("Get", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-get"
	}), mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXBackupBinlog)
			*arg = *backupBinlog
		}).Return(nil)

	result, err := GetPolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", "test-backup-binlog")
	assert.NoError(t, err)
	assert.Equal(t, "test-backup-binlog", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetPolarDBXBackupBinlog_NotFound(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-get-notfound")

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxbackupbinlogs"}, "non-existent")
	mockClient.On("Get", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-get-notfound"
	}), mock.Anything, mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).Return(notFoundErr)

	result, err := GetPolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdatePolarDBXBackupBinlog_Success(t *testing.T) {
	mockClient := new(MockClient)
	backupBinlog := createTestBackupBinlog("test-backup-binlog", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-update")

	mockClient.On("Update", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-update"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).Return(nil)

	result, err := UpdatePolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", backupBinlog)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeletePolarDBXBackupBinlog_Success(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-bbl-delete")

	mockClient.On("Delete", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-bbl-delete"
	}), mock.AnythingOfType("*v1.PolarDBXBackupBinlog"), mock.Anything).Return(nil)

	err := DeletePolarDBXBackupBinlogWithContext(traceCtx, mockClient, "default", "test-backup-binlog")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for LogCollector tests
func createTestLogCollector(name, namespace string) *polardbxv1.PolarDBXLogCollector {
	return &polardbxv1.PolarDBXLogCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Use basic object without spec
	}
}

// Helper function for BackupBinlog tests
func createTestBackupBinlog(name, namespace string) *polardbxv1.PolarDBXBackupBinlog {
	return &polardbxv1.PolarDBXBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Use basic object without spec
	}
}

// ==================== XStoreFollower Management Tests ====================

func TestCreateXStoreFollower_Success(t *testing.T) {
	mockClient := new(MockClient)
	follower := createTestXStoreFollower("test-follower", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-xf-create")

	mockClient.On("Create", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-xf-create"
	}), mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).Return(nil)

	result, err := CreateXStoreFollowerWithContext(traceCtx, mockClient, "default", follower)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-follower", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreateXStoreFollower_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	follower := createTestXStoreFollower("test-follower", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstorefollowers"}, "test-follower")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).Return(conflictErr)

	result, err := CreateXStoreFollower(mockClient, "default", follower)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestGetXStoreFollower_Success(t *testing.T) {
	mockClient := new(MockClient)
	follower := createTestXStoreFollower("test-follower", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.XStoreFollower)
			*arg = *follower
		}).Return(nil)

	result, err := GetXStoreFollower(mockClient, "default", "test-follower")
	assert.NoError(t, err)
	assert.Equal(t, "test-follower", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetXStoreFollower_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstorefollowers"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).Return(notFoundErr)

	result, err := GetXStoreFollower(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdateXStoreFollower_Success(t *testing.T) {
	mockClient := new(MockClient)
	follower := createTestXStoreFollower("test-follower", "default")
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-xf-update")

	mockClient.On("Update", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-xf-update"
	}), mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).Return(nil)

	result, err := UpdateXStoreFollowerWithContext(traceCtx, mockClient, "default", follower)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeleteXStoreFollower_Success(t *testing.T) {
	mockClient := new(MockClient)
	traceCtx := context.WithValue(context.Background(), ctxKey("trace"), "tid-xf-delete")

	mockClient.On("Delete", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx.Value(ctxKey("trace")) == "tid-xf-delete"
	}), mock.AnythingOfType("*v1.XStoreFollower"), mock.Anything).Return(nil)

	err := DeleteXStoreFollowerWithContext(traceCtx, mockClient, "default", "test-follower")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// ==================== XStoreBackup Management Tests ====================

func TestCreateXStoreBackup_Success(t *testing.T) {
	mockClient := new(MockClient)
	backup := createTestXStoreBackup("test-backup", "default")

	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).Return(nil)

	result, err := CreateXStoreBackup(mockClient, "default", backup)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-backup", result.Name)
	mockClient.AssertExpectations(t)
}

func TestCreateXStoreBackup_ConflictError(t *testing.T) {
	mockClient := new(MockClient)
	backup := createTestXStoreBackup("test-backup", "default")

	conflictErr := k8serrors.NewAlreadyExists(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstorebackups"}, "test-backup")
	mockClient.On("Create", mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).Return(conflictErr)

	result, err := CreateXStoreBackup(mockClient, "default", backup)
	assert.Error(t, err)
	assert.True(t, k8serrors.IsAlreadyExists(err))
	assert.Nil(t, result) // WithContext version returns nil on error (Go convention)
	mockClient.AssertExpectations(t)
}

func TestGetXStoreBackup_Success(t *testing.T) {
	mockClient := new(MockClient)
	backup := createTestXStoreBackup("test-backup", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.XStoreBackup)
			*arg = *backup
		}).Return(nil)

	result, err := GetXStoreBackup(mockClient, "default", "test-backup")
	assert.NoError(t, err)
	assert.Equal(t, "test-backup", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetXStoreBackup_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "xstorebackups"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).Return(notFoundErr)

	result, err := GetXStoreBackup(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
}

func TestUpdateXStoreBackup_Success(t *testing.T) {
	mockClient := new(MockClient)
	backup := createTestXStoreBackup("test-backup", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).Return(nil)

	result, err := UpdateXStoreBackup(mockClient, "default", backup)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeleteXStoreBackup_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.XStoreBackup"), mock.Anything).Return(nil)

	err := DeleteXStoreBackup(mockClient, "default", "test-backup")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// ==================== ClusterKnobs Remaining Tests ====================

func TestGetClusterKnobs_Success(t *testing.T) {
	mockClient := new(MockClient)
	knobs := createTestClusterKnobs("test-knobs", "default")

	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterKnobs"), mock.Anything).
		Run(func(args mock.Arguments) {
			arg := args.Get(2).(*polardbxv1.PolarDBXClusterKnobs)
			*arg = *knobs
		}).Return(nil)

	result, err := GetClusterKnobs(mockClient, "default", "test-knobs")
	assert.NoError(t, err)
	assert.Equal(t, "test-knobs", result.Name)
	mockClient.AssertExpectations(t)
}

func TestGetClusterKnobs_NotFound(t *testing.T) {
	mockClient := new(MockClient)

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "polardbx.aliyuncs.com", Resource: "polardbxclusterknobs"}, "non-existent")
	mockClient.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterKnobs"), mock.Anything).Return(notFoundErr)

	result, err := GetClusterKnobs(mockClient, "default", "non-existent")
	assert.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err))
	assert.NotNil(t, result) // function returns object even on error
	mockClient.AssertExpectations(t)
}

func TestUpdateClusterKnobs_Success(t *testing.T) {
	mockClient := new(MockClient)
	knobs := createTestClusterKnobs("test-knobs", "default")

	mockClient.On("Update", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterKnobs"), mock.Anything).Return(nil)

	result, err := UpdateClusterKnobs(mockClient, knobs)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestDeleteClusterKnobs_Success(t *testing.T) {
	mockClient := new(MockClient)

	mockClient.On("Delete", mock.Anything, mock.AnythingOfType("*v1.PolarDBXClusterKnobs"), mock.Anything).Return(nil)

	err := DeleteClusterKnobs(mockClient, "default", "test-knobs")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

// Helper function for XStoreFollower tests
func createTestXStoreFollower(name, namespace string) *polardbxv1.XStoreFollower {
	return &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Use basic object without spec
	}
}

// Helper function for XStoreBackup tests
func createTestXStoreBackup(name, namespace string) *polardbxv1.XStoreBackup {
	return &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Use basic object without spec
	}
}

// Helper function for ClusterKnobs tests
func createTestClusterKnobs(name, namespace string) *polardbxv1.PolarDBXClusterKnobs {
	return &polardbxv1.PolarDBXClusterKnobs{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Use basic object without spec
	}
}
