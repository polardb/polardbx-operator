package services

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestConvertConfigToCluster_AppliesGMSAndStorageAndRules(t *testing.T) {
	cfg := &ClusterCreationConfig{
		Name:    "c1",
		Version: "8.0.18",
		Image: &ImageConfig{
			Repository: "repo.example.com/polardbx",
			Tag:        "v1",
			PullPolicy: "IfNotPresent",
		},
		Topology: ClusterTopologyConfig{
			CN:  ClusterNodeConfig{Replicas: 2, Resources: NodeResources{CPU: "500m", Memory: "1Gi"}},
			DN:  ClusterNodeConfig{Replicas: 3, Resources: NodeResources{CPU: "1", Memory: "2Gi"}},
			GMS: ClusterNodeConfig{Replicas: 1, Resources: NodeResources{CPU: "200m", Memory: "512Mi"}},
			CDC: &ClusterNodeConfig{Replicas: 1, Resources: NodeResources{CPU: "100m", Memory: "256Mi"}},
		},
		Storage: StorageConfig{
			Size: "20Gi",
		},
		Network: &NetworkConfig{
			ServiceType: "ClusterIP",
			HostNetwork: true,
		},
		Advanced: &AdvancedConfig{
			NodeSelector: map[string]string{"k": "v"},
		},
	}

	cluster := convertConfigToCluster(cfg, "default")
	require.NotNil(t, cluster)
	require.Equal(t, "c1", cluster.Name)
	require.Equal(t, "default", cluster.Namespace)

	// DN DiskQuota derived from storage.size
	require.NotNil(t, cluster.Spec.Topology.Nodes.DN.Template.DiskQuota)
	require.Equal(t, resource.MustParse("20Gi"), *cluster.Spec.Topology.Nodes.DN.Template.DiskQuota)

	// GMS template must be present (so resources/image/pullPolicy can apply)
	require.NotNil(t, cluster.Spec.Topology.Nodes.GMS.Template)
	require.NotNil(t, cluster.Spec.Topology.Nodes.GMS.Template.DiskQuota)
	require.Equal(t, resource.MustParse("20Gi"), *cluster.Spec.Topology.Nodes.GMS.Template.DiskQuota)

	// GMS replicas via topology.rules.components.gms.rolling.replicas
	require.NotNil(t, cluster.Spec.Topology.Rules.Components.GMS)
	require.NotNil(t, cluster.Spec.Topology.Rules.Components.GMS.Rolling)
	require.Equal(t, int32(1), cluster.Spec.Topology.Rules.Components.GMS.Rolling.Replicas)

	// NodeSelector should not wipe out existing Components.
	require.Len(t, cluster.Spec.Topology.Rules.Selectors, 1)
	require.NotNil(t, cluster.Spec.Topology.Rules.Components.GMS)

	// Image should apply to CN/DN/GMS and CDC when enabled.
	require.Equal(t, "repo.example.com/polardbx:v1", cluster.Spec.Topology.Nodes.CN.Template.Image)
	require.Equal(t, "repo.example.com/polardbx:v1", cluster.Spec.Topology.Nodes.DN.Template.Image)
	require.Equal(t, "repo.example.com/polardbx:v1", cluster.Spec.Topology.Nodes.GMS.Template.Image)
	require.NotNil(t, cluster.Spec.Topology.Nodes.CDC)
	require.Equal(t, "repo.example.com/polardbx:v1", cluster.Spec.Topology.Nodes.CDC.Template.Image)
}

