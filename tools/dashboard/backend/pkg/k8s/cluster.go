package k8s

import (
	"context"
	"log"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ListPolarDBXClusters lists all PolarDBXCluster resources in the given namespace.
// Deprecated: Use ListPolarDBXClustersWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXClusters(c client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXClusters without context. Please migrate to ListPolarDBXClustersWithContext.")
	return ListPolarDBXClustersWithContext(context.Background(), c, namespace)
}

// CreatePolarDBXCluster creates a PolarDBXCluster resource.
// Deprecated: Use CreatePolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXCluster(c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXCluster without context. Please migrate to CreatePolarDBXClusterWithContext.")
	return CreatePolarDBXClusterWithContext(context.Background(), c, namespace, cluster)
}

// GetPolarDBXCluster gets a PolarDBXCluster resource by name.
// Deprecated: Use GetPolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXCluster(c client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXCluster without context. Please migrate to GetPolarDBXClusterWithContext.")
	return GetPolarDBXClusterWithContext(context.Background(), c, namespace, name)
}

// DeletePolarDBXCluster deletes a PolarDBXCluster resource by name.
// Deprecated: Use DeletePolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXCluster(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXCluster without context. Please migrate to DeletePolarDBXClusterWithContext.")
	return DeletePolarDBXClusterWithContext(context.Background(), c, namespace, name)
}

// UpdatePolarDBXCluster updates a PolarDBXCluster resource.
// Deprecated: Use UpdatePolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXCluster(c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXCluster without context. Please migrate to UpdatePolarDBXClusterWithContext.")
	return UpdatePolarDBXClusterWithContext(context.Background(), c, namespace, cluster)
}

// PatchPolarDBXCluster patches a PolarDBXCluster resource.
// Deprecated: Use PatchPolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func PatchPolarDBXCluster(c client.Client, namespace, name string, patchData []byte) (*polardbxv1.PolarDBXCluster, error) {
	log.Printf("WARNING: Using deprecated PatchPolarDBXCluster without context. Please migrate to PatchPolarDBXClusterWithContext.")
	return PatchPolarDBXClusterWithContext(context.Background(), c, namespace, name, patchData)
}

// Context-aware variants for cluster operations
func ListPolarDBXClustersWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	var clusterList polardbxv1.PolarDBXClusterList
	if err := c.List(ctx, &clusterList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return clusterList.Items, nil
}

func CreatePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	if err := c.Create(ctx, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func GetPolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	var cluster polardbxv1.PolarDBXCluster
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err != nil {
		return nil, err
	}
	return &cluster, nil
}

func DeletePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	var cluster polardbxv1.PolarDBXCluster
	cluster.Name = name
	cluster.Namespace = namespace
	return c.Delete(ctx, &cluster)
}

func UpdatePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	if err := c.Update(ctx, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func PatchPolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string, patchData []byte) (*polardbxv1.PolarDBXCluster, error) {
	cluster := &polardbxv1.PolarDBXCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		return nil, err
	}
	patch := client.RawPatch(types.MergePatchType, patchData)
	if err := c.Patch(ctx, cluster, patch); err != nil {
		return nil, err
	}
	updated := &polardbxv1.PolarDBXCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, updated); err != nil {
		return nil, err
	}
	return updated, nil
}
