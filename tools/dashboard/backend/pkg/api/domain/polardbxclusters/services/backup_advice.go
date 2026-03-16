package services

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetBackupAdvice returns suggested backup role based on cluster topology (whether followers exist for all XStores)
// in a pure service form (no direct HTTP dependencies).
func (s *BackupService) GetBackupAdvice(ctx context.Context, cli client.Client, namespace, clusterName string) (hasFollower bool, role string, reason interface{}, err error) {
	var xstoreList polardbxv1.XStoreList
	if err = cli.List(ctx, &xstoreList, client.InNamespace(namespace), client.MatchingLabels{
		"polardbx/name": clusterName,
	}); err != nil {
		return false, "", nil, err
	}
	if len(xstoreList.Items) == 0 {
		return false, "leader", "no xstores found for cluster", nil
	}
	namesWithoutFollower := make([]string, 0)
	for _, xs := range xstoreList.Items {
		if xs.Status.TotalPods <= 1 {
			namesWithoutFollower = append(namesWithoutFollower, xs.Name)
		}
	}
	if len(namesWithoutFollower) == 0 {
		return true, "follower", nil, nil
	}
	return false, "leader", namesWithoutFollower, nil
}
