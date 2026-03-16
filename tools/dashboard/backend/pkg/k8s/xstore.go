package k8s

import (
	"context"
	"log"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ---- XStore ----

// Deprecated: Use ListXStoresWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListXStores(c client.Client, namespace string) ([]polardbxv1.XStore, error) {
	log.Printf("WARNING: Using deprecated ListXStores without context. Please migrate to ListXStoresWithContext.")
	return ListXStoresWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreateXStoreWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateXStore(c client.Client, namespace string, xstore *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	log.Printf("WARNING: Using deprecated CreateXStore without context. Please migrate to CreateXStoreWithContext.")
	return CreateXStoreWithContext(context.Background(), c, namespace, xstore)
}

// Deprecated: Use GetXStoreWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetXStore(c client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	log.Printf("WARNING: Using deprecated GetXStore without context. Please migrate to GetXStoreWithContext.")
	return GetXStoreWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdateXStoreWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateXStore(c client.Client, namespace string, xstore *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	log.Printf("WARNING: Using deprecated UpdateXStore without context. Please migrate to UpdateXStoreWithContext.")
	return UpdateXStoreWithContext(context.Background(), c, namespace, xstore)
}

// Deprecated: Use DeleteXStoreWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteXStore(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteXStore without context. Please migrate to DeleteXStoreWithContext.")
	return DeleteXStoreWithContext(context.Background(), c, namespace, name)
}

func ListXStoresWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStore, error) {
	var list polardbxv1.XStoreList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreateXStoreWithContext(ctx context.Context, c client.Client, namespace string, x *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if x.Namespace == "" {
		x.Namespace = namespace
	}
	if err := c.Create(ctx, x); err != nil {
		return nil, err
	}
	return x, nil
}

func GetXStoreWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	var out polardbxv1.XStore
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdateXStoreWithContext(ctx context.Context, c client.Client, namespace string, x *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if err := c.Update(ctx, x); err != nil {
		return nil, err
	}
	return x, nil
}

func DeleteXStoreWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStore{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- XStoreBackup ----

// Deprecated: Use ListXStoreBackupsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListXStoreBackups(c client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	log.Printf("WARNING: Using deprecated ListXStoreBackups without context. Please migrate to ListXStoreBackupsWithContext.")
	return ListXStoreBackupsWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreateXStoreBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateXStoreBackup(c client.Client, namespace string, backup *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	log.Printf("WARNING: Using deprecated CreateXStoreBackup without context. Please migrate to CreateXStoreBackupWithContext.")
	return CreateXStoreBackupWithContext(context.Background(), c, namespace, backup)
}

// Deprecated: Use GetXStoreBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetXStoreBackup(c client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	log.Printf("WARNING: Using deprecated GetXStoreBackup without context. Please migrate to GetXStoreBackupWithContext.")
	return GetXStoreBackupWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdateXStoreBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateXStoreBackup(c client.Client, namespace string, backup *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	log.Printf("WARNING: Using deprecated UpdateXStoreBackup without context. Please migrate to UpdateXStoreBackupWithContext.")
	return UpdateXStoreBackupWithContext(context.Background(), c, namespace, backup)
}

// Deprecated: Use DeleteXStoreBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteXStoreBackup(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteXStoreBackup without context. Please migrate to DeleteXStoreBackupWithContext.")
	return DeleteXStoreBackupWithContext(context.Background(), c, namespace, name)
}

func ListXStoreBackupsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	var list polardbxv1.XStoreBackupList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreateXStoreBackupWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func GetXStoreBackupWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	var out polardbxv1.XStoreBackup
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdateXStoreBackupWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func DeleteXStoreBackupWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStoreBackup{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- XStoreFollower ----

// Deprecated: Use ListXStoreFollowersWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListXStoreFollowers(c client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	log.Printf("WARNING: Using deprecated ListXStoreFollowers without context. Please migrate to ListXStoreFollowersWithContext.")
	return ListXStoreFollowersWithContext(context.Background(), c, namespace)
}

func ListXStoreFollowersWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	var followerList polardbxv1.XStoreFollowerList
	if err := c.List(ctx, &followerList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return followerList.Items, nil
}

// Deprecated: Use CreateXStoreFollowerWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateXStoreFollower(c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	log.Printf("WARNING: Using deprecated CreateXStoreFollower without context. Please migrate to CreateXStoreFollowerWithContext.")
	return CreateXStoreFollowerWithContext(context.Background(), c, namespace, follower)
}

func CreateXStoreFollowerWithContext(ctx context.Context, c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if follower.Namespace == "" {
		follower.Namespace = namespace
	}
	err := c.Create(ctx, follower)
	return follower, err
}

// Deprecated: Use GetXStoreFollowerWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetXStoreFollower(c client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	log.Printf("WARNING: Using deprecated GetXStoreFollower without context. Please migrate to GetXStoreFollowerWithContext.")
	return GetXStoreFollowerWithContext(context.Background(), c, namespace, name)
}

func GetXStoreFollowerWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	var follower polardbxv1.XStoreFollower
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &follower)
	if err != nil {
		return nil, err
	}
	return &follower, nil
}

// Deprecated: Use UpdateXStoreFollowerWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateXStoreFollower(c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	log.Printf("WARNING: Using deprecated UpdateXStoreFollower without context. Please migrate to UpdateXStoreFollowerWithContext.")
	return UpdateXStoreFollowerWithContext(context.Background(), c, namespace, follower)
}

func UpdateXStoreFollowerWithContext(ctx context.Context, c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	err := c.Update(ctx, follower)
	return follower, err
}

// Deprecated: Use DeleteXStoreFollowerWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteXStoreFollower(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteXStoreFollower without context. Please migrate to DeleteXStoreFollowerWithContext.")
	return DeleteXStoreFollowerWithContext(context.Background(), c, namespace, name)
}

func DeleteXStoreFollowerWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	follower := &polardbxv1.XStoreFollower{}
	follower.Name = name
	follower.Namespace = namespace
	return c.Delete(ctx, follower)
}

// ---- XStoreBackupBinlog ----

// Deprecated: Use ListXStoreBackupBinlogsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListXStoreBackupBinlogs(c client.Client, namespace string) ([]polardbxv1.XStoreBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated ListXStoreBackupBinlogs without context. Please migrate to ListXStoreBackupBinlogsWithContext.")
	return ListXStoreBackupBinlogsWithContext(context.Background(), c, namespace)
}

func ListXStoreBackupBinlogsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStoreBackupBinlog, error) {
	var list polardbxv1.XStoreBackupBinlogList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Deprecated: Use CreateXStoreBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateXStoreBackupBinlog(c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated CreateXStoreBackupBinlog without context. Please migrate to CreateXStoreBackupBinlogWithContext.")
	return CreateXStoreBackupBinlogWithContext(context.Background(), c, namespace, obj)
}

func CreateXStoreBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// Deprecated: Use GetXStoreBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetXStoreBackupBinlog(c client.Client, namespace, name string) (*polardbxv1.XStoreBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated GetXStoreBackupBinlog without context. Please migrate to GetXStoreBackupBinlogWithContext.")
	return GetXStoreBackupBinlogWithContext(context.Background(), c, namespace, name)
}

func GetXStoreBackupBinlogWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStoreBackupBinlog, error) {
	var out polardbxv1.XStoreBackupBinlog
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Deprecated: Use UpdateXStoreBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateXStoreBackupBinlog(c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated UpdateXStoreBackupBinlog without context. Please migrate to UpdateXStoreBackupBinlogWithContext.")
	return UpdateXStoreBackupBinlogWithContext(context.Background(), c, namespace, obj)
}

func UpdateXStoreBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// Deprecated: Use DeleteXStoreBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteXStoreBackupBinlog(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteXStoreBackupBinlog without context. Please migrate to DeleteXStoreBackupBinlogWithContext.")
	return DeleteXStoreBackupBinlogWithContext(context.Background(), c, namespace, name)
}

func DeleteXStoreBackupBinlogWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStoreBackupBinlog{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}
