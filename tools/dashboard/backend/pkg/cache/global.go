package cache

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	globalCache     *Cache
	globalCacheOnce sync.Once
)

// GetGlobalCache returns the global cache instance (singleton)
func GetGlobalCache() *Cache {
	globalCacheOnce.Do(func() {
		cfg := Config{
			DefaultExpiration: getEnvDuration("CACHE_DEFAULT_TTL", 30*time.Second),
			CleanupInterval:   getEnvDuration("CACHE_CLEANUP_INTERVAL", 60*time.Second),
		}
		globalCache = New(cfg)
	})
	return globalCache
}

// K8s resource cache key generators

// ClusterListKey generates a cache key for cluster list
func ClusterListKey(namespace string) string {
	if namespace == "" {
		return "k8s:clusters:all"
	}
	return fmt.Sprintf("k8s:clusters:ns:%s", namespace)
}

// ClusterKey generates a cache key for a specific cluster
func ClusterKey(namespace, name string) string {
	return fmt.Sprintf("k8s:cluster:%s:%s", namespace, name)
}

// BackupListKey generates a cache key for backup list
func BackupListKey(namespace, clusterName string) string {
	if clusterName == "" {
		return fmt.Sprintf("k8s:backups:ns:%s", namespace)
	}
	return fmt.Sprintf("k8s:backups:cluster:%s:%s", namespace, clusterName)
}

// BackupKey generates a cache key for a specific backup
func BackupKey(namespace, name string) string {
	return fmt.Sprintf("k8s:backup:%s:%s", namespace, name)
}

// NamespacesKey generates a cache key for namespace list
func NamespacesKey() string {
	return "k8s:namespaces"
}

// MonitorKey generates a cache key for a specific monitor
func MonitorKey(namespace, name string) string {
	return fmt.Sprintf("k8s:monitor:%s:%s", namespace, name)
}

// InvalidateCluster invalidates all cache entries related to a cluster
func InvalidateCluster(namespace, name string) {
	c := GetGlobalCache()
	c.Delete(ClusterKey(namespace, name))
	c.Delete(ClusterListKey(namespace))
	c.Delete(ClusterListKey(""))
}

// InvalidateBackups invalidates all cache entries related to backups
func InvalidateBackups(namespace, clusterName string) {
	c := GetGlobalCache()
	c.DeleteByPrefix(fmt.Sprintf("k8s:backup:%s:", namespace))
	c.Delete(BackupListKey(namespace, clusterName))
	c.Delete(BackupListKey(namespace, ""))
}

// InvalidateNamespaces invalidates namespace cache
func InvalidateNamespaces() {
	GetGlobalCache().Delete(NamespacesKey())
}

// InvalidateAll clears all cached data
func InvalidateAll() {
	GetGlobalCache().Clear()
}

// Helper functions

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
