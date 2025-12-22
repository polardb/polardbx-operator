package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/cache"
)

type K8sClients struct {
	Client    client.Client
	Clientset kubernetes.Interface
	Dynamic   dynamic.Interface
}

type NewAllClientsFunc func(kubeconfig []byte) (client.Client, kubernetes.Interface, dynamic.Interface, error)

const (
	kubeconfigClientCacheKeyPrefix = "auth:kubeconfig:clients:"
	defaultKubeconfigClientTTL     = 5 * time.Minute
	kubeconfigClientTTLEnv         = "KUBECONFIG_CLIENT_CACHE_TTL"
	defaultKubeconfigClientMax     = 128
	kubeconfigClientMaxEnv         = "KUBECONFIG_CLIENT_CACHE_MAX_ENTRIES"
)

func kubeconfigClientCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv(kubeconfigClientTTLEnv))
	if raw == "" {
		return defaultKubeconfigClientTTL
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultKubeconfigClientTTL
	}
	return d
}

func kubeconfigClientCacheMaxEntries() int {
	raw := strings.TrimSpace(os.Getenv(kubeconfigClientMaxEnv))
	if raw == "" {
		return defaultKubeconfigClientMax
	}
	var v int
	_, err := fmt.Sscanf(raw, "%d", &v)
	if err != nil || v <= 0 {
		return defaultKubeconfigClientMax
	}
	return v
}

func kubeconfigClientCacheKey(normalizedKubeconfig []byte) string {
	sum := sha256.Sum256(normalizedKubeconfig)
	return kubeconfigClientCacheKeyPrefix + hex.EncodeToString(sum[:])
}

var kubeconfigClientSingleflight singleflight.Group

type kubeconfigClientResult struct {
	clients   *K8sClients
	fromCache bool
}

// GetOrCreateK8sClientsFromKubeconfig reuses a cached client bundle for the same kubeconfig (by hash),
// to avoid rebuilding REST configs and clients on every request.
func GetOrCreateK8sClientsFromKubeconfig(normalizedKubeconfig []byte, newClients NewAllClientsFunc) (*K8sClients, bool, error) {
	if len(normalizedKubeconfig) == 0 {
		return nil, false, fmt.Errorf("normalized kubeconfig is empty")
	}
	if newClients == nil {
		return nil, false, fmt.Errorf("newClients func is nil")
	}

	key := kubeconfigClientCacheKey(normalizedKubeconfig)
	c := cache.GetGlobalCache()

	if v, ok := c.Get(key); ok {
		if clients, ok := v.(*K8sClients); ok && clients != nil {
			return clients, true, nil
		}
		c.Delete(key)
	}

	ttl := kubeconfigClientCacheTTL()

	v, err, shared := kubeconfigClientSingleflight.Do(key, func() (interface{}, error) {
		// Double check inside singleflight to avoid duplicate client creation.
		if v, ok := c.Get(key); ok {
			if clients, ok := v.(*K8sClients); ok && clients != nil {
				return kubeconfigClientResult{clients: clients, fromCache: true}, nil
			}
			c.Delete(key)
		}

		cli, cs, dyn, err := newClients(normalizedKubeconfig)
		if err != nil {
			return nil, err
		}
		clients := &K8sClients{
			Client:    cli,
			Clientset: cs,
			Dynamic:   dyn,
		}
		c.SetWithExpiration(key, clients, ttl)

		// Enforce a max entry limit to reduce memory DoS risk from untrusted kubeconfigs.
		maxEntries := kubeconfigClientCacheMaxEntries()
		c.DeleteOldestByPrefix(kubeconfigClientCacheKeyPrefix, maxEntries)

		return kubeconfigClientResult{clients: clients, fromCache: false}, nil
	})
	if err != nil {
		return nil, false, err
	}

	res, ok := v.(kubeconfigClientResult)
	if !ok || res.clients == nil {
		return nil, false, fmt.Errorf("cached value has unexpected type: %T", v)
	}
	// If shared=true, another goroutine created/reused it during our call (i.e. equivalent to cache hit).
	return res.clients, shared || res.fromCache, nil
}
