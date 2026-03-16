package provider

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	podrepo "polardbx-dashboard-backend/pkg/api/domain/platform/pod/repository"
	podsvc "polardbx-dashboard-backend/pkg/api/domain/platform/pod/service"
	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	xstorerepo "polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
	xstoreservices "polardbx-dashboard-backend/pkg/api/domain/xstores/services"
	"polardbx-dashboard-backend/pkg/api/util"
)

const providerKey = "serviceProvider"

// Provider defines how handlers acquire services/repositories.
type Provider interface {
	// Cluster services
	ClusterService(*gin.Context) *services.ClusterService

	// XStore services
	XStoreService(*gin.Context) *xstoreservices.XStoreService
	BackupsService(*gin.Context) *xstoreservices.BackupsService
	BackupBinlogService(*gin.Context) *xstoreservices.BackupBinlogService

	// XStore follower & rebuild services
	FollowersService(*gin.Context) *xstoreservices.FollowersService
	RebuildService(*gin.Context) *xstoreservices.RebuildService

	// Pod service
	PodService(*gin.Context) (*podsvc.PodService, bool)
}

type defaultProvider struct{}

// NewDefaultProvider constructs the default provider using in-process factories.
func NewDefaultProvider() Provider {
	return &defaultProvider{}
}

// Inject attaches provider to gin context.
func Inject(p Provider) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(providerKey, p)
		c.Next()
	}
}

// FromContext fetches provider from gin context.
func FromContext(c *gin.Context) (Provider, bool) {
	v, ok := c.Get(providerKey)
	if !ok {
		return nil, false
	}
	p, ok := v.(Provider)
	return p, ok
}

// Must returns provider or panics (programming error).
func Must(c *gin.Context) Provider {
	p, ok := FromContext(c)
	if !ok || p == nil {
		if !allowProviderFallback() {
			panic("service provider not injected; ensure provider.Inject middleware is registered or enable PROVIDER_FALLBACK_ENABLED for non-production paths")
		}
		p = NewDefaultProvider()
		c.Set(providerKey, p)
	}
	return p
}

func (p *defaultProvider) ClusterService(_ *gin.Context) *services.ClusterService {
	return services.NewClusterService()
}

func (p *defaultProvider) XStoreService(_ *gin.Context) *xstoreservices.XStoreService {
	repo := xstorerepo.NewXStoreRepository()
	return xstoreservices.NewXStoreService(repo)
}

func (p *defaultProvider) BackupsService(_ *gin.Context) *xstoreservices.BackupsService {
	repo := xstorerepo.NewXStoreRepository()
	return xstoreservices.NewBackupsService(repo)
}

func (p *defaultProvider) BackupBinlogService(_ *gin.Context) *xstoreservices.BackupBinlogService {
	return xstoreservices.NewBackupBinlogService()
}

func (p *defaultProvider) FollowersService(_ *gin.Context) *xstoreservices.FollowersService {
	return xstoreservices.NewFollowersService()
}

func (p *defaultProvider) RebuildService(_ *gin.Context) *xstoreservices.RebuildService {
	return xstoreservices.NewRebuildService()
}

func (p *defaultProvider) PodService(c *gin.Context) (*podsvc.PodService, bool) {
	cli, cs, _, ok := util.GetK8sClients(c)
	if !ok {
		return nil, false
	}
	repo := podrepo.NewK8sPodRepositorySimple(cli, cs)
	return podsvc.NewPodService(repo), true
}

func allowProviderFallback() bool {
	if gin.Mode() == gin.TestMode {
		return true
	}
	enabled := strings.TrimSpace(strings.ToLower(os.Getenv("PROVIDER_FALLBACK_ENABLED")))
	return enabled == "1" || enabled == "true" || enabled == "yes" || enabled == "on"
}
