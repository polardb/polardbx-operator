package service

import (
	"context"
	"time"

	"polardbx-dashboard-backend/pkg/api/domain/platform/system/repository"
)

// NamespaceInfo namespace information
type NamespaceInfo struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

// StorageClassInfo storage class information
type StorageClassInfo struct {
	Name              string            `json:"name"`
	Provisioner       string            `json:"provisioner"`
	ReclaimPolicy     string            `json:"reclaimPolicy,omitempty"`
	VolumeBindingMode string            `json:"volumeBindingMode,omitempty"`
	IsDefault         bool              `json:"isDefault"`
	Parameters        map[string]string `json:"parameters,omitempty"`
}

// PolarDBXVersionInfo PolarDB-X version information
type PolarDBXVersionInfo struct {
	Version     string `json:"version"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Recommended bool   `json:"recommended"`
	Deprecated  bool   `json:"deprecated"`
}

// ContextInfo context information
type ContextInfo struct {
	User             string `json:"user"`
	Context          string `json:"context"`
	DefaultNamespace string `json:"defaultNamespace"`
}

// SystemService defines system business logic layer
type SystemService struct {
	repo repository.SystemRepository
}

// NewSystemService creates a new SystemService
func NewSystemService(repo repository.SystemRepository) *SystemService {
	return &SystemService{repo: repo}
}

// ListNamespaces lists all namespaces
func (s *SystemService) ListNamespaces(ctx context.Context) ([]NamespaceInfo, error) {
	namespaces, err := s.repo.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]NamespaceInfo, 0, len(namespaces))
	for _, ns := range namespaces {
		items = append(items, NamespaceInfo{
			Name:      ns.Name,
			Status:    string(ns.Status.Phase),
			CreatedAt: ns.CreationTimestamp.Time,
		})
	}
	return items, nil
}

// ListStorageClasses lists all storage classes
func (s *SystemService) ListStorageClasses(ctx context.Context) ([]StorageClassInfo, error) {
	storageClasses, err := s.repo.ListStorageClasses(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]StorageClassInfo, 0, len(storageClasses))
	for _, sc := range storageClasses {
		isDefault := false
		if sc.Annotations != nil {
			if v, ok := sc.Annotations["storageclass.kubernetes.io/is-default-class"]; ok && v == "true" {
				isDefault = true
			}
		}

		reclaimPolicy := ""
		if sc.ReclaimPolicy != nil {
			reclaimPolicy = string(*sc.ReclaimPolicy)
		}

		volumeBindingMode := ""
		if sc.VolumeBindingMode != nil {
			volumeBindingMode = string(*sc.VolumeBindingMode)
		}

		items = append(items, StorageClassInfo{
			Name:              sc.Name,
			Provisioner:       sc.Provisioner,
			ReclaimPolicy:     reclaimPolicy,
			VolumeBindingMode: volumeBindingMode,
			IsDefault:         isDefault,
			Parameters:        sc.Parameters,
		})
	}
	return items, nil
}

// GetPolarDBXVersions returns supported PolarDB-X version list
func (s *SystemService) GetPolarDBXVersions() []PolarDBXVersionInfo {
	// Version information can be read from config file or CRD, hardcoded common versions for now
	return []PolarDBXVersionInfo{
		{Version: "8.0.18", Label: "8.0.18 (latest stable)", Recommended: true},
		{Version: "8.0.17", Label: "8.0.17"},
		{Version: "8.0.16", Label: "8.0.16"},
		{Version: "5.7.14", Label: "5.7.14 (legacy version)", Deprecated: true},
	}
}
