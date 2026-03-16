package services

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxcommon "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/k8srepo"
	svcerr "polardbx-dashboard-backend/pkg/api/errors"
)

// ValidationError validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidateClusterCreationConfig validates cluster creation configuration
func ValidateClusterCreationConfig(config *ClusterCreationConfig) []ValidationError {
	var errors []ValidationError

	// 1. Name validation
	if config.Name == "" {
		errors = append(errors, ValidationError{Field: "name", Message: "cluster name cannot be empty"})
	} else if !isValidK8sName(config.Name) {
		errors = append(errors, ValidationError{Field: "name", Message: "cluster name can only contain lowercase letters, numbers, and hyphens, and must start with a letter"})
	} else if len(config.Name) > 63 {
		errors = append(errors, ValidationError{Field: "name", Message: "cluster name cannot exceed 63 characters"})
	}

	// 2. Namespace validation
	if config.Namespace != "" && !isValidK8sName(config.Namespace) {
		errors = append(errors, ValidationError{Field: "namespace", Message: "namespace format is incorrect"})
	}

	// 3. Version validation
	if config.Version != "" && !isValidVersion(config.Version) {
		errors = append(errors, ValidationError{Field: "version", Message: "version format is incorrect, should be in x.y.z format"})
	}

	// 4. Topology validation
	if err := validateNodeConfig("topology.cn", config.Topology.CN, 100); err != nil {
		errors = append(errors, *err)
	}
	if err := validateNodeConfig("topology.dn", config.Topology.DN, 100); err != nil {
		errors = append(errors, *err)
	}
	if err := validateNodeConfig("topology.gms", config.Topology.GMS, 3); err != nil {
		errors = append(errors, *err)
	}
	if config.Topology.CDC != nil {
		if err := validateNodeConfig("topology.cdc", *config.Topology.CDC, 10); err != nil {
			errors = append(errors, *err)
		}
	}

	// 5. Storage validation
	if config.Storage.Size != "" && !isValidStorageSize(config.Storage.Size) {
		errors = append(errors, ValidationError{Field: "storage.size", Message: "storage size format is incorrect, should be in format like 10Gi, 100Gi, 1Ti"})
	}

	// 6. Network validation
	if config.Network != nil {
		if config.Network.ServiceType != "" &&
			config.Network.ServiceType != "ClusterIP" &&
			config.Network.ServiceType != "NodePort" &&
			config.Network.ServiceType != "LoadBalancer" {
			errors = append(errors, ValidationError{Field: "network.serviceType", Message: "service type must be ClusterIP, NodePort, or LoadBalancer"})
		}
	}

	// 7. Security configuration validation
	if config.Security != nil {
		if config.Security.EnableTLS && config.Security.SecretName == "" {
			errors = append(errors, ValidationError{Field: "security.secretName", Message: "secret name must be specified when TLS is enabled"})
		}
	}

	// 8. Image configuration validation
	if config.Image != nil {
		if config.Image.PullPolicy != "" &&
			config.Image.PullPolicy != "Always" &&
			config.Image.PullPolicy != "IfNotPresent" &&
			config.Image.PullPolicy != "Never" {
			errors = append(errors, ValidationError{Field: "image.pullPolicy", Message: "pull policy must be Always, IfNotPresent, or Never"})
		}
	}

	return errors
}

// isValidK8sName checks if it's a valid Kubernetes name
func isValidK8sName(name string) bool {
	pattern := regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$`)
	return pattern.MatchString(name)
}

// isValidVersion checks version format
func isValidVersion(version string) bool {
	pattern := regexp.MustCompile(`^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)?$`)
	return pattern.MatchString(version)
}

// isValidStorageSize checks storage size format
func isValidStorageSize(size string) bool {
	pattern := regexp.MustCompile(`^\d+(\.\d+)?(Ki|Mi|Gi|Ti|Pi|Ei)?$`)
	return pattern.MatchString(size)
}

// isValidResourceQuantity checks resource quantity format (CPU/Memory)
func isValidResourceQuantity(quantity string) bool {
	if quantity == "" {
		return true
	}
	cpuPattern := regexp.MustCompile(`^\d+(\.\d+)?(m)?$`)
	memPattern := regexp.MustCompile(`^\d+(\.\d+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$`)
	return cpuPattern.MatchString(quantity) || memPattern.MatchString(quantity)
}

// validateNodeConfig validates node configuration
func validateNodeConfig(fieldPrefix string, node ClusterNodeConfig, maxReplicas int) *ValidationError {
	if node.Replicas < 1 {
		return &ValidationError{
			Field:   fieldPrefix + ".replicas",
			Message: fmt.Sprintf("%s replicas must be at least 1", fieldPrefix),
		}
	}

	if node.Replicas > maxReplicas {
		return &ValidationError{
			Field:   fieldPrefix + ".replicas",
			Message: fmt.Sprintf("%s replicas cannot exceed %d", fieldPrefix, maxReplicas),
		}
	}

	if node.Resources.CPU != "" && !isValidResourceQuantity(node.Resources.CPU) {
		return &ValidationError{
			Field:   fieldPrefix + ".resources.cpu",
			Message: "CPU resource format is incorrect",
		}
	}
	if node.Resources.Memory != "" && !isValidResourceQuantity(node.Resources.Memory) {
		return &ValidationError{
			Field:   fieldPrefix + ".resources.memory",
			Message: "memory resource format is incorrect",
		}
	}

	return nil
}

// --- Cluster CRUD (behavior remains unchanged) ---

// ListClusters lists clusters in the given namespace using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *ClusterService) ListClusters(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	clusters, err := k8srepo.NewClusterRepository().List(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list clusters in namespace %s: %w", namespace, err)
	}
	return clusters, nil
}

// CreateCluster creates a cluster in the given namespace using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *ClusterService) CreateCluster(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	created, err := k8srepo.NewClusterRepository().Create(ctx, cli, namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("create cluster %s/%s: %w", namespace, obj.GetName(), err)
	}
	return created, nil
}

// convertConfigToCluster converts user-friendly configuration to PolarDBXCluster object
func convertConfigToCluster(config *ClusterCreationConfig, namespace string) *polardbxv1.PolarDBXCluster {
	// Build CN replicas pointer
	cnReplicas := int32(config.Topology.CN.Replicas)

	// Handle HostNetwork configuration
	var cnHostNetwork, dnHostNetwork bool
	if config.Network != nil {
		cnHostNetwork = config.Network.HostNetwork
		dnHostNetwork = config.Network.HostNetwork
	}

	// Determine protocol version from version string (e.g., "8.0.18" -> 8, "5.7.x" -> 5)
	protocolVersion := 8 // default to MySQL 8
	if strings.HasPrefix(config.Version, "5.") {
		protocolVersion = 5
	}

	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: namespace,
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			ProtocolVersion: intstr.FromInt(protocolVersion),
			Config: polardbx.Config{
				CN: polardbx.CNConfig{
					Static: &polardbx.CNStaticConfig{
						EnableCoroutine:    true,
						RPCProtocolVersion: intstr.FromString("2"),
					},
				},
			},
			Topology: polardbx.Topology{
				Version: config.Version,
				Nodes: polardbx.TopologyNodes{
					GMS: polardbx.TopologyNodeGMS{
						Template: &polardbx.XStoreTemplate{
							Resources:   buildExtendedResourceRequirements(config.Topology.GMS.Resources),
							HostNetwork: boolPtr(dnHostNetwork),
						},
					},
					CN: polardbx.TopologyNodeCN{
						Replicas: &cnReplicas,
						Template: polardbx.CNTemplate{
							Resources:   buildResourceRequirements(config.Topology.CN.Resources),
							HostNetwork: cnHostNetwork,
						},
					},
					DN: polardbx.TopologyNodeDN{
						Replicas: int32(config.Topology.DN.Replicas),
						Template: polardbx.XStoreTemplate{
							Resources:   buildExtendedResourceRequirements(config.Topology.DN.Resources),
							HostNetwork: boolPtr(dnHostNetwork),
						},
					},
				},
			},
		},
	}

	// Apply storage size as DiskQuota (storageClass/accessModes are not part of PolarDBXCluster CRD).
	if config.Storage.Size != "" {
		if q, err := resource.ParseQuantity(config.Storage.Size); err == nil {
			cluster.Spec.Topology.Nodes.DN.Template.DiskQuota = &q
			if cluster.Spec.Topology.Nodes.GMS.Template != nil {
				cluster.Spec.Topology.Nodes.GMS.Template.DiskQuota = &q
			}
		}
	}

	// Apply GMS replicas via topology.rules.components.gms (GMS has no replicas field in topology.nodes).
	if config.Topology.GMS.Replicas > 0 {
		cluster.Spec.Topology.Rules.Components.GMS = &polardbx.XStoreTopologyRule{
			Rolling: &polardbx.XStoreTopologyRuleRolling{
				Replicas: int32(config.Topology.GMS.Replicas),
			},
		}
	}

	// Set image configuration
	if config.Image != nil {
		if config.Image.Repository != "" || config.Image.Tag != "" {
			image := config.Image.Repository
			if config.Image.Tag != "" {
				if image != "" {
					image = image + ":" + config.Image.Tag
				} else {
					image = config.Image.Tag
				}
			}
			// Set CN image
			cluster.Spec.Topology.Nodes.CN.Template.Image = image
			// Set DN image
			cluster.Spec.Topology.Nodes.DN.Template.Image = image
			// Set GMS image
			if cluster.Spec.Topology.Nodes.GMS.Template != nil {
				cluster.Spec.Topology.Nodes.GMS.Template.Image = image
			}
			// Set CDC image (if enabled)
			if cluster.Spec.Topology.Nodes.CDC != nil {
				cluster.Spec.Topology.Nodes.CDC.Template.Image = image
			}
		}
		// Set image pull policy
		if config.Image.PullPolicy != "" {
			pullPolicy := corev1.PullPolicy(config.Image.PullPolicy)
			cluster.Spec.Topology.Nodes.CN.Template.ImagePullPolicy = pullPolicy
			cluster.Spec.Topology.Nodes.DN.Template.ImagePullPolicy = pullPolicy
			if cluster.Spec.Topology.Nodes.GMS.Template != nil {
				cluster.Spec.Topology.Nodes.GMS.Template.ImagePullPolicy = pullPolicy
			}
			if cluster.Spec.Topology.Nodes.CDC != nil {
				cluster.Spec.Topology.Nodes.CDC.Template.ImagePullPolicy = pullPolicy
			}
		}
	}

	// Set ShareGMS mode
	if config.Advanced != nil && config.Advanced.ShareGMS {
		cluster.Spec.ShareGMS = true
	}

	// Set description
	if config.Description != "" {
		if cluster.Annotations == nil {
			cluster.Annotations = make(map[string]string)
		}
		cluster.Annotations["description"] = config.Description
	}

	// Set CDC configuration (if any)
	if config.Topology.CDC != nil && config.Topology.CDC.Replicas > 0 {
		cluster.Spec.Topology.Nodes.CDC = &polardbx.TopologyNodeCDC{
			Replicas: intstr.FromInt(config.Topology.CDC.Replicas),
			Template: polardbx.CDCTemplate{
				Resources:   buildResourceRequirements(config.Topology.CDC.Resources),
				HostNetwork: cnHostNetwork, // CDC also uses the same HostNetwork configuration
			},
		}

		// CDC node is constructed after the global image block above; apply image/pullPolicy here as well.
		if config.Image != nil {
			if config.Image.Repository != "" || config.Image.Tag != "" {
				image := config.Image.Repository
				if config.Image.Tag != "" {
					if image != "" {
						image = image + ":" + config.Image.Tag
					} else {
						image = config.Image.Tag
					}
				}
				cluster.Spec.Topology.Nodes.CDC.Template.Image = image
			}
			if config.Image.PullPolicy != "" {
				cluster.Spec.Topology.Nodes.CDC.Template.ImagePullPolicy = corev1.PullPolicy(config.Image.PullPolicy)
			}
		}
	}

	// Set network service type
	if config.Network != nil && config.Network.ServiceType != "" {
		serviceType := corev1.ServiceType(config.Network.ServiceType)
		cluster.Spec.ServiceType = serviceType
	}

	// Set TLS configuration
	if config.Security != nil && config.Security.EnableTLS {
		if cluster.Spec.Security == nil {
			cluster.Spec.Security = &polardbx.Security{}
		}
		cluster.Spec.Security.TLS = &polardbx.TLS{
			SecretName: config.Security.SecretName,
		}
	}

	// Set custom labels, annotations, and node selectors
	if config.Advanced != nil {
		if len(config.Advanced.CustomLabels) > 0 {
			if cluster.Labels == nil {
				cluster.Labels = make(map[string]string)
			}
			for k, v := range config.Advanced.CustomLabels {
				cluster.Labels[k] = v
			}
		}
		if len(config.Advanced.CustomAnnotations) > 0 {
			if cluster.Annotations == nil {
				cluster.Annotations = make(map[string]string)
			}
			for k, v := range config.Advanced.CustomAnnotations {
				cluster.Annotations[k] = v
			}
		}
		// Set node selector (via TopologyRules)
		if len(config.Advanced.NodeSelector) > 0 {
			nodeSelector := buildNodeSelector(config.Advanced.NodeSelector)
			cluster.Spec.Topology.Rules.Selectors = []polardbx.NodeSelectorItem{
				{
					Name:         "default",
					NodeSelector: nodeSelector,
				},
			}
		}
	}

	return cluster
}

// boolPtr returns pointer to bool value
func boolPtr(b bool) *bool {
	return &b
}

// buildNodeSelector converts map to corev1.NodeSelector
func buildNodeSelector(labels map[string]string) corev1.NodeSelector {
	var matchExpressions []corev1.NodeSelectorRequirement
	for key, value := range labels {
		matchExpressions = append(matchExpressions, corev1.NodeSelectorRequirement{
			Key:      key,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{value},
		})
	}
	return corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{
				MatchExpressions: matchExpressions,
			},
		},
	}
}

// buildExtendedResourceRequirements builds extended resource requirements (for CN/DN)
func buildExtendedResourceRequirements(res NodeResources) polardbxcommon.ExtendedResourceRequirements {
	return polardbxcommon.ExtendedResourceRequirements{
		ResourceRequirements: buildResourceRequirements(res),
	}
}

// buildResourceRequirements builds resource requirements
func buildResourceRequirements(res NodeResources) corev1.ResourceRequirements {
	requirements := corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{},
		Requests: corev1.ResourceList{},
	}

	if res.CPU != "" {
		cpuQty := resource.MustParse(res.CPU)
		requirements.Limits[corev1.ResourceCPU] = cpuQty
		requirements.Requests[corev1.ResourceCPU] = cpuQty
	}

	if res.Memory != "" {
		memQty := resource.MustParse(res.Memory)
		requirements.Limits[corev1.ResourceMemory] = memQty
		requirements.Requests[corev1.ResourceMemory] = memQty
	}

	return requirements
}

// GetCluster fetches a single cluster using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *ClusterService) GetCluster(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	cluster, err := k8srepo.NewClusterRepository().Get(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get cluster %s/%s: %w", namespace, name, err)
	}
	return cluster, nil
}

// UpdateCluster updates an existing cluster using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *ClusterService) UpdateCluster(ctx context.Context, cli client.Client, namespace, name string, body *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	existing, err := k8srepo.NewClusterRepository().Get(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get cluster %s/%s for update: %w", namespace, name, err)
	}
	body.SetResourceVersion(existing.GetResourceVersion())
	updated, err := k8srepo.NewClusterRepository().Update(ctx, cli, namespace, body)
	if err != nil {
		return nil, fmt.Errorf("update cluster %s/%s: %w", namespace, name, err)
	}
	return updated, nil
}

// DeleteCluster deletes a cluster using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *ClusterService) DeleteCluster(ctx context.Context, cli client.Client, namespace, name string) error {
	if err := k8srepo.NewClusterRepository().Delete(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete cluster %s/%s: %w", namespace, name, err)
	}
	return nil
}

// CreateClusterFromConfig creates a cluster from a user-friendly configuration in a pure service form.
// Business validation and K8s interaction are handled here; HTTP concerns stay in handlers.
func (s *ClusterService) CreateClusterFromConfig(ctx context.Context, cli client.Client, namespace string, config *ClusterCreationConfig) (*polardbxv1.PolarDBXCluster, error) {
	// Parameter validation
	if validationErrors := ValidateClusterCreationConfig(config); len(validationErrors) > 0 {
		return nil, svcerr.ValidationError("configuration validation failed", validationErrors)
	}

	// Convert to PolarDBXCluster object
	cluster := convertConfigToCluster(config, namespace)

	created, err := k8srepo.NewClusterRepository().Create(ctx, cli, namespace, cluster)
	if err != nil {
		// Extract more meaningful error information
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, svcerr.ConflictError(fmt.Sprintf("cluster '%s/%s' already exists", namespace, config.Name))
		}
		return nil, fmt.Errorf("create cluster from config %s/%s: %w", namespace, config.Name, err)
	}

	return created, nil
}
