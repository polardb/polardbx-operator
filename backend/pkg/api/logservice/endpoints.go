package logservice

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Status aggregates log-collector components readiness and config presence
// Query: namespace (default: polardbx-logcollector)
func Status(c *gin.Context) {
	clientset, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "polardbx-logcollector")

	// Probe components
	var ds *appsv1.DaemonSet
	dsObj, dsErr := clientset.AppsV1().DaemonSets(ns).Get(c.Request.Context(), "filebeat", metav1.GetOptions{})
	if dsErr == nil {
		ds = dsObj
	}
	var dep *appsv1.Deployment
	depObj, depErr := clientset.AppsV1().Deployments(ns).Get(c.Request.Context(), "logstash", metav1.GetOptions{})
	if depErr == nil {
		dep = depObj
	}
	// Pipeline config
	var cm *corev1.ConfigMap
	cmObj, _ := clientset.CoreV1().ConfigMaps(ns).Get(c.Request.Context(), "logstash-pipeline", metav1.GetOptions{})
	if cmObj != nil {
		cm = cmObj
	}

	existsFB := ds != nil
	existsLS := dep != nil
	readyFB := int32(0)
	desiredFB := int32(0)
	if ds != nil {
		readyFB = ds.Status.NumberReady
		desiredFB = ds.Status.DesiredNumberScheduled
	}
	readyLS := int32(0)
	desiredLS := int32(0)
	if dep != nil {
		readyLS = dep.Status.ReadyReplicas
		desiredLS = dep.Status.Replicas
	}

	// Pod-level details: CrashLoopBackOff and frequent restarts (flapping)
	var fbPodName, fbReason, fbLastReason string
	var fbRestarts int32
	if existsFB {
		if pods, err := clientset.CoreV1().Pods(ns).List(c.Request.Context(), metav1.ListOptions{LabelSelector: "app=filebeat"}); err == nil {
			for i := range pods.Items {
				p := pods.Items[i]
				for _, cs := range p.Status.ContainerStatuses {
					if cs.RestartCount > fbRestarts {
						fbRestarts = cs.RestartCount
						fbPodName = p.Name
						if cs.LastTerminationState.Terminated != nil {
							fbLastReason = cs.LastTerminationState.Terminated.Reason
						}
					}
					if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
						fbReason = cs.State.Waiting.Reason
					}
				}
			}
		}
	}
	var lsPodName, lsReason, lsLastReason string
	var lsRestarts int32
	if existsLS {
		if pods, err := clientset.CoreV1().Pods(ns).List(c.Request.Context(), metav1.ListOptions{LabelSelector: "app=logstash"}); err == nil {
			for i := range pods.Items {
				p := pods.Items[i]
				for _, cs := range p.Status.ContainerStatuses {
					if cs.RestartCount > lsRestarts {
						lsRestarts = cs.RestartCount
						lsPodName = p.Name
						if cs.LastTerminationState.Terminated != nil {
							lsLastReason = cs.LastTerminationState.Terminated.Reason
						}
					}
					if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
						lsReason = cs.State.Waiting.Reason
					}
				}
			}
		}
	}

	state := "not_installed"
	if existsFB || existsLS {
		state = "degraded"
		if existsFB && existsLS && desiredFB > 0 && desiredLS > 0 && readyFB == desiredFB && readyLS == desiredLS {
			state = "running"
		}
	}

	// thresholds for flapping
	const restartFlappingThreshold int32 = 5

	fbStatus := "not_found"
	if existsFB {
		if fbReason == "CrashLoopBackOff" {
			fbStatus = "crashloop"
		} else if desiredFB > 0 && readyFB == desiredFB {
			if fbRestarts >= restartFlappingThreshold {
				fbStatus = "flapping"
			} else {
				fbStatus = "running"
			}
		} else {
			fbStatus = "error"
		}
	}
	lsStatus := "not_found"
	if existsLS {
		if lsReason == "CrashLoopBackOff" {
			lsStatus = "crashloop"
		} else if desiredLS > 0 && readyLS == desiredLS {
			if lsRestarts >= restartFlappingThreshold {
				lsStatus = "flapping"
			} else {
				lsStatus = "running"
			}
		} else {
			lsStatus = "error"
		}
	}

	resp := gin.H{
		"namespace": ns,
		"components": gin.H{
			"filebeat": gin.H{
				"status":   fbStatus,
				"replicas": gin.H{"ready": readyFB, "total": desiredFB},
				"exists":   existsFB,
				"error":    errString(dsErr),
			},
			"logstash": gin.H{
				"status":   lsStatus,
				"replicas": gin.H{"ready": readyLS, "total": desiredLS},
				"exists":   existsLS,
				"error":    errString(depErr),
			},
		},
		"pipelineConfigMapExists": cm != nil,
		"state":                   state,
		"status":                  state,
	}
	if existsFB {
		resp["components"].(gin.H)["filebeat"].(gin.H)["pod"] = gin.H{"name": fbPodName, "restarts": fbRestarts, "lastReason": fbLastReason, "crashReason": fbReason}
	}
	if existsLS {
		resp["components"].(gin.H)["logstash"].(gin.H)["pod"] = gin.H{"name": lsPodName, "restarts": lsRestarts, "lastReason": lsLastReason, "crashReason": lsReason}
	}
	if state == "not_installed" {
		resp["installHint"] = "helm install polardbx-logcollector charts/polardbx-logcollector -n polardbx-logcollector --create-namespace"
	}
	c.JSON(http.StatusOK, resp)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
