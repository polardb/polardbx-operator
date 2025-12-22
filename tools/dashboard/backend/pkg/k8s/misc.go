package k8s

import (
	"context"
	"log"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ListPodsForPolarDBXCluster lists all pods for a PolarDBXCluster.
// Deprecated: Use ListPodsForPolarDBXClusterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPodsForPolarDBXCluster(c client.Client, namespace, clusterName string) ([]corev1.Pod, error) {
	log.Printf("WARNING: Using deprecated ListPodsForPolarDBXCluster without context. Please migrate to ListPodsForPolarDBXClusterWithContext.")
	return ListPodsForPolarDBXClusterWithContext(context.Background(), c, namespace, clusterName)
}

func ListPodsForPolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, clusterName string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	opts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{
			"polardbx/name": clusterName,
		},
	}
	if err := c.List(ctx, &podList, opts...); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// ---- PolarDBXParameter CRUD ----

// Deprecated: Use ListPolarDBXParametersWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXParameters(c client.Client, namespace string) ([]polardbxv1.PolarDBXParameter, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXParameters without context. Please migrate to ListPolarDBXParametersWithContext.")
	return ListPolarDBXParametersWithContext(context.Background(), c, namespace)
}

// Deprecated: Use GetPolarDBXParameterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXParameter(c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameter, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXParameter without context. Please migrate to GetPolarDBXParameterWithContext.")
	return GetPolarDBXParameterWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use CreatePolarDBXParameterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXParameter(c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXParameter without context. Please migrate to CreatePolarDBXParameterWithContext.")
	return CreatePolarDBXParameterWithContext(context.Background(), c, namespace, param)
}

// Deprecated: Use UpdatePolarDBXParameterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXParameter(c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXParameter without context. Please migrate to UpdatePolarDBXParameterWithContext.")
	return UpdatePolarDBXParameterWithContext(context.Background(), c, namespace, param)
}

// Deprecated: Use DeletePolarDBXParameterWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXParameter(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXParameter without context. Please migrate to DeletePolarDBXParameterWithContext.")
	return DeletePolarDBXParameterWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXParametersWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXParameter, error) {
	var list polardbxv1.PolarDBXParameterList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func GetPolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameter, error) {
	var obj polardbxv1.PolarDBXParameter
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func CreatePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if param.Namespace == "" {
		param.Namespace = namespace
	}
	if err := c.Create(ctx, param); err != nil {
		return nil, err
	}
	return param, nil
}

func UpdatePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if err := c.Update(ctx, param); err != nil {
		return nil, err
	}
	return param, nil
}

func DeletePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXParameter{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- PolarDBXParameterTemplate CRUD ----

// Deprecated: Use ListPolarDBXParameterTemplatesWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXParameterTemplates(c client.Client, namespace string) ([]polardbxv1.PolarDBXParameterTemplate, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXParameterTemplates without context. Please migrate to ListPolarDBXParameterTemplatesWithContext.")
	return ListPolarDBXParameterTemplatesWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreatePolarDBXParameterTemplateWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXParameterTemplate(c client.Client, namespace string, template *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXParameterTemplate without context. Please migrate to CreatePolarDBXParameterTemplateWithContext.")
	return CreatePolarDBXParameterTemplateWithContext(context.Background(), c, namespace, template)
}

// Deprecated: Use GetPolarDBXParameterTemplateWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXParameterTemplate(c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXParameterTemplate without context. Please migrate to GetPolarDBXParameterTemplateWithContext.")
	return GetPolarDBXParameterTemplateWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdatePolarDBXParameterTemplateWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXParameterTemplate(c client.Client, namespace string, template *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXParameterTemplate without context. Please migrate to UpdatePolarDBXParameterTemplateWithContext.")
	return UpdatePolarDBXParameterTemplateWithContext(context.Background(), c, namespace, template)
}

// Deprecated: Use DeletePolarDBXParameterTemplateWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXParameterTemplate(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXParameterTemplate without context. Please migrate to DeletePolarDBXParameterTemplateWithContext.")
	return DeletePolarDBXParameterTemplateWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXParameterTemplatesWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXParameterTemplate, error) {
	var list polardbxv1.PolarDBXParameterTemplateList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace string, tpl *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if tpl.Namespace == "" {
		tpl.Namespace = namespace
	}
	if err := c.Create(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}

func GetPolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	var obj polardbxv1.PolarDBXParameterTemplate
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func UpdatePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace string, tpl *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if err := c.Update(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}

func DeletePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXParameterTemplate{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- SystemTask CRUD ----

// Deprecated: Use ListSystemTasksWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListSystemTasks(c client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	log.Printf("WARNING: Using deprecated ListSystemTasks without context. Please migrate to ListSystemTasksWithContext.")
	return ListSystemTasksWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreateSystemTaskWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateSystemTask(c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	log.Printf("WARNING: Using deprecated CreateSystemTask without context. Please migrate to CreateSystemTaskWithContext.")
	return CreateSystemTaskWithContext(context.Background(), c, namespace, task)
}

// Deprecated: Use GetSystemTaskWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetSystemTask(c client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	log.Printf("WARNING: Using deprecated GetSystemTask without context. Please migrate to GetSystemTaskWithContext.")
	return GetSystemTaskWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdateSystemTaskWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateSystemTask(c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	log.Printf("WARNING: Using deprecated UpdateSystemTask without context. Please migrate to UpdateSystemTaskWithContext.")
	return UpdateSystemTaskWithContext(context.Background(), c, namespace, task)
}

// Deprecated: Use DeleteSystemTaskWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteSystemTask(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteSystemTask without context. Please migrate to DeleteSystemTaskWithContext.")
	return DeleteSystemTaskWithContext(context.Background(), c, namespace, name)
}

func ListSystemTasksWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	var list polardbxv1.SystemTaskList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreateSystemTaskWithContext(ctx context.Context, c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	if task.Namespace == "" {
		task.Namespace = namespace
	}
	if err := c.Create(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

func GetSystemTaskWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	var obj polardbxv1.SystemTask
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func UpdateSystemTaskWithContext(ctx context.Context, c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	if err := c.Update(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

func DeleteSystemTaskWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.SystemTask{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- PolarDBXParameterTemplate Management Functions ----
// (Already covered above with WithContext variants and deprecated wrappers)

// ---- PolarDBXMonitor CRUD ----

// Deprecated: Use ListPolarDBXMonitorsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXMonitors(c client.Client, namespace string) ([]polardbxv1.PolarDBXMonitor, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXMonitors without context. Please migrate to ListPolarDBXMonitorsWithContext.")
	return ListPolarDBXMonitorsWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreatePolarDBXMonitorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXMonitor(c client.Client, namespace string, monitor *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXMonitor without context. Please migrate to CreatePolarDBXMonitorWithContext.")
	return CreatePolarDBXMonitorWithContext(context.Background(), c, namespace, monitor)
}

// Deprecated: Use GetPolarDBXMonitorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXMonitor(c client.Client, namespace, name string) (*polardbxv1.PolarDBXMonitor, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXMonitor without context. Please migrate to GetPolarDBXMonitorWithContext.")
	return GetPolarDBXMonitorWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdatePolarDBXMonitorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXMonitor(c client.Client, namespace string, monitor *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXMonitor without context. Please migrate to UpdatePolarDBXMonitorWithContext.")
	return UpdatePolarDBXMonitorWithContext(context.Background(), c, namespace, monitor)
}

// Deprecated: Use DeletePolarDBXMonitorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXMonitor(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXMonitor without context. Please migrate to DeletePolarDBXMonitorWithContext.")
	return DeletePolarDBXMonitorWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXMonitorsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXMonitor, error) {
	var list polardbxv1.PolarDBXMonitorList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace string, m *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	if m.Namespace == "" {
		m.Namespace = namespace
	}
	if err := c.Create(ctx, m); err != nil {
		return nil, err
	}
	return m, nil
}

func GetPolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXMonitor, error) {
	var out polardbxv1.PolarDBXMonitor
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdatePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace string, m *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	if err := c.Update(ctx, m); err != nil {
		return nil, err
	}
	return m, nil
}

func DeletePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXMonitor{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- ClusterKnobs ----

// Deprecated: Use GetClusterKnobsListWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetClusterKnobsList(c client.Client) (*polardbxv1.PolarDBXClusterKnobsList, error) {
	log.Printf("WARNING: Using deprecated GetClusterKnobsList without context. Please migrate to GetClusterKnobsListWithContext.")
	return GetClusterKnobsListWithContext(context.Background(), c)
}

func GetClusterKnobsListWithContext(ctx context.Context, c client.Client) (*polardbxv1.PolarDBXClusterKnobsList, error) {
	knobsList := &polardbxv1.PolarDBXClusterKnobsList{}
	err := c.List(ctx, knobsList)
	return knobsList, err
}

// Deprecated: Use CreateClusterKnobsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreateClusterKnobs(c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	log.Printf("WARNING: Using deprecated CreateClusterKnobs without context. Please migrate to CreateClusterKnobsWithContext.")
	return CreateClusterKnobsWithContext(context.Background(), c, knobs)
}

func CreateClusterKnobsWithContext(ctx context.Context, c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	err := c.Create(ctx, knobs)
	return knobs, err
}

// Deprecated: Use GetClusterKnobsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetClusterKnobs(c client.Client, namespace, name string) (*polardbxv1.PolarDBXClusterKnobs, error) {
	log.Printf("WARNING: Using deprecated GetClusterKnobs without context. Please migrate to GetClusterKnobsWithContext.")
	return GetClusterKnobsWithContext(context.Background(), c, namespace, name)
}

func GetClusterKnobsWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXClusterKnobs, error) {
	knobs := &polardbxv1.PolarDBXClusterKnobs{}
	err := c.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, knobs)
	return knobs, err
}

// Deprecated: Use UpdateClusterKnobsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdateClusterKnobs(c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	log.Printf("WARNING: Using deprecated UpdateClusterKnobs without context. Please migrate to UpdateClusterKnobsWithContext.")
	return UpdateClusterKnobsWithContext(context.Background(), c, knobs)
}

func UpdateClusterKnobsWithContext(ctx context.Context, c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	err := c.Update(ctx, knobs)
	return knobs, err
}

// Deprecated: Use DeleteClusterKnobsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeleteClusterKnobs(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeleteClusterKnobs without context. Please migrate to DeleteClusterKnobsWithContext.")
	return DeleteClusterKnobsWithContext(context.Background(), c, namespace, name)
}

func DeleteClusterKnobsWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	knobs := &polardbxv1.PolarDBXClusterKnobs{}
	knobs.Name = name
	knobs.Namespace = namespace
	return c.Delete(ctx, knobs)
}

// ---- PolarDBXLogCollector CRUD ----

// Deprecated: Use ListPolarDBXLogCollectorsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXLogCollectors(c client.Client, namespace string) ([]polardbxv1.PolarDBXLogCollector, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXLogCollectors without context. Please migrate to ListPolarDBXLogCollectorsWithContext.")
	return ListPolarDBXLogCollectorsWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreatePolarDBXLogCollectorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXLogCollector(c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXLogCollector without context. Please migrate to CreatePolarDBXLogCollectorWithContext.")
	return CreatePolarDBXLogCollectorWithContext(context.Background(), c, namespace, obj)
}

// Deprecated: Use GetPolarDBXLogCollectorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXLogCollector(c client.Client, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXLogCollector without context. Please migrate to GetPolarDBXLogCollectorWithContext.")
	return GetPolarDBXLogCollectorWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdatePolarDBXLogCollectorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXLogCollector(c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXLogCollector without context. Please migrate to UpdatePolarDBXLogCollectorWithContext.")
	return UpdatePolarDBXLogCollectorWithContext(context.Background(), c, namespace, obj)
}

// Deprecated: Use DeletePolarDBXLogCollectorWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXLogCollector(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXLogCollector without context. Please migrate to DeletePolarDBXLogCollectorWithContext.")
	return DeletePolarDBXLogCollectorWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXLogCollectorsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXLogCollector, error) {
	var list polardbxv1.PolarDBXLogCollectorList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func GetPolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error) {
	var out polardbxv1.PolarDBXLogCollector
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdatePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func DeletePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXLogCollector{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}
