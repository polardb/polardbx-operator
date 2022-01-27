/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package polardbxcluster

import (
	"context"
	"database/sql"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/k8s/helper/selector"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	"github.com/alibaba/polardbx-operator/pkg/util/database"
	"github.com/alibaba/polardbx-operator/pkg/util/network"
	"github.com/alibaba/polardbx-operator/test/framework"
	"github.com/alibaba/polardbx-operator/test/framework/common"
	"github.com/alibaba/polardbx-operator/test/framework/local"
)

func ExpectBeInPhase(polardbxcluster *polardbxv1.PolarDBXCluster, phase polardbxv1polardbx.Phase) {
	gomega.Expect(polardbxcluster.Status.Phase).To(gomega.BeEquivalentTo(phase), "phase not match")
}

type Expectation struct {
	kubeconfig string
	ctx        context.Context
	c          client.Client
	obj        *polardbxv1.PolarDBXCluster
}

func NewExpectation(f *framework.Framework, obj *polardbxv1.PolarDBXCluster) *Expectation {
	return &Expectation{
		ctx:        f.Ctx,
		c:          f.Client,
		kubeconfig: f.KubeConfigFile,
		obj:        obj,
	}
}

func (e *Expectation) startGmsPortForward(ctx context.Context, localPort int) error {
	errC := make(chan error, 1)

	go func() {
		ns := e.obj.Namespace
		svc := convention.NewGMSName(e.obj)
		kill, err := common.StartPortForward(e.kubeconfig, svc, ns, "mysql", localPort)

		// Pass error to channel.
		errC <- err
		close(errC)
		if err != nil {
			return
		}

		defer kill()

		<-ctx.Done()
	}()

	return <-errC
}

func (e *Expectation) startPortForward(ctx context.Context, localPort int) error {
	errC := make(chan error, 1)

	go func() {
		ns := e.obj.Namespace
		svc := e.obj.Spec.ServiceName
		kill, err := common.StartPortForward(e.kubeconfig, svc, ns, "mysql", localPort)

		// Pass error to channel.
		errC <- err
		close(errC)
		if err != nil {
			return
		}

		defer kill()

		<-ctx.Done()
	}()

	return <-errC
}

func (e *Expectation) ExpectQueriesOk(f func(ctx context.Context, db *sql.DB) error, explain ...interface{}) {
	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")

	db, err := database.OpenMySQLDB(&database.MySQLDataSource{
		Host:     "localhost",
		Port:     localPort,
		Username: "polardbx_root",
		Timeout:  1,
	})

	err = f(ctx, db)
	common.ExpectNoError(err, explain...)
}

func (e *Expectation) ExpectBasicFunctionalities() {

}

func (e *Expectation) ExpectAccessible() {
	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection
func (e *Expectation) ExpectCNDynamicConfigurationsOk() {
	expectedConfigs := e.obj.Spec.Config.CN.Dynamic

	if len(expectedConfigs) == 0 {
		return
	}

	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startGmsPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")

	// Expect root to connect localhost
	db, err := database.OpenMySQLDB(&database.MySQLDataSource{
		Host:     "localhost",
		Port:     localPort,
		Username: "root",
		Database: "polardbx_meta_db",
		Timeout:  2,
	})
	common.ExpectNoError(err, "failed to connect to gms")
	defer database.DeferClose(db)

	rs, err := db.QueryContext(ctx, "select * from inst_config where inst_id = ?", e.obj.Name)
	common.ExpectNoError(err, "failed to query dynamic configs")
	defer database.DeferClose(rs)

	var key, val string
	dest := map[string]interface{}{
		"param_key": &key,
		"param_val": &val,
	}
	nowConfigs := make(map[string]string)
	for rs.Next() {
		err = database.Scan(rs, dest, database.ScanOpt{CaseInsensitive: true})
		common.ExpectNoError(err, "error when scan config table")
		nowConfigs[key] = val
	}

	// Test if now contains the expected
	for k, v := range expectedConfigs {
		gomega.Expect(nowConfigs).To(gomega.HaveKeyWithValue(k, v.String()), "config not found or match")
	}
}

func (e *Expectation) ExpectConfigurationsOk() {
	e.ExpectCNDynamicConfigurationsOk()
}

func (e *Expectation) ExpectSecurityTLSNotOk() {
	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")

	// Test TLS connection.
	db, err := database.OpenMySQLDB(&database.MySQLDataSource{
		Host:     "localhost",
		Port:     localPort,
		Username: "polardbx_root",
		Timeout:  1,
		SSL:      "skip-verify",
	})
	common.ExpectNoError(err, "failed to connect")
	defer db.Close()

	_, err = db.QueryContext(ctx, "select 1")
	common.ExpectErr(err, "err should be tls not enabled")
}

func (e *Expectation) ExpectSecurityTLSOk() {
	if !helper.IsTLSEnabled(e.obj) {
		return
	}

	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")

	// Test TLS connection.
	db, err := database.OpenMySQLDB(&database.MySQLDataSource{
		Host:     "localhost",
		Port:     localPort,
		Username: "polardbx_root",
		Timeout:  1,
		SSL:      "skip-verify",
	})
	common.ExpectNoError(err, "failed to connect")
	defer db.Close()

	_, err = db.QueryContext(ctx, "select 1")
	common.ExpectNoError(err, "query via tls should be ok")
}

func (e *Expectation) expectsAccountsOk(accounts map[string]string) {
	localPort := local.AcquireLocalPort()
	defer local.ReleaseLocalPort(localPort)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.startPortForward(ctx, localPort)
	common.ExpectNoError(err, "failed to start port-forward")

	// Wait 10 second for port-forward to work.
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		childCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		err = network.TestTcpConnectivity(childCtx, "localhost", uint16(localPort))
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	common.ExpectNoError(err, "not connectable")

	// Test accounts.
	for username, password := range accounts {
		db, err := database.OpenMySQLDB(&database.MySQLDataSource{
			Host:     "localhost",
			Port:     localPort,
			Username: username,
			Password: password,
			Timeout:  1,
		})
		common.ExpectNoError(err, "failed to connect with "+username+":"+password)
		_ = db.Close()
	}
}

func (e *Expectation) ExpectAccountsOk() {
	// Read accounts from secret.
	var secret corev1.Secret
	err := e.c.Get(e.ctx, types.NamespacedName{
		Name:      convention.NewSecretName(e.obj, convention.SecretTypeAccount),
		Namespace: e.obj.Namespace,
	}, &secret)
	common.ExpectNoError(err, "fail to get account secret")

	accounts := make(map[string]string)
	for k, v := range secret.Data {
		accounts[k] = string(v)
	}
	e.expectsAccountsOk(accounts)
}

func (e *Expectation) ExpectServicesOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
	}
	ns := e.obj.Namespace

	expectServiceName := e.obj.Spec.ServiceName
	expectServiceType := e.obj.Spec.ServiceType

	var serviceList corev1.ServiceList
	common.ExpectNoError(e.c.List(e.ctx, &serviceList, client.InNamespace(ns), client.MatchingLabels(labels)))
	services := serviceList.Items

	e.ExpectOwnerReferenceCorrect(common.GetObjectsFromObjectListByLabels(services, map[string]string{"polardbx/role": "cn"})...)
	e.ExpectOwnerReferenceCorrect(common.GetObjectsFromObjectListByLabels(services, map[string]string{"polardbx/role": "cdc"})...)

	gomega.Expect(common.GetObjectsFromObjectListByLabels(services, map[string]string{
		"polardbx/role": "cn",
	})).To(gomega.HaveLen(2), "must be 2 services for role CN")
	mainSvcObj := common.GetObjectFromObjectList(services, expectServiceName)
	gomega.Expect(mainSvcObj).NotTo(gomega.BeNil(), "read-write service not found")
	mainService := mainSvcObj.(*corev1.Service)
	gomega.Expect(mainService.Spec.Type).To(gomega.BeEquivalentTo(expectServiceType), "read-write service should have type "+expectServiceName)
}

func (e *Expectation) ExpectSecretsOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
	}
	ns := e.obj.Namespace

	var secretList corev1.SecretList
	common.ExpectNoError(e.c.List(e.ctx, &secretList, client.InNamespace(ns), client.MatchingLabels(labels)))

	e.ExpectOwnerReferenceCorrect(common.GetObjectList(secretList.Items)...)

	secretMap := common.GetObjectMapFromObjectListByName(secretList.Items)
	gomega.Expect(secretMap).NotTo(gomega.BeEmpty(), "no secret found")
	common.ExpectHaveKeys(secretMap,
		convention.NewSecretName(e.obj, convention.SecretTypeAccount),
		convention.NewSecretName(e.obj, convention.SecretTypeSecurity),
	)
	accountSecret := secretMap[convention.NewSecretName(e.obj, convention.SecretTypeAccount)].(*corev1.Secret)
	gomega.Expect(accountSecret.Data).To(gomega.HaveKey("polardbx_root"), "must contain the password for root account")

	for _, priv := range e.obj.Spec.Privileges {
		if len(priv.Password) > 0 {
			gomega.Expect(accountSecret.Data).To(gomega.HaveKeyWithValue(priv.Username, priv.Password), "must contain the exact password for account "+priv.Username)
		} else {
			gomega.Expect(accountSecret.Data).To(gomega.HaveKey(priv.Username), "must contain the password for account "+priv.Username)
		}
	}

	securitySecret := secretMap[convention.NewSecretName(e.obj, convention.SecretTypeSecurity)].(*corev1.Secret)
	gomega.Expect(securitySecret.Data).To(gomega.HaveKey(convention.SecretKeyEncodeKey), "must contain the key for encode")
}

func (e *Expectation) expectDeploymentsMatchesRulesAndReplicas(deploys []appsv1.Deployment, nodeSelectors []polardbxv1polardbx.NodeSelectorItem,
	rules []polardbxv1polardbx.StatelessTopologyRuleItem, replicas int, role string) {
	sumReplicas := 0
	for _, deploy := range deploys {
		gomega.Expect(deploy.Spec.Replicas).NotTo(gomega.BeNil(), role, "invalid running deployment, replicas' nil")
		sumReplicas += int(*deploy.Spec.Replicas)
	}
	gomega.Expect(sumReplicas).To(gomega.BeEquivalentTo(replicas), role, "replicas should match")

	// Build group-deploy map.
	deployMap := make(map[string]*appsv1.Deployment)
	for i := range deploys {
		deploy := &deploys[i]
		group, ok := deploy.Labels["polardbx/group"]
		gomega.Expect(ok).To(gomega.BeTrue(), role, "group label not found: "+deploy.Name)
		deployMap[group] = deploy
	}

	// Build node selector map.
	nodeSelectorMap := make(map[string]*corev1.NodeSelector)
	for _, ns := range nodeSelectors {
		nodeSelectorMap[ns.Name] = &ns.NodeSelector
	}

	// Expect each rule matches a deployment
	for _, rule := range rules {
		deploy, ok := deployMap[rule.Name]
		gomega.Expect(ok).To(gomega.BeTrue(), role, "associate deploy not found for rule: "+rule.Name)

		// Skip replica check if it's default rule.
		if rule.Replicas != nil {
			expectReplicas, err := CalculateReplicas(replicas, *rule.Replicas)
			common.ExpectNoError(err, role, "failed to calculate replicas for rule: "+rule.Name)
			gomega.Expect(int(*deploy.Spec.Replicas)).To(gomega.BeEquivalentTo(expectReplicas), role, "associate deployment's replicas not match: "+rule.Name, deploy.Name)
		}

		template := deploy.Spec.Template

		if rule.NodeSelector != nil {
			ns := rule.NodeSelector.NodeSelector
			if ns == nil {
				if rule.NodeSelector.Reference != "" {
					ns = nodeSelectorMap[rule.NodeSelector.Reference]
				}
			}

			var nsInDeploy *corev1.NodeSelector
			if template.Spec.Affinity != nil && template.Spec.Affinity.NodeAffinity != nil {
				nsInDeploy = template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			}
			gomega.Expect(selector.DoesNodeSelectorCoversAnother(ns, nsInDeploy)).To(gomega.BeTrue(), role, "associate deployment's node selector not match: "+rule.Name, deploy.Name)
		}
	}
}

func (e *Expectation) expectDeploymentsMatchesTemplate(deploys []appsv1.Deployment, templateMatcher func(deployment *appsv1.Deployment)) {
	for _, deploy := range deploys {
		templateMatcher(&deploy)
	}
}

func (e *Expectation) ExpectCNDeploymentsOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
		"polardbx/role": "cn",
	}
	ns := e.obj.Namespace

	var deploymentList appsv1.DeploymentList
	framework.ExpectNoError(e.c.List(e.ctx, &deploymentList, client.InNamespace(ns), client.MatchingLabels(labels)))
	deployments := deploymentList.Items
	e.ExpectOwnerReferenceCorrect(common.GetObjectList(deployments)...)

	nodeSelectors := e.obj.Spec.Topology.Rules.Selectors
	cnRules := e.obj.Spec.Topology.Rules.Components.CN
	replicas := e.obj.Spec.Topology.Nodes.CN.Replicas
	template := e.obj.Spec.Topology.Nodes.CN.Template

	if replicas == 0 {
		gomega.Expect(deployments).To(gomega.BeEmpty(), "cn replicas is 0, should be empty")
	}

	gomega.Expect(deployments).NotTo(gomega.BeEmpty(), "cn replicas isn't 0, should not be empty")

	e.expectDeploymentsMatchesRulesAndReplicas(deployments, nodeSelectors, cnRules, int(replicas), "cn")
	e.expectDeploymentsMatchesTemplate(deployments, func(deploy *appsv1.Deployment) {
		podSpec := &deploy.Spec.Template.Spec
		gomega.Expect(podSpec.HostNetwork).To(gomega.BeEquivalentTo(template.HostNetwork), "host network should be the same as template: "+deploy.Name)
		engineContainer := k8shelper.GetContainerFromPodSpec(podSpec, "engine")
		gomega.Expect(engineContainer.Resources).To(gomega.BeEquivalentTo(template.Resources), "resources of engine container should be the same as template: "+deploy.Name)
		if template.Image != "" {
			gomega.Expect(engineContainer.Image).To(gomega.BeEquivalentTo(template.Image), "image of engine container should be the same as template if specified: "+deploy.Name)
		}
	})
}

func (e *Expectation) ExpectCDCDeploymentsOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
		"polardbx/role": "cdc",
	}
	ns := e.obj.Namespace

	var deploymentList appsv1.DeploymentList
	framework.ExpectNoError(e.c.List(e.ctx, &deploymentList, client.InNamespace(ns), client.MatchingLabels(labels)))
	deployments := deploymentList.Items
	e.ExpectOwnerReferenceCorrect(common.GetObjectList(deployments)...)

	cdcNode := e.obj.Spec.Topology.Nodes.CDC
	if cdcNode == nil || cdcNode.Replicas == 0 {
		gomega.Expect(deployments).To(gomega.BeEmpty(), "cdc not defined or replicas is 0, should be empty")
		return
	}

	gomega.Expect(deployments).NotTo(gomega.BeEmpty(), "cdc is defined, should not empty")

	nodeSelectors := e.obj.Spec.Topology.Rules.Selectors
	cdcRules := e.obj.Spec.Topology.Rules.Components.CDC
	replicas := cdcNode.Replicas
	template := cdcNode.Template

	e.expectDeploymentsMatchesRulesAndReplicas(deployments, nodeSelectors, cdcRules, int(replicas), "cdc")
	e.expectDeploymentsMatchesTemplate(deployments, func(deploy *appsv1.Deployment) {
		podSpec := &deploy.Spec.Template.Spec
		gomega.Expect(podSpec.HostNetwork).To(gomega.BeEquivalentTo(template.HostNetwork), "host network should be the same as template: "+deploy.Name)
		engineContainer := k8shelper.GetContainerFromPodSpec(podSpec, "engine")
		gomega.Expect(engineContainer.Resources).To(gomega.BeEquivalentTo(template.Resources), "resources of engine container should be the same as template: "+deploy.Name)
		if template.Image != "" {
			gomega.Expect(engineContainer.Image).To(gomega.BeEquivalentTo(template.Image), "image of engine container should be the same as template if specified: "+deploy.Name)
		}
	})
}

func (e *Expectation) ExpectDeploymentsOk() {
	e.ExpectCNDeploymentsOk()
	e.ExpectCDCDeploymentsOk()
}

func (e *Expectation) ExpectOwnerReferenceCorrect(subResources ...client.Object) {
	for _, r := range subResources {
		err := k8shelper.CheckControllerReference(r, e.obj)
		common.ExpectNoError(err, "owner reference should be correct", r.GetObjectKind().GroupVersionKind().String(), r.GetName())
	}
}

func (e *Expectation) ExpectConfigMapsOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
	}
	ns := e.obj.Namespace

	var configMapList corev1.ConfigMapList
	framework.ExpectNoError(e.c.List(e.ctx, &configMapList, client.InNamespace(ns), client.MatchingLabels(labels)))
	e.ExpectOwnerReferenceCorrect(common.GetObjectList(configMapList.Items)...)

	configMaps := common.GetObjectMapFromObjectListByName(configMapList.Items)
	gomega.Expect(configMaps).NotTo(gomega.BeEmpty(), "no config map found")
	framework.ExpectHaveKeys(configMaps,
		convention.NewConfigMapName(e.obj, convention.ConfigMapTypeConfig),
		convention.NewConfigMapName(e.obj, convention.ConfigMapTypeTask),
	)
}

func getNodeSelectorFromRef(ref *polardbxv1polardbx.NodeSelectorReference, nodeSelectors map[string]*corev1.NodeSelector) *corev1.NodeSelector {
	if ref == nil {
		return nil
	}
	if ref.NodeSelector != nil {
		return ref.NodeSelector
	}
	if ref.Reference != "" {
		return nodeSelectors[ref.Reference]
	}
	return nil
}

func (e *Expectation) expectXStoreToMatch(xs *polardbxv1.XStore, nodeSelectors []polardbxv1polardbx.NodeSelectorItem,
	rule *polardbxv1polardbx.XStoreTopologyRule, template *polardbxv1polardbx.XStoreTemplate) {
	defaultTemplate := xs.Spec.Topology.Template
	nodeSets := xs.Spec.Topology.NodeSets
	nodeSetMap := make(map[string]*polardbxv1xstore.NodeSet)
	replicasNow := 0
	for i := range nodeSets {
		ns := &nodeSets[i]
		replicasNow += int(ns.Replicas)
		nodeSetMap[ns.Name] = ns
	}

	// Build node selector map.
	nodeSelectorMap := make(map[string]*corev1.NodeSelector)
	for _, ns := range nodeSelectors {
		nodeSelectorMap[ns.Name] = &ns.NodeSelector
	}

	// Check rules.
	if rule != nil {
		if rule.Rolling != nil {
			replicas := int(rule.Rolling.Replicas)
			gomega.Expect(replicasNow).To(gomega.BeEquivalentTo(replicas), "replicas of xstore nodes should match: "+xs.Name)

			nodeSelector := getNodeSelectorFromRef(rule.Rolling.NodeSelector, nodeSelectorMap)

			for _, ns := range nodeSets {
				t := ns.Template
				if t == nil {
					t = &defaultTemplate
				}

				var nsSelector *corev1.NodeSelector = nil
				if t != nil && t.Spec.Affinity != nil && t.Spec.Affinity.NodeAffinity != nil {
					nsSelector = t.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
				}
				gomega.Expect(selector.DoesNodeSelectorCoversAnother(nodeSelector, nsSelector)).To(gomega.BeTrue(),
					"node selector in xstore not matches that in rule: "+xs.Name, ns.Name)
			}
		} else if len(rule.NodeSets) > 0 {
			for _, nsInRule := range rule.NodeSets {
				ns, ok := nodeSetMap[nsInRule.Name]
				gomega.Expect(ok).To(gomega.BeTrue(), "node set in rule not found in xstore: "+xs.Name, nsInRule.Name)
				gomega.Expect(ns.Role).To(gomega.BeEquivalentTo(nsInRule.Role), "node set's role in xstore not matches that in rule: "+xs.Name, ns.Name)
				gomega.Expect(ns.Replicas).To(gomega.BeEquivalentTo(nsInRule.Replicas), "node set's replicas in xstore not matches that in rule: "+xs.Name, ns.Name)

				nodeSelector := getNodeSelectorFromRef(nsInRule.NodeSelector, nodeSelectorMap)

				t := ns.Template
				if t == nil {
					t = &defaultTemplate
				}

				var nsSelector *corev1.NodeSelector = nil
				if t != nil && t.Spec.Affinity != nil && t.Spec.Affinity.NodeAffinity != nil {
					nsSelector = t.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
				}
				gomega.Expect(selector.DoesNodeSelectorCoversAnother(nodeSelector, nsSelector)).To(gomega.BeTrue(),
					"node selector in xstore not matches that in rule: "+xs.Name, ns.Name)
			}
		}
	}

	// Check template.
	for _, ns := range nodeSets {
		t := ns.Template
		if t == nil {
			t = &defaultTemplate
		}

		gomega.Expect(t.Spec.HostNetwork).To(gomega.BeEquivalentTo(template.HostNetwork), "host network should be equal to what in template: "+xs.Name, ns.Name)
		if template.Image != "" {
			gomega.Expect(t.Spec.Image).To(gomega.BeEquivalentTo(template.Image), "image should be equal to what in template if provided: "+xs.Name, ns.Name)
		}
		gomega.Expect(t.Spec.Resources).NotTo(gomega.BeNil())
		gomega.Expect(*t.Spec.Resources).To(gomega.BeEquivalentTo(template.Resources), "resources should be equal to what in template: "+xs.Name, ns.Name)
	}
}

func (e *Expectation) ExpectXStoresOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
	}
	ns := e.obj.Namespace

	var xstoreList polardbxv1.XStoreList
	framework.ExpectNoError(e.c.List(e.ctx, &xstoreList, client.InNamespace(ns), client.MatchingLabels(labels)))
	e.ExpectOwnerReferenceCorrect(common.GetObjectList(xstoreList.Items)...)

	xstoresByRole := common.MapObjectsFromObjectListByLabel(xstoreList.Items, "polardbx/role")
	shareGms := e.obj.Spec.ShareGMS
	nodeSelectors := e.obj.Spec.Topology.Rules.Selectors
	dnRule := e.obj.Spec.Topology.Rules.Components.DN
	dnNode := &e.obj.Spec.Topology.Nodes.DN
	dnReplicas := dnNode.Replicas

	if !shareGms {
		gomega.Expect(xstoresByRole).To(gomega.HaveLen(2), "must have 2 roles of xstores")
		gomega.Expect(xstoresByRole["gms"]).To(gomega.HaveLen(1), "1 gms")

		gmsStore := xstoresByRole["gms"][0]
		gmsRule := e.obj.Spec.Topology.Rules.Components.GMS
		if gmsRule == nil {
			gmsRule = dnRule
		}
		gmsTemplate := e.obj.Spec.Topology.Nodes.GMS.Template
		if gmsTemplate == nil {
			gmsTemplate = &dnNode.Template
		}
		e.expectXStoreToMatch(gmsStore.(*polardbxv1.XStore), nodeSelectors, gmsRule, gmsTemplate)
	} else {
		gomega.Expect(xstoresByRole).To(gomega.HaveLen(1), "must have 1 roles of xstores in share gms mode")
	}

	dnStores := xstoresByRole["dn"]
	gomega.Expect(dnStores).To(gomega.HaveLen(int(dnReplicas)), "must have match replicas of DN xstores")
	for _, xs := range dnStores {
		e.expectXStoreToMatch(xs.(*polardbxv1.XStore), nodeSelectors, dnRule, &dnNode.Template)
	}
}

func (e *Expectation) ExpectPodsOk() {
	labels := map[string]string{
		"polardbx/name": e.obj.Name,
	}
	ns := e.obj.Namespace

	var podList corev1.PodList
	framework.ExpectNoError(e.c.List(e.ctx, &podList, client.InNamespace(ns), client.MatchingLabels(labels)))
	pods := podList.Items
	gomega.Expect(pods).NotTo(gomega.BeEmpty(), "no pods found")
	podsByRole := common.MapObjectsFromObjectListByLabel(pods, "polardbx/role")

	gomega.Expect(podsByRole).To(gomega.HaveLen(4), "must be pods with 4 roles (when running)")

	framework.ExpectHaveKeys(podsByRole, "cn", "dn", "gms", "cdc")
	gomega.Expect(podsByRole["cn"]).To(gomega.HaveLen(1), "must be 1 cn pod")
	gomega.Expect(podsByRole["cdc"]).To(gomega.HaveLen(1), "must be 1 cdc pod")
	gomega.Expect(podsByRole["dn"]).To(gomega.HaveLen(1), "must be 1 dn pod")
	gomega.Expect(podsByRole["gms"]).To(gomega.HaveLen(1), "must be 1 gms pod")
}

func (e *Expectation) ExpectSubResourcesOk() {
	e.ExpectServicesOk()
	e.ExpectConfigMapsOk()
	e.ExpectSecretsOk()
	e.ExpectDeploymentsOk()
	e.ExpectXStoresOk()
	e.ExpectPodsOk()
}

func (e *Expectation) ExpectGenerationCatchUp() {
	gomega.Expect(e.obj.Status.ObservedGeneration).To(gomega.BeEquivalentTo(e.obj.Generation), "generation not catch up")
}

func (e *Expectation) ExpectRunning() {
	ExpectBeInPhase(e.obj, polardbxv1polardbx.PhaseRunning)
}

func (e *Expectation) ExpectObservableStatusUpdated() {
	status := e.obj.Status.StatusForPrint

	// Replica status.
	gomega.Expect(status.ReplicaStatus.GMS).NotTo(gomega.BeEmpty(), "gms replica status is empty")
	gomega.Expect(status.ReplicaStatus.CN).NotTo(gomega.BeEmpty(), "cn replica status is empty")
	gomega.Expect(status.ReplicaStatus.CDC).NotTo(gomega.BeEmpty(), "cdc replica status is empty")
	gomega.Expect(status.ReplicaStatus.DN).NotTo(gomega.BeEmpty(), "dn replica status is empty")

	// Storage size.
	gomega.Expect(status.StorageSize).NotTo(gomega.BeEmpty(), "storage size is empty")

	// Detailed version.
	gomega.Expect(status.DetailedVersion).NotTo(gomega.BeEmpty(), "detailed version is empty")
}

func (e *Expectation) ExpectAllOk() {
	e.ExpectRunning()
	e.ExpectGenerationCatchUp()
	e.ExpectObservableStatusUpdated()

	e.ExpectSubResourcesOk()
	e.ExpectAccessible()
	e.ExpectSecurityTLSOk()
	e.ExpectAccountsOk()
	e.ExpectConfigurationsOk()
	e.ExpectBasicFunctionalities()
}
