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

package polardbxparameter

import (
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

	pxcmanager "github.com/alibaba/polardbx-operator/test/e2e/polardbxcluster/lifecycle"
	"github.com/alibaba/polardbx-operator/test/framework"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
	pxpframework "github.com/alibaba/polardbx-operator/test/framework/polardbxparameter"
	"github.com/onsi/ginkgo"
	"k8s.io/apimachinery/pkg/types"
)

var _ = ginkgo.Describe("[PolarDBXParameter]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)
	var cnParamTemplateList, dnParamTemplateList []polardbxv1.TemplateParams
	var cnParamList, dnParamList []polardbxv1.Params
	var cnRestartParamList, dnRestartParamList []polardbxv1.Params

	ginkgo.BeforeEach(func() {
		cnParamTemplateList = []polardbxv1.TemplateParams{
			{
				Name:               "CONN_POOL_BLOCK_TIMEOUT",
				DefaultValue:       "5000",
				Mode:               "readwrite",
				Restart:            false,
				Unit:               "INT",
				DivisibilityFactor: 1,
				Optional:           "[1000-60000]",
			},
			{
				Name:               "ENABLE_COROUTINE",
				DefaultValue:       "false",
				Mode:               "readwrite",
				Restart:            true,
				Unit:               "STRING",
				DivisibilityFactor: 1,
				Optional:           "[true|false]",
			},
		}
		dnParamTemplateList = []polardbxv1.TemplateParams{
			{
				Name:               "auto_increment_increment",
				DefaultValue:       "1",
				Mode:               "readwrite",
				Restart:            false,
				Unit:               "INT",
				DivisibilityFactor: 1,
				Optional:           "[1-65535]",
			},
			{
				Name:               "back_log",
				DefaultValue:       "3000",
				Mode:               "readwrite",
				Restart:            true,
				Unit:               "INT",
				DivisibilityFactor: 1,
				Optional:           "[0-65535]",
			},
		}
		cnParamList = []polardbxv1.Params{
			{
				Name:  "CONN_POOL_BLOCK_TIMEOUT",
				Value: "10000",
			},
		}
		dnParamList = []polardbxv1.Params{
			{
				Name:  "auto_increment_increment",
				Value: "10",
			},
		}
		cnRestartParamList = []polardbxv1.Params{
			//{
			//	Name:  "ENABLE_COROUTINE",
			//	Value: intstr.IntOrString{Type: intstr.String, StrVal: "true"},
			//},
			{
				Name:  "CONN_POOL_BLOCK_TIMEOUT",
				Value: "10000",
			},
		}
		dnRestartParamList = []polardbxv1.Params{
			{
				Name:  "back_log",
				Value: "300",
			},
		}
	})

	ginkgo.It("parameter template", func() {
		pxc := pxcframework.NewPolarDBXCluster(
			"e2e-test-parameter-template",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.ParameterTemplate("e2e-test-param-template"),
			pxcframework.TopologyModeGuide("quick-start-paxos"),
		)
		pxpt := pxpframework.NewPolarDBXParameterTemplate(
			"e2e-test-param-template",
			pxc.Namespace,
			pxpframework.SetTemplateNodeType("cn", cnParamTemplateList),
			pxpframework.SetTemplateNodeType("dn", dnParamTemplateList),
		)

		// Always run clean up to make sure objects are cleaned.
		defer pxcmanager.DeletePolarDBXClusterAndWaitUntilItDisappear(f, pxc, 1*time.Minute)
		defer DeletePolarDBXParameterTemplateAndWaitUntilItDisappear(f, pxpt, 1*time.Minute)

		// create parameter template
		CreatePolarDBXParameterTemplate(f, pxpt, 1*time.Minute)

		// Do create and verify.
		pxcmanager.CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, pxc, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: pxc.Name, Namespace: f.Namespace,
		}, pxc))

		cnExpectedParams := make(map[string]string)
		for _, p := range cnParamTemplateList {
			cnExpectedParams[p.Name] = p.DefaultValue
		}
		dnExpectedParams := make(map[string]string)
		for _, p := range dnParamTemplateList {
			dnExpectedParams[p.Name] = p.DefaultValue
		}

		exp := pxcframework.NewExpectation(f, pxc)
		exp.ExpectCNParameterPersistenceOk(cnExpectedParams)
		exp.ExpectDNParameterPersistenceOk(dnExpectedParams, false)
	})

	ginkgo.It("read-write parameters", func() {
		pxc := pxcframework.NewPolarDBXCluster(
			"e2e-test-parameter-rw",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
			pxcframework.ParameterTemplate("e2e-test-param-template-rw"),
		)
		pxp := pxpframework.NewPolarDBXParameter(
			"e2e-test-param-rw",
			pxc.Namespace,
			pxpframework.SetLabel("parameter", "dynamic"),
			pxpframework.SetTemplateNameAndClusterName("e2e-test-param-template-rw", pxc.Name),
			pxpframework.SetNodeType("cn", "e2e-test-cn", pxpframework.RollingRestart, cnParamList),
			pxpframework.SetNodeType("dn", "e2e-test-dn", pxpframework.RollingRestart, dnParamList),
		)
		pxpt := pxpframework.NewPolarDBXParameterTemplate(
			"e2e-test-param-template-rw",
			pxc.Namespace,
			pxpframework.SetTemplateNodeType("cn", cnParamTemplateList),
			pxpframework.SetTemplateNodeType("dn", dnParamTemplateList),
		)

		// Always run clean up to make sure objects are cleaned.
		defer pxcmanager.DeletePolarDBXClusterAndWaitUntilItDisappear(f, pxc, 1*time.Minute)
		defer DeletePolarDBXParameterAndWaitUntilItDisappear(f, pxp, 1*time.Minute)
		defer DeletePolarDBXParameterTemplateAndWaitUntilItDisappear(f, pxpt, 1*time.Minute)

		// create parameter template
		CreatePolarDBXParameterTemplate(f, pxpt, 1*time.Minute)

		// Do create and verify.
		pxcmanager.CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, pxc, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: pxc.Name, Namespace: f.Namespace,
		}, pxc))

		// create dynamic parameter parameter
		CreatePolarDBXParameterAndWaitUntilRunningOrFail(f, pxp, 5*time.Minute)

		cnExpectedParams := make(map[string]string)
		for _, p := range cnParamList {
			cnExpectedParams[p.Name] = p.Value
		}
		dnExpectedParams := make(map[string]string)
		for _, p := range dnParamList {
			dnExpectedParams[p.Name] = p.Value
		}

		exp := pxcframework.NewExpectation(f, pxc)
		exp.ExpectCNParameterPersistenceOk(cnExpectedParams)
		exp.ExpectDNParameterPersistenceOk(dnExpectedParams, true)
	})

	ginkgo.It("restart parameters", func() {
		pxc := pxcframework.NewPolarDBXCluster(
			"e2e-test-parameter-restart",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start-paxos"),
			pxcframework.ParameterTemplate("e2e-test-param-template-restart"),
		)
		pxp := pxpframework.NewPolarDBXParameter(
			"e2e-test-param-restart",
			pxc.Namespace,
			pxpframework.SetLabel("parameter", "dynamic"),
			pxpframework.SetTemplateNameAndClusterName("e2e-test-param-template-restart", pxc.Name),
			pxpframework.SetNodeType("cn", "e2e-test-cn", pxpframework.RollingRestart, cnRestartParamList),
			pxpframework.SetNodeType("dn", "e2e-test-dn", pxpframework.RollingRestart, dnRestartParamList),
		)
		pxpt := pxpframework.NewPolarDBXParameterTemplate(
			"e2e-test-param-template-restart",
			pxc.Namespace,
			pxpframework.SetTemplateNodeType("cn", cnParamTemplateList),
			pxpframework.SetTemplateNodeType("dn", dnParamTemplateList),
		)

		// Always run clean up to make sure objects are cleaned.
		defer pxcmanager.DeletePolarDBXClusterAndWaitUntilItDisappear(f, pxc, 1*time.Minute)
		defer DeletePolarDBXParameterAndWaitUntilItDisappear(f, pxp, 1*time.Minute)
		defer DeletePolarDBXParameterTemplateAndWaitUntilItDisappear(f, pxpt, 1*time.Minute)

		// create parameter template
		CreatePolarDBXParameterTemplate(f, pxpt, 1*time.Minute)

		// Do create and verify.
		pxcmanager.CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, pxc, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: pxc.Name, Namespace: f.Namespace,
		}, pxc))

		CreatePolarDBXParameterAndWaitUntilRunningOrFail(f, pxp, 5*time.Minute)

		cnExpectedParams := make(map[string]string)
		for _, p := range cnRestartParamList {
			cnExpectedParams[p.Name] = p.Value
		}
		dnExpectedParams := make(map[string]string)
		for _, p := range dnRestartParamList {
			dnExpectedParams[p.Name] = p.Value
		}

		exp := pxcframework.NewExpectation(f, pxc)
		exp.ExpectRestartOk(f.Client, pxc.Name, pxc.Namespace, 10*time.Minute)
		exp.ExpectCNParameterPersistenceOk(cnExpectedParams)
		exp.ExpectDNParameterPersistenceOk(dnExpectedParams, true)
	})

})
