package configuration

import (
	"context"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"strings"
	"time"

	v12 "github.com/alibaba/polardbx-operator/api/v1"
	convention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	lifecycle "github.com/alibaba/polardbx-operator/test/e2e/polardbxcluster/lifecycle"
	"github.com/alibaba/polardbx-operator/test/framework"
	"github.com/alibaba/polardbx-operator/test/framework/common"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
	"github.com/onsi/ginkgo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = ginkgo.Describe("[PolarDBXCluster] [DnMyCnf:Modify]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)
	//f.Namespace = "development"
	ginkgo.It("modify dnMyCnf", func() {
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-mycnf-modify",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start-paxos"),
		)

		// Always run clean up to make sure objects are cleaned.
		defer lifecycle.DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		lifecycle.CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj.Spec.Config.DN.MycnfOverwrite = "table_open_cache: 1123"

		framework.ExpectNoError(f.Client.Update(f.Ctx, obj))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		pxcframework.WaitForPolarDBXClusterToInPhases(f.Client,
			obj.Name, obj.Namespace,
			[]polardbxv1polardbx.Phase{
				polardbxv1polardbx.PhaseRunning,
			},
			5*time.Minute)

		var xstores v12.XStoreList

		listOpts := []client.ListOption{
			client.InNamespace(obj.Namespace),
			client.MatchingLabels{"polardbx/role": "dn", "polardbx/name": obj.Name},
		}
		err := wait.PollImmediate(2*time.Second, 60*time.Minute, func() (bool, error) {
			err := f.Client.List(ctx, &xstores, listOpts...)
			if err != nil {
				return false, err
			}
			for _, xstore := range xstores.Items {
				var xstoreMyCnConfigMap v1.ConfigMap
				getConfigMapErr := f.Client.Get(ctx, types.NamespacedName{
					Namespace: xstore.Namespace,
					Name:      convention.NewConfigMapName(&xstore, convention.ConfigMapTypeConfig),
				}, &xstoreMyCnConfigMap)
				if getConfigMapErr != nil {
					return false, err
				}

				configData, ok := xstoreMyCnConfigMap.Data["my.cnf.override"]
				if !ok {
					return false, err
				}
				var paramValChanged bool = false
				for _, val := range strings.Split(configData, "\n") {
					lowerVal := strings.ToLower(val)
					if strings.Contains(lowerVal, "table_open_cache") {
						paramVal := strings.Trim(strings.Split(lowerVal, "=")[1], " ")
						if strings.EqualFold(paramVal, "1123") {
							paramValChanged = true
						}
					}
				}
				if !paramValChanged {
					return false, nil
				}
			}

			return true, nil
		})
		common.ExpectNoError(err, "param not changed")
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect all ok in running.
		pxcframework.NewExpectation(f, obj).ExpectAllOk(true)
	})
})
