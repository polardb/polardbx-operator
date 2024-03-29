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

package e2e

import (
	"os"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	// test sources
	_ "github.com/alibaba/polardbx-operator/test/e2e/polardbxcluster"
	_ "github.com/alibaba/polardbx-operator/test/e2e/polardbxmonitor"
	_ "github.com/alibaba/polardbx-operator/test/e2e/polardbxparameter"
	_ "github.com/alibaba/polardbx-operator/test/e2e/xstore"
	"github.com/alibaba/polardbx-operator/test/framework"
)

func TestMain(m *testing.M) {
	framework.RegisterFlagsAndParse()

	os.Exit(m.Run())
}

func TestE2E(t *testing.T) {
	RunE2ETests(t)
}

func RunE2ETests(t *testing.T) {
	gomega.RegisterFailHandler(framework.Fail)

	ginkgo.RunSpecs(t, "PolarDB-X operator e2e suite")
}
