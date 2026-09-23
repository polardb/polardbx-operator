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

package factory

import (
	"fmt"
	"github.com/onsi/gomega"
	"testing"
)

func TestSplitMatchRules(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	matchingRulesMap := map[string]matchingRule{
		"rule1": {
			replicas: 1,
		},
		"rule2": {
			replicas: 2,
		},
		"rule3": {
			replicas: 3,
		},
	}
	resultMatchRules := SplitMatchRules(matchingRulesMap, 3, 3)
	fmt.Println(resultMatchRules)
	g.Expect(len(resultMatchRules), 2)
}
