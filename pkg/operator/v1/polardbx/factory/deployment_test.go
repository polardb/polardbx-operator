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
