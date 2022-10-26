package _map

import (
	. "github.com/onsi/gomega"
	"testing"
)

func TestMergeMapOverwriteFalse1(t *testing.T) {
	g := NewGomegaWithT(t)
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m2": "m1",
	}
	result := MergeMap(map1, map2, false).(map[string]string)
	g.Expect(result["m1"]).Should(BeEquivalentTo("m2"))
	g.Expect(result["m2"]).Should(BeEquivalentTo("m1"))
}

func TestMergeMapOverwriteFalse2(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeEquivalentTo("overwriting key is not allowed"))
	}()
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m1": "m1",
	}
	MergeMap(map1, map2, false)
}

func TestMapOverwriteTrue(t *testing.T) {
	g := NewGomegaWithT(t)
	defer func() {
		err := recover()
		g.Expect(err).Should(BeNil())
	}()
	map1 := map[string]string{
		"m1": "m2",
	}
	map2 := map[string]string{
		"m1": "m1",
	}
	MergeMap(map1, map2, true)
}
