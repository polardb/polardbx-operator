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

package security

import (
	"testing"

	"github.com/onsi/gomega"
)

func TestSha1Hash(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	hash, err := Sha1Hash("84r7xlzc22nj")
	g.Expect(err).To(gomega.BeNil())
	g.Expect(hash).To(gomega.Equal("1abc69f0c63fe079080ee6f8246f14f50a694568"))
}
