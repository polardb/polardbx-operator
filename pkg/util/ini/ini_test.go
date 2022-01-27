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

package ini

import (
	"strings"
	"testing"

	"github.com/onsi/gomega"
	"gopkg.in/ini.v1"
)

func TestCanonicalizeMyCnfFile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	r := strings.NewReader("core- = true")

	f, err := ini.LoadSources(ini.LoadOptions{
		AllowBooleanKeys:           true,
		AllowPythonMultilineValues: true,
		SpaceBeforeInlineComment:   true,
		PreserveSurroundedQuote:    true,
		IgnoreInlineComment:        true,
	}, r)

	g.Expect(err, nil)

	err = CanonicalizeMyCnfFile(f, true)

	g.Expect(err, nil)
}
