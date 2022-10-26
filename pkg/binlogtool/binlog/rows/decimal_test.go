/*
Copyright 2022 Alibaba Group Holding Limited.

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

package rows

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestExtractNewDecimalFromBlock(t *testing.T) {
	// 1234567890.1234
	var pos = [7]byte{0x81, 0x0d, 0xfb, 0x38, 0xd2, 0x04, 0xd2}
	// -1234567890.1234
	var neg = [7]byte{0x7e, 0xf2, 0x04, 0xc7, 0x2d, 0xfb, 0x2d}

	x, _, err := ExtractNewDecimalFromBlock(pos[:], 14, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !x.Equal(decimal.New(12345678901234, -4)) {
		t.Fatal("not equal")
	}

	x, _, err = ExtractNewDecimalFromBlock(neg[:], 14, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !x.Equal(decimal.New(-12345678901234, -4)) {
		t.Fatal("not equal")
	}
}
