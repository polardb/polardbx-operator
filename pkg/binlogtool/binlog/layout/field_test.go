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

package layout

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"golang.org/x/exp/constraints"
)

func TestNumberField_SizeRange(t *testing.T) {
	testcases := map[string]struct {
		Field
		Size int
	}{
		"uint8":   {Field: NumberField[uint8]{}, Size: 1},
		"int8":    {Field: NumberField[int8]{}, Size: 1},
		"uint16":  {Field: NumberField[uint16]{}, Size: 2},
		"int16":   {Field: NumberField[int16]{}, Size: 2},
		"uint32":  {Field: NumberField[uint32]{}, Size: 4},
		"int32":   {Field: NumberField[int32]{}, Size: 4},
		"uint64":  {Field: NumberField[uint64]{}, Size: 8},
		"int64":   {Field: NumberField[int64]{}, Size: 8},
		"float32": {Field: NumberField[float32]{}, Size: 4},
		"float64": {Field: NumberField[float64]{}, Size: 8},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			s, _ := tc.SizeRange()
			if s != tc.Size {
				t.Fatalf("%t's size not match, expect %d, but is %d", tc.Field, tc.Size, s)
			}
		})
	}
}

func testNumberExtraction[T constraints.Integer | constraints.Float](t *testing.T, expect T) {
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, expect)
	fmt.Printf("%T %v in LE: %s\n", expect, expect, base64.StdEncoding.EncodeToString(buf.Bytes()))

	var extract T
	f := Number(&extract)
	if err := f.FromStream(buf); err != nil {
		t.Fatal(err)
	}

	if extract != extract {
		t.Fatalf("not equals")
	}
}

func TestNumberField_FromBytes(t *testing.T) {
	testNumberExtraction(t, float32(1.0))
	testNumberExtraction(t, float32(0.0))
	testNumberExtraction(t, float32(1.1))
	testNumberExtraction(t, float32(-1.0))
	testNumberExtraction(t, float32(math.MaxFloat32))

	testNumberExtraction(t, float64(1.0))
	testNumberExtraction(t, float64(0.0))
	testNumberExtraction(t, float64(1.1))
	testNumberExtraction(t, float64(-1.0))
	testNumberExtraction(t, float64(math.MaxFloat32))
	testNumberExtraction(t, float64(math.MaxFloat64))
}
