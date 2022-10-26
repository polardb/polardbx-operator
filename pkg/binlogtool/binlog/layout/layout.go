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
	"fmt"
	"io"
	"math"
)

type Layout struct {
	fields []Field
}

func (l *Layout) IsVariant() bool {
	for _, f := range l.fields {
		if f.IsVariant() {
			return true
		}
	}
	return false
}

func (l *Layout) SizeRange() (int, int) {
	sl, sr := 0, 0
	for _, f := range l.fields {
		l, r := f.SizeRange()
		sl += l
		if sr == math.MaxInt || r == math.MaxInt {
			sr = math.MaxInt
		} else {
			sr += r
		}
	}
	return sl, sr
}

func (l *Layout) FromBlock(data []byte) (int, error) {
	off := 0

	for i, f := range l.fields {
		n, err := f.FromBlock(data[off:])
		if err != nil {
			return 0, fmt.Errorf("unable to extract field(%s, %d): %w", FieldTypeName(f.Type()), i, err)
		}
		off += n
	}

	return off, nil
}

func (l *Layout) FromStream(r io.Reader) error {
	for i, f := range l.fields {
		err := f.FromStream(r)
		if err != nil {
			return fmt.Errorf("unable to extract field(%s, %d): %w", FieldTypeName(f.Type()), i, err)
		}
	}
	return nil
}

func removeNils(s []Field) []Field {
	if s == nil {
		return nil
	}

	nilCnt := 0
	for _, x := range s {
		if x == nil {
			nilCnt++
		}
	}
	if nilCnt == 0 {
		return s
	}

	r := make([]Field, 0, len(s)-nilCnt)
	for _, x := range s {
		if x != nil {
			r = append(r, x)
		}
	}
	return r
}

func Decl(fields ...Field) *Layout {
	return &Layout{fields: removeNils(fields)}
}

func Block(fields ...Field) []Field {
	return fields
}

func If(p bool, field Field) Field {
	if p {
		return field
	}
	return nil
}

func IfBlock(p bool, fields ...Field) []Field {
	if p {
		return Block(fields...)
	}
	return nil
}

func Conditional(p bool, onTrue Field, onFalse Field) Field {
	if p {
		return onTrue
	}
	return onFalse
}

func ConditionalBlock(p bool, onTrue []Field, onFalse []Field) []Field {
	if p {
		return onTrue
	} else {
		return onFalse
	}
}
