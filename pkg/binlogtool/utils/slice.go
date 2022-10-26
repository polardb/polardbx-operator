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

package utils

import (
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

func ConvertSlice[T any, R any](s []T, f func(T) R) []R {
	if s == nil {
		return nil
	}

	r := make([]R, len(s))
	for i, t := range s {
		r[i] = f(t)
	}

	return r
}

func ConvertAndConcatenateSlice[T any, R any](s []T, f func(T) []R) []R {
	if s == nil {
		return nil
	}

	r := make([]R, 0)
	for _, t := range s {
		r = append(r, f(t)...)
	}

	return r
}

func DistinctSlice[T comparable](s []T) []T {
	m := make(map[T]int)
	for _, x := range s {
		m[x] = 1
	}
	return MapKeys(m)
}

func SortedSlice[T constraints.Ordered](s []T) []T {
	slices.Sort(s)
	return s
}

func FilterSlice[T any](s []T, pred func(*T) bool) []T {
	r := make([]T, 0)
	for _, x := range s {
		if pred(&x) {
			r = append(r, x)
		}
	}
	return r
}
