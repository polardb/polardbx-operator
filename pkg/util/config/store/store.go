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

package store

import "sync/atomic"

type Store struct {
	val atomic.Value
}

func (s *Store) Get() interface{} {
	return s.val.Load()
}

func (s *Store) Update(val interface{}) interface{} {
	return s.val.Swap(val)
}

func (s *Store) Cached(lazy bool) *CachedStore {
	if lazy {
		return &CachedStore{
			Store:  s,
			valPtr: nil,
		}
	} else {
		val := s.Get()
		return &CachedStore{
			Store:  nil,
			valPtr: &val,
		}
	}
}

type CachedStore struct {
	*Store
	valPtr *interface{}
}

func (s *CachedStore) Get() interface{} {
	if s.valPtr != nil {
		return *s.valPtr
	}
	val := s.Store.Get()
	s.valPtr = &val
	return *s.valPtr
}

func (s *CachedStore) Cached(lazy bool) *CachedStore {
	if lazy || s.valPtr != nil {
		return s
	} else {
		val := s.Store.Get()
		return &CachedStore{
			Store:  nil,
			valPtr: &val,
		}
	}
}
