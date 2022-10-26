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

import "bytes"

type multiError struct {
	errs []error
}

func (e *multiError) Add(err error) {
	e.errs = append(e.errs, err)
}

func (e *multiError) Size() int {
	return len(e.errs)
}

func (e *multiError) Error() string {
	buf := &bytes.Buffer{}
	buf.WriteString("multiple errors: ")
	for i := 0; i < len(e.errs); i++ {
		buf.WriteString(e.errs[i].Error())
		if i != len(e.errs)-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func MultiError(errs ...error) *multiError {
	return &multiError{
		errs: errs,
	}
}
