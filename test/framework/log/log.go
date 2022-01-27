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

package log

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
)

func log(level string, format string, args ...interface{}) {
	ts := time.Now().Format(time.StampMilli)
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, ts+": "+level+": "+format+"\n", args...)
}

func Logf(format string, args ...interface{}) {
	log("INFO", format, args...)
}

func Failf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log("FAIL", msg)
	ts := time.Now().Format(time.StampMilli)
	ginkgo.Fail(ts+": "+msg, 1)
}
