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

package main

import (
	"flag"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/probe"
)

var (
	port         int
	featureGates string
)

func init() {
	flag.IntVar(&port, "listen-port", 9090, "Listen port.")
	flag.StringVar(&featureGates, "feature-gates", "", "Feature gates to enable.")
	flag.Parse()

	// Enable feature gates.
	featuregate.SetupFeatureGates(strings.Split(strings.ReplaceAll(featureGates, " ", ""), ","))
}

func main() {
	proxy := probe.ProxyServer{}
	if err := proxy.Start(port); err != nil {
		panic(err)
	}
}
