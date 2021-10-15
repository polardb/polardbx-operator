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
	"fmt"
	"os"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/exporter/cdc"
	"github.com/alibaba/polardbx-operator/pkg/exporter/polardbx"
)

// Common arguments
var (
	listenAddr  string
	metricsPath string
	targetPort  int
	targetType  string
)

// Arguments for PolarDB-X CN
var (
	enableProcessCollector bool
	enableJvmStatCollector bool
)

func init() {
	flag.StringVar(&listenAddr, "web.listen-addr", ":8081", "Address to listen on for web interface and telemetry.")
	flag.StringVar(&metricsPath, "web.metrics-path", "/metrics", "Path under which to expose metrics.")
	flag.IntVar(&targetPort, "target.port", 0, "Target port to collect metrics, default: 3406 for CN, 9090 for CDC.")
	flag.StringVar(&targetType, "target.type", "", "Target collector type, values: \"CN\" or \"CDC\".")
	flag.BoolVar(&enableProcessCollector, "collectors.process", false, "Enable process collector, only valid when target type is CN.")
	flag.BoolVar(&enableJvmStatCollector, "collectors.jvm", false, "Enable JVM collector, only valid when target type is CN.")
}

func main() {
	flag.Parse()

	targetType = strings.ToUpper(targetType)
	if targetType != "CN" && targetType != "CDC" {
		fmt.Println("Unrecognized target type: " + targetType)
		os.Exit(-1)
	}

	// Set default port
	if targetPort == 0 {
		if targetType == "CN" {
			targetPort = 3406
		} else if targetType == "CDC" {
			targetPort = 9090
		}
	}

	// Start service
	if targetType == "CN" {
		polardbx.Start(listenAddr, metricsPath, targetPort, enableProcessCollector, enableJvmStatCollector)
	} else {
		cdc.Start(listenAddr, metricsPath, targetPort)
	}
}
