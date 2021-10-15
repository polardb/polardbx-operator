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

package cdc

import (
	"net/http"
	"os"

	"github.com/prometheus/common/promlog"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
)

func Start(listenAddr, metricsPath string, cdcPort int) {
	loggerConfig := &promlog.Config{
		Level:  &promlog.AllowedLevel{},
		Format: &promlog.AllowedFormat{},
	}
	_ = loggerConfig.Level.Set("info")
	_ = loggerConfig.Format.Set("logfmt")
	logger := promlog.New(loggerConfig)

	level.Info(logger).Log("msg", "Starting polardbx cdc exporter", "version", version.Info())
	level.Info(logger).Log("msg", "build context", "context", version.BuildContext())

	prometheus.MustRegister(newAdaptor(logger, newEndpoint("127.0.0.1", cdcPort, "cdc/metrics")))
	prometheus.MustRegister(version.NewCollector("polardbx_cdc_exporter"))

	// Listen and serve
	level.Info(logger).Log("msg", "Listening on address", "address", listenAddr)

	http.Handle(metricsPath, promhttp.Handler())
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		level.Error(logger).Log("msg", "Error starting HTTP server", "err", err)
		os.Exit(1)
	}
}
