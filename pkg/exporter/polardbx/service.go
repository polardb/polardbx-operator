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

package polardbx

import (
	"net/http"
	"os"
	"sync/atomic"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/version"

	"github.com/alibaba/polardbx-operator/pkg/exporter/extensions/jvm"
	"github.com/alibaba/polardbx-operator/pkg/exporter/process"
)

func Start(listenAddr string, metricsPath string, mgrPort int, enableProcExporter, enableJvmStatExporter bool) {
	loggerConfig := &promlog.Config{
		Level:  &promlog.AllowedLevel{},
		Format: &promlog.AllowedFormat{},
	}
	_ = loggerConfig.Level.Set("info")
	_ = loggerConfig.Format.Set("logfmt")
	logger := promlog.New(loggerConfig)

	level.Info(logger).Log("msg", "Starting polardbx exporter", "version", version.Info())
	level.Info(logger).Log("msg", "build context", "context", version.BuildContext())

	tddlServerPidFn := func() func() (int, error) {
		lastPid := int32(-1)
		return func() (int, error) {
			pid, err := process.CheckJavaProcessOrFind(int(atomic.LoadInt32(&lastPid)), "TddlLauncher")
			if err != nil {
				return -1, err
			}
			atomic.StoreInt32(&lastPid, int32(pid))
			return pid, err
		}
	}()

	// Register process exporter for tddl process if enabled.
	if enableProcExporter {
		prometheus.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			PidFn:     tddlServerPidFn,
			Namespace: namespace,
		}))
	}

	// Register jvmstat exporter for tddl process if enabled.
	if enableJvmStatExporter {
		prometheus.MustRegister(jvm.NewJvmStatExporter(jvm.JvmStatExporterOpt{
			PidFn:     tddlServerPidFn,
			Namespace: namespace,
		}))
	}

	// Register the prometheus collector
	exporter := NewExporter(mgrPort, logger)
	err := exporter.TryInit()
	if err != nil {
		level.Error(logger).Log("msg", "Error initializing exporter. Try lazy init.", "err", err)
	}
	prometheus.MustRegister(exporter)
	prometheus.MustRegister(version.NewCollector("polardbx_exporter"))

	// Listen and serve
	level.Info(logger).Log("msg", "Listening on address", "address", listenAddr)

	http.Handle(metricsPath, promhttp.Handler())
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		level.Error(logger).Log("msg", "Error starting HTTP server", "err", err)
		os.Exit(1)
	}
}
