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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/prometheus/client_golang/prometheus"
)

type Endpoint struct {
	url *url.URL
}

func newEndpoint(host string, port int, path string) *Endpoint {
	return &Endpoint{
		url: &url.URL{Scheme: "http", Host: fmt.Sprintf("%s:%d", host, port), Path: path},
	}
}

func (e *Endpoint) Url() string {
	return e.url.String()
}

type Adaptor struct {
	logger   log.Logger
	endpoint *Endpoint

	totalScrapes prometheus.Counter
	mu           sync.Mutex
}

func (a *Adaptor) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range metrics {
		ch <- m.Desc
	}
	ch <- up
	ch <- a.totalScrapes.Desc()
}

func (a *Adaptor) parse(in io.Reader) (map[string]*dto.MetricFamily, error) {
	parser := &expfmt.TextParser{}
	return parser.TextToMetricFamilies(in)
}

func getMetricFamilyValue(mf *dto.MetricFamily) float64 {
	if mf == nil {
		return 0.0
	}
	switch mf.GetType() {
	case dto.MetricType_GAUGE:
		return mf.Metric[0].Gauge.GetValue()
	case dto.MetricType_COUNTER:
		return mf.Metric[0].Counter.GetValue()
	case dto.MetricType_UNTYPED:
		return mf.Metric[0].Untyped.GetValue()
	default:
		panic("unsupported")
	}
}

func (a *Adaptor) collect(ch chan<- prometheus.Metric) error {
	// Request from CDC endpoint
	resp, err := http.Get(a.endpoint.Url())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	mf, err := a.parse(resp.Body)
	if err != nil {
		return err
	}

	for key, val := range mf {
		if m, ok := metrics[key]; ok {
			// Only consider gauge/counter values.
			ch <- prometheus.MustNewConstMetric(m.Desc, m.Type, getMetricFamilyValue(val))
		} else {
			level.Info(a.logger).Log("msg", "Unrecognized key", "key", key, "value", val)
		}
	}
	return nil
}

func (a *Adaptor) Collect(metrics chan<- prometheus.Metric) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalScrapes.Inc()

	err := a.collect(metrics)
	if err != nil {
		metrics <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 0.0)
	} else {
		metrics <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 1.0)
	}
}

func newAdaptor(logger log.Logger, endpoint *Endpoint) *Adaptor {
	return &Adaptor{
		logger:   logger,
		endpoint: endpoint,
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total PolarDB-X CDC scrapes.",
		}),
	}
}
