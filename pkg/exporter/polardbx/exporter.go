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
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alibaba/polardbx-operator/pkg/exporter/metric"
)

type Exporter struct {
	mgrPort int
	db      *sql.DB
	dbMu    sync.RWMutex
	mutex   sync.Mutex
	logger  log.Logger

	up                            prometheus.Gauge
	totalScrapes, connectFailures prometheus.Counter
	statsMetrics                  map[string]metric.Metric
	stcMetrics                    map[string]metric.Metric
	htcMetrics                    map[string]metric.Metric
}

func (e *Exporter) init() error {
	e.dbMu.Lock()
	defer e.dbMu.Unlock()

	// Open connections if not initialized
	if e.db == nil {
		db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s:%d)/", polardbxRoot, "127.0.0.1", e.mgrPort))
		if err != nil {
			return err
		}
		e.db = db
	}

	// Ping
	err := e.db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (e *Exporter) TryInit() error {
	return e.init()
}

// Describe describes all the metrics ever exported by the PolarDB-X exporter.
// It implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.statsMetrics {
		ch <- m.Desc
	}
	for _, m := range e.stcMetrics {
		ch <- m.Desc
	}
	for _, m := range e.htcMetrics {
		ch <- m.Desc
	}
	ch <- polardbxUp
	ch <- e.totalScrapes.Desc()
	ch <- e.connectFailures.Desc()
}

// Collect fetches the stats from PolarDB-X manager port and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(metrics chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	up := e.scrape(metrics)

	metrics <- prometheus.MustNewConstMetric(polardbxUp, prometheus.GaugeValue, up)
	metrics <- e.totalScrapes
	metrics <- e.connectFailures
}

func (e *Exporter) parseRows(rs *sql.Rows, ch chan<- prometheus.Metric, metrics map[string]metric.Metric, labelKeys ...string) error {
	columns, err := rs.Columns()
	if err != nil {
		return err
	}
	// Lowercase all
	for i := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	// Find the label
	labels := make([]string, len(labelKeys))
	labelIdx := make(map[string]int)
	for i, key := range labelKeys {
		labelIdx[key] = i
	}

	// Scan all rows
	for rs.Next() {
		columnValues := make([]sql.NullString, len(columns))
		valueRefs := make([]interface{}, len(columns))
		for i := range columnValues {
			valueRefs[i] = &columnValues[i]
		}
		if err = rs.Scan(valueRefs...); err != nil {
			return err
		}

		for i := range labels {
			labels[i] = ""
		}
		for i := range columns {
			if idx, ok := labelIdx[columns[i]]; ok {
				labels[idx] = columnValues[i].String
			}
		}

		// If any of the labels is empty, drop it
		// labelsValid := true
		// for i := range labels {
		// 	if len(labels[i]) == 0 {
		// 		labelsValid = false
		// 		break
		// 	}
		// }
		// if !labelsValid {
		// 	continue
		// }

		for i := range columns {
			name := columns[i]
			valueStr := columnValues[i]

			m, ok := metrics[name]
			if !ok {
				continue
			}

			if !valueStr.Valid {
				ch <- prometheus.MustNewConstMetric(m.Desc, m.Type, 0.0, labels...)
			} else {
				value, err := strconv.ParseFloat(valueStr.String, 64)
				if err != nil {
					return err
				}
				ch <- prometheus.MustNewConstMetric(m.Desc, m.Type, value, labels...)
			}
		}
	}

	return nil
}

func (e *Exporter) getConn(ctx context.Context) (*sql.Conn, error) {
	e.dbMu.RLock()
	if e.db == nil {
		// Lazy init if init failed on startup.
		e.dbMu.RUnlock()
		if err := e.init(); err != nil {
			return nil, err
		}
	} else {
		defer e.dbMu.RUnlock()
	}

	conn, err := e.db.Conn(ctx)
	if err != nil {
		e.connectFailures.Inc()
		return nil, err
	}
	return conn, nil
}

func (e *Exporter) scrapeByQuery(query string, metrics map[string]metric.Metric, ch chan<- prometheus.Metric, labelKeys ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := e.getConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rs, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	return e.parseRows(rs, ch, metrics, labelKeys...)
}

type ScrapeTask struct {
	Query     string
	Metrics   map[string]metric.Metric
	VarLabels []string
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) (up float64) {
	e.totalScrapes.Inc()

	scrapeTasks := []ScrapeTask{
		{Query: "SHOW @@STATS", Metrics: e.statsMetrics, VarLabels: []string{"name"}},
		{Query: "SHOW @@STC", Metrics: e.stcMetrics, VarLabels: []string{"dbname", "mysqladdr", "appname", "groupname", "atomname"}},
	}

	errs := make([]error, len(scrapeTasks))
	errCnt := int32(0)
	wg := &sync.WaitGroup{}
	wg.Add(len(scrapeTasks))

	for i := range scrapeTasks {
		err := errs[i]
		task := scrapeTasks[i]
		go func() {
			defer wg.Done()
			err = e.scrapeByQuery(task.Query, task.Metrics, ch, task.VarLabels...)
			if err != nil {
				level.Error(e.logger).Log("msg", "failed to scrape by query", "query", task.Query, "error", err)
				atomic.AddInt32(&errCnt, 1)
			}
		}()
	}
	wg.Wait()

	if int(errCnt) == len(scrapeTasks) {
		return 0
	}

	return 1.0
}

func NewExporter(mgrPort int, logger log.Logger) *Exporter {
	exporter := &Exporter{
		mgrPort: mgrPort,
		logger:  logger,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scrape of polardbx successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total PolarDB-X scrapes.",
		}),
		connectFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_connect_failures_total",
			Help:      "Number of errors while connecting to manager port.",
		}),
		statsMetrics: statsMetrics,
		stcMetrics:   stcMetrics,
		htcMetrics:   htcMetrics,
	}
	return exporter
}
