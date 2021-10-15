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

package jvm

import (
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/alibaba/polardbx-operator/third-party/hsperfdata"
)

// The JvmStat Exporter

const jvmStatSubsystem = "jvmstats"

type JvmStatExporter struct {
	pidFn        func() (int, error)
	reportErrors bool

	threadsDaemonCount      *prometheus.Desc
	threadsLiveCount        *prometheus.Desc
	threadsLivePeak         *prometheus.Desc
	threadsStartedCount     *prometheus.Desc
	gcAgeTableSize          *prometheus.Desc
	gcGenerationCapacity    *prometheus.Desc
	gcGenerationMinCapacity *prometheus.Desc
	gcGenerationMaxCapacity *prometheus.Desc
	gcGenerationUsed        *prometheus.Desc
	gcSpaceCapacity         *prometheus.Desc
	gcSpaceInitCapacity     *prometheus.Desc
	gcSpaceMinCapacity      *prometheus.Desc
	gcSpaceMaxCapacity      *prometheus.Desc
	gcSpaceUsed             *prometheus.Desc
	gcCollectorInvocations  *prometheus.Desc
	gcCollectorTime         *prometheus.Desc
	gcTlabAlloc             *prometheus.Desc
	gcTlabAllocThreads      *prometheus.Desc
	gcTlabFastWaste         *prometheus.Desc
	gcTlabFills             *prometheus.Desc
	gcTlabGcWaste           *prometheus.Desc
	gcTlabMaxFastWaste      *prometheus.Desc
	gcTlabMaxFills          *prometheus.Desc
	gcTlabMaxGcWaste        *prometheus.Desc
	gcTlabSlowAlloc         *prometheus.Desc
	gcTlabSlowWaste         *prometheus.Desc
	gcDesiredSurvivorSize   *prometheus.Desc
	gcTenuringThreshold     *prometheus.Desc
	gcMaxTenuringThreshold  *prometheus.Desc
}

type JvmStatExporterOpt struct {
	PidFn        func() (int, error)
	Namespace    string
	ReportErrors bool
}

func NewJvmStatExporter(opt JvmStatExporterOpt) *JvmStatExporter {
	if opt.PidFn == nil {
		panic("nil pid fn")
	}

	return &JvmStatExporter{
		pidFn:        opt.PidFn,
		reportErrors: opt.ReportErrors,

		threadsDaemonCount: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "threads_daemon_count"),
			"Current daemon threads count.",
			[]string{"pid"},
			nil,
		),
		threadsLiveCount: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "threads_live_count"),
			"Current live threads count.",
			[]string{"pid"},
			nil,
		),
		threadsLivePeak: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "threads_live_peak"),
			"Current peak live threads count.",
			[]string{"pid"},
			nil,
		),
		threadsStartedCount: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "threads_started_count"),
			"Current started threads count.",
			[]string{"pid"},
			nil,
		),
		gcAgeTableSize: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_agetable_size"),
			"Current age table size in bytes.",
			[]string{"pid", "gc", "generation", "index", "size"},
			nil,
		),
		gcGenerationCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_generation_capacity"),
			"Current generation capacity.",
			[]string{"pid", "gc", "generation"},
			nil,
		),
		gcGenerationMinCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_generation_min_capacity"),
			"Current generation min capacity.",
			[]string{"pid", "gc", "generation"},
			nil,
		),
		gcGenerationMaxCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_generation_max_capacity"),
			"Current generation max capacity.",
			[]string{"pid", "gc", "generation"},
			nil,
		),
		gcGenerationUsed: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_generation_used"),
			"Current generation used.",
			[]string{"pid", "gc", "generation"},
			nil,
		),
		gcSpaceCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_space_capacity"),
			"Current space capacity.",
			[]string{"pid", "gc", "generation", "space"},
			nil,
		),
		gcSpaceMaxCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_space_max_capacity"),
			"Current space max capacity.",
			[]string{"pid", "gc", "generation", "space"},
			nil,
		),
		gcSpaceInitCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_space_init_capacity"),
			"Current space ini capacity.",
			[]string{"pid", "gc", "generation", "space"},
			nil,
		),
		gcSpaceMinCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_space_min_capacity"),
			"Current space min capacity.",
			[]string{"pid", "gc", "generation", "space"},
			nil,
		),
		gcSpaceUsed: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_space_used"),
			"Current space used.",
			[]string{"pid", "gc", "generation", "space"},
			nil,
		),
		gcCollectorInvocations: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_collector_invocation_count"),
			"Current collector invocation count.",
			[]string{"pid", "gc", "collector", "type"},
			nil,
		),
		gcCollectorTime: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_collector_time_total"),
			"Total collector used time in nanoseconds.",
			[]string{"pid", "gc", "collector", "type"},
			nil,
		),
		gcTlabAlloc: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_alloc"),
			"Total tlab (thread local allocation buffer) allocation times.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabAllocThreads: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_alloc_threads"),
			"Current number of threads allocate from tlab (thread local allocation buffer).",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabFastWaste: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_fast_waste"),
			"Current tlab (thread local allocation buffer) fast waste.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabFills: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_fills"),
			"Current tlab (thread local allocation buffer) fills.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabGcWaste: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_gc_waste"),
			"Current tlab (thread local allocation buffer) GC waste.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabMaxFastWaste: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_max_fast_waste"),
			"Current tlab (thread local allocation buffer) max fast waste.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabMaxFills: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_max_fills"),
			"Current tlab (thread local allocation buffer) max fills.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabMaxGcWaste: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_max_gc_waste"),
			"Current tlab (thread local allocation buffer) max GC waste.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabSlowAlloc: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_slow_alloc"),
			"Current tlab (thread local allocation buffer) slow allocation times.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTlabSlowWaste: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_tlab_slow_waste"),
			"Current tlab (thread local allocation buffer) slow waste.",
			[]string{"pid", "gc"},
			nil,
		),
		gcDesiredSurvivorSize: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_policy_desired_survivor_size"),
			"Current desired survivor size.",
			[]string{"pid", "gc"},
			nil,
		),
		gcTenuringThreshold: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_policy_tenuring_threshold"),
			"Current tenuring threshold.",
			[]string{"pid", "gc"},
			nil,
		),
		gcMaxTenuringThreshold: prometheus.NewDesc(
			prometheus.BuildFQName(opt.Namespace, jvmStatSubsystem, "gc_policy_max_tenuring_threshold"),
			"Current max tenuring threshold.",
			[]string{"pid", "gc"},
			nil,
		),
	}
}

func (e *JvmStatExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.threadsDaemonCount
	ch <- e.threadsLiveCount
	ch <- e.threadsLivePeak
	ch <- e.threadsStartedCount
	ch <- e.gcAgeTableSize
	ch <- e.gcGenerationCapacity
	ch <- e.gcGenerationMinCapacity
	ch <- e.gcGenerationMaxCapacity
	ch <- e.gcGenerationUsed
	ch <- e.gcSpaceCapacity
	ch <- e.gcSpaceMaxCapacity
	ch <- e.gcSpaceInitCapacity
	ch <- e.gcSpaceMinCapacity
	ch <- e.gcSpaceUsed
	ch <- e.gcCollectorInvocations
	ch <- e.gcCollectorTime
	ch <- e.gcTlabAlloc
	ch <- e.gcTlabAllocThreads
	ch <- e.gcTlabFastWaste
	ch <- e.gcTlabFills
	ch <- e.gcTlabGcWaste
	ch <- e.gcTlabMaxFastWaste
	ch <- e.gcTlabMaxFills
	ch <- e.gcTlabMaxGcWaste
	ch <- e.gcTlabSlowAlloc
	ch <- e.gcTlabSlowWaste
	ch <- e.gcDesiredSurvivorSize
	ch <- e.gcTenuringThreshold
	ch <- e.gcMaxTenuringThreshold
}

func (e *JvmStatExporter) Collect(ch chan<- prometheus.Metric) {
	e.processCollect(ch)
}

func (e *JvmStatExporter) processCollect(ch chan<- prometheus.Metric) {
	pid, err := e.pidFn()
	if err != nil {
		e.reportError(ch, nil, err)
		return
	}

	perfpath, err := hsperfdata.PerfDataPath(strconv.Itoa(pid))
	if err != nil {
		e.reportError(ch, nil, err)
		return
	}

	perf, err := hsperfdata.ReadPerfData(perfpath, true)
	if err != nil {
		e.reportError(ch, nil, err)
		return
	}

	jvmStats := JvmStats{}
	if err := jvmStats.Parse(perf); err != nil {
		e.reportError(ch, nil, err)
		return
	}

	e.processJvmStats(strconv.Itoa(pid), ch, &jvmStats)
}

func (e *JvmStatExporter) processJvmStats(pid string, ch chan<- prometheus.Metric, stats *JvmStats) {
	// threads
	ch <- prometheus.MustNewConstMetric(e.threadsDaemonCount, prometheus.GaugeValue, float64(stats.JavaThreads.Daemon), []string{pid}...)
	ch <- prometheus.MustNewConstMetric(e.threadsLiveCount, prometheus.GaugeValue, float64(stats.JavaThreads.Live), []string{pid}...)
	ch <- prometheus.MustNewConstMetric(e.threadsLivePeak, prometheus.GaugeValue, float64(stats.JavaThreads.LivePeak), []string{pid}...)
	ch <- prometheus.MustNewConstMetric(e.threadsStartedCount, prometheus.GaugeValue, float64(stats.JavaThreads.Started), []string{pid}...)

	gc := stats.Gc.Policy.Name

	// collector
	for _, collector := range stats.Gc.Collectors {
		// FIXME Currently only G1 gc types was considered.
		t := "unknown"
		if strings.Contains(collector.Name, "stop-the-world") {
			t = "fgc"
		} else if collector.Name == "G1 incremental collections" {
			t = "ygc"
		}
		ch <- prometheus.MustNewConstMetric(e.gcCollectorInvocations, prometheus.GaugeValue, float64(collector.Invocations), []string{pid, gc, collector.Name, t}...)
		ch <- prometheus.MustNewConstMetric(e.gcCollectorTime, prometheus.GaugeValue, float64(collector.Time), []string{pid, gc, collector.Name, t}...)
	}

	// generations
	for _, generation := range stats.Gc.Generations {
		if generation.AgeTableSize > 0 {
			for i, ageTableSize := range generation.AgeTableBytes {
				ch <- prometheus.MustNewConstMetric(e.gcAgeTableSize, prometheus.GaugeValue, float64(ageTableSize), []string{pid, gc, generation.Name, strconv.Itoa(i), strconv.Itoa(int(generation.AgeTableSize))}...)
			}
		}

		used := int64(0)
		for _, space := range generation.Spaces {
			used += space.Used

			ch <- prometheus.MustNewConstMetric(e.gcSpaceCapacity, prometheus.GaugeValue, float64(space.Capacity), []string{pid, gc, generation.Name, space.Name}...)
			ch <- prometheus.MustNewConstMetric(e.gcSpaceInitCapacity, prometheus.GaugeValue, float64(space.InitCapacity), []string{pid, gc, generation.Name, space.Name}...)
			ch <- prometheus.MustNewConstMetric(e.gcSpaceMaxCapacity, prometheus.GaugeValue, float64(space.MaxCapacity), []string{pid, gc, generation.Name, space.Name}...)
			ch <- prometheus.MustNewConstMetric(e.gcSpaceUsed, prometheus.GaugeValue, float64(space.Used), []string{pid, gc, generation.Name, space.Name}...)
		}

		ch <- prometheus.MustNewConstMetric(e.gcGenerationCapacity, prometheus.GaugeValue, float64(generation.Capacity), []string{pid, gc, generation.Name}...)
		ch <- prometheus.MustNewConstMetric(e.gcGenerationUsed, prometheus.GaugeValue, float64(used), []string{pid, gc, generation.Name}...)
		ch <- prometheus.MustNewConstMetric(e.gcGenerationMinCapacity, prometheus.GaugeValue, float64(generation.MinCapacity), []string{pid, gc, generation.Name}...)
		ch <- prometheus.MustNewConstMetric(e.gcGenerationMaxCapacity, prometheus.GaugeValue, float64(generation.MaxCapacity), []string{pid, gc, generation.Name}...)
	}

	// meta and compressed class spaces
	ch <- prometheus.MustNewConstMetric(e.gcSpaceCapacity, prometheus.GaugeValue, float64(stats.Gc.MetaSpace.Capacity), []string{pid, gc, "permanent", "meta"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceMinCapacity, prometheus.GaugeValue, float64(stats.Gc.MetaSpace.MinCapacity), []string{pid, gc, "permanent", "meta"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceMaxCapacity, prometheus.GaugeValue, float64(stats.Gc.MetaSpace.MaxCapacity), []string{pid, gc, "permanent", "meta"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceUsed, prometheus.GaugeValue, float64(stats.Gc.MetaSpace.Used), []string{pid, gc, "permanent", "meta"}...)

	ch <- prometheus.MustNewConstMetric(e.gcSpaceCapacity, prometheus.GaugeValue, float64(stats.Gc.CompressedClassSpace.Capacity), []string{pid, gc, "permanent", "compressedclass"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceMinCapacity, prometheus.GaugeValue, float64(stats.Gc.CompressedClassSpace.MinCapacity), []string{pid, gc, "permanent", "compressedclass"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceMaxCapacity, prometheus.GaugeValue, float64(stats.Gc.CompressedClassSpace.MaxCapacity), []string{pid, gc, "permanent", "compressedclass"}...)
	ch <- prometheus.MustNewConstMetric(e.gcSpaceUsed, prometheus.GaugeValue, float64(stats.Gc.CompressedClassSpace.Used), []string{pid, gc, "permanent", "compressedclass"}...)

	// tlab
	ch <- prometheus.MustNewConstMetric(e.gcTlabAlloc, prometheus.GaugeValue, float64(stats.Gc.Tlab.Alloc), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabAllocThreads, prometheus.GaugeValue, float64(stats.Gc.Tlab.AllocThreads), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabFastWaste, prometheus.GaugeValue, float64(stats.Gc.Tlab.FastWaste), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabFills, prometheus.GaugeValue, float64(stats.Gc.Tlab.Fills), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabGcWaste, prometheus.GaugeValue, float64(stats.Gc.Tlab.GcWaste), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabMaxFastWaste, prometheus.GaugeValue, float64(stats.Gc.Tlab.MaxFastWaste), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabMaxFills, prometheus.GaugeValue, float64(stats.Gc.Tlab.MaxFills), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabMaxGcWaste, prometheus.GaugeValue, float64(stats.Gc.Tlab.MaxGcWaste), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabSlowAlloc, prometheus.GaugeValue, float64(stats.Gc.Tlab.SlowAlloc), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTlabSlowWaste, prometheus.GaugeValue, float64(stats.Gc.Tlab.SlowWaste), []string{pid, gc}...)

	// others
	ch <- prometheus.MustNewConstMetric(e.gcDesiredSurvivorSize, prometheus.GaugeValue, float64(stats.Gc.Policy.DesiredSurvivorSize), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcTenuringThreshold, prometheus.GaugeValue, float64(stats.Gc.Policy.TenuringThreshold), []string{pid, gc}...)
	ch <- prometheus.MustNewConstMetric(e.gcMaxTenuringThreshold, prometheus.GaugeValue, float64(stats.Gc.Policy.MaxTenuringThreshold), []string{pid, gc}...)
}

func (c *JvmStatExporter) reportError(ch chan<- prometheus.Metric, desc *prometheus.Desc, err error) {
	if !c.reportErrors {
		return
	}
	if desc == nil {
		desc = prometheus.NewInvalidDesc(err)
	}
	ch <- prometheus.NewInvalidMetric(desc, err)
}
