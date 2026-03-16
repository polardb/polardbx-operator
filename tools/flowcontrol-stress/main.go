package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
)

type runMetrics struct {
	Duration time.Duration
	Bytes    int64
	Err      error
	Panicked bool
}

type aggregate struct {
	Runs          int           `json:"runs"`
	Concurrency   int           `json:"concurrency"`
	PayloadBytes  int64         `json:"payload_bytes"`
	Success       int           `json:"success"`
	Throttled     int           `json:"throttled"`
	OtherErrors   int           `json:"other_errors"`
	Panics        int           `json:"panics"`
	MinDuration   time.Duration `json:"min_duration"`
	MaxDuration   time.Duration `json:"max_duration"`
	AvgDuration   time.Duration `json:"avg_duration"`
	TotalDuration time.Duration `json:"total_duration"`
	TotalBytes    int64         `json:"total_bytes"`
}

func (a *aggregate) add(metric runMetrics) {
	a.Runs++
	a.TotalDuration += metric.Duration
	if metric.Duration < a.MinDuration || a.Runs == 1 {
		a.MinDuration = metric.Duration
	}
	if metric.Duration > a.MaxDuration {
		a.MaxDuration = metric.Duration
	}
	a.TotalBytes += metric.Bytes
	if metric.Panicked {
		a.Panics++
		return
	}
	switch {
	case metric.Err == nil || errors.Is(metric.Err, io.EOF):
		a.Success++
	case metric.Err.Error() == "Throttled":
		a.Throttled++
	default:
		a.OtherErrors++
	}
}

func (a *aggregate) finalize() {
	if a.Runs == 0 {
		return
	}
	a.AvgDuration = time.Duration(int64(a.TotalDuration) / int64(a.Runs))
}

func main() {
	var (
		iterations  = flag.Int("iterations", 32, "number of repeated flow control runs")
		concurrency = flag.Int("concurrency", runtime.NumCPU(), "number of concurrent LimitFlow invocations per iteration")
		payloadKB   = flag.Int("payload-kb", 512, "payload size per worker in KiB")
		minFlow     = flag.Float64("min-flow", 8*1024*1024, "minimum flow in bytes per second")
		maxFlow     = flag.Float64("max-flow", 32*1024*1024, "maximum flow in bytes per second")
		totalFlow   = flag.Float64("total-flow", 64*1024*1024, "global flow in bytes per second")
		bufferSize  = flag.Int("buffer-size", 256*1024, "buffer size in bytes for each read chunk")
		jsonOutput  = flag.Bool("json", true, "emit metrics as JSON")
	)
	flag.Parse()

	if *bufferSize <= 0 {
		fmt.Fprintln(os.Stderr, "buffer-size must be positive")
		os.Exit(2)
	}

	payloadBytes := int64(*payloadKB) * 1024
	if payloadBytes <= 0 {
		fmt.Fprintln(os.Stderr, "payload-kb must be positive")
		os.Exit(2)
	}

	agg := aggregate{Concurrency: *concurrency, PayloadBytes: payloadBytes}

	for i := 0; i < *iterations; i++ {
		flowControl := filestream.NewFlowControl(filestream.FlowControlConfig{
			MinFlow:    *minFlow,
			MaxFlow:    *maxFlow,
			TotalFlow:  *totalFlow,
			BufferSize: *bufferSize,
		})
		flowControl.Start()

		var (
			wg      sync.WaitGroup
			metrics = make([]runMetrics, *concurrency)
		)

		wg.Add(*concurrency)
		for worker := 0; worker < *concurrency; worker++ {
			idx := worker
			go func() {
				defer wg.Done()
				start := time.Now()
				defer func() {
					if r := recover(); r != nil {
						metrics[idx].Panicked = true
						metrics[idx].Duration = time.Since(start)
					}
				}()

				reader := bytes.NewReader(make([]byte, payloadBytes))
				written, err := flowControl.LimitFlow(reader, io.Discard, nil)
				metrics[idx].Duration = time.Since(start)
				metrics[idx].Bytes = written
				metrics[idx].Err = err
			}()
		}

		wg.Wait()
		flowControl.Stop()

		for _, m := range metrics {
			agg.add(m)
		}
	}

	agg.finalize()

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(agg); err != nil {
			fmt.Fprintf(os.Stderr, "failed to encode metrics: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("runs=%d concurrency=%d payloadBytes=%d success=%d throttled=%d otherErrors=%d panics=%d min=%s max=%s avg=%s total=%s totalBytes=%d\n",
			agg.Runs, agg.Concurrency, agg.PayloadBytes, agg.Success, agg.Throttled, agg.OtherErrors, agg.Panics,
			agg.MinDuration, agg.MaxDuration, agg.AvgDuration, agg.TotalDuration, agg.TotalBytes)
	}
}
