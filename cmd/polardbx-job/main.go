package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/pitr"
	"io"
	"os"
	"os/signal"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"syscall"
)

type JobType string

const (
	PitrHeartbeatJobType              JobType = "PitrHeartbeat"
	PitrPrepareBinlogs                JobType = "PitrPrepareBinlogs"
	PitrDownloadFile                  JobType = "PitrDownloadFile"
	PitrPrepareBinlogForXStoreJobType JobType = "PitrPrepareBinlogsForXStore"
)

var (
	jobType          string
	binlogSourceJson string
	output           string
)

func init() {
	flag.StringVar(&jobType, "job-type", "PitrHeartbeat", "the job type")
	flag.StringVar(&binlogSourceJson, "binlog-source", "{}", "the BinlogSource json")
	flag.StringVar(&output, "output", "", "output filepath")
	flag.Parse()
}

func main() {
	log := zap.New(zap.UseDevMode(true))
	log.Info(fmt.Sprintf("jobType=%s", jobType))
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	exitActions := make([]func(), 0)
	waitActions := make([]func(), 0)
	switch JobType(jobType) {
	case PitrHeartbeatJobType:
		heartbeat := backupbinlog.NewHeatBeat()
		heartbeat.Do()
		exitActions = append(exitActions, func() {
			heartbeat.Cancel()
		})
		waitActions = append(waitActions, func() {
			heartbeat.Wait()
		})
	case PitrPrepareBinlogs:
		waitGroup := pitr.RunAsync()
		exitActions = append(exitActions, func() {
			pitr.Exit()
		})
		waitActions = append(waitActions, func() {
			waitGroup.Wait()
		})
	case PitrPrepareBinlogForXStoreJobType:
		waitGroup := pitr.RunAsyncForXStore()
		exitActions = append(exitActions, func() {
			pitr.Exit()
		})
		waitActions = append(waitActions, func() {
			waitGroup.Wait()
		})
	case PitrDownloadFile:
		binlogSource := pitr.BinlogSource{}
		err := json.Unmarshal([]byte(binlogSourceJson), &binlogSource)
		if err != nil {
			panic(err)
		}
		reader, err := binlogSource.OpenStream()
		if err != nil {
			panic(err)
		}
		filews, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		defer filews.Close()
		_, err = io.CopyN(filews, reader, int64(*(binlogSource.GetTrueLength())))
		if err != nil {
			panic(err)
		}
	default:
		panic("invalid job type")
	}

	go func() {
		select {
		case <-ch:
			defer os.Exit(1)
			for _, exitAction := range exitActions {
				exitAction()
			}
			for _, waitAction := range waitActions {
				waitAction()
			}
		}
	}()

	for _, waitAction := range waitActions {
		waitAction()
	}

}
