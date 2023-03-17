package main

import (
	"flag"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/pitr"
	"os"
	"os/signal"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"syscall"
)

type JobType string

const (
	PitrHeartbeatJobType JobType = "PitrHeartbeat"
	PitrPrepareBinlogs   JobType = "PitrPrepareBinlogs"
)

var (
	jobType string
)

func init() {
	flag.StringVar(&jobType, "job-type", "PitrHeartbeat", "the job type")
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
