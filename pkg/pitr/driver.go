package pitr

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
	"net/http"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sync"
	"sync/atomic"
)

const (
	DefaultSpillOutDirectory = "/workspace/spill"
	EnvSpillOutDirectory     = "EnvSpillOutDirectory"
	DefaultConfigFilepath    = "/workspace/conf/config.json"
	EnvConfigFilepath        = "EnvConfigFilepath"
)

var configValue atomic.Value

func Run() error {
	configFilepath := defaults.NonEmptyStrOrDefault(os.Getenv(EnvConfigFilepath), DefaultConfigFilepath)
	spillOutDirectory := defaults.NonEmptyStrOrDefault(os.Getenv(EnvSpillOutDirectory), DefaultSpillOutDirectory)
	logger := zap.New(zap.UseDevMode(true)).WithName("pitr")
	config, err := os.ReadFile(configFilepath)
	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to read filepath=%s", configFilepath))
		return err
	}
	logger.Info("config content", "config", string(config))
	var taskConfig TaskConfig
	err = json.Unmarshal(config, &taskConfig)
	if err != nil {
		logger.Error(err, "failed to parse config file")
		return err
	}
	taskConfig.SpillDirectory = spillOutDirectory
	configValue.Store(taskConfig)
	pCtx := &Context{
		TaskConfig: &taskConfig,
		Logger:     zap.New(zap.UseDevMode(true)).WithName("pitr"),
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer func() {
			waitGroup.Done()
			obj := recover()
			if obj != nil {
				pCtx.Logger.Info("panic", "obj", obj)
			}
		}()
		steps := []Step{
			LoadAllBinlog,
			PrepareBinlogMeta,
			CollectInterestedTxEvents,
			Checkpoint,
		}
		for _, step := range steps {
			err := step(pCtx)
			if err != nil {
				pCtx.LastErr = err
				break
			}
		}
	}()
	waitGroup.Wait()
	err = FinishAndStartHttpServer(pCtx)
	if err != nil {
		pCtx.Logger.Error(err, "failed to start http server")
	}
	return nil
}

func RunAsync() *sync.WaitGroup {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		Run()
	}()
	return &waitGroup
}

func Exit() {
	config := configValue.Load().(TaskConfig)
	if config.HttpServerPort != 0 {
		http.Get(fmt.Sprintf("http://127.0.0.1:%d/exit", config.HttpServerPort))
		//ignore err
	}
}
