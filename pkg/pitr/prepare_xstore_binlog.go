package pitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
	"os"
	"runtime/debug"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sync"
)

func RunForXStore() error {
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
				pCtx.LastErr = errors.New(fmt.Sprintf("panic %s", debug.Stack()))
			}
		}()
		steps := []Step{
			LoadAllBinlog,
			SelectBinlogForStandard,
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

func RunAsyncForXStore() *sync.WaitGroup {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		RunForXStore()
	}()
	return &waitGroup
}
