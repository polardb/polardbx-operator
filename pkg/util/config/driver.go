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

package config

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/alibaba/polardbx-operator/pkg/util/config/bundle"
	"github.com/alibaba/polardbx-operator/pkg/util/config/loader"
)

type ParseFunc func(bundle bundle.Bundle) (interface{}, error)
type Watcher func(interface{})

type WatchDriver interface {
	Start(ctx context.Context) error
}

type watchDriver struct {
	once sync.Once

	logger   logr.Logger
	loader   loader.Loader
	parser   ParseFunc
	watchers []Watcher
	interval time.Duration
}

func (w *watchDriver) drive(ctx context.Context) error {
	b, err := w.loader.Load(ctx)
	if err != nil {
		return err
	}

	c, err := w.parser(b)
	if err != nil {
		return err
	}

	for _, watcher := range w.watchers {
		watcher(c)
	}

	return nil
}

func (w *watchDriver) start(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	for {
		err := w.drive(ctx)
		if err != nil {
			w.logger.Error(err, "Watch failed.")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(w.interval):
		}
	}
}

func (w *watchDriver) Start(ctx context.Context) error {
	var err error
	w.once.Do(func() {
		err = w.drive(ctx)
		if err != nil {
			return
		}
		go w.start(ctx)
	})
	return err
}

type WatchDriverOptions struct {
	Logger   logr.Logger
	Interval time.Duration
	Watchers []Watcher
}

func NewConfigWatchDriver(loader loader.Loader, parser ParseFunc, opts WatchDriverOptions) WatchDriver {
	if opts.Logger == nil {
		opts.Logger = logr.DiscardLogger{}
	}

	return &watchDriver{
		logger:   opts.Logger,
		loader:   loader,
		parser:   parser,
		watchers: opts.Watchers,
		interval: opts.Interval,
	}
}
