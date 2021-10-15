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

package probe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/alibaba/polardbx-operator/pkg/probe/xstore_ext"
	_ "github.com/alibaba/polardbx-operator/pkg/probe/xstore_ext/plugin"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
)

const (
	TypePolarDBX = "polardbx"
	TypeXStore   = "xstore"
	TypeSelf     = "server"
)

type Prober struct {
	target string
	extra  string

	user    string
	host    string
	port    int
	timeout time.Duration

	db     *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
}

func (p *Prober) valid() bool {
	for _, t := range []string{
		TypePolarDBX, TypeXStore, TypeSelf,
	} {
		if p.target == t {
			return true
		}
	}
	return false
}

func (p *Prober) defaultUser() string {
	switch p.target {
	case TypeXStore:
		return "root"
	case TypePolarDBX:
		return "polardbx_root"
	default:
		return ""
	}
}

func NewProber(r *http.Request) (*Prober, error) {
	p := &Prober{
		target: r.Header.Get("Probe-Target"),
		extra:  r.Header.Get("Probe-Extra"),
	}

	if !p.valid() {
		return nil, errors.New("invalid probe target: %s" + p.target)
	}

	// Extract parameters from http headers.
	p.host = defaults.NonEmptyStrOrDefault(r.Header.Get("Probe-Host"), "127.0.0.1")
	p.user = defaults.NonEmptyStrOrDefault(r.Header.Get("Probe-User"), p.defaultUser())

	var err error
	probePort := defaults.NonEmptyStrOrDefault(r.Header.Get("Probe-Port"), "3306")
	p.port, err = strconv.Atoi(probePort)
	if err != nil {
		return nil, errors.New("invalid probe parameters: failed to parse port, " + err.Error())
	}

	probeTimeout := defaults.NonEmptyStrOrDefault(r.Header.Get("Probe-Timeout"), "5s")
	timeout, err := time.ParseDuration(probeTimeout)
	if err != nil {
		return nil, errors.New("invalid probe parameters: failed to parse timeout, " + err.Error())
	}
	p.timeout = timeout
	if timeout > 0 {
		p.ctx, p.cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		p.ctx, p.cancel = context.WithCancel(context.Background())
	}

	return p, nil
}

func (p *Prober) Close() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *Prober) connect() error {
	db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s:%d)/?timeout=%s",
		p.user, p.host, p.port, p.timeout))
	if err != nil {
		return err
	}
	p.db = db
	return nil
}

func (p *Prober) ping() error {
	return p.db.PingContext(p.ctx)
}

func (p *Prober) Liveness() error {
	switch p.target {
	case TypeXStore, TypePolarDBX:
		if err := p.connect(); err != nil {
			return err
		}

		// Return if already timeout.
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}

		return p.ping()
	case TypeSelf:
		return nil
	default:
		return errors.New("unknown probe type")
	}
}

func (p *Prober) extraReadinessForXStore() error {
	xstoreExt := xstore_ext.GetXStoreExt(p.extra)
	if xstoreExt == nil {
		return nil
	}
	return xstoreExt.Readiness(p.ctx, p.host, p.db)
}

func (p *Prober) ProbeReadiness() error {
	err := p.Liveness()
	if err != nil {
		return err
	}

	if p.target == TypeXStore {
		err = p.extraReadinessForXStore()
		if err != nil {
			return err
		}
	}

	return nil
}
