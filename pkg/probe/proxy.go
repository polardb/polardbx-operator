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
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func handleErr(w http.ResponseWriter, err error) {
	if err == nil {
		log.Println("Succeeds!")
		w.WriteHeader(http.StatusOK)
	} else {
		if err == context.DeadlineExceeded {
			log.Println("Timeout!")
			w.WriteHeader(http.StatusRequestTimeout)
		} else {
			log.Println("Failed!" + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

type LivenessHandler struct {
	server *ProxyServer
}

func (handler *LivenessHandler) Handle(r *http.Request) error {
	if handler.server.IsDebugModeEnabled() {
		log.Println("Debug enabled, return alive!")
		return nil
	}

	prober, err := NewProber(r)
	if err != nil {
		return err
	}
	defer prober.Close()

	return prober.Liveness()
}

func (handler *LivenessHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println(formatRequest(r))
	err := handler.Handle(r)
	handleErr(w, err)
}

type ReadinessHandler struct {
	server *ProxyServer
}

func (handler *ReadinessHandler) Handle(r *http.Request) error {
	if handler.server.IsDebugModeEnabled() {
		log.Println("Debug enabled, return alive!")
		return nil
	}

	prober, err := NewProber(r)
	if err != nil {
		return err
	}
	defer prober.Close()

	return prober.ProbeReadiness()
}

func (handler *ReadinessHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println(formatRequest(r))
	err := handler.Handle(r)
	handleErr(w, err)
}

type ProxyServer struct {
	isDebugModeEnabled int32
}

func (server *ProxyServer) IsDebugModeEnabled() bool {
	return atomic.LoadInt32(&server.isDebugModeEnabled) == 1
}

func (server *ProxyServer) setIsDebugModeEnabled(val int32, message string) {
	curVal := atomic.LoadInt32(&server.isDebugModeEnabled)
	if curVal != val {
		atomic.StoreInt32(&server.isDebugModeEnabled, val)

		if val > 0 {
			fmt.Println("Debug mode enabled! Detail: " + message)
		} else {
			fmt.Println("Debug mode disabled! Detail: " + message)
		}
	}
}

func (server *ProxyServer) reloadRunmode() {
	file, err := os.Open("/etc/podinfo/runmode")
	if err != nil {
		if os.IsNotExist(err) {
			server.setIsDebugModeEnabled(0, "file for runmode annotation not exists")
			return
		}
		// Keep unchanged if error not determined
		fmt.Println("Unknown error: " + err.Error())
		return
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Unknown error: " + err.Error())
	}

	annotationVal := strings.Trim(string(content), " \n")
	if annotationVal == "debug" {
		server.setIsDebugModeEnabled(1, fmt.Sprintf("value of annotation runmode is '%s'", annotationVal))
	} else {
		server.setIsDebugModeEnabled(0, fmt.Sprintf("value of annotation runmode is '%s'", annotationVal))
	}
}

func (server *ProxyServer) loopReloadRunmode(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			server.reloadRunmode()
		}
	}
}

func (server *ProxyServer) Start(port int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.loopReloadRunmode(ctx)

	http.Handle("/liveness", &LivenessHandler{server: server})
	http.Handle("/readiness", &ReadinessHandler{server: server})
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
