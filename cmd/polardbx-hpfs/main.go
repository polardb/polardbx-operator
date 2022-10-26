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

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/local"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/task"
	"github.com/prometheus/common/log"
	"net"
	"os"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gofrs/flock"
	"google.golang.org/grpc"
)

var (
	//hdfs
	debug        bool
	debugHosts   string
	hostName     string
	hpfsPort     int
	limitedPaths string
	taskDb       string
	k8sNamespace string
	k8sSelector  string
	lockFile     string
)

var (
	// filestream server
	filestreamServerPort int
	filestreamRootPath   string
)

var (
	//flow control
	flowControlMinFlow    float64
	flowControlMaxFlow    float64
	flowControlTotalFLow  float64
	flowControlBufferSize int
)

func init() {
	flag.BoolVar(&debug, "debug", false, "Start in debug mode.")
	flag.StringVar(&debugHosts, "debug-hosts", "", "Debug hosts, in format of {host-name}@{host-addr}:{hpfs-port}, multiple values separated with comma.")
	flag.StringVar(&hostName, "host-name", "", "Current host name, use ${NODE_NAME} if not specified.")
	flag.IntVar(&hpfsPort, "port", 6543, "Port for host path file grpc service.")
	flag.StringVar(&limitedPaths, "limited-paths", "", "Paths that permitted to operated on.")
	flag.StringVar(&taskDb, "task-db", "tasks.db", "Database file in sqlite3 format that stores the tasks information.")
	flag.StringVar(&k8sNamespace, "k8s-namespace", "default", "Specify namespace to discovery hpfs services.")
	flag.StringVar(&k8sSelector, "k8s-selector", "", "Specify label selector to discovery hpfs services.")
	flag.StringVar(&lockFile, "lock-file", "", "Used to do file lock to ensure exclusivity.")
	flag.IntVar(&filestreamServerPort, "fss-port", 6643, "Port for file stream server")
	flag.StringVar(&filestreamRootPath, "fss-root-path", "", "Root path for storing uploaded files")
	flag.Float64Var(&flowControlMinFlow, "fc-min-flow", float64(1<<20), "min flow speed, unit: bytes/s")
	flag.Float64Var(&flowControlMaxFlow, "fc-max-flow", float64((1<<20)*20), "max flow speed, unit: bytes/s")
	flag.Float64Var(&flowControlTotalFLow, "fc-total-flow", float64((1<<20)*50), "total flow speed, unit: bytes/s")
	flag.IntVar(&flowControlBufferSize, "fc-buffer-size", (1<<20)*2, "transfer buffer size, unit: bytes")
	flag.Parse()

	if len(hostName) == 0 {
		hostName = os.Getenv("NODE_NAME")
	}
}

func parseDebugHosts() (map[string]discovery.HostInfo, error) {
	hosts := make(map[string]discovery.HostInfo)

	for _, s := range strings.Split(debugHosts, ",") {
		firstSplit := strings.SplitN(s, "@", 2)
		if len(firstSplit) < 2 {
			return nil, errors.New("invalid debug hosts")
		}
		hostName, addrPort := firstSplit[0], firstSplit[1]
		secondSplit := strings.SplitN(addrPort, ":", 2)
		if len(secondSplit) < 2 {
			return nil, errors.New("invalid debug hosts")
		}

		port, err := strconv.Atoi(secondSplit[1])
		if err != nil {
			return nil, fmt.Errorf("invalid debug hosts: %w", err)
		}
		if port <= 0 || port >= 65536 {
			return nil, errors.New("invalid debug hosts")
		}
		hosts[hostName] = discovery.HostInfo{
			NodeName: hostName,
			HpfsHost: secondSplit[0],
			HpfsPort: uint32(port),
			SshPort:  22,
		}
	}

	return hosts, nil
}

func parseK8sLabelSelector() (map[string]string, error) {
	splits := strings.Split(k8sSelector, ",")
	selector := make(map[string]string)
	for _, s := range splits {
		kv := strings.SplitN(s, "=", 2)
		if len(kv) < 2 {
			return nil, errors.New("invalid selector")
		}
		selector[kv[0]] = kv[1]
	}
	return selector, nil
}

func newHpfsServer() proto.HpfsServiceServer {
	// Host discovery
	var hostDis discovery.HostDiscovery
	var err error

	if debug {
		var hosts map[string]discovery.HostInfo
		hosts, err = parseDebugHosts()
		if err == nil {
			hostDis, err = discovery.NewDebugHostDiscovery(hostName, hosts)
		}
	} else {
		var selector map[string]string
		selector, err = parseK8sLabelSelector()
		if err == nil {
			hostDis, err = discovery.NewK8sHostDiscovery(hostName, k8sNamespace, selector)
		}
	}
	if err != nil {
		fmt.Printf("failed to create host discovery: %s\n", err.Error())
		os.Exit(-1)
	}

	// Local file service
	lfs, err := local.NewLocalFileService(append(strings.Split(limitedPaths, ","), "/sys/fs/cgroup"))
	if err != nil {
		fmt.Printf("failed to create local file service: %s\n", err.Error())
		os.Exit(-1)
	}

	// Task manager
	tm, err := task.NewTaskManager(taskDb)
	if err != nil {
		fmt.Printf("failed to create task manager: %s\n", err.Error())
		os.Exit(-1)
	}

	return hpfs.NewHpfsServiceServer(hostDis, lfs, tm)
}

func startHpfs() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", hpfsPort))
	if err != nil {
		fmt.Printf("failed to listen on port %d: %s\n", hpfsPort, err.Error())
		os.Exit(-1)
	}

	server := grpc.NewServer()
	proto.RegisterHpfsServiceServer(server, newHpfsServer())
	if err := server.Serve(lis); err != nil {
		fmt.Printf("failed to start server; %s\n", err.Error())
		os.Exit(-1)
	}
}

func startFileStreamServer() {
	go func() {
		log.Info("Start filestream server")
		fileServer := filestream.NewFileServer("0.0.0.0", filestreamServerPort, filestreamRootPath, filestream.GlobalFlowControl)
		err := fileServer.Start()
		if err != nil {
			log.Error(err, "Failed to start file server")
			os.Exit(1)
		}
	}()
}

func main() {
	log := zap.New(zap.UseDevMode(true))

	// Grab the file lock.
	if len(lockFile) > 0 {
		log.Info("getting file lock...")
		fl := flock.New(lockFile)
		if err := fl.Lock(); err != nil {
			panic("failed to lock: " + err.Error())
		}
		defer fl.Unlock()
		log.Info("locked, continue")
	}

	log.Info("starting grpc service...")
	//init flow control
	flowControl := filestream.NewFlowControl(filestream.FlowControlConfig{
		MaxFlow:    flowControlMaxFlow,
		TotalFlow:  flowControlTotalFLow,
		MinFlow:    flowControlMinFlow,
		BufferSize: flowControlBufferSize,
	})
	flowControl.Start()
	filestream.GlobalFlowControl = flowControl
	//start file stream server
	startFileStreamServer()
	// Start hpfs.
	startHpfs()
}
