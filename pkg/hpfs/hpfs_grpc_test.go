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

package hpfs

import (
	"context"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
)

func TestCleanFileThroughHpfs(t *testing.T) {
	address := fmt.Sprintf("localhost:%d", 22222)
	logger := zap.New(zap.UseDevMode(true)).WithName("hpfs")
	g := NewWithT(t)
	config.ConfigFilepath = "/tmp/config.yaml"
	config.InitConfig()

	rpcsService := &rpcService{
		Logger: logger,
	}
	listen, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(err, "failed to listen")
	}
	grpcServer := grpc.NewServer()
	proto.RegisterHpfsServiceServer(grpcServer, rpcsService)
	go grpcServer.Serve(listen)
	logger.Info("Server started.")
	defer grpcServer.Stop()

	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := proto.NewHpfsServiceClient(conn)
	response, err := client.DeleteRemoteFile(context.Background(), &proto.DeleteRemoteFileRequest{
		Target:   &proto.RemoteFsEndpoint{Path: "polardbx-filestream-validation"},
		SinkName: "default",
		SinkType: "sftp",
	})
	logger.Info(response.String())
	g.Expect(response.GetStatus().Code).Should(BeEquivalentTo(proto.Status_OK))
}
