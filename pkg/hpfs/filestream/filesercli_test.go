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

package filestream

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestServerStart(t *testing.T) {
	g := NewGomegaWithT(t)
	flowControl := NewFlowControl(FlowControlConfig{
		MaxFlow:    20, // 1 byte/s
		TotalFlow:  40,
		MinFlow:    1,
		BufferSize: 2,
	})
	fileServer := NewFileServer("0.0.0.0", 22222, ".", flowControl)
	go func() {
		time.Sleep(1 * time.Second)
		fileServer.Stop()
	}()
	err := fileServer.Start()
	g.Expect(strings.Contains(err.Error(), "accept tcp4")).Should(BeEquivalentTo(true))
}

func startFileServer() *FileServer {
	flowControl := NewFlowControl(FlowControlConfig{
		MaxFlow:    1 << 40, // 1 byte/s
		TotalFlow:  1 << 40,
		MinFlow:    1,
		BufferSize: 1 << 9,
	})
	flowControl.Start()
	fileServer := NewFileServer("0.0.0.0", 22222, ".", flowControl)
	go func() {
		fileServer.Start()
	}()
	time.Sleep(1 * time.Second)
	return fileServer
}

func checkFile(g *WithT) {
	fileInfo, err := os.Stat("./busuinstanceid/busutest.txt")
	g.Expect(err).Should(BeNil())
	g.Expect(fileInfo.Name()).Should(BeEquivalentTo("busutest.txt"))
	sourceFileInfo, err := os.Stat("./filesercli_test.go")
	g.Expect(err).Should(BeNil())
	g.Expect(fileInfo.Size()).Should(BeEquivalentTo(sourceFileInfo.Size()))
}

func TestUploadAndDownloadOssUsingOssBufferSize(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	defer os.RemoveAll("./busuinstanceid/")
	for i := 0; i < 10; i++ {
		fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
		g.Expect(err).Should(BeNil())
		actionMetadata := ActionMetadata{
			Action:        UploadOss,
			InstanceId:    "busuinstanceid",
			Filename:      "busutest.txt",
			RequestId:     uuid.New().String(),
			OssBufferSize: "102400",
		}
		_, err = client.Upload(fd, actionMetadata)
		fd.Close()
		client.Check(actionMetadata)
		g.Expect(err).Should(BeNil())
		actionMetadata = ActionMetadata{
			Action:     DownloadOss,
			InstanceId: "busuinstanceid",
			Filename:   "busutest.txt",
		}
		os.MkdirAll("./busuinstanceid", os.ModePerm)
		fd, err = os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
		g.Expect(err).Should(BeNil())
		_, err = client.Download(fd, actionMetadata)
		fd.Close()
		g.Expect(err).Should(BeNil())
		checkFile(g)
		os.RemoveAll("./busuinstanceid/")
	}
}

func TestUploadAndDownloadOss(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)

	defer os.RemoveAll("./busuinstanceid/")
	for i := 0; i < 10; i++ {
		fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
		g.Expect(err).Should(BeNil())
		actionMetadata := ActionMetadata{
			Action:     UploadOss,
			InstanceId: "busuinstanceid",
			Filename:   "busutest.txt",
			RequestId:  uuid.New().String(),
		}
		_, err = client.Upload(fd, actionMetadata)
		fd.Close()
		client.Check(actionMetadata)
		g.Expect(err).Should(BeNil())
		actionMetadata = ActionMetadata{
			Action:     DownloadOss,
			InstanceId: "busuinstanceid",
			Filename:   "busutest.txt",
		}
		os.MkdirAll("./busuinstanceid", os.ModePerm)
		fd, err = os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
		g.Expect(err).Should(BeNil())
		_, err = client.Download(fd, actionMetadata)
		fd.Close()
		g.Expect(err).Should(BeNil())
		checkFile(g)
		os.RemoveAll("./busuinstanceid/")
	}
}

func TestUploadLocal(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetadata := ActionMetadata{
		Action:     UploadLocal,
		InstanceId: "busuinstanceid",
		Filename:   "busutest.txt",
	}
	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	g.Expect(err).Should(BeNil())
	_, err = client.Upload(fd, actionMetadata)
	g.Expect(err).Should(BeNil())
	fd.Close()
	time.Sleep(1 * time.Second)
	checkFile(g)
}

func TestUploadAndDownloadSsh(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetatdata := ActionMetadata{
		Action:     UploadSsh,
		InstanceId: "busuinstanceid",
		Filename:   "busutest",
		RequestId:  uuid.New().String(),
	}
	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	defer fd.Close()
	g.Expect(err).Should(BeNil())
	_, err = client.Upload(fd, actionMetatdata)
	client.Check(actionMetatdata)
	actionMetatdata.Action = DownloadSsh
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fdd, err := os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	client.Download(fdd, actionMetatdata)
	fdd.Close()
	time.Sleep(2 * time.Second)
	checkFile(g)
}

func TestUploadLocalXbStream(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetadata := ActionMetadata{
		Action:     UploadLocal,
		InstanceId: "busuinstanceid",
		Filename:   "busutest",
		Stream:     "xbstream",
	}
	defer os.RemoveAll("./busuinstanceid/")
	xbdatabase64 := "WEJTVENLMDEAUA4AAAAvZGF0YS9idXN1LnR4dAQAAAAAAAAAAAAAAAAAAACp5SzGYnVzdVhCU1RDSzAxAEUOAAAAL2RhdGEvYnVzdS50eHQ="
	decodesBytes := make([]byte, base64.RawStdEncoding.DecodedLen(len(xbdatabase64)))
	decodedLen, err := base64.StdEncoding.Decode(decodesBytes, []byte(xbdatabase64))
	g.Expect(err).Should(BeNil())
	client.Upload(bytes.NewReader(decodesBytes[:decodedLen]), actionMetadata)
	f, err := os.OpenFile("busuinstanceid/busutest/data/busu.txt", os.O_RDONLY, 0664)
	data, err := io.ReadAll(f)
	g.Expect(string(data)).Should(BeEquivalentTo("busu"))
}

func TestUploadLocalTar(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetadata := ActionMetadata{
		Action:     UploadLocal,
		InstanceId: "busuinstanceid",
		Filename:   "busutest.txt",
		Stream:     "tar",
	}
	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	g.Expect(err).Should(BeNil())
	body, err := io.ReadAll(fd)
	hdr := &tar.Header{
		Name: "/1/filesercli_test.go",
		Mode: 0600,
		Size: int64(len(body)),
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err = tw.WriteHeader(hdr)
	buf.Write(body)
	g.Expect(err).Should(BeNil())
	_, err = client.Upload(bytes.NewReader(buf.Bytes()), actionMetadata)
	g.Expect(err).Should(BeNil())
	fd.Close()
	time.Sleep(1 * time.Second)
	fileInfo, err := os.Stat("./busuinstanceid/busutest.txt/1/filesercli_test.go")
	g.Expect(err).Should(BeNil())
	g.Expect(fileInfo.Name()).Should(BeEquivalentTo("filesercli_test.go"))
	sourceFileInfo, err := os.Stat("./filesercli_test.go")
	g.Expect(err).Should(BeNil())
	g.Expect(fileInfo.Size()).Should(BeEquivalentTo(sourceFileInfo.Size()))
}

func TestUploadRemote(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetadata := ActionMetadata{
		Action:       UploadRemote,
		InstanceId:   "busuinstanceid",
		Filename:     "busutest.txt",
		RedirectAddr: "127.0.0.1:22222",
	}
	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	g.Expect(err).Should(BeNil())
	_, err = client.Upload(fd, actionMetadata)
	g.Expect(err).Should(BeNil())
	fd.Close()
	time.Sleep(1 * time.Second)
	checkFile(g)
}

func TestDownloadLocal(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	actionMetadata := ActionMetadata{
		Action:       DownloadLocal,
		InstanceId:   ".",
		Filename:     "filesercli_test.go",
		RedirectAddr: "127.0.0.1:22222",
	}
	defer os.RemoveAll("./busuinstanceid/")
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fd, err := os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	client.Download(fd, actionMetadata)
	fd.Close()
	time.Sleep(2 * time.Second)
	checkFile(g)
}

func TestDownloadRemote(t *testing.T) {
	g := NewGomegaWithT(t)
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	//actionMetadata ActionMetadata
	actionMetadata := ActionMetadata{
		Action:       DownloadRemote,
		InstanceId:   ".",
		Filename:     "filesercli_test.go",
		RedirectAddr: "127.0.0.1:22222",
	}
	defer os.RemoveAll("./busuinstanceid/")
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fd, err := os.OpenFile("./busuinstanceid/busutest.txt", os.O_TRUNC|os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	client.Download(fd, actionMetadata)
	fd.Close()
	time.Sleep(2 * time.Second)
	checkFile(g)
}

func TestUploadAndDownloadMinio(t *testing.T) {
	config.ConfigFilepath = "/Users/wkf/hpfs/config.yaml"
	g := NewGomegaWithT(t)
	config.InitConfig()
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)

	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	g.Expect(err).Should(BeNil())
	actionMetadata := ActionMetadata{
		Action:     UploadMinio,
		InstanceId: "busuinstanceid",
		Filename:   "busutest.txt",
		RequestId:  uuid.New().String(),
	}
	_, err = client.Upload(fd, actionMetadata)
	fd.Close()
	client.Check(actionMetadata)
	g.Expect(err).Should(BeNil())
	actionMetadata = ActionMetadata{
		Action:     DownloadMinio,
		InstanceId: "busuinstanceid",
		Filename:   "busutest.txt",
	}
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fd, err = os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	_, err = client.Download(fd, actionMetadata)
	fd.Close()
	g.Expect(err).Should(BeNil())
	checkFile(g)
	os.RemoveAll("./busuinstanceid/")
}

func TestUploadAndDownloadMinioUsingMinioBufferSize(t *testing.T) {
	config.ConfigFilepath = "/Users/wkf/hpfs/config.yaml"
	g := NewGomegaWithT(t)
	config.InitConfig()
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)
	defer os.RemoveAll("./busuinstanceid/")
	fd, err := os.OpenFile("./filesercli_test.go", os.O_RDONLY, 0664)
	g.Expect(err).Should(BeNil())
	actionMetadata := ActionMetadata{
		Action:          UploadMinio,
		InstanceId:      "busuinstanceid",
		Filename:        "busutest.txt",
		RequestId:       uuid.New().String(),
		MinioBufferSize: "102400",
	}
	_, err = client.Upload(fd, actionMetadata)
	fd.Close()
	client.Check(actionMetadata)
	g.Expect(err).Should(BeNil())
	actionMetadata = ActionMetadata{
		Action:     DownloadMinio,
		InstanceId: "busuinstanceid",
		Filename:   "busutest.txt",
	}
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fd, err = os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	_, err = client.Download(fd, actionMetadata)
	fd.Close()
	g.Expect(err).Should(BeNil())
	checkFile(g)
	os.RemoveAll("./busuinstanceid/")

}

func TestListFilesMinio(t *testing.T) {
	config.ConfigFilepath = "/Users/wkf/hpfs/config.yaml"
	g := NewGomegaWithT(t)
	config.InitConfig()
	fileServer := startFileServer()
	defer fileServer.Stop()
	client := NewFileClient("127.0.0.1", 22222, nil)

	actionMetadata := ActionMetadata{
		Action:   ListMinio,
		Filepath: "busuhhhh",
	}
	os.MkdirAll("./busuinstanceid", os.ModePerm)
	fd, err := os.OpenFile("./busuinstanceid/busutest.txt", os.O_CREATE|os.O_RDWR, os.ModePerm)
	g.Expect(err).Should(BeNil())
	_, err = client.List(fd, actionMetadata)
	fd.Close()
	g.Expect(err).Should(BeNil())
}
