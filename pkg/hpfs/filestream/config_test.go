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
	"os"
	"testing"
	"time"
)
import . "github.com/onsi/gomega"

func TestConfig3(t *testing.T) {
	g := NewGomegaWithT(t)
	PrepareConfig()
	defer ClearConfig()
	defer func() {
		err := recover()
		g.Expect(err).Should(BeNil())
	}()
	ReloadConfig()
	sink, err := GetSink("default", "oss")
	g.Expect(err).Should(BeNil())
	g.Expect(sink.Name).Should(BeEquivalentTo("default"))
	g.Expect(sink.Endpoint).Should(BeEquivalentTo("xxx"))
	g.Expect(sink.AccessKey).Should(BeEquivalentTo("xxx"))
	g.Expect(sink.AccessSecret).Should(BeEquivalentTo("xxxxx"))
	g.Expect(sink.Bucket).Should(BeEquivalentTo("xxx"))

	sink, err = GetSink("default", "sftp")
	g.Expect(err).Should(BeNil())
	g.Expect(sink.Name).Should(BeEquivalentTo("default"))
	g.Expect(sink.Type).Should(BeEquivalentTo("sftp"))
	g.Expect(sink.Host).Should(BeEquivalentTo("xxxxx"))
	g.Expect(sink.Port).Should(BeEquivalentTo(22))
	g.Expect(sink.User).Should(BeEquivalentTo("admin"))
	g.Expect(sink.Password).Should(BeEquivalentTo("xxxx"))
	g.Expect(sink.RootPath).Should(BeEquivalentTo("/xxx"))
}

func TestConfig4(t *testing.T) {
	g := NewGomegaWithT(t)
	PrepareConfig()
	defer ClearConfig()
	defer func() {
		err := recover()
		g.Expect(err).Should(BeNil())
	}()
	for {
		time.Sleep(1 * time.Second)
		ReloadConfig()
	}

}

func PrepareConfig() {
	ConfigFilepath = "./config.yaml"
	f, err := os.OpenFile("./config.yaml", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.Write([]byte("sinks:\n  - name: default\n    type: oss\n    endpoint: xxx\n    accessKey: xxx\n    accessSecret: xxxxx\n    bucket: xxx\n  - name: default\n    type: sftp\n    host: xxxxx\n    port: 22\n    user: admin\n    password: xxxx\n    rootPath: /xxx"))
	InitConfig()
}

func ClearConfig() {
	os.Remove("./config.yaml")
}
