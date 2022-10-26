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
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"reflect"
	"sync/atomic"
	"time"
)

var ConfigFilepath = "/config/config.yaml"

const (
	SinkTypeOss  = "oss"
	SinkTypeSftp = "sftp"
)

type OssSink struct {
	Endpoint     string `json:"endpoint,omitempty"`
	AccessKey    string `json:"accessKey,omitempty"`
	AccessSecret string `json:"accessSecret,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
}

type SftpSink struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	RootPath string `json:"rootPath,omitempty"`
}
type Sink struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
	OssSink
	SftpSink
}

type Config struct {
	Sinks []Sink `json:"sinks,omitempty"`
}

var configValue atomic.Value

func InitConfig() {
	config := loadConfig()
	configValue.Store(config)
}

func GetConfig() Config {
	return configValue.Load().(Config)
}

func GetSink(sinkName string, sinkType string) (*Sink, error) {
	if sinkName == "" {
		sinkName = "default"
	}
	config := GetConfig()
	if config.Sinks != nil {
		for _, sink := range config.Sinks {
			if sink.Name == sinkName && sink.Type == sinkType {
				return &sink, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func loadConfig() Config {
	fd, err := os.OpenFile(ConfigFilepath, os.O_RDONLY, 0664)
	if err != nil {
		panic("failed to load open " + ConfigFilepath)
	}
	defer fd.Close()
	var config Config
	decoder := yaml.NewYAMLOrJSONDecoder(fd, 512)
	err = decoder.Decode(&config)
	if err != nil {
		panic("failed to parse config")
	}
	return config
}

func ReloadConfig() {
	config := loadConfig()
	nowConfig := configValue.Load()
	if reflect.DeepEqual(config, nowConfig) {
		return
	}
	configValue.Swap(config)
	fmt.Println(time.Now().Format("2006-01-02 15:04:05") + "  filestream config changed")
}
