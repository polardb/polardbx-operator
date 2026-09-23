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
	"encoding/json"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	"os"
)

func GetClientActionBySinkType(sinkType string) Action {
	switch sinkType {
	case config.SinkTypeOss:
		return DownloadOss
	case config.SinkTypeSftp:
		return DownloadSsh
	case config.SinkTypeMinio:
		return DownloadMinio
	}
	return InvalidAction
}

func GetHostInfoFromConfig(filepath string) (map[string]discovery.HostInfo, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	result := map[string]discovery.HostInfo{}
	if err = json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}
