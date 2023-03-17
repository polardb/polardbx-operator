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
