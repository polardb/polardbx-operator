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

import "strings"

type Action string

const (
	UploadLocal    Action = "uploadLocal"
	UploadRemote   Action = "uploadRemote"
	DownloadLocal  Action = "downloadLocal"
	DownloadRemote Action = "downloadRemote"
	UploadOss      Action = "uploadOss"
	DownloadOss    Action = "downloadOss"
	ListOss        Action = "listOss"
	CheckTask      Action = "checkTask"
	UploadSsh      Action = "uploadSsh"
	DownloadSsh    Action = "downloadSsh"
	ListSsh        Action = "listSsh"
	InvalidAction  Action = ""
)

const (
	StreamTar      = "tar"
	StreamXBStream = "xbstream"
	StreamNormal   = ""
)

const (
	MetaDataLenLen              = 4
	MetaFiledLen                = 11
	MetadataActionOffset        = 0
	MetadataInstanceIdOffset    = 1
	MetadataFilenameOffset      = 2
	MetadataRedirectOffset      = 3
	MetadataFilepathOffset      = 4
	MetadataRetentionTimeOffset = 5
	MetadataStreamOffset        = 6
	MetadataSinkOffset          = 7
	MetadataRequestIdOffset     = 8
	MetadataOssBufferSizeOffset = 9
	MetadataLimitSize           = 10
)

var ActionLocal2Remote2 = map[Action]Action{
	UploadLocal:    UploadRemote,
	UploadRemote:   UploadLocal,
	DownloadLocal:  DownloadRemote,
	DownloadRemote: DownloadLocal,
}

type ActionMetadata struct {
	Action        `json:"action,omitempty"`
	InstanceId    string `json:"instanceId,omitempty"`
	Filename      string `json:"filename,omitempty"`
	RedirectAddr  string `json:"redirectAddr,omitempty"`
	Filepath      string `json:"filepath,omitempty"`
	RetentionTime string `json:"retentionTime,omitempty"`
	Stream        string `json:"stream,omitempty"`
	Sink          string `json:"sink,omitempty"`
	RequestId     string `json:"requestId,omitempty"`
	OssBufferSize string `json:"ossBufferSize,omitempty"`
	LimitSize     string `json:"limitSize,omitempty"`
	redirect      bool
}

func (action *ActionMetadata) ToString() string {
	return strings.Join([]string{string(action.Action), action.InstanceId, action.Filename, action.RedirectAddr, action.Filepath, action.RetentionTime, action.Stream, action.Sink, action.RequestId, action.OssBufferSize, action.LimitSize}, ",")
}

const (
	RemoteNodePrefix = "RemoteNode="
)

func GetRemoteAddrByNodeName(nodeName string) string {
	return RemoteNodePrefix + nodeName
}

func GetNodeNameFromRemoteAddr(addr string) string {
	if strings.HasPrefix(addr, RemoteNodePrefix) {
		return addr[len(RemoteNodePrefix):]
	}
	return ""
}
