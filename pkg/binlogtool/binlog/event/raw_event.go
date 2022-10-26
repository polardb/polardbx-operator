/*
Copyright 2022 Alibaba Group Holding Limited.

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

package event

type RawLogEventData []byte

type RawLogEventV1 struct {
	Header LogEventHeaderV1 `json:"header,omitempty"`
	Data   RawLogEventData  `json:"data,omitempty"`
}

type RawLogEventV3 struct {
	Header LogEventHeaderV3 `json:"header,omitempty"`
	Data   RawLogEventData  `json:"data,omitempty"`
}

type RawLogEventV4 struct {
	Header LogEventHeaderV4 `json:"header,omitempty"`
	Data   RawLogEventData  `json:"data,omitempty"`
}
