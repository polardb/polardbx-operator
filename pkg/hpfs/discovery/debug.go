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

package discovery

import "errors"

type debugHostDiscovery struct {
	local string
	hosts map[string]HostInfo
}

func (d *debugHostDiscovery) IsLocal(name string) bool {
	return d.local == name
}

func (d *debugHostDiscovery) GetHost(name string) (HostInfo, error) {
	if h, ok := d.hosts[name]; ok {
		return h, nil
	}
	return HostInfo{}, ErrHostNotFound
}

func (d *debugHostDiscovery) GetHosts() (map[string]HostInfo, error) {
	return d.hosts, nil
}

func (d *debugHostDiscovery) Close() {
	return
}

func NewDebugHostDiscovery(local string, hosts map[string]HostInfo) (HostDiscovery, error) {
	if _, ok := hosts[local]; !ok {
		return nil, errors.New("invalid hosts, local not found")
	}

	return &debugHostDiscovery{
		local: local,
		hosts: hosts,
	}, nil
}
