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

package network

import (
	"errors"
	"net"
)

func preferIface(iface net.Interface) bool {
	return iface.Name == "en0" || iface.Name == "eth0"
}

func getTheFirstIPv4IP(iface net.Interface) (net.IP, error) {
	// ignore interface not up
	if (iface.Flags & net.FlagUp) == 0 {
		return nil, nil
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return nil, err
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil {
			continue
		}

		if ip = ip.To4(); ip == nil {
			// ignore IPv6 addresses
			continue
		}

		if !ip.IsLoopback() {
			return ip, nil
		}
	}

	return nil, nil
}

// GetLocalIP get the local IPv4 ip
func GetLocalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	// find preferred interface
	for _, iface := range interfaces {
		if preferIface(iface) {
			ip, err := getTheFirstIPv4IP(iface)
			if err != nil || ip != nil {
				return ip, err
			}
		}
	}

	for _, iface := range interfaces {
		ip, err := getTheFirstIPv4IP(iface)
		if err != nil || ip != nil {
			return ip, err
		}
	}

	return nil, errors.New("no non-loopback ip found")
}

func GetOutBoundIp() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP, nil
}
