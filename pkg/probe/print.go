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

package probe

import (
	"fmt"
	"net/http"
	"strings"
)

// formatRequest generates ascii representation of a request
func formatRequest(r *http.Request) string {
	var request []string

	// Client: 127.0.0.1:32451
	request = append(request, fmt.Sprintf("Client: "+r.RemoteAddr))

	// GET http://127.0.0.1:9090/liveness HTTP/1.0
	url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
	request = append(request, url)

	// Host 127.0.0.1:9090
	request = append(request, fmt.Sprintf("Host: %v", r.Host))

	// Content-Type: application/x-www-form-urlencoded
	for name, headers := range r.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			request = append(request, fmt.Sprintf("%v: %v", name, h))
		}
	}

	// Post data
	if r.Method == "POST" {
		_ = r.ParseForm()
		request = append(request, "\n")
		request = append(request, r.Form.Encode())
	}

	return strings.Join(request, "\n")
}
