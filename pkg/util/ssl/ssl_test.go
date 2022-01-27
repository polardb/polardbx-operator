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

package ssl

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var domain = "localhost"

func TestGenerateSelfCertificatedCertificates(t *testing.T) {
	caCert, caPriv, caPem, err := GenerateCA(2048, "")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("root.crt: ")
	caPemBytes, _ := MarshalCert(caPem)
	fmt.Println(string(caPemBytes))
	_ = os.MkdirAll("/tmp/test-certs", 0755)
	_ = ioutil.WriteFile("/tmp/test-certs/root.crt", caPemBytes, 0644)

	_, priv, pem, err := GenerateSelfSignedCert(caCert, caPriv, 2048, domain)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("server.key: ")
	keyBytes, _ := MarshalKey(priv)
	fmt.Println(string(keyBytes))
	_ = ioutil.WriteFile("/tmp/test-certs/server.key", keyBytes, 0644)
	pemBytes, _ := MarshalCert(pem)
	fmt.Println("server.crt: ")
	fmt.Println(string(pemBytes))
	_ = ioutil.WriteFile("/tmp/test-certs/server.crt", pemBytes, 0644)
}
