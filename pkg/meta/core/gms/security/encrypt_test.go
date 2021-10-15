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

package security

import (
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/rand"
)

const (
	key    = "xxxxxxxxxxxxxxxx"
	passwd = "test1234"
	enc    = "FWBuZzBWotXwK/vyL+2GFw=="
)

func TestPasswordCipher_Decrypt(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	pc := MustNewPasswordCipher(key)
	s, err := pc.Decrypt(enc)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(s).To(gomega.Equal(passwd))
}

func TestPasswordCipher_Encrypt(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	pc := MustNewPasswordCipher(key)
	s := pc.Encrypt(passwd)
	g.Expect(s).To(gomega.Equal(enc))
}

const randomTestTimes = 10000

func TestRandomEncodeDecodeTest(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	keyLen := []int{16, 24, 32}

	for i := 0; i < randomTestTimes; i++ {
		key := rand.String(keyLen[rand.Intn(3)])
		pc := MustNewPasswordCipher(key)

		plain := rand.String(rand.Intn(64))
		decrypt, err := pc.Decrypt(pc.Encrypt(plain))
		g.Expect(err).To(gomega.BeNil())
		g.Expect(decrypt).To(gomega.Equal(plain))
	}
}
