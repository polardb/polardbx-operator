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

package name

import (
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"strings"
)

// Naming functions related to restore phase

func StableNamePrefix(xstore *polardbxv1.XStore) string {
	if len(xstore.Status.Rand) > 0 {
		return fmt.Sprintf("%s-%s-", xstore.Name, xstore.Status.Rand)
	} else {
		return fmt.Sprintf("%s-", xstore.Name)
	}
}

func StableName(xstore *polardbxv1.XStore, suffix string) string {
	return StableNamePrefix(xstore) + suffix
}

func GetStableNameSuffix(xstore *polardbxv1.XStore, name string) string {
	prefix := StableNamePrefix(xstore)
	if strings.HasPrefix(name, prefix) {
		return name[len(prefix):]
	} else {
		panic("not start with prefix: " + prefix)
	}
}

func PolarDBXBackupStableNamePrefix(polardbxBackup *polardbxv1.PolarDBXBackup) string {
	return fmt.Sprintf("%s-", polardbxBackup.Name)
}

func PolarDBXBackupStableName(polardbxBackup *polardbxv1.PolarDBXBackup, suffix string) string {
	return PolarDBXBackupStableNamePrefix(polardbxBackup) + suffix
}

func XStoreBackupStableNamePrefix(xstoreBackup *polardbxv1.XStoreBackup) string {
	return fmt.Sprintf("%s-", xstoreBackup.Name)
}

func XStoreBackupStableName(xstoreBackup *polardbxv1.XStoreBackup, suffix string) string {
	return XStoreBackupStableNamePrefix(xstoreBackup) + suffix
}

// Splicer is a helper to splice object name, which also provides alternative name to ensure length of name is under limit
type Splicer struct {
	Tokens    *[]string
	Delimiter string
	Limit     int
	Prefix    string
}

func NewSplicer(options ...func(*Splicer)) *Splicer {
	splicer := &Splicer{
		Delimiter: "-",
		Limit:     253,
	}
	for _, option := range options {
		option(splicer)
	}
	return splicer
}

// getAbbreviateName generate a hashed string from sourceName
func (s *Splicer) getAbbreviateName(sourceName string) string {
	hashVal := security.MustSha1Hash(sourceName)
	if s.Prefix == "" {
		return hashVal
	}
	return fmt.Sprintf("%s%s%s", s.Prefix, s.Delimiter, hashVal)
}

// GetName returns spliced name if length less than limit, otherwise an abbreviate name with hash value
func (s *Splicer) GetName() string {
	name := strings.Join(*s.Tokens, s.Delimiter)
	if s.Limit == 0 || len(name) < s.Limit {
		return name
	}
	return s.getAbbreviateName(name)
}

func WithTokens(tokens ...string) func(*Splicer) {
	return func(splicer *Splicer) {
		splicer.Tokens = &tokens
	}
}

func WithDelimiter(delimiter string) func(*Splicer) {
	return func(splicer *Splicer) {
		splicer.Delimiter = delimiter
	}
}

func WithLimit(limit int) func(*Splicer) {
	return func(splicer *Splicer) {
		splicer.Limit = limit
	}
}

func WithPrefix(prefix string) func(*Splicer) {
	return func(splicer *Splicer) {
		splicer.Prefix = prefix
	}
}

func NewSplicedName(options ...func(*Splicer)) string {
	return NewSplicer(options...).GetName()
}
