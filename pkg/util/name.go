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

package util

import (
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"strings"
)

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

// BackupRootPath is used to identify backup set by backup time
func BackupRootPath(polardbxBackup *polardbxv1.PolarDBXBackup) string {
	startTime := polardbxBackup.Status.StartTime
	timestamp := startTime.Format("20060102150405") // golang standard format
	rootPath := fmt.Sprintf("%s/%s/%s-%s",
		polardbxmeta.BackupPath, polardbxBackup.Labels[polardbxmeta.LabelName], polardbxBackup.Name, timestamp)
	return rootPath
}
