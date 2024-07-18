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

package meta

import "strings"

const AnnotationControllerHints = "xstore/controller.hints"

const (
	HintForbidden = "forbidden"
)

const (
	AnnotationBlkioResourceLimit = "xstore/blkio"
)

// Annotations for async tasks.
const (
	AnnotationAsyncTaskTransfer = "xstore/async/transfer"
	AnnotationAsyncTaskBackup   = "xstore/async/backup"
)

// AnnotationGuideRand for guiding rand generation.
const (
	AnnotationGuideRand = "xstore/guide.rand"
)

const (
	AnnotationRebuildFromPod = "xstore/rebuild_from_pod"
)

const (
	AnnotationAdapting = "xstore/adapting"
)

func IsAdaptingTrue(val string) bool {
	val = strings.ToLower(val)
	return val == "1" || val == "on" || val == "true"
}

// Annotations for backup for xstore
const (
	// AnnotationCollectJobProbeLimit denotes retry limit of getting collect job when waiting collect job finished
	AnnotationCollectJobProbeLimit = "xstore-backup/collect-job-probe-limit"
)

const (
	AnnotationAutoRebuild = "xstore/auto-rebuild"
	AnnotationFlushLocal  = "xstore/flushlocal"
	AnnotationFlushIp     = "xstore/fluship"
	AnnotationDeleteOnce  = "xstore/delete-once"
)

const (
	AnnotationDummyBackup  = "xstore/dummy-backup"
	AnnotationBackupBinlog = "xstore/backupbinlog"
)

const (
	AnnotationPitrConfig = "xstore/pitr-config"
)
