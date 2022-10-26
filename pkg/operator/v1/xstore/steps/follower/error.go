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

package follower

import "errors"

var (
	ErrorFromPodNotFound            = errors.New("FromPodNotFound")
	ErrorGetXStorePod               = errors.New("ErrorGetXStorePod")
	ErrorLeaderPodNotFound          = errors.New("ErrorLeaderPodNotFound")
	ErrorFormPodShouldNotBeLogger   = errors.New("ErrorFormPodShouldNotBeLogger")
	ErrorTargetPodShouldNotBeLeader = errors.New("ErrorTargetPodShouldNotBeLeader")
	ErrorLabelNotFound              = errors.New("ErrorLabelNotFound")
	ErrorInvalidNodeRole            = errors.New("ErrorInvalidNodeRole")
	ErrorInvalidCommitIndex         = errors.New("ErrorInvalidCommitIndex")
	ErrorPodScheduledFailed         = errors.New("ErrorPodScheduledFailed")
	ErrorInvalidNodeName            = errors.New("ErrorInvalidNodeName")
	ErrorNotFoundPodVolume          = errors.New("ErrorNotFoundPodVolume")
	ErrorFailGetPod                 = errors.New("ErrorFailGetPod")
	ErrorFailDeletePod              = errors.New("ErrorFailDeletePod")
	ErrorFailUpdateXStore           = errors.New("ErrorFailUpdateXStore")
)
