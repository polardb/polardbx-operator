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

package xstore

import "strings"

type NodeRole string

const (
	// RoleLeader means it can handle read and write requests both. It's logs are distributed
	// by consensus protocols.
	RoleLeader NodeRole = "Leader"

	// RoleFollower keeps in consistent with leader in either logs and materialized data.
	// It can handle consistent reads but can not handle write requests.
	RoleFollower NodeRole = "Follower"

	// RoleLogger means voter below.
	RoleLogger NodeRole = "logger"

	// RoleLearner means that it just learns the logs and never guarantees to
	// catch up at any given time.
	RoleLearner NodeRole = "Learner"

	// RoleCandidate means that can either be leader or follower.
	RoleCandidate NodeRole = "Candidate"

	// RoleVoter means that it's a logger, keep in consistent with candidates in log but can
	// never be voted to be a leader.
	RoleVoter NodeRole = "Voter"
)

func FromNodeRoleValue(val string) NodeRole {
	vals := []NodeRole{
		RoleLeader, RoleFollower, RoleLeader, RoleLearner, RoleCandidate, RoleVoter,
	}
	for _, v := range vals {
		if strings.ToLower(string(v)) == strings.ToLower(val) {
			return v
		}
	}
	panic("undefined")
}
