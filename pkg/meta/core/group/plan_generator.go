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

package group

import (
	"errors"
	"fmt"
	"sort"

	mathutil "github.com/alibaba/polardbx-operator/pkg/util/math"
)

type Migrate struct {
	Group           Group
	TargetStorageID string
}

func (m *Migrate) String() string {
	return fmt.Sprintf("%s(%s)->%s", m.Group.Group, m.Group.StorageId, m.TargetStorageID)
}

type Plan struct {
	Schema       string
	MigrateTasks []Migrate
}

type innerPlan struct {
	Plan
	idx int
}

func (p *innerPlan) newMigrate(group Group, storageId string) {
	p.MigrateTasks = append(p.MigrateTasks, Migrate{
		Group:           group,
		TargetStorageID: storageId,
	})
}

func (p *innerPlan) markMoveTo(storageId string, n int) {
	n = mathutil.MinInt(n, len(p.MigrateTasks)-p.idx)
	for i := 0; i < n; i++ {
		p.MigrateTasks[i+p.idx].TargetStorageID = storageId
	}
	p.idx += n
}

func (p *innerPlan) cancelLeft() {
	p.MigrateTasks = p.MigrateTasks[:p.idx]
}

func (p *innerPlan) completed() bool {
	return p.idx >= len(p.MigrateTasks)
}

type PlanGenerator interface {
	GenerateForNewStorages(curStorageIds []string, newStorageIds []string, schemaGroups map[string][]Group) []Plan
	GenerateForToRemoveStorages(curStorageIds []string, toRemoveStorageIds []string, schemaGroups map[string][]Group) ([]Plan, error)
}

type schemaGroups struct {
	schema string
	groups []Group
}

type groupsOnStorage struct {
	schemaGroups []schemaGroups
}

func buildGroupsOnStorage(groups []Group, reverse bool) *groupsOnStorage {
	if groups == nil {
		return &groupsOnStorage{schemaGroups: make([]schemaGroups, 0)}
	}

	gos := &groupsOnStorage{schemaGroups: make([]schemaGroups, 0, 4)}

	gosMap := make(map[string][]Group)
	for _, grp := range groups {
		slice, ok := gosMap[grp.DB]
		if !ok {
			slice = make([]Group, 0, 2)
			gosMap[grp.DB] = slice
		}
		gosMap[grp.DB] = append(slice, grp)
	}

	for schema, groups := range gosMap {
		gos.schemaGroups = append(gos.schemaGroups, schemaGroups{
			schema: schema,
			groups: groups,
		})
	}

	sort.Slice(gos.schemaGroups, func(i, j int) bool {
		if reverse {
			return len(gos.schemaGroups[i].groups) > len(gos.schemaGroups[j].groups)
		} else {
			return len(gos.schemaGroups[i].groups) < len(gos.schemaGroups[j].groups)
		}
	})

	return gos
}

type storageTopology struct {
	schemaGroups map[string][]Group
}

func (t *storageTopology) addGroup(g *Group) {
	groups, ok := t.schemaGroups[g.DB]
	if !ok {
		t.schemaGroups[g.DB] = []Group{*g}
	}
	t.schemaGroups[g.DB] = append(groups, *g)
}

func (t *storageTopology) groups(schema string) []Group {
	if t == nil {
		return make([]Group, 0)
	}

	groups := t.schemaGroups[schema]
	if groups == nil {
		return make([]Group, 0)
	}
	return groups
}

func (t *storageTopology) groupSize(schema string) int {
	if t == nil {
		return 0
	}

	groups := t.schemaGroups[schema]
	if groups == nil {
		return 0
	}
	return len(groups)
}

type planGenerator struct{}

func (p *planGenerator) GenerateForNewStorages(curStorageIds []string, newStorageIds []string, schemaGroups map[string][]Group) []Plan {
	// The algorithm is quite straight-forward. Assume that there are n databases labeled with d_i, where i in [0, n).
	// Each of them has x_i groups distributed on M storages. Now we are going to add N storages into the cluster.
	// Assume that X = sum(x_i) denotes the total groups on storages.
	//
	// Our goal is to try our best to make each database's groups evenly distributed on every storage, and the overall
	// distribution is also even.
	//
	// In general, each storage should have at most ceil(X/(M+N)) groups. And for each database, each storage should have
	// ceil(x_i/(M+N)) its groups. It' trivial that sum(ceil(x_i/(M+N))) >= ceil(X/(M+N)).

	storageIds := append(newStorageIds, curStorageIds...)
	storageCount := len(storageIds)
	desiredGroupCnt := make(map[string]int) // for each schema

	// Initialize storage topologies
	storageTopologies := make(map[string]*storageTopology)
	for _, storageId := range curStorageIds {
		storageTopologies[storageId] = &storageTopology{schemaGroups: make(map[string][]Group)}
	}

	for schema, groups := range schemaGroups {
		movableGroupCnt := 0
		for i := range groups {
			group := groups[i]
			if !group.Movable {
				continue
			}
			movableGroupCnt++
			storageTopologies[group.StorageId].addGroup(&group)
		}

		// Left at least desired group count on each storages.
		if movableGroupCnt >= storageCount {
			desiredGroupCnt[schema] = movableGroupCnt / storageCount
		} else {
			desiredGroupCnt[schema] = 1
		}
	}

	// Mark groups to move
	plans := make(map[string]*innerPlan)
	for schema := range schemaGroups {
		plans[schema] = &innerPlan{
			Plan: Plan{
				Schema:       schema,
				MigrateTasks: make([]Migrate, 0),
			},
			idx: 0,
		}
	}

	for _, topology := range storageTopologies {
		for schema, groups := range topology.schemaGroups {
			if len(groups) <= desiredGroupCnt[schema] {
				continue
			}

			for _, group := range groups[desiredGroupCnt[schema]:] {
				plans[schema].newMigrate(group, "")
			}
		}
	}

	// Mark the destination of moving groups
	for _, storageId := range storageIds {
		topology := storageTopologies[storageId]
		for schema := range schemaGroups {
			plan := plans[schema]
			if plan == nil {
				continue
			}
			moveToCnt := desiredGroupCnt[schema] - topology.groupSize(schema)
			if moveToCnt < 0 {
				continue
			}
			plan.markMoveTo(storageId, moveToCnt)
		}
	}

	// Some groups might not be moved to reduce the size of data migrated.
	// Just cancel the tasks.
	for _, plan := range plans {
		plan.cancelLeft()
	}

	// Check
	for _, plan := range plans {
		if !plan.completed() {
			panic("bug, plan for " + plan.Schema + " not completed")
		}
	}

	// Construct and return the plan
	planSlice := make([]Plan, 0)
	for _, innerPlan := range plans {
		if len(innerPlan.MigrateTasks) > 0 {
			planSlice = append(planSlice, innerPlan.Plan)
		}
	}

	return planSlice
}

func removeSlice(current, toRemove []string) []string {
	removeMap := make(map[string]interface{})
	for _, s := range toRemove {
		removeMap[s] = nil
	}

	resultMap := make(map[string]interface{})
	for _, s := range current {
		if _, ok := removeMap[s]; !ok {
			resultMap[s] = nil
		}
	}

	r := make([]string, 0, len(resultMap))
	for s := range resultMap {
		r = append(r, s)
	}
	return r
}

func (p *planGenerator) GenerateForToRemoveStorages(curStorageIds []string, toRemoveStorageIds []string, schemaGroups map[string][]Group) ([]Plan, error) {
	// Check if there're unmovable groups
	toRemoveMap := make(map[string]interface{})
	for _, s := range toRemoveStorageIds {
		toRemoveMap[s] = nil
	}
	for schema, groups := range schemaGroups {
		for _, group := range groups {
			_, onRemoveStorage := toRemoveMap[group.StorageId]
			if !group.Movable && onRemoveStorage {
				return nil, errors.New("unable to generate plans for removing storages, unmovable group " + group.Group +
					" of " + schema + " found on to remove storage " + group.StorageId)
			}
		}
	}

	storageIds := removeSlice(curStorageIds, toRemoveStorageIds)
	storageCount := len(storageIds)

	storageTopologies := make(map[string]*storageTopology)
	for _, storageId := range curStorageIds {
		storageTopologies[storageId] = &storageTopology{schemaGroups: make(map[string][]Group)}
	}

	desiredGroupCnt := make(map[string]int) // for each schema
	for schema, groups := range schemaGroups {
		movableGroupCnt := 0
		for _, group := range groups {
			if !group.Movable {
				continue
			}
			movableGroupCnt++
			storageTopologies[group.StorageId].addGroup(&group)
		}
		desiredGroupCnt[schema] = (movableGroupCnt + storageCount - 1) / storageCount
	}

	plans := make(map[string]*innerPlan)
	for schema := range schemaGroups {
		plans[schema] = &innerPlan{
			Plan: Plan{
				Schema:       schema,
				MigrateTasks: make([]Migrate, 0),
			},
			idx: 0,
		}
	}

	for _, storageId := range toRemoveStorageIds {
		topology := storageTopologies[storageId]
		if topology == nil {
			continue
		}
		for schema, groups := range topology.schemaGroups {
			for _, group := range groups {
				plans[schema].newMigrate(group, "")
			}
		}
	}

	for _, storageId := range storageIds {
		topology := storageTopologies[storageId]
		for schema, groups := range topology.schemaGroups {
			moveToCnt := desiredGroupCnt[schema] - len(groups)
			if moveToCnt < 0 {
				continue
			}

			plans[schema].markMoveTo(storageId, moveToCnt)
		}
	}

	// Check
	for _, plan := range plans {
		if !plan.completed() {
			panic("bug, plan for " + plan.Schema + " not completed")
		}
	}

	// Construct and return the plan
	planSlice := make([]Plan, 0)
	for _, innerPlan := range plans {
		if len(innerPlan.MigrateTasks) > 0 {
			planSlice = append(planSlice, innerPlan.Plan)
		}
	}

	return planSlice, nil
}

func NewPlanGenerator() PlanGenerator {
	return &planGenerator{}
}
