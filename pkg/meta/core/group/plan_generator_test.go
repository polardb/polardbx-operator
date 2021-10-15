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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/rand"
)

func newSchemaGroups(storageIds []string) map[string][]Group {
	// Generate a "x" on 0 storage
	schemaGroups := make(map[string][]Group)

	schemaGroups["x"] = make([]Group, 8)
	for i := 0; i < 8; i++ {
		schemaGroups["x"][i] = Group{
			ID:         i,
			StorageId:  storageIds[0],
			DB:         "x",
			Group:      "X_00000" + strconv.Itoa(i),
			PhysicalDB: "x_00000" + strconv.Itoa(i),
			Movable:    true,
		}
	}

	// Generate a "y" on each storage
	schemaGroups["y"] = make([]Group, 8*len(storageIds))
	for i := 0; i < 8*len(storageIds); i++ {
		schemaGroups["y"][i] = Group{
			ID:         i,
			StorageId:  storageIds[i/8],
			DB:         "y",
			Group:      "Y_00000" + strconv.Itoa(i),
			PhysicalDB: "y_00000" + strconv.Itoa(i),
			Movable:    true,
		}
	}

	// Random generate some
	db := rand.String(2)
	schemaGroups[db] = make([]Group, 0)
	for i := 0; i < rand.Intn(12)+8; i++ {
		schemaGroups[db] = append(schemaGroups[db], Group{
			StorageId:  storageIds[rand.Intn(len(storageIds))],
			DB:         db,
			Group:      strings.ToUpper(db) + "_00000" + strconv.Itoa(i),
			PhysicalDB: db + "_00000" + strconv.Itoa(i),
			Movable:    true,
		})
	}

	// Print topology
	storageGroups := make(map[string][]Group)
	for _, storageId := range storageIds {
		storageGroups[storageId] = make([]Group, 0)
	}
	for _, groups := range schemaGroups {
		for _, group := range groups {
			storageGroups[group.StorageId] = append(storageGroups[group.StorageId], group)
		}
	}
	fmt.Println("Topology: ")
	for storageId, groups := range storageGroups {
		fmt.Printf("  - %s: ", storageId)
		for i, group := range groups {
			fmt.Printf("%s", group.Group)
			if i < len(groups)-1 {
				fmt.Print(", ")
			} else {
				fmt.Println()
			}
		}
	}

	return schemaGroups
}

func newSchemaGroup332() ([]string, []string, map[string][]Group) {
	curStorages := []string{"s0", "s1", "s2"}
	toDeleteStorages := []string{"s2"}
	schemaGroups := make(map[string][]Group)

	schemaGroups["x"] = make([]Group, 8)
	for i := 0; i < 8; i++ {
		index := 0
		if i >= 6 {
			index = 2
		} else if i >= 3 {
			index = 1
		}
		schemaGroups["x"][i] = Group{
			ID:         i,
			StorageId:  curStorages[index],
			DB:         "x",
			Group:      "X_00000" + strconv.Itoa(i),
			PhysicalDB: "x_00000" + strconv.Itoa(i),
			Movable:    true,
		}
	}

	schemaGroups["y"] = make([]Group, 8)
	for i := 0; i < 8; i++ {
		index := 0
		if i >= 6 {
			index = 2
		} else if i >= 3 {
			index = 1
		}
		schemaGroups["y"][i] = Group{
			ID:         i,
			StorageId:  curStorages[index],
			DB:         "y",
			Group:      "Y_00000" + strconv.Itoa(i),
			PhysicalDB: "y_00000" + strconv.Itoa(i),
			Movable:    true,
		}
	}

	return curStorages, toDeleteStorages, schemaGroups
}

func TestPlanGenerator_GenerateForNewStorages(t *testing.T) {
	pg := NewPlanGenerator()

	curStorageIds := []string{"s0", "s1"}
	newStorageIds := []string{"s2", "s3"}
	schemaGroups := newSchemaGroups(curStorageIds)

	plans := pg.GenerateForNewStorages(curStorageIds, newStorageIds, schemaGroups)

	fmt.Println("Plans: ")
	for _, plan := range plans {
		fmt.Printf("  - %s: ", plan.Schema)
		for i := 0; i < len(plan.MigrateTasks); i++ {
			fmt.Print(plan.MigrateTasks[i].String())
			if i < len(plan.MigrateTasks)-1 {
				fmt.Print(", ")
			} else {
				fmt.Println()
			}
		}
	}
}

func newGroupsFixed2(idx int, id string) []Group {
	return []Group{
		{
			ID:         idx * 2,
			StorageId:  id,
			DB:         "x",
			Group:      "X_00000" + strconv.Itoa(idx*2),
			PhysicalDB: "x_00000" + strconv.Itoa(idx*2),
			Movable:    true,
		},
		{
			ID:         idx*2 + 1,
			StorageId:  id,
			DB:         "x",
			Group:      "X_00000" + strconv.Itoa(idx*2+1),
			PhysicalDB: "x_00000" + strconv.Itoa(idx*2+1),
			Movable:    true,
		},
	}
}

func TestPlanGenerator_GenerateForNewStorages2(t *testing.T) {
	pg := NewPlanGenerator()

	curStorageIds := []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"}
	newStorageIds := []string{"s8", "s9", "s10", "s11"}
	schemaGroups := map[string][]Group{}
	schemaGroups["x"] = make([]Group, 0, 16)
	for i, id := range curStorageIds {
		schemaGroups["x"] = append(schemaGroups["x"], newGroupsFixed2(i, id)...)
	}

	plans := pg.GenerateForNewStorages(curStorageIds, newStorageIds, schemaGroups)

	fmt.Println("Plans: ")
	for _, plan := range plans {
		fmt.Printf("  - %s: ", plan.Schema)
		for i := 0; i < len(plan.MigrateTasks); i++ {
			fmt.Print(plan.MigrateTasks[i].String())
			if i < len(plan.MigrateTasks)-1 {
				fmt.Print(", ")
			} else {
				fmt.Println()
			}
		}
	}
}

func TestPlanGenerator_GenerateForToRemoveStorages(t *testing.T) {
	pg := NewPlanGenerator()

	curStorageIds := []string{"s0", "s1", "s2"}
	toRemoveStorageIds := []string{"s2"}
	schemaGroups := newSchemaGroups(curStorageIds)

	plans, err := pg.GenerateForToRemoveStorages(curStorageIds, toRemoveStorageIds, schemaGroups)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Plans: ")
	for _, plan := range plans {
		fmt.Printf("  - %s: ", plan.Schema)
		for i := 0; i < len(plan.MigrateTasks); i++ {
			fmt.Print(plan.MigrateTasks[i].String())
			if i < len(plan.MigrateTasks)-1 {
				fmt.Print(", ")
			} else {
				fmt.Println()
			}
		}
	}
}

func TestPlanGenerator_GenerateForToRemoveStorages2(t *testing.T) {
	pg := NewPlanGenerator()

	curStorageIds, toRemoveStorageIds, schemaGroups := newSchemaGroup332()

	plans, err := pg.GenerateForToRemoveStorages(curStorageIds, toRemoveStorageIds, schemaGroups)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Plans: ")
	for _, plan := range plans {
		fmt.Printf("  - %s: ", plan.Schema)
		for i := 0; i < len(plan.MigrateTasks); i++ {
			fmt.Print(plan.MigrateTasks[i].String())
			if i < len(plan.MigrateTasks)-1 {
				fmt.Print(", ")
			} else {
				fmt.Println()
			}
		}
	}
}
