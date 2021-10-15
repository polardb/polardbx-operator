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

package guide

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestTopologyRuleHandler_parseTopologyRuleGuide(t *testing.T) {
	h := &topologyRuleHandler{}

	testcases := map[string]struct {
		s              string
		mode           TopologyRuleGuideMode
		regionAndZones []RegionZones
		err            error
	}{
		"standard-1l2c": {
			s:    "zone-mode-1l2c(zone-a|zone-b)",
			mode: RuleGuide1l2c,
			regionAndZones: []RegionZones{
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-a", "zone-b"},
				},
			},
			err: nil,
		},
		"complex-1l2c-with-zone-label": {
			s:    "zone-mode-1l2c(zone=zone-a|zone-b)",
			mode: RuleGuide1l2c,
			regionAndZones: []RegionZones{
				{
					ZoneLabel: "zone",
					Zones:     []string{"zone-a", "zone-b"},
				},
			},
			err: nil,
		},
		"complex-1l2c-with-region": {
			s:    "zone-mode-1l2c(region_r, zone-a|zone-b)",
			mode: RuleGuide1l2c,
			regionAndZones: []RegionZones{
				{
					RegionLabel: corev1.LabelTopologyRegion,
					Region:      "region_r",
					ZoneLabel:   corev1.LabelTopologyZone,
					Zones:       []string{"zone-a", "zone-b"},
				},
			},
			err: nil,
		},
		"complex-1l2c-with-region-label": {
			s:    "zone-mode-1l2c(region=region_r, zone-a|zone-b)",
			mode: RuleGuide1l2c,
			regionAndZones: []RegionZones{
				{
					RegionLabel: "region",
					Region:      "region_r",
					ZoneLabel:   corev1.LabelTopologyZone,
					Zones:       []string{"zone-a", "zone-b"},
				},
			},
			err: nil,
		},
		"complex-1l2c-with-region-and-zone-label": {
			s:    "zone-mode-1l2c(region=region_r, zone=zone-a|zone-b)",
			mode: RuleGuide1l2c,
			regionAndZones: []RegionZones{
				{
					RegionLabel: "region",
					Region:      "region_r",
					ZoneLabel:   "zone",
					Zones:       []string{"zone-a", "zone-b"},
				},
			},
			err: nil,
		},
		"standard-1l3c": {
			s:    "zone-mode-1l3c(zone-a|zone-b|zone-c)",
			mode: RuleGuide1l3c,
			regionAndZones: []RegionZones{
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-a", "zone-b", "zone-c"},
				},
			},
			err: nil,
		},
		"standard-2l3c": {
			s:    "zone-mode-2l3c(zone-a|zone-b)(zone-c)",
			mode: RuleGuide2l3c,
			regionAndZones: []RegionZones{
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-a", "zone-b"},
				},
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-c"},
				},
			},
			err: nil,
		},
		"standard-3l5c": {
			s:    "zone-mode-3l5c(zone-a|zone-b)(zone-c|zone-d)(zone-e)",
			mode: RuleGuide3l5c,
			regionAndZones: []RegionZones{
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-a", "zone-b"},
				},
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-c", "zone-d"},
				},
				{
					ZoneLabel: corev1.LabelTopologyZone,
					Zones:     []string{"zone-e"},
				},
			},
			err: nil,
		},
		"broken-1l2c": {
			s:   "zone-mode-1l2c(",
			err: ErrInvalidRuleGuide,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			mode, rzs, err := h.parseTopologyRuleGuide(tc.s)
			if err != nil {
				if tc.err == nil {
					t.Fatal("unexpected error", err)
				}
				if err.Error() != tc.err.Error() {
					t.Fatal("unexpected error", err)
				}
			}
			if mode != tc.mode {
				t.Fatal("unexpected mode")
			}
			if !reflect.DeepEqual(rzs, tc.regionAndZones) {
				t.Fatal("unexpected region and zones")
			}
		})
	}
}
