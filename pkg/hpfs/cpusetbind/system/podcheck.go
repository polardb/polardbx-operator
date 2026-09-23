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

package cpusetbind

import "strings"

type CmdLineKeyWord string

const (
	MysqlD       CmdLineKeyWord = "mysqld"
	Java         CmdLineKeyWord = "java"
	Socket       CmdLineKeyWord = "--socket"
	TddlLauncher CmdLineKeyWord = "com.alibaba.polardbx.server.TddlLauncher"
	PodId        CmdLineKeyWord = "-Dpod.id"
	PodName      CmdLineKeyWord = "--loose-pod-name"
	EnvPodId     CmdLineKeyWord = "POD_ID"
	CdcDaemon    CmdLineKeyWord = "com.aliyun.polardbx.binlog.daemon.DaemonBootStrap"
)

type PodProcCheckerItem struct {
	firstLine    bool
	kv           bool
	keyword      CmdLineKeyWord
	prefix       bool
	suffix       bool
	matched      bool
	processCount int
	value        string
}

func (p *PodProcCheckerItem) ProcessLine(line string) {
	if p.firstLine {
		if p.processCount == 0 {
			p.processLine(line)
			return
		}
	}
	p.processLine(line)
	p.processCount = p.processCount + 1
}

func (p *PodProcCheckerItem) processLine(line string) {

	if p.prefix && strings.HasPrefix(line, string(p.keyword)) {
		p.matched = true
		return
	}

	if p.suffix && strings.HasSuffix(line, string(p.keyword)) {
		p.matched = true
		return
	}

	if p.kv && strings.Contains(line, "=") {
		kv := strings.Split(line, "=")
		if len(kv) == 2 {
			if strings.EqualFold(kv[0], string(p.keyword)) {
				p.value = kv[1]
				p.matched = true
			}
		}
	}

}

func (p *PodProcCheckerItem) MatchSuccess() bool {
	return p.matched
}

func (p *PodProcCheckerItem) Value() string {
	return p.value
}

func ProcessItems(lines []string, items []*PodProcCheckerItem) {
	for _, line := range lines {
		for _, item := range items {
			item.ProcessLine(line)
		}
	}
}

func BuildCdcCheckerItems() []*PodProcCheckerItem {
	return []*PodProcCheckerItem{
		{
			firstLine: true,
			keyword:   Java,
			suffix:    true,
		},
		{
			keyword: EnvPodId,
			kv:      true,
		},
		{
			keyword: CdcDaemon,
			prefix:  true,
		},
	}
}

func BuildCnCheckerItems() []*PodProcCheckerItem {
	return []*PodProcCheckerItem{
		{
			firstLine: true,
			keyword:   Java,
			suffix:    true,
		},
		{
			keyword: TddlLauncher,
			suffix:  true,
		},
		{
			keyword: PodId,
			kv:      true,
		},
	}
}

func GetPodName(items []*PodProcCheckerItem) string {
	var podId string
	for _, item := range items {
		if !item.MatchSuccess() {
			return ""
		}
		if podId == "" && (item.keyword == PodId || item.keyword == PodName || item.keyword == EnvPodId) {
			podId = item.Value()
		}
	}
	return podId
}

func BuildDnCheckerItems() []*PodProcCheckerItem {
	return []*PodProcCheckerItem{
		{
			firstLine: true,
			keyword:   MysqlD,
			suffix:    true,
		},
		{
			keyword: Socket,
			prefix:  true,
		},
		{
			keyword: PodName,
			kv:      true,
		},
	}
}

func GetPodNameByPid(pid int) (string, error) {
	cmdLines, err := GetCmdLineByPid(pid)
	if err != nil {
		return "", err
	}

	//check if it is dn
	dnItems := BuildDnCheckerItems()
	ProcessItems(cmdLines, dnItems)
	podName := GetPodName(dnItems)
	if podName != "" {
		return podName, nil
	}

	// check if it is cn
	cnItems := BuildCnCheckerItems()
	ProcessItems(cmdLines, cnItems)
	podName = GetPodName(cnItems)
	if podName != "" {
		return podName, nil
	}

	// check if it is cdc
	cdcItems := BuildCdcCheckerItems()
	ProcessItems(cmdLines, cdcItems)
	envLines, err := GetEnvByPid(pid)
	if err != nil {
		return "", err
	}
	ProcessItems(envLines, cdcItems)
	podName = GetPodName(cdcItems)
	if podName != "" {
		return podName, nil
	}
	return "", nil
}
