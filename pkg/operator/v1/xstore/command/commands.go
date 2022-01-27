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

package command

import (
	"path"
	"strconv"
)

var DefaultXStoreToolsPath = "/tools/xstore/current"

type commandBuilder struct {
	root        string
	interpreter string
	script      string
	args        []string
}

func (b *commandBuilder) build() []string {
	return append([]string{b.interpreter, b.script}, b.args...)
}

func NewCanonicalCommandBuilder() *nullCommandBuilder {
	return NewCommandBuilder(DefaultXStoreToolsPath)
}

func NewCommandBuilder(rootPath string) *nullCommandBuilder {
	return (*nullCommandBuilder)(&commandBuilder{
		root:        rootPath,
		interpreter: path.Join(rootPath, "venv/bin/python3"),
		script:      "",
		args:        make([]string, 0),
	})
}

type CommandBuilder struct {
	*commandBuilder
}

func (b *CommandBuilder) Build() []string {
	return b.build()
}

func (b *commandBuilder) end() *CommandBuilder {
	return &CommandBuilder{
		commandBuilder: b,
	}
}

type commandEntrypointBuilder struct {
	*commandBuilder
}

type nullCommandBuilder commandBuilder

func (b *nullCommandBuilder) Entrypoint() *commandEntrypointBuilder {
	b.script = path.Join(b.root, "entrypoint.py")
	return &commandEntrypointBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *nullCommandBuilder) end() *CommandBuilder {
	return &CommandBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandEntrypointBuilder) Start() *CommandBuilder {
	return b.end()
}

func (b *commandEntrypointBuilder) Initialize() *CommandBuilder {
	b.args = append(b.args, "--initialize")
	return b.end()
}

type commandConsensusBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Consensus() *commandConsensusBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "consensus")

	return &commandConsensusBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandConsensusBuilder) ReportRole(withLeader bool) *CommandBuilder {
	b.args = append(b.args, "role")
	if withLeader {
		b.args = append(b.args, "--report-leader")
	}
	return b.end()
}

func (b *commandConsensusBuilder) PurgeLogs(local, force bool) *CommandBuilder {
	b.args = append(b.args, "log", "purge")
	if local {
		b.args = append(b.args, "--local")
	}
	if force {
		b.args = append(b.args, "--force")
	}
	return b.end()
}

func (b *commandConsensusBuilder) SetLeader(pod string) *CommandBuilder {
	b.args = append(b.args, "change-leader", "--node", pod)
	return b.end()
}

func (b *commandConsensusBuilder) ChangeRole(pod, from, to string) *CommandBuilder {
	b.args = append(b.args, "change", "--node", pod, "--from-role", from, "--to-role", to)
	return b.end()
}

func (b *commandConsensusBuilder) ConfigureElectionWeight(weight int, pods ...string) *CommandBuilder {
	b.args = append(b.args, "configure", "--weight", strconv.Itoa(weight))
	b.args = append(b.args, pods...)
	return b.end()
}

func (b *commandConsensusBuilder) AddNode(pod, role string) *CommandBuilder {
	b.args = append(b.args, "add", "--node", pod, "--role", role, "--idempotent")
	return b.end()
}

func (b *commandConsensusBuilder) DropNode(pod, role string) *CommandBuilder {
	b.args = append(b.args, "drop", "--node", pod, "--role", role, "--idempotent")
	return b.end()
}

func (b *commandConsensusBuilder) ForceSingleMode() *CommandBuilder {
	b.args = append(b.args, "force-single-mode")
	return b.end()
}

type commandAccountBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Account() *commandAccountBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "account")

	return &commandAccountBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandAccountBuilder) Create(user, passwd string) *CommandBuilder {
	b.args = append(b.args, "create", "-u", user, "-p", passwd)
	return b.end()
}

func (b *commandAccountBuilder) Reset(user, passwd string) *CommandBuilder {
	b.args = append(b.args, "reset", "-u", user, "-p", passwd)
	return b.end()
}

type commandHealthyBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Healthy() *commandHealthyBuilder {
	b.script = path.Join(b.root, "healthy.py")
	return &commandHealthyBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandHealthyBuilder) Check(checkReadWriteReadyOnLeader bool) *CommandBuilder {
	if checkReadWriteReadyOnLeader {
		b.args = append(b.args, "--leader-check")
	}
	return b.end()
}

func (b *nullCommandBuilder) Ping() *CommandBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "ping")
	return b.end()
}

type commandConfigBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Config() *commandConfigBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "vars")
	return &commandConfigBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandConfigBuilder) Set(variables map[string]string) *CommandBuilder {
	b.args = append(b.args, "set")
	for k, v := range variables {
		b.args = append(b.args, "--name", k, "--value", v)
	}
	return b.end()
}

type commandLogBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Log() *commandLogBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "log")
	return &commandLogBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandLogBuilder) Purge(local, force bool) *CommandBuilder {
	b.args = append(b.args, "purge")
	if local {
		b.args = append(b.args, "--local")
	}
	if force {
		b.args = append(b.args, "--force")
	}
	return b.end()
}

type commandEngineBuilder struct {
	*commandBuilder
}

func (b *nullCommandBuilder) Engine() *commandEngineBuilder {
	b.script = path.Join(b.root, "cli.py")
	b.args = append(b.args, "engine")
	return &commandEngineBuilder{
		commandBuilder: (*commandBuilder)(b),
	}
}

func (b *commandEngineBuilder) Version() *CommandBuilder {
	b.args = append(b.args, "version")
	return b.end()
}
