# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import os
import subprocess

BuildGolangEnv = collections.namedtuple(
    'BuildGolangEnv',
    [
        'goflags'
    ]
)

BuildTarget = collections.namedtuple(
    'BuildTarget',
    [
        'target',
        'binary',
        'image',
        'image_build_path',
    ]
)


class BuildEnv:
    def __init__(self, root_dir, targets, golang, arch):
        self.root_dir = root_dir
        self.golang = golang
        self.targets = targets
        self.arch = arch

    def get_dockerfile_parent_path(self, target):
        return os.path.join(self.root_dir, 'build/images', target)

    def select_targets(self, selected: [str] or None) -> [BuildTarget]:
        if not selected:
            return self.targets
        return [t for t in self.targets if t.target in selected]

    def is_git_repository_dirty(self) -> bool or None:
        try:
            output = subprocess.check_output('git diff HEAD --quiet || echo dirty',
                                             shell=True, cwd=self.root_dir).decode('utf-8').strip()
            if 'dirty' == output:
                return True
            else:
                return False
        except subprocess.CalledProcessError as e:
            return None

    def current_git_revision_or_tag(self) -> str or None:
        if not os.path.exists(os.path.join(self.root_dir, '.git')):
            return None

        cmd = 'git describe --exact-match --tags 2> /dev/null || git rev-parse --short HEAD'
        try:
            return subprocess.check_output(cmd, shell=True, cwd=self.root_dir).decode('utf-8').strip(' \t\r\n')
        except subprocess.CalledProcessError as e:
            return None

    def current_git_tags(self, pattern: str or None) -> [str] or None:
        if not os.path.exists(os.path.join(self.root_dir, '.git')):
            return None

        cmd = ['git', 'tag', '--list']
        if pattern:
            cmd.extend(pattern)

        try:
            output = subprocess.check_output(cmd, cwd=self.root_dir).decode('utf-8')
            return [s.strip() for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return None

    def docker_build_platform(self):
        if self.arch == 'arm64':
            return 'linux/arm64'
        else:
            return 'linux/amd64'

    def golang_build_env(self):
        env = os.environ.copy()
        env['GOARCH'] = self.arch
        return env

def _parent_path(path: str, parent_idx: int) -> str:
    while parent_idx > 0:
        path = os.path.dirname(path)
        parent_idx -= 1
    return path


BASIC_BUILD_ENV = BuildEnv(
    root_dir=_parent_path(__file__, 4),
    golang=BuildGolangEnv(goflags=''),
    targets=[
        BuildTarget(target='cmd/polardbx-hpfs', binary='polardbx-hpfs', image='polardbx-hpfs', image_build_path=None),
        BuildTarget(target='cmd/polardbx-job', binary='polardbx-job', image='polardbx-job', image_build_path=None),
        BuildTarget(target='cmd/polardbx-init', binary='polardbx-init', image='polardbx-init', image_build_path=None),
        BuildTarget(target='cmd/polardbx-exporter', binary='polardbx-exporter', image='polardbx-exporter',image_build_path=None),
        BuildTarget(target='cmd/probe-proxy', binary='probe-proxy', image='probe-proxy', image_build_path=None),
        BuildTarget(target='cmd/polardbx-operator', binary='polardbx-operator', image='polardbx-operator', image_build_path=None),
        BuildTarget(target='cmd/polardbx-clinic', binary='polardbx-clinic', image='polardbx-clinic', image_build_path=None),
        BuildTarget(target='tools/xstore', binary=None, image='xstore-tools', image_build_path=None),
        BuildTarget(target='tools/logstash-filter-polardbx', binary=None, image='polardbx-logstash', image_build_path=None)
    ],
    arch=os.environ.get('ARCH', 'amd64')
)
