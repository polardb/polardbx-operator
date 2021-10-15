#!/usr/bin/env python3

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

import argparse
import os
import subprocess

import lib


def build_target(build_env: lib.BuildEnv, target: lib.BuildTarget):
    if not target.binary:
        raise Exception('no binary target defined.')
    subprocess.check_call([
        'go', 'build',
        '-o', os.path.join('target', build_env.arch, target.binary),
        './' + target.target
    ], cwd=build_env.root_dir, env=build_env.golang_build_env())


def build_targets(build_env: lib.BuildEnv, targets: [str] or None):
    # Select targets to build
    targets = [t for t in build_env.select_targets(targets) if t.binary]

    print('Targets: [%s] ...' % ', '.join(
        [t.target for t in targets]
    ))
    print('Target arch: %s' % build_env.arch)

    for t in targets:
        print('[%s] Start build...' % t.target)
        build_target(build_env, t)
        print('[%s] Built!' % t.target)

    print('All targets built.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('targets', metavar='target', type=str, nargs='*', help='targets to build.')
    args = parser.parse_args()

    build_targets(lib.BASIC_BUILD_ENV, args.targets)
