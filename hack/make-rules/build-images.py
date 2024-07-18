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
import time

import lib

DEFAULT_IMAGE_SOURCE = 'polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx'


def build_image_name(repo, name, tag) -> str:
    r = ''
    if repo and len(repo) > 0:
        if repo[-1] == '/':
            r += repo
        else:
            r += repo + '/'
    r += name
    if tag and len(tag) > 0:
        r += ':' + tag

    return r

def retag_image(src, dest):
    return subprocess.check_call([
        'docker', 'tag', src, dest,
    ])

def build_image_for(build_env: lib.BuildEnv, target: lib.BuildTarget, repo, tag, image_source) -> str:
    dockerfile = os.path.join(build_env.get_dockerfile_parent_path(target.image), 'Dockerfile')

    current_revision = build_env.current_git_revision_or_tag()
    version = current_revision

    # If not dirty, just use the revision.
    is_repo_dirty = build_env.is_git_repository_dirty()
    if is_repo_dirty is None or is_repo_dirty:
        version += "-" + str(time.time()) if current_revision else str(time.time())

    if not tag or tag == '':
        tag = version

    image_name = build_image_name(repo, target.image, tag)
    # if build_env.arch != 'amd64':
    #     image_name += '-' + build_env.arch

    image_build_path = os.path.join(build_env.root_dir, target.image_build_path) \
        if target.image_build_path else build_env.root_dir

    print('[%s] Building image %s... @ %s' % (target.target, image_name, image_build_path))

    subprocess.check_call([
        'docker', 'build', '--network', 'host',
        '--build-arg', 'VERSION=' + version,
        '--build-arg', 'IMAGE_SOURCE=' + image_source,
        '--platform', build_env.docker_build_platform(),
        '-t', image_name,
        '-f', dockerfile,
        '.'
    ], cwd=image_build_path)
    return image_name


def build_images(build_env: lib.BuildEnv, targets: [str] or None, repo, tag, image_source, *, also_latest=False) -> [str]:
    # Select targets to build
    targets = [t for t in build_env.select_targets(targets) if t.image]

    print('Targets: [%s] ...' % ', '.join(
        [t.target for t in targets]
    ))
    print('Target arch: %s' % build_env.arch)

    images = []
    for t in targets:
        print('[%s] Start build...' % t.target)
        image = build_image_for(build_env, t, repo, tag, image_source)
        print('[%s] Built: %s' % (t.target, image))

        if tag != 'latest' and also_latest:
            name = image.split(':')[0]
            latest_img = name + ':latest'
            retag_image(image, latest_img)
            print('[%s] Tag to latest: %s' % (t.target, latest_img))
            images.append(latest_img)

        images.append(image)

    print('All targets built.')

    return images


def push_image(image):
    print('Start pushing image: %s' % image)
    subprocess.check_call('docker push ' + image, shell=True)
    print('Pushed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', dest='repo', default='', type=str, help='registry and repo of images.')
    parser.add_argument('--tag', dest='tag', default='', type=str, help='tag of images.')
    parser.add_argument('targets', metavar='target', type=str, nargs='*', help='targets to build.')
    parser.add_argument('--push', dest='push', action='store_true', help='push to registry.')
    parser.add_argument('--also-latest', dest='also_latest', action='store_true', help='also tag to latest')
    parser.add_argument('--image-source', dest='image_source', default=DEFAULT_IMAGE_SOURCE, help='custom docker source registry/namespace')
    args = parser.parse_args()

    images = build_images(
        lib.BASIC_BUILD_ENV,
        args.targets,
        args.repo,
        args.tag,
        args.image_source,
        also_latest=args.also_latest,
    )

    if args.push:
        for image in images:
            push_image(image)
