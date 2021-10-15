#!/bin/ash

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

set -e

if [[ $# -lt 1 ]]; then
  echo "$0 [target dir]"
fi

TARGET_DIR=$1

if [ ! -d "$TARGET_DIR" ]; then
  echo "target dir not exists"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null && pwd)"
TOOLS_DIR="$SCRIPT_DIR/.."

if [ ! -f "$TOOLS_DIR"/version ]; then
  echo "version file not exists"
  exit 1
fi

VERSION="$(cat "$TOOLS_DIR"/version)"
if [ -z "$VERSION" ]; then
  echo "version is empty"
  exit 1
fi

echo "version: $VERSION"

VERSION_DIR="$TARGET_DIR"/"$VERSION"
LINK_VALUE=""
if [ -h "$TARGET_DIR"/current ]; then
  LINK_VALUE="$(readlink "$TARGET_DIR"/current)"
fi

if [[ "$LINK_VALUE" == "$VERSION" ]]; then
  echo "up-to-date! exit!"
  exit 0
fi

# Copy to temp
rm -rf "$VERSION_DIR" && mkdir -p "$VERSION_DIR"
cp -Rp "$TOOLS_DIR"/. "$VERSION_DIR"

# Not atomic
cd "$TARGET_DIR" && ln -sfn "$VERSION" current

# Clean old files
if [ -n "$LINK_VALUE" ]; then
  rm -rf "$LINK_VALUE"
fi

echo "updated!"
