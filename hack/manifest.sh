#!/bin/bash

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

ANSI_GREEN="\033[0;32m"
ANSI_NC="\033[0m"
GREEN_OK="${ANSI_GREEN}OK${ANSI_NC}"

REGISTRY=$1
TAG=$2

case "$1" in
    -h|--help|?)
    echo "Usage: 1st arg: registry, 2nd arg: tag"
    exit 0
;;
esac

if [ ! -n "$1" ]; then
    echo "please input 1st arg: registry"
    exit
fi

if [ ! -n "$2" ]; then
    echo "please input 2nd arg: tag"
    exit
fi

TARGETS="xstore-tools polardbx-operator probe-proxy polardbx-exporter polardbx-init polardbx-hpfs polardbx-logstash"

# 构建镜像
make build REPO="${REGISTRY}" ARCH=arm64 TAG="${TAG}"-arm64
make build REPO="${REGISTRY}" ARCH=amd64 TAG="${TAG}"-amd64
echo -e "[$GREEN_OK] image built"
echo

# 推送镜像
for i in $(docker images | grep "${TAG}" | awk 'BEGIN{OFS=":"}{print $1,$2}'); do docker push $i; done
echo -e "[$GREEN_OK] image pushed"
echo

# 创建manifest
for i in $TARGETS
do
  docker manifest create "${REGISTRY}"/$i:"${TAG}" \
  "${REGISTRY}"/$i:"${TAG}"-arm64 \
  "${REGISTRY}"/$i:"${TAG}"-amd64 --amend
done
echo -e "[$GREEN_OK] manifest created"
echo

# 设置manifest
for i in $TARGETS
do
  docker manifest annotate "${REGISTRY}"/$i:"${TAG}" \
  "${REGISTRY}"/$i:"${TAG}"-arm64 \
  --os linux --arch arm64

  docker manifest annotate "${REGISTRY}"/$i:"${TAG}" \
  "${REGISTRY}"/$i:"${TAG}"-amd64 \
  --os linux --arch amd64
done
echo -e "[$GREEN_OK] manifest architecture set"
echo

# 推送manifest
for i in $TARGETS
do
  docker manifest push "${REGISTRY}"/$i:"${TAG}"
done
echo -e "[$GREEN_OK] manifest pushed"