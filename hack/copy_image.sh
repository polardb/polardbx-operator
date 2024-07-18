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


SOURCE_REPO=polarximages-registry.cn-zhangjiakou.cr.aliyuncs.com/daixingpolarximages
SOURCE_VERSION=v1.6.2
DEST_REPO=polardbx
DEST_VERSION=v1.6.2
TARGETS="xstore-tools polardbx-operator probe-proxy polardbx-exporter polardbx-init polardbx-hpfs polardbx-job polardbx-clinic polardbx-logstash"

#pull from source image and tag it
for i in $TARGETS
do
  docker pull $SOURCE_REPO/$i:$SOURCE_VERSION-arm64
  docker tag $SOURCE_REPO/$i:$SOURCE_VERSION-arm64  $DEST_REPO/$i:$DEST_VERSION-arm64
  docker pull $SOURCE_REPO/$i:$SOURCE_VERSION-amd64
  docker tag $SOURCE_REPO/$i:$SOURCE_VERSION-amd64  $DEST_REPO/$i:$DEST_VERSION-amd64
  docker tag $SOURCE_REPO/$i:$SOURCE_VERSION-amd64  $DEST_REPO/$i:$DEST_VERSION
done

#push
for i in $TARGETS
do
  docker push $DEST_REPO/$i:$DEST_VERSION-arm64
  docker push $DEST_REPO/$i:$DEST_VERSION-amd64
  docker push $DEST_REPO/$i:$DEST_VERSION
done

#create manifest
for i in $TARGETS
do
  docker manifest rm "${DEST_REPO}"/$i:"${DEST_VERSION}"
  docker manifest create "${DEST_REPO}"/$i:"${DEST_VERSION}" \
  "${DEST_REPO}"/$i:"${DEST_VERSION}"-arm64 \
  "${DEST_REPO}"/$i:"${DEST_VERSION}"-amd64 --amend
done


# 设置manifest
for i in $TARGETS
do
  docker manifest annotate "${DEST_REPO}"/$i:"${DEST_VERSION}" \
  "${DEST_REPO}"/$i:"${DEST_VERSION}"-arm64 \
  --os linux --arch arm64

  docker manifest annotate "${DEST_REPO}"/$i:"${DEST_VERSION}" \
  "${DEST_REPO}"/$i:"${DEST_VERSION}"-amd64 \
  --os linux --arch amd64
done

#push manifest
for i in $TARGETS
do
  docker manifest push "${DEST_REPO}"/$i:"${DEST_VERSION}"
done