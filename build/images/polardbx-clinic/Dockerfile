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
# See the License for the specific language governing permissions and
# limitations under the License.

# Build the manager binary
ARG IMAGE_SOURCE
FROM ${IMAGE_SOURCE}${IMAGE_SOURCE:+"/"}centos:7

WORKDIR /root
USER root
COPY build/images/polardbx-clinic/diagnose.sh /root/diagnose.sh
COPY build/images/polardbx-clinic/install_tool.sh /root/install_tool.sh
COPY build/images/polardbx-clinic/diagnose /usr/bin/diagnose
RUN sh install_tool.sh
RUN chmod +x /usr/bin/diagnose

