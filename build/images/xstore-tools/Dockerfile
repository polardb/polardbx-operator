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

FROM centos:7 AS builder

# Install python3.
RUN rpm --rebuilddb && yum install -y python3 && yum clean all

# Copy files.
WORKDIR /tools/xstore/current
COPY tools/xstore/requirements.txt requirements.txt

# Download dependencies.
# RUN mkdir -p deps && python3 -m pip download -r requirements.txt -d deps

# Build the virtualenv.
RUN python3 -m venv --copies ./venv

#Upgrade pip in the virtualenv
RUN ./venv/bin/pip install --upgrade pip

RUN ./venv/bin/pip install -r requirements.txt -f deps && ./venv/bin/pip uninstall -y pip setuptools
COPY tools/xstore .

# Precompile scripts and imported libs.
RUN ./venv/bin/python3 -m compileall . && LC_LANG=en_US.utf8 LC_ALL=en_US.utf8 ENGINE=mock ./venv/bin/python3 cli.py


FROM golang:1.21.1 as gobuilder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download

# Copy the go source
ADD api api
ADD cmd cmd
ADD pkg pkg
ADD third-party third-party

# Build binlog tools
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -tags polardbx -o polardbx-binlog cmd/polardbx-binlog/main.go
# Build filestream tools
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -o polardbx-filestream-client cmd/polardbx-filestream-cli/main.go
# Build polardbx-job
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -tags polardbx -o polardbx-job cmd/polardbx-job/main.go


# Build the image with scripts
FROM polardbx/xstore-tools-base:ts20240417185353

ARG VERSION=test

# Copy dependencies
WORKDIR /tools/xstore/current

# Copy source before
COPY tools/xstore .
COPY --from=builder /tools/xstore/current/venv venv
COPY --from=builder /tools/xstore/current/__pycache__ __pycache__
COPY --from=gobuilder /workspace/polardbx-binlog bb
RUN mkdir -p /tools/xstore/current/bin
COPY --from=gobuilder /workspace/polardbx-filestream-client /tools/xstore/current/bin/
COPY --from=gobuilder /workspace/polardbx-job /tools/xstore/current/bin/

# Copy all scripts
RUN ln -s cli.py xsl && chmod +x xsl
RUN echo "$VERSION" > version

# Set entrypoint to ash
ENTRYPOINT ash