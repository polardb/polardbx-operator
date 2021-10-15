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

FROM golang:1.17 as builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
ENV GOPROXY=https://goproxy.cn
RUN go mod download

# Copy the go source
ADD api api
ADD cmd cmd
ADD pkg pkg
ADD third-party third-party

# Build
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -o polardbx-operator cmd/polardbx-operator/main.go

FROM  busybox:latest
WORKDIR /
COPY --from=builder /workspace/polardbx-operator .
USER root

ARG VERSION=test
RUN echo "$VERSION" > version

ENV GOMAXPROCS=1
ENTRYPOINT ["/polardbx-operator"]
