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

# Build the init command
FROM golang:1.21.1 as builder

WORKDIR /workspace

# Copy the Go modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

ENV GOPROXY=https://goproxy.cn,direct
# Cache deps before building and copying source
RUN go mod download

# Copy the Go sources
ADD api api
ADD cmd cmd
ADD pkg pkg
ADD third-party third-party

# Build
RUN mkdir -p target
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -o target/polardbx-hpfs cmd/polardbx-hpfs/main.go

FROM alpine:latest as certs

# install ca-certificates
RUN apk --update add ca-certificates


# Use alpine as the base image
FROM busybox:latest

WORKDIR /
COPY --from=builder /workspace/target/polardbx-hpfs .

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER root:root

ARG VERSION=test
RUN echo "$VERSION" > version

ENTRYPOINT ["/polardbx-hpfs"]
