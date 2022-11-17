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

PRINT_HELP ?=
CURRENT_YEAR=$(shell date +"%Y")

define ALL_HELP_INFO
# Build code.
#
# Example:
#   make
#   make all
#   make all TARGET=cmd/hpfs
endef
export ALL_HELP_INFO

.PHONY: target
all: target

ifeq ($(PRINT_HELP), y)
target:
	@echo "$$ALL_HELP_INFO"
else
target: plugin-gen
	@hack/make-rules/build.py $(TARGET)
endif

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: lint
lint:
	@go vet ./...

.PHONY: clean
clean:
	@rm -rf target

BUILD_IMAGE_ARGS=
ifneq (, $(REPO))
	BUILD_IMAGE_ARGS:=${BUILD_IMAGE_ARGS} --repo $(REPO)
endif
ifneq (, $(TAG))
	BUILD_IMAGE_ARGS:=${BUILD_IMAGE_ARGS} --tag $(TAG)
endif
ifneq (, $(LATEST))
	BUILD_IMAGE_ARGS:=${BUILD_IMAGE_ARGS} --also-latest
endif
ifneq (, $(TARGET))
	BUILD_IMAGE_ARGS:=${BUILD_IMAGE_ARGS} $(TARGET)
endif

E2E_PARALLELISM=3

define BUILD_IMAGES_HELP_INFO
# Build images.
#
# Example:
#   make build
#   make build TARGET=cmd/hpfs
#   make build REPO=registry.aliyun.com/custom
#   make build REPO=registry.aliyun.com/custom TARGET=cmd/hpfs
endef
.PHONY: build
ifeq ($(PRINT_HELP), y)
build:
	@echo "$$BUILD_IMAGES_HELP_INFO"
else
build: plugin-gen
	@hack/make-rules/build-images.py ${BUILD_IMAGE_ARGS}
endif

define PUSH_IMAGES_HELP_INFO
# Build & push images.
#
# Example:
#   make push
#   make push TARGET=cmd/hpfs
#   make push REPO=registry.aliyun.com/custom
#   make push REPO=registry.aliyun.com/custom TARGET=cmd/hpfs
endef
.PHONY: push
ifeq ($(PRINT_HELP), y)
push:
	@echo "$$PUSH_IMAGES_HELP_INFO"
else
push: plugin-gen
	@hack/make-rules/build-images.py --push ${BUILD_IMAGE_ARGS}
endif

define GENERATE_HELP_INFO
# Generate files.
#
# Example:
#   make generate
endef
.PHONY: generate
ifeq ($(PRINT_HELP), y)
generate:
	@echo "$$GENERATE_HELP_INFO"
else
generate:
	@go generate ./... && go fmt ./...
endif

.PHONY: plugin-gen
plugin-gen:
	@go generate ./pkg/operator/v1/xstore/plugin

.PHONY: controller-gen
controller-gen:
	@controller-gen object:headerFile="hack/boilerplates/boilerplate.go.txt",year=$(CURRENT_YEAR) paths="./api/..."
	@go fmt ./api/...

define UPDATE_VENDOR_LICENSES_HELP_INFO
# Update vendor and vendor licenses.
#
# Examples:
#   make update-vendor-licenses
endef
.PHONY: update-vendor-licenses
ifeq ($(PRINT_HELP), y)
update-vendor-licenses:
	@echo "$$UPDATE_VENDOR_LICENSES_HELP_INFO"
else
update-vendor-licenses:
	@hack/make-rules/update-vendor-licenses.py
endif

define UNIT_TEST_HELP_INFO
# Run unit tests.
#
# Examples:
#   make unit-test
endef
.PHONY: unit-test
ifeq ($(PRINT_HELP), y)
unit-test:
	@echo "$$UNIT_TEST_HELP_INFO"
else
unit-test:
	@go test -v -short `go list ./... | grep -v "/third-party"`
endif

define E2E_TEST_HELP_INFO
# Run e2e tests.
#
# Examples:
#   make e2e-test
endef
.PHONY: e2e-test
ifeq ($(PRINT_HELP), y)
e2e-test:
	@echo "$$E2E_TEST_HELP_INFO"
else
# @go test ./test/e2e -v -args timeout 6000 -ginkgo.progress -ginkgo.v
e2e-test:
	@ginkgo -nodes=${E2E_PARALLELISM} -v -progress -p ./test/e2e
endif

.PHONY: manifests
manifests:
	@controller-gen crd:crdVersions=v1 rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=charts/polardbx-operator/crds

.PHONY: generate-notice
generate-notice:
	@hack/make-rules/generate-notice.py

.PHONY: verify-boilerplates
verify-boilerplates:
	@hack/make-rules/verify-boilerplates.py --language go --include-dirs api,cmd,pkg,test --exclude-dirs vendor --start-year 2021
	@hack/make-rules/verify-boilerplates.py --language bash --exclude-dirs vendor --start-year 2021
	@hack/make-rules/verify-boilerplates.py --language python --exclude-dirs vendor,tools/xstore/venv --start-year 2021

.PHONY: helm-package
helm-package:
	@mkdir -p target/charts
	@helm package charts/polardbx-operator -d target/charts
	@helm package charts/polardbx-monitor -d target/charts
	@helm package charts/polardbx-logcollector -d target/charts