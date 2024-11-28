PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go) -linkmode 'external' -extldflags '-static'

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
##TAG ?= minio/minio-$(GOARCH):0.0.2
##TAG ?= registry.cn-beijing.aliyuncs.com/kubegems/juicefs-gateway:v0.0.6

BIN_DIR?=bin
IMAGE_REGISTRY?=registry.cn-beijing.aliyuncs.com


define go-build
	CGO_ENABLED=1 GOOS=$(1) GOARCH=$(2) CC=$(3) CXX=$(4) go build -tags kqueue -trimpath --ldflags "$(LDFLAGS)" -o $(PWD)/${BIN_DIR}/minio-$(2) 1>/dev/null
endef

build: ## Build local binary.
	- mkdir -p ${BIN_DIR}
	$(call go-build,linux,amd64,x86_64-linux-musl-gcc,x86_64-linux-musl-g++)
	$(call go-build,linux,arm64,aarch64-linux-musl-gcc,aarch64-linux-musl-g++)



PLATFORM?=linux/amd64,linux/arm64
release-image: build
	docker buildx build --platform=${PLATFORM} --push -t ${IMAGE_REGISTRY}/kubegems/juicefs-gateway:v0.0.7 -f Dockerfile ${BIN_DIR}



clean: ## cleanup all generated assets
	@rm -rvf bin/minio-*
	@rm -rvf minio
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
