UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Race detector works on macOS (any arch) and Linux x86_64
# Disabled on Linux ARM64 due to ThreadSanitizer VMA limitation
ifeq ($(UNAME_S)-$(filter aarch64 arm%,$(UNAME_M)),Linux-$(UNAME_M))
  RACE :=
else
  RACE := -race
endif

.PHONY: default build test cover

default: build test

build:
	go build ./...
	go vet ./...

test:
	go clean -testcache
	go test $(RACE) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

cover: test
	go tool cover -html=coverage.out
