UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# race detector works on macOS (any arch) and Linux x86_64
# Disabled on Linux ARM64 due to ThreadSanitizer VMA limitation
ifeq ($(UNAME_S)-$(filter aarch64 arm%,$(UNAME_M)),Linux-$(UNAME_M))
  RACE :=
else
  RACE := -race
endif

.PHONY: default build test integration cover clean

default: build test integration

build:
	go build ./...
	go vet ./...

test:
	go clean -testcache
	go test $(RACE) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# integration are in a separate module under ./integration/ so that
# testcontainers/Docker dependencies don't bloat the adapter module
integration:
	cd integration && go test $(RACE) -timeout 15m ./...

cover: test
	go tool cover -html=coverage.out

clean:
	rm -f coverage.out integration/coverage.out
