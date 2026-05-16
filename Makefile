BIN_DIR := bin
BIN     := $(BIN_DIR)/smols3
GO      ?= go

.PHONY: all build test vet clean

all: build

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN) ./cmd/smols3

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BIN_DIR)
