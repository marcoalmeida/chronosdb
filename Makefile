root_dir:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SRC = $(shell find . -name '*.go')
TEST = $(shell find . -name '*_test.go')

chronosdb: $(SRC)
	go build

.PHONY: fmt
fmt: $(SRC)
	$(foreach file, $^, go fmt $(file);)

.PHONY: test
test: $(TEST)
	$(foreach file, $^, cd $(dir $(file)) && go test ; cd ..;)
