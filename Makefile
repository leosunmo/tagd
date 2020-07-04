EXTRA_RUN_ARGS?=

build:
	CGO_ENABLED=0 go build  -ldflags "-s -w " -o ./bin/tagd ./cmd/tagd/*

build-docker:
	docker build . -t tagd:dev