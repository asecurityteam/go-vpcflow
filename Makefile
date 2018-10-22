TAG := $(shell git rev-parse --short HEAD)

dep:
	dep ensure

lint:
	golangci-lint run --config .golangci.yaml ./...

test:
	go get github.com/axw/gocov
	go get github.com/AlekSi/gocov-xml
	go get github.com/wadey/gocovmerge
	mkdir -p .coverage
	go test -v -cover -coverpkg=./... -coverprofile=.coverage/unit.cover.out ./...
	gocov convert .coverage/unit.cover.out | gocov-xml > .coverage/unit.xml

integration: ;

coverage:
	go get github.com/axw/gocov
	go get github.com/AlekSi/gocov-xml
	go get github.com/wadey/gocovmerge
	mkdir -p .coverage
	gocovmerge .coverage/*.cover.out > .coverage/combined.cover.out
	gocov convert .coverage/combined.cover.out | gocov-xml > .coverage/combined.xml

doc: ;

build-dev: ;

build: ;

run: ;

deploy-dev: ;

deploy: ;