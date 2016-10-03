.PHONY: build fmt clean

build:
	go build -o schema ./cmd/schema/

fmt:
	gofmt -w .

clean:
	rm -f schema

