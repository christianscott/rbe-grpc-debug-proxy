default: build

build: *.go go.mod go.sum
	go build -o rbe-debug-proxy
