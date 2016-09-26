
CMDS := thriftproxy

all: clean format gen $(CMDS) test

$(CMDS): %: bin/%

gen:
	sh gen.sh

bin/%:
	go build -o $@ -v ./cmd/$*

format:
	find ./ -name "*.go" | xargs goimports -w
	find ./ -name "*.go" | xargs gofmt -w

test:
	go test -v ./proxy ./thriftext

clean:
	rm -rf bin
	rm -rf example
