all: main

main: $(shell find . -type f -name "*.go")
	go build -o main

clean:
	go mod tidy
	rm -rf main
