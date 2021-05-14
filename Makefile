test:
	go test -race ./...

dep:
	go mod tidy
