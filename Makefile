test:
	go test -race -v -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...

dep:
	go mod tidy

run-examples:
	go run examples/in-memory/*.go
