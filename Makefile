test:
	go test -race -v -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...

dep:
	go mod tidy
