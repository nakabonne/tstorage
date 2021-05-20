test:
	go test -race -v -coverpkg=./... -benchtime=1s -benchmem -bench=. -covermode=atomic -coverprofile=coverage.txt ./...

dep:
	go mod tidy

godoc:
	godoc -http=:6060
