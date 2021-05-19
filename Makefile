test:
	go test -race -v -coverpkg=./... -benchtime=2s -benchmem -bench=. -covermode=atomic -coverprofile=coverage.txt ./...

dep:
	go mod tidy

godoc:
	godoc -http=:6060
