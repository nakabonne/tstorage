test:
	go test -race -v -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt -benchtime=1s -benchmem -bench=. ./...

test-bench:
	go test -race -v -benchtime=1s -benchmem -bench=. -o=pprof/test.bin  -cpuprofile=pprof/cpu.out -memprofile=pprof/mem.out .

pprof-mem:
	go tool pprof pprof/mem.out

pprof-cpu:
	go tool pprof pprof/cpu.out

dep:
	go mod tidy

godoc:
	godoc -http=:6060
