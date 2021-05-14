package main

import (
	"log"

	"github.com/nakabonne/tstorage"
)

func main() {
	_, err := tstorage.NewStorage()
	if err != nil {
		log.Fatal(err)
	}
}
